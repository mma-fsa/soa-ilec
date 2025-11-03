library(openxlsx)
library(glmnet)
library(splines)
library(DBI)
library(duckdb)
library(rpart)
library(tidyverse)

export_factors <- function(model_rds_path, export_location) {

  model_data <- read_rds(model_rds_path)
  md_env <- environment(model_data)
  
  # pull required information out of model object
  dmat_formula <- get("dmat_formula", envir = md_env)
  fit_glmnet <- get("fit_glmnet", envir = md_env)
  best_lambda <- get("best_lambda", envir = md_env)
  training_vals <- get("training_data_vals", envir = md_env)
  args <- get("args", envir = md_env)
  prep_glmnet_data <- get("prep_glmnet_data", envir = md_env)
  
  factor_vars <- args$factor_vars_levels
  num_vars <- args$num_var_clip
  
  df_map <- get_map_df(dmat_formula, training_vals, prep_glmnet_data)
  
  df_coefs <- sparse_coefs_to_df(predict(
    fit_glmnet,
    type="coefficients",
    s = best_lambda
  ))
  
  model_factors <- compute_model_factors(
    training_vals,
    df_coefs,
    df_map,
    dmat_formula,
    prep_glmnet_data
  )
  
  # setup the overview tab
  df_overview <- tribble(
    ~Item, ~Value,
    "Model Formula", as.character(dmat_formula),
    "Intercept", df_coefs %>% filter(term == "(Intercept)") %>% pull(estimate),
    "RData", model_rds_path
  )
  
  # create the excel workbook
  system(paste0("mkdir -p ", export_location))
  
  model_factor_wb <- createWorkbook()
  addWorksheet(model_factor_wb, "Overview")
  writeData(model_factor_wb, sheet = "Overview", x=df_overview)
  
  model_factor_wb <- get_factor_wb(model_factor_wb, model_factors)
  
  out_path <- file.path(export_location, "/model_factors.xlsx")
  
  saveWorkbook(model_factor_wb, out_path)
}

get_default_covariate_df <- function(training_vals, prep_glmnet_data) {
  
  covars <- prep_glmnet_data$term_info %>% 
    filter(role == "predictor") %>%
    pull(variable)
  
  default_covar_df <- 0
}

map_model_matrix_terms <- function(formula, data) {
  if (!inherits(formula, "formula")) formula <- as.formula(formula)
  
  mm <- model.matrix(formula, data = data)
  term_names <- colnames(mm)
  
  terms_obj   <- terms(formula)
  term_labels <- attr(terms_obj, "term.labels")
  assign_idx  <- attr(mm, "assign")
  
  # Map each term label -> underlying variables (unique, in order of appearance)
  var_map <- lapply(term_labels, function(lbl) {
    expr <- parse(text = lbl)[[1]]
    unique(all.vars(expr))
  })
  names(var_map) <- term_labels
  
  # Build mapping rows
  df_map <- data.frame(
    term_name  = term_names,
    term_label = term_labels[assign_idx],
    stringsAsFactors = FALSE
  )
  
  # Group names like Issue_Age_X_Duration and pipe-list like Issue_Age|Duration
  df_map$term_group    <- vapply(df_map$term_label, function(lbl) {
    vars <- var_map[[lbl]]
    if (length(vars) > 1) paste(vars, collapse = "_X_") else vars
  }, character(1))
  
  df_map$vars_in_group <- vapply(df_map$term_label, function(lbl) {
    paste(var_map[[lbl]], collapse = "|")
  }, character(1))
  
  df_map[, c("term_name", "term_group", "vars_in_group")]
}

get_map_df <- function(dmat_formula, training_vals, prep_glmnet_data) {
  
  map_data <- list()
  
  for (t_val in names(training_vals)) {
    map_data[[t_val]] <- training_vals[[t_val]][1]
  }
  
  df_map <- map_model_matrix_terms(
    dmat_formula, 
    recipes::bake(prep_glmnet_data, data.frame(map_data)))
  
  return(df_map)
}

# Handles a dgCMatrix with 1 or many columns (e.g., multiple lambdas)
# Returns a data.frame with zeros included.
sparse_coefs_to_df <- function(coefs) {
  if (!inherits(coefs, "dgCMatrix")) {
    stop("Expected a 'dgCMatrix' (e.g., from predict(..., type='coefficients')).")
  }
  `%||%` <- function(a, b) if (!is.null(a)) a else b
  
  n <- nrow(coefs)
  k <- ncol(coefs)
  rn <- rownames(coefs) %||% paste0("V", seq_len(n))
  
  # Column pointer vector coefs@p has length k+1; entries are 0-based indices into @x/@i
  build_col <- function(j) {
    # indices for nonzeros in column j (convert to 1-based for R slicing)
    nz_start <- coefs@p[j] + 1L
    nz_end   <- coefs@p[j + 1L]
    v <- numeric(n)  # all zeros by default
    
    if (nz_end >= nz_start) {
      idx  <- nz_start:nz_end
      rows <- coefs@i[idx] + 1L  # convert 0-based to 1-based row indices
      vals <- coefs@x[idx]
      v[rows] <- vals
    }
    data.frame(
      term      = rn,
      estimate  = v,
      lambda_ix = j,
      stringsAsFactors = FALSE
    )
  }
  
  out <- do.call(rbind, lapply(seq_len(k), build_col))
  if (k == 1L) out$lambda_ix <- NULL
  out
}

## small infix for defaults
`%||%` <- function(a, b) if (!is.null(a)) a else b

compute_model_factors <- function(training_vals, df_coefs, df_map, formula, prep_glmnet_data) {
  stopifnot(is.data.frame(df_coefs), is.data.frame(df_map))
  
  ## ---- Normalize df_coefs to have columns: term_name, estimate ----
  dfc <- df_coefs
  if ("term" %in% names(dfc) && !("term_name" %in% names(dfc))) dfc$term_name <- dfc$term
  if (!("term_name" %in% names(dfc))) {
    rn <- rownames(dfc)
    if (!is.null(rn)) dfc$term_name <- rn else stop("df_coefs must have 'term' or 'term_name' (or rownames).")
  }
  if (!("estimate" %in% names(dfc))) stop("df_coefs must contain an 'estimate' column.")
  dfc <- dfc[, c("term_name", "estimate")]
  
  ## helper: parse variables for a term_group
  get_vars_for_group <- function(tg) {
    entry <- df_map$vars_in_group[df_map$term_group == tg]
    if (length(entry) == 0L) character()
    else unique(unlist(strsplit(entry[1], "\\|")))
  }
  
  term_groups <- unique(df_map$term_group)
  results <- list()
  
  for (tg in term_groups) {
    vars <- get_vars_for_group(tg)
    if (length(vars) == 0L) next
    
    ## ---- build grid of ONLY the variables in this term_group (pre-bake) ----
    vals_list <- lapply(vars, function(v) {
      if (v %in% names(training_vals)) {
        vals <- training_vals[[v]]
        if (is.numeric(vals) && length(vals) == 2L) {
          seq(from = vals[1], to = vals[2], by = 1L)  # exact integer range
        } else {
          unique(vals)
        }
      }  else {
        NA
      }
    })
    names(vals_list) <- vars
    
    df_grid <- do.call(expand.grid, c(vals_list, stringsAsFactors = FALSE))
    
    ## force integer type for numeric sequences in the grid
    for (v in vars) if (is.numeric(df_grid[[v]])) df_grid[[v]] <- as.integer(df_grid[[v]])
    
    ## ---- build full frame for model.matrix (fill non-group vars with typicals), then bake ----
    full_vars <- unique(names(training_vals))
    df_vals_full <- df_grid
    for (v in setdiff(full_vars, names(df_vals_full))) {
      if (is.character(training_vals[[v]])) {
        df_vals_full[[v]] <- training_vals[[v]][1]
      } else if (is.numeric(training_vals[[v]])) {
        rng <- training_vals[[v]]
        df_vals_full[[v]] <- as.integer(floor(mean(rng)))
      }
    }
    
    ## bake with your recipe so encodings/levels match training
    df_baked <- recipes::bake(prep_glmnet_data, df_vals_full)
    
    ## ---- model matrix and contributions ----
    mm <- model.matrix(formula, data = df_baked)
    mm_df <- as.data.frame(mm, stringsAsFactors = FALSE)
    mm_df$obs_id <- seq_len(nrow(mm_df))
    
    term_cols <- df_map$term_name[df_map$term_group == tg]
    term_cols <- intersect(term_cols, colnames(mm_df))
    
    if (length(term_cols) == 0L) {
      out_df <- df_grid
      out_df$contribution_log <- 0
      out_df$contribution_multiplier <- 1
      ## drop columns that are not changing
      keep_cols <- names(out_df)[vapply(out_df, function(x) length(unique(x)) > 1L, logical(1))]
      if (length(keep_cols)) {
        out_df <- out_df[, c(keep_cols, "contribution_log", "contribution_multiplier"), drop = FALSE]
      } else {
        out_df <- out_df[, c("contribution_log", "contribution_multiplier"), drop = FALSE]
      }
      results[[tg]] <- out_df
      next
    }
    
    ## melt term columns
    if (requireNamespace("reshape2", quietly = TRUE)) {
      long_df <- reshape2::melt(mm_df[, c("obs_id", term_cols), drop = FALSE],
                                id.vars = "obs_id",
                                variable.name = "term_name",
                                value.name = "basis_value")
    } else {
      tmp <- mm_df[, term_cols, drop = FALSE]
      long_df <- data.frame(
        obs_id = rep(mm_df$obs_id, times = ncol(tmp)),
        term_name = rep(colnames(tmp), each = nrow(tmp)),
        basis_value = as.vector(as.matrix(tmp)),
        check.names = FALSE
      )
    }
    
    coef_sub <- dfc[dfc$term_name %in% term_cols, , drop = FALSE]
    joined <- merge(long_df, coef_sub, by = "term_name", all.x = TRUE)
    joined$estimate[is.na(joined$estimate)] <- 0
    
    ## LOG scale contribution for this term group = sum_j (basis_j * beta_j)
    joined$contribution_piece <- joined$basis_value * joined$estimate
    agg <- aggregate(contribution_piece ~ obs_id, data = joined, sum, na.rm = TRUE)
    
    ## attach contributions back to the ORIGINAL grid (only group vars)
    final_df <- merge(data.frame(obs_id = seq_len(nrow(df_grid)), df_grid),
                      agg, by = "obs_id", all.x = TRUE)
    final_df$contribution_piece[is.na(final_df$contribution_piece)] <- 0
    final_df$obs_id <- NULL
    
    ## rename to requested columns and add multiplier on the rate scale
    final_df$contribution_log <- final_df$contribution_piece
    final_df$contribution_multiplier <- exp(final_df$contribution_log)
    final_df$contribution_piece <- NULL
    
    ## ---- drop columns that are not changing; keep integers for numeric ----
    change_mask <- if (length(vars)) vapply(final_df[, vars, drop = FALSE], function(x) length(unique(x)) > 1L, logical(1)) else logical(0)
    keep_vars <- vars[change_mask]
    if (length(keep_vars)) {
      for (v in keep_vars) if (is.numeric(final_df[[v]])) final_df[[v]] <- as.integer(final_df[[v]])
      out_df <- final_df[, c(keep_vars, "contribution_log", "contribution_multiplier"), drop = FALSE]
    } else {
      out_df <- final_df[, c("contribution_log", "contribution_multiplier"), drop = FALSE]
    }
    
    results[[tg]] <- out_df
  }
  
  results
}


# ---- helper: sanitize and uniquify sheet names (<=31 chars, no []:*?/\) ----
sanitize_sheet_names <- function(nms) {
  if (is.null(nms) || any(nms == "")) {
    nms <- paste0("Sheet", seq_along(nms))
  }
  nms <- gsub("[\\[\\]\\*\\:\\?/\\\\]", "_", nms) # replace illegal chars
  nms <- trimws(nms)
  nms[nms == ""] <- "Sheet"
  # truncate to 31 chars
  nms <- substr(nms, 1, 31)
  
  # ensure uniqueness
  seen <- character()
  out <- character(length(nms))
  for (i in seq_along(nms)) {
    base <- nms[i]
    candidate <- base
    suffix <- 1L
    while (candidate %in% seen) {
      # leave space for suffix like _2 (max 3 chars); re-truncate if needed
      cand_base <- substr(base, 1, max(1, 31 - nchar(paste0("_", suffix))))
      candidate <- paste0(cand_base, "_", suffix)
      suffix <- suffix + 1L
    }
    out[i] <- candidate
    seen <- c(seen, candidate)
  }
  out
}

get_factor_wb <- function(wb, results) {
  
  # Example: results is the list returned by compute_model_factors(...)
  # results <- compute_model_factors(...)
  
  stopifnot(is.list(results), all(vapply(results, is.data.frame, logical(1))))
  
  sheet_names <- names(results)
  if (is.null(sheet_names)) sheet_names <- paste0("Sheet", seq_along(results))
  sheet_names <- sanitize_sheet_names(sheet_names)
  
  # optional styles
  hdr_style <- createStyle(textDecoration = "bold", halign = "center", valign = "center")
  num_style <- createStyle(numFmt = "0.000000") # adjust as needed
  
  for (i in seq_along(results)) {
    df <- results[[i]]
    addWorksheet(wb, sheet_names[i])
    writeData(wb, sheet = sheet_names[i], x = df, withFilter = TRUE, headerStyle = hdr_style)
    
    # auto width
    setColWidths(wb, sheet = sheet_names[i], cols = 1:ncol(df), widths = "auto")
    
    # freeze header row
    freezePane(wb, sheet = sheet_names[i], firstActiveRow = 2, firstActiveCol = 1)
    
    # apply numeric style to numeric columns
    if (ncol(df) > 0) {
      num_cols <- which(vapply(df, is.numeric, logical(1)))
      if (length(num_cols)) {
        addStyle(
          wb, sheet = sheet_names[i], style = num_style,
          rows = 2:(nrow(df) + 1), cols = num_cols, gridExpand = TRUE, stack = TRUE
        )
      }
    }
  }
  
  return(wb)
}

# export_factors(
#   "~/workspace/soa-ilec/soa-ilec/data/workspaces/ul_model_data/workspace_d4386607-1f7e-4e80-80ec-5f295ca5dbef/run_model.rds",
#   "~/workspace/soa-ilec/soa-ilec/data/workspaces/ul_model_data/"
# )

