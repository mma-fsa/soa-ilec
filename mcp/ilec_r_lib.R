# todo: install if these packages are missing
library(glmnet)
library(recipes)
library(butcher)
library(carrier)
library(rpart)
library(rsample)
library(arrow)
library(tidyverse)

# this should be called prior to cmd_rpart / cmd_glmnet to create data
# conn is setup automatically and is a connection to a DuckDB database, 
# dataset name is the output file, should only create a dataset ONCE
# only sql needs to be passed by the agent
cmd_create_dataset <- function(conn, dataset_name, sql) {
  
  # set the dataset output local
  dataset_output_file <- paste0(getwd(), "/", dataset_name, ".parquet")
  
  # do not allow overwrites of datasets
  if (file.exists(dataset_output_file)) {
    stop(sprintf("Cannot create a dataset: '%s', already exists"))
  }
  
  # view definition
  sql_create_view <- sprintf(
    "create or replace view vw_dataset as (%s)", sql
  )
  
  # create the modeling view
  DBI::dbExecute(conn, sql_create_view)
  # write the dataset
  sql_create_data <- sprintf(
    "copy(select * from vw_dataset) to '%s' (FORMAT PARQUET, ROW_GROUP_SIZE 100000)",
    dataset_output_file
  )
  DBI::dbExecute(conn, sql_create_data)
  
  # return dataset summary statistics
  tbl_dataset <- tbl(conn, sprintf("read_parquet('%s')", dataset_output_file))
    
  ds_summary <- tbl_dataset %>%
    summarise(n_rows = n()) %>% 
    collect()
  
  return(list("n_rows" = ds_summary$n_rows))
}

cmd_run_inference <- function(conn, dataset_in, dataset_out) {
  
  if (!file.exists("run_model.rds")) {
    stop("cmd_glmnet() must be successfully called before running cmd_run_inference()")
  }
  
  run_model <- read_rds("run_model.rds")
  
  path_dataset_in <- paste0(getwd(), "/", dataset_in, ".parquet")
  if (!file.exists(path_dataset_in)) {
    stop(sprintf("dataset_in '%s' does not exist", dataset_in))
  }
  
  path_dataset_out <- paste0(getwd(), "/", dataset_out, ".parquet")
  tmpdir_dataset_out <- paste0(getwd(), "/", dataset_out)
  
  if (file.exists(path_dataset_out)) {
    stop(sprintf("dataset_out '%s' already exists", dataset_out))
  } 
  
  system(paste0("mkdir -p ", tmpdir_dataset_out))
  
  # basic streaming inference 
  reader <- ParquetFileReader$create(path_dataset_in)
  n_row_groups <- reader$num_row_groups
  for (i in seq_len(n_row_groups)) {
    # read one row group as Arrow Table
    
    tbl <- reader$ReadRowGroup(i - 1L)
    tbl$MODEL_PRED <- run_model(tbl)
    arrow::write_parquet(
      tbl, 
      paste0(tmpdir_dataset_out, "/", sprintf("chunk_%d.parquet", i)))
    
    rm(tbl); gc()
  }
  
  # consolidate via duckdb()
  sql_create_data <- sprintf(
    "copy(select * from read_parquet('%s')) to '%s' (FORMAT PARQUET, ROW_GROUP_SIZE 100000)",
    paste0(tmpdir_dataset_out, "/*.parquet"),
    path_dataset_out 
  )
  DBI::dbExecute(conn, sql_create_data)
  
  system(paste0("rm -rf ", tmpdir_dataset_out))
  
  return(TRUE)
}

# conn is setup automatically, and is a connection to a DuckDB database, 
# builds a poisson decision tree
cmd_rpart <- function(conn, dataset, x_vars, offset_var, y_var, max_depth, cp) {

  # check if the dataset is a parquet file
  pq_filename <- sprintf("%s.parquet", dataset)
  if (file.exists(pq_filename)) {
    tbl_dataset <- tbl(conn, sprintf("read_parquet('%s.parquet')", dataset))  
  } else {
    # assume that it exists in duckdb (e.g. table / view)
    tbl_dataset <- tbl(conn, dataset)
  }
  
  rpart_data <- tbl_dataset %>%
    group_by(across(all_of(x_vars))) %>%
    summarise(
      offset = sum(!!sym(offset_var)),
      y = sum(!!sym(y_var)),
      .groups = "drop"
    ) %>%
    ungroup() %>%
    collect() %>%
    mutate(across(where(is.character), as.factor))
  
  rhs <- reformulate(x_vars)          
  rpart_formula <- update(rhs, cbind(offset, y) ~ .)  
  
  fit_rpart <- rpart(
    rpart_formula,
    rpart_data,
    method="poisson",
    control = rpart.control(
      cp = cp,
      maxdepth = max_depth
    )
  )
  
  return(fit_rpart)
}

# build a poisson glm via glmnet(),
# use the variable names only in x_vars, e.g. c("x1", "x2", "x3")
# design_matrix_vars can use transformations, e.g. c("splines::ns(x1)", "x2*x3", "x3^2")
# factor_var_levels is a list of x_var names the default level used in the factor, e.g. list("Gender"="Male", "Smoker_Status"="NonSmoker")
# num_var_clip clips a numeric variable at the specified range, e.g. list("Issue_Age"= c(17, 85)) clips the min at 17 and max at 85
cmd_glmnet <- function(conn, dataset, x_vars, design_matrix_vars, factor_vars_levels, num_var_clip, offset_var, y_var, lambda_strat) {
  
  tbl_dataset <- tbl(conn, sprintf("read_parquet('%s.parquet')", dataset)) %>%
      group_by(across(all_of(x_vars))) %>%
      summarise(
        offset = sum(!!sym(offset_var)),
        y = sum(!!sym(y_var)),
        .groups = "drop"
      ) %>%
      ungroup()
  
  glmnet_data <- tbl_dataset %>% collect()
  
  # check that offset / exposure is non-zero
  num_zero_expos <- glmnet_data %>%
    filter(offset <= 0) %>%
    nrow()
  
  if (num_zero_expos > 0) {
    stop("Detected model data rows w/ zero exposure")
  }
  
  recipe_rhs <- reformulate(x_vars)
  recipe_formula <- update(recipe_rhs, y ~ .)  
  
  prep_glmnet_data <- recipe(
    recipe_formula,
    data=glmnet_data)
  
  # get a list of all character variables in x_vars
  char_vars <- glmnet_data %>%
    select(all_of(x_vars)) %>%
    select(where(is.character)) %>%
    colnames()
  
  # check that factor_vars are all in char_var using set_diff
  invalid_factors <- setdiff(names(factor_vars_levels), char_vars)
  if (length(invalid_factors) > 0) {
    bad_factors <- paste0(invalid_factors, collapse=",")
    stop(sprintf("Invalid factor names: %s", bad_factors))
  }
  
  # convert them to factors using step_string2factor, 
  # use the default level if it is present in the factor_vars_levels
  for (cv in char_vars) {
    
    default_level <- factor_vars_levels[[cv]]
    unique_values <- unique(glmnet_data[[cv]])
    
    if (length(unique_values) > 25) {
      stop(sprintf("Error: %s has more than 25 levels", cv))
    }
    
    if (!is.null(default_level)) {
      var_levels = c(default_level, setdiff(unique_values, default_level))
    } else {
      var_levels = sort(unique_values)
    }
    
    prep_glmnet_data <- prep_glmnet_data %>%
      step_string2factor(!!sym(cv), levels = var_levels)
  }
  
  # get a list of numeric / non-character variables in x_vars
  numeric_vars <- glmnet_data %>%
    select(all_of(x_vars)) %>%
    select(where(is.numeric)) %>%
    colnames()

  # apply a numeric clip to max min of all numeric_vars using the ranges in the
  # training data, prevents issues with splines in formula
  for (nv in numeric_vars) {
    
    var_range <- range(glmnet_data[[nv]], na.rm=TRUE)
    
    # only respect the clipping if they are within min / max of training data
    if (nv %in% names(num_var_clip)) {
      var_range <- c(
        pmax(var_range[1], num_var_clip[[nv]][1]),
        pmin(var_range[2], num_var_clip[[nv]][2])
      )
    } 
    
    prep_glmnet_data <- prep_glmnet_data %>%
      step_range(!!sym(nv), min = var_range[1], max = var_range[2])
  }
  
  prep_glmnet_data <- prep_glmnet_data %>%
    prep(glmnet_data, retain=F)
  
  # apply transformations before building design matrix
  glmnet_data.prepped <- bake(prep_glmnet_data, glmnet_data)
  
  # create design matrix + response & offset vectors, no intercept
  # since this is handled by glmnet
  dmat_rhs <- reformulate(design_matrix_vars)
  dmat_formula <- update(dmat_rhs, ~ . - 1)  
  
  # build design matrix, response, and offset vectors
  X_mat <- model.matrix(dmat_formula, data = glmnet_data.prepped)
  y_vec <- glmnet_data$y
  offset_vec <- log(glmnet_data$offset)
  
  # run GLMNET
  fit_glmnet <- glmnet::glmnet(
    X_mat,
    y_vec,
    family = "poisson",
    offset = offset_vec,
    nlambda = 100
  )
  
  if (lambda_strat == "1se") {
    
    # use the top 50% deviance ratios to compute the standard deviations
    n_fit_lambdas <- length(fit_glmnet$dev.ratio)
    best_dev_ratios <- fit_glmnet$dev.ratio[as.integer(n_fit_lambdas/2):n_fit_lambdas]
    sd_dev_ratios <- sd(log(best_dev_ratios))
    
    best_dev_ratio_1se <- exp(log(max(best_dev_ratios)) - sd_dev_ratios)
    best_lambda_idx <- max(which(fit_glmnet$dev.ratio <= best_dev_ratio_1se))
      
  } else if (lambda_strat %in% c("AIC", "BIC")) {
    # not 100% sure the math checks out here...
    compute_AIC_BIC <- function(fit){
      tLL <- -deviance(fit)
      k <- dim(X_mat)[2]
      n <- sum(y_vec)
      fit_AIC <- -tLL+2*k
      fit_BIC <- log(n)*k - tLL
      return(list(
        "AIC" = fit_AIC,
        "BIC" = fit_BIC
      ))
    }
    strat_criteria <- compute_AIC_BIC(fit_glmnet)[[lambda_strat]]
    best_lambda_idx <- which(
      strat_criteria == min(strat_criteria)
    )
  } else {
    stop("Unknown lambda strategy: %s", lambda_strat)
  }
  
  # select best lambda based on criteria
  best_lambda <- fit_glmnet$lambda[best_lambda_idx]
  
  # bundle up model into carrier crate
  run_model <- carrier::crate(function(df, use_offset = T) {
      
      df_prepped <- recipes::bake(prep_glmnet_data, df)
      X_mat <- stats::model.matrix(dmat_formula, df_prepped)
      
      if (use_offset) {
        pred_vec <- stats::predict(
          fit_glmnet,
          X_mat,
          newoffset = log(df[[offset_var]]),
          s = best_lambda,
          type = "response"
        )
      } else {
        pred_vec <- stats::predict(
          fit_glmnet,
          X_mat,
          s = best_lambda,
          type = "response"
        )
      }
      return(as.double(pred_vec))
    },
    dataset = dataset,
    prep_glmnet_data = prep_glmnet_data,
    fit_glmnet = fit_glmnet,
    offset_var = offset_var,
    dmat_formula = dmat_formula,
    best_lambda = best_lambda,
    lambda_strat = lambda_strat
  )
  
  # rename offset column back to original
  names(glmnet_data)[names(glmnet_data) == "offset"] <- offset_var
  
  # rename y-var columns ack to original
  names(glmnet_data)[names(glmnet_data) == "y"] <- y_var
  
  # run inference on training data
  glmnet_data[["MODEL_PRED"]] <- run_model(glmnet_data)
  
  # sanity check
  ae_train <- sum(glmnet_data[[y_var]]) / sum(glmnet_data$MODEL_PRED)
  if (abs(log(ae_train)) >= log(1.03)) {
    stop("Model training sanity check failed (A/E > 100% +- 3%)")
  }
  
  # expose inference data as a dataset / duckdb view called "vw_glmnet_data"
  duckdb::duckdb_register(conn, "vw_glmnet_data", glmnet_data)
  
  # use decision tree as test / train summary for design_matrix_vars refinement and fit diagnostics
  rpart_train <- butcher::butcher(cmd_rpart(
    conn,
    "vw_glmnet_data",
    x_vars, 
    offset_var = "MODEL_PRED", 
    y_var = y_var,
    3,
    0.001)
  )
  
  # remove vw_glmnet_data
  duckdb::duckdb_unregister(conn, "vw_glmnet_data")
  
  # save the rpart diagnostics to the crate
  run_model_env <- environment(run_model)
  assign("rpart_train", rpart_train, envir=run_model_env)
  
  # serialize model, only serialize ONCE
  if (!file.exists("run_model.rds")) {
    saveRDS(run_model, "run_model.rds")  
  }
  
  # return diagnostics to agent
  return(
    list(
      "rpart_train" = rpart_train,
      "ae_train" = ae_train
    ))
}

