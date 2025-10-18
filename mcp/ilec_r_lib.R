library(tidyverse)

# save the SQL used to build df_model_data
df_model_data_sql <- NULL

# all calls to modeling functions operate on this dataframe,
# which is set by cmd_load_model_data()
df_model_data <- NULL
df_model_data.train <- NULL
df_model_data.test <- NULL

# df_model_data.(train|test) w/ model predictions appended
df_fit.train <- NULL
df_fit.test <- NULL

# stores the data prep for glmnet so that we can use it in run_inference()
prep_glmnet_data <- NULL

# stores the last glmnet fit so that we can use it in run_inference()
fit_glmnet <- NULL

# this should be called prior to any modeling functions
# conn is setup automatically, only SQL must be passed
cmd_set_modeling_data <- function(conn, sql, strata_vars) {
  library(rsample)
  
  df_model_data <<- DBI::dbGetQuery(conn, sql)
  df_model_data_sql <<- sql
  
  # the most informative holdout set to validate
  # design matrix specification is future years
  df_model_data.train <- df_model_data %>%
    filter(Observation_Year <= 2016)
  df_model_data.test <- df_model_data %>%
    filter(Observation_Year > 2016)
}

# builds a poisson decision tree
cmd_rpart <- function(dataset, x_vars, offset, y_var, max_depth, cp) {
  
  library(rpart)
  
  rpart_data <- .GlobalEnv[[dataset]] %>%
    group_by(across(all_of(x_vars))) %>%
    summarise(
      offset = sum(!!sym(offset)),
      y_var = sum(!!sym(y_var)),
      .groups = "drop"
    ) %>%
    ungroup() %>%
    mutate(across(where(is.character), as.factor))
  
  rhs <- reformulate(x_vars)          
  rpart_formula <- update(rhs, cbind(offset, y_var) ~ .)  
  
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
# num_var_clip clips a numeric variable at the specified range, e.g. c("Issue_Age"= c(17, 85)) clips the min at 17 and max at 85
cmd_glmnet <- function(x_vars, design_matrix_vars, factor_vars_levels, num_var_clip, offset, y_var, lambda_strat) {
  library(glmnet)
  library(recipes)
  
  if (is.null(df_model_data)) {
    return("must call cmd_load_model_data() first to assign df_model_data")
  }
  
  glmnet_data <- df_model_data.train %>%
    group_by(across(all_of(x_vars))) %>%
    summarise(
      offset = sum(!!sym(offset)),
      y_var = sum(!!sym(y_var)),
      .groups = "drop"
    ) %>%
    ungroup()
  
  # check that offset / exposure is non-zero
  num_zero_expos <- glmnet_data %>%
    filter(offset <= 0) %>%
    nrow()
  
  if (num_zero_expos > 0) {
    stop("Detected model data rows w/ zero exposure")
  }
  
  recipe_rhs <- reformulate(x_vars)
  recipe_formula <- update(recipe_rhs, y_var ~ .)  
  
  prep_glmnet_data <<- recipe(
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
    unique_values <- unique(df_model_data.train[[cv]])
    
    if (length(unique_values) > 25) {
      stop(sprintf("Error: %s has more than 25 levels", cv))
    }
    
    if (!is.null(default_level)) {
      var_levels = c(default_level, setdiff(unique_values, default_level))
    } else {
      var_levels = sort(unique_values)
    }
    
    prep_glmnet_data <<- prep_glmnet_data %>%
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
    
    var_range <- range(df_model_data.train[[nv]], na.rm=TRUE)
    
    # only respect the clipping if they are within min / max of training data
    if (nv %in% names(num_var_clip)) {
      var_range <- c(
        pmax(var_range[1], num_var_clip[[nv]][1]),
        pmin(var_range[2], num_var_clip[[nv]][2])
      )
    } 
    
    prep_glmnet_data <<- prep_glmnet_data %>%
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
  
  browser()
  
  # build design matrix, response, and offset vectors
  X_mat <- model.matrix(dmat_formula, data = glmnet_data.prepped)
  y_vec <- glmnet_data$y_var
  offset_vec <- log(glmnet_data$offset)
  
  # run GLMNET
  fit_glmnet <<- glmnet::glmnet(
    X_mat,
    y_vec,
    family = "poisson",
    offset = offset_vec,
    nlambda = 100
  )
  
  # choose the best lambda according to a strategy
  valid_lambda_strats <- c("1se", "AIC", "BIC")
  if (!(lambda_strat %in% valid_lambda_strats)) {
    stop("Unknown lambda strategy: %s", lambda_strat)
  }
  
  if (lambda_strat == "1se") {
    
    # use the top 50% deviance ratios to compute the standard deviations
    n_fit_lambdas <- length(fit_glmnet$dev.ratio)
    best_dev_ratios <- fit_glmnet$dev.ratio[as.integer(n_fit_lambdas/2):n_fit_lambdas]
    sd_dev_ratios <- sd(log(best_dev_ratios))
    
    # choose lambda as max(dev.ratio) - 1se
    
  } else if (lambda_strat %in% c("AIC", "BIC")) {
    compute_AIC_BIC <- function(fit){
      tLL <- -deviance(fit)
      k <- dim(model.matrix(fit))[2]
      n <- nobs(fit)
      AICc <- -tLL+2*k+2*k*(k+1)/(n-k-1)
      AIC_ <- -tLL+2*k
      BIC<-log(n)*k - tLL
      res=c(AIC_, BIC, AICc)
      names(res)=c("AIC", "BIC", "AICc")
      return(res)
    }
    
  }
  
  # run inference @ optimal s (based on strategy)
  
  return(NULL)
}

run_inference <- function() {
  
}
