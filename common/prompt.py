# I think it's easier to have all this in one place, to make sure that the agent's insructions
# are cohesive / using the same terminology

from env_vars import DEFAULT_DDB_ROW_LIMIT, AGENT_R_LIB

# ---- MCP Tool Descriptions ----

SQL_SCHEMA_DESC = "Returns database schema information for the table_name argument."\
    "Useful for sql_run() and cmd_create_dataset()."

SQL_RUN_DESC = "Provide a short description of your rationale for this tool call via the desc argument."\
    "Use the query argument to run a single duckdb compatible select statement."\
    f"Do not use CTEs. Must include LIMIT with no greater than {DEFAULT_DDB_ROW_LIMIT} records (enforced)."

CMD_INIT_DESC = """Initializes an immutable workspace for calls to cmd_* methods, returns a workspace_id."""\
    """each call to a cmd_* method produces a new workspace_id to be used in subsequent calls if they depend on some """\
    """change in state occuring due to that method call, like fitting a model, creating a dataset, or running inference."""\
    """This allows back-tracking via workspace_ids to a previous state without side-effects from subsequent method calls."""

CMD_CREATE_DATASET_DESC = "creates a new dataset. executes R code for cmd_create_dataset()."\
    "The dataset is created using the sql argument under the name proved with the dataset_name argument."\
    "Called prior to cmd_rpart(), cmd_glmnet() and cmd_run_inference()."

CMD_RUN_INFERENCE_DESC = "executes R code for cmd_run_inference()."\
    "must be called after cmd_glmnet() in a chain of workspace_ids." \
    "runs on an existing dataset (dataset_in)."\
    "creates a new dataset (dataset_out) with predictions stored in the MODEL_PRED column."

CMD_RPART_DESC = "executes R code for cmd_rpart(), returns workspace_id reflecting updated workspace."\
    "does not cause side-effects when called, so can be called as many times as necessary in a workspace_id chain."\
    "limit x_vars to a maximum of 5 variables (enforced) and max_depth to 4."

CMD_GLMNET_DESC = "executes R code for cmd_glmnet(), any subsequent calls to cmd_run_inference() will"\
    "use this model. can only be called once in a chain of workspace_ids. If it needs to be called again,"\
    "back-track to the workspace_id before cmd_glmnet() was called."

# ---- Agent Instructions + Prompt Templates ----
with open(AGENT_R_LIB, "r") as fh:
    AGENT_R_CODE = "".join(fh.readlines())

BASE_AGENT_INSTRUCTIONS = "You are assisting an actuary performing data analysis and predictive modeling on life insurance data."\
    "You may call any available MCP tools like sql_schema, sql_run, cmd_init(), etc."\
    f"The cmd_* methods are implemented using the following R code: \n {AGENT_R_CODE}"

class ModelingPrompt:

    def __init__(self, model_data_vw, target_var, offset_var):
        
        train_dataset_name = "model_data_train"
        test_dataset_name = "model_data_test"

        self.modeling_prompt = \
            f"""The goal is to create a model to predict mortality on the sql table '{model_data_vw}'."""\
            f"""Use the column '{target_var}' as the target (y_var) and column '{offset_var}' as the offset (offset_var)"""\
            f"""The {model_data_vw} can be queried with sql_query() and its schema is available with sql_schema(), perform exploratory data analysis with these methods."""\
            f"""The {model_data_vw} has data quality problems, so identify and fix when possible. Remove any rows where {offset_var} <= 0."""\
            """To build a model, first call cmd_init(), this returns a workspace_id that should be passed as the first argument to the next cmd_*() call."""\
            """Each call to a cmd_*() method returns a workspace_id, which is an immutable workspace associated with any changes in state resulting from that method call, e.g. creation of datasets, models, and inference data."""\
            """This allows backtracking to previous states in modeling."""\
            f"""After calling cmd_init(), create two datasets called "{train_dataset_name}" and "{test_dataset_name}" with calls to cmd_create_dataset(), apply fixes for any data quality problems."""\
            """Next, determine variable importance call(s) to cmd_rpart(), use both actuarial soundness and predictive power as criteria for selecting variables."""\
            """Then, call cmd_glmnet() to build a model, any splines specified in design_matrix_vars must have both `Boundary.knots` and `knots` fully specified, do not use the `df` argument."""\
            """Use the decision tree returned by cmd_glmnet() to refine the design matrix, particularly inner knot locations."""\
            f"""Then run inference using cmd_run_inference() on both {train_dataset_name} and {test_dataset_name} to create perm_data_*_preds."""\
            """Run a decision tree on the results using cmd_rpart() and evaluate the final results."""\
            """Back-track if necessary. Provide a final summary with a short description of the model rationale, any data quality problems and adjustments,"""\
                """and the workspace_id associated with the final step in the modeling process."""


    def __str__(self):
        return self.modeling_prompt
