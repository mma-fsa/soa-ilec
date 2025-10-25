#!/usr/bin/env python3
"""
ILEC MCP server (Streamable HTTP) + simple /health endpoint (ASGI wrapper).

Run:
  uvicorn parquet_mcp_server:app --host 127.0.0.1 --port 8000 --reload
POST to:
  http://127.0.0.1:8000/mcp
"""

import time, re, uuid, json
from typing import Any, Dict, List
from starlette.responses import PlainTextResponse
from mcp.server.fastmcp import FastMCP, Context
from pathlib import Path

# ---- local imports ----
from ilec_r_lib import ILECREnvironment as REnv, AgentRCommands as RCmd
from app_shared import Database, AppSession
from env_vars import DEFAULT_DDB_ROW_LIMIT

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

# ---- Default R environment setup ----
def create_REnv(workspace_id, no_cmd=False):
    with Database.get_session_conn() as con:
        session = AppSession(con)
        return REnv(
            work_dir=session["MCP_WORK_DIR"],
            db_pragmas=Database.DDB_PRAGMAS,
            last_workspace_id=workspace_id,
            no_cmd=no_cmd
        )

# ---- MCP server + tools ----
mcp = FastMCP("ilec")

@mcp.tool(description=SQL_SCHEMA_DESC)
def sql_schema(table_name : str, ctx: Context) -> Dict[str, Any]:

    # CAUTION: PRAMGA's don't support prepared statements,
    # doing the best we can with regexp
    DUCKDB_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

    if DUCKDB_IDENTIFIER_RE.match(table_name):
        try:
            with Database.get_duckdb_conn() as con:
                rows = con.execute(f"PRAGMA table_info({table_name})").fetchall()
            res = {
                "success" : True,
                "schema" : {str(r[1]): str(r[2]).upper() for r in rows}
            }
        except Exception as e:
            res = {
                "success" : False,
                "message" : str(e)
            }
    else:
        res = {
            "success" : False,
            "message" : f"Provided table_name is not a valid duckdb identifier '{table_name}'"
        }

    return res

@mcp.tool(description=SQL_RUN_DESC)
def sql_run(desc : str, query: str, ctx: Context) -> Dict[str, Any]:

    DUCKDB_SELECT_STMT = re.compile(r'(?is)^\s*select\b[^;]*\blimit\s+\d+\s*(?:offset\s+\d+\s*)?;?\s*$')

    if not DUCKDB_SELECT_STMT.match(query):
        return {
            "success" : False,
            "message" : "Not a valid duckdb select query w/ a limit clause."
        }
    
    # helper func for running sql query
    def run_query(ddb_con, desc, query):
        t0 = time.time()
        clean_query = query.strip().rstrip(";")
        try:
            query_res = ddb_con.execute(clean_query)
            cols = [d[0] for d in query_res.description]
            rows = query_res.fetchmany(DEFAULT_DDB_ROW_LIMIT + 1)
            res = {
                "success" : True,
                "desc" : desc,
                "query" : clean_query,
                "results" : {
                    "columns": cols,
                    "rows": rows[:DEFAULT_DDB_ROW_LIMIT],
                    "truncated": len(rows) > DEFAULT_DDB_ROW_LIMIT,
                    "elapsed_s": round(time.time() - t0, 3),
                }                
            }
        except Exception as e:
            res = {
                "success" : False,
                "desc" : desc,
                "query" : clean_query,
                "message" : str(e)
            }
        return res        

    # get session data   
    with Database.get_session_conn() as sess_con:
        session_data = AppSession(sess_con)._get_data()

    sql_log_dir = Path(session_data["MCP_WORK_DIR"]) / Path("sql_run")
    if not sql_log_dir.exists():
        # allow for inter-leaved calls via exist_ok = True
        sql_log_dir.mkdir(parents=True, exist_ok=True)
    
    # run query + create audit log entry
    with Database.get_duckdb_conn() as ddb_con:
        query_id = uuid.uuid4()
        query_log_file = sql_log_dir / Path(f"query_{query_id}.json")
        with open(query_log_file.resolve(), "w") as log_fh:
            query_res = run_query(ddb_con, desc, query)
            log_entry = {
                "success" : query_res["success"],                
                "desc" : desc,
                "query" : query
            }            
            json.dump(log_entry, log_fh)
        
    return query_res
    
@mcp.tool(description=CMD_INIT_DESC)
def cmd_init() -> Dict[str, Any]:
    workspace_id = None
    renv = create_REnv(None, no_cmd=True)
    with renv:
        workspace_id = renv.workspace_id
    return {
        "workspace_id": workspace_id, 
        "result": {
            "success" : True,
            "message" : "workspace created"
        }}

@mcp.tool(description=CMD_CREATE_DATASET_DESC)
def cmd_create_dataset(workspace_id, dataset_name, sql) -> Dict[str, Any]:    
    
    r_env = create_REnv(workspace_id)
    new_workspace_id = r_env.workspace_id    
    dataset_res = RCmd.run_command(
        RCmd.cmd_create_dataset,
        (
            dataset_name,
            sql
        ),
        r_env
    )

    return {
        "workspace_id": new_workspace_id,
        "result": dataset_res
    }

@mcp.tool(description=CMD_RUN_INFERENCE_DESC)
def cmd_run_inference(workspace_id, dataset_in, dataset_out) -> Dict[str, Any]:
    r_env = create_REnv(workspace_id)
    new_workspace_id = r_env.workspace_id
    dataset_res = RCmd.run_command(
        RCmd.cmd_run_inference,
        (
            dataset_in, 
            dataset_out            
        ),
        r_env
    )
    return {
        "workspace_id": new_workspace_id,
        "result": dataset_res
    }


@mcp.tool(description=CMD_RPART_DESC)
def cmd_rpart(workspace_id: str, dataset: str, x_vars: List[str], offset: str, y_var: str, max_depth : int, cp : float, ctx: Context) -> Dict[str, Any]:    
    
    r_env = create_REnv(workspace_id)
    new_workspace_id = r_env.workspace_id

    if len(x_vars) > 5:
        return {
            "workspace_id": new_workspace_id,
            "result": {
                "success": False,
                "message": "more than 5 x_vars passed, maximum of 5"
            }
        }
    if max_depth > 4:
        return {
            "workspace_id": new_workspace_id,
            "result": {
                "success": False,
                "message": "max_depth > 4 passed, maximum of 4"
            }
        } 
    
    dataset_res = RCmd.run_command(
        RCmd.cmd_rpart,
        (
            dataset, 
            x_vars,
            offset, 
            y_var, 
            min(max_depth, 4), 
            max(cp, 0.0001)
        ),
        r_env
    )
    return {
        "workspace_id": new_workspace_id,
        "result": dataset_res
    }

@mcp.tool(description=CMD_GLMNET_DESC)
def cmd_glmnet(workspace_id, dataset : str, x_vars : List[str], design_matrix_vars : List[str], \
              factor_vars_levels: dict, num_var_clip : dict, offset_var : str, y_var : str, lambda_strat : str):
    
    r_env = create_REnv(workspace_id)
    new_workspace_id = r_env.workspace_id
    dataset_res = RCmd.run_command(
        RCmd.cmd_glmnet,
        (
            dataset, 
            x_vars,
            design_matrix_vars,
            factor_vars_levels,
            num_var_clip,
            offset_var,
            y_var,
            lambda_strat
        ),
        r_env
    )
    return {
        "workspace_id": new_workspace_id,
        "result": dataset_res
    }

# Build the MCP ASGI app
mcp_app = mcp.streamable_http_app()  # exposes /mcp

# ---- Minimal ASGI wrapper to add /health without touching lifespan ----
class HealthWrapper:
    def __init__(self, inner):
        self.inner = inner

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope.get("path") in ("/", "/health") and scope.get("method") == "GET":
            resp = PlainTextResponse("ok", status_code=200)
            await resp(scope, receive, send)
            return
        # Delegate everything else (/mcp, streams, lifespan) to the MCP app
        await self.inner(scope, receive, send)

# Uvicorn entrypoint
app = HealthWrapper(mcp_app)
