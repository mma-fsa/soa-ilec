#!/usr/bin/env python3
"""
ILEC MCP server (Streamable HTTP) + simple /health endpoint (ASGI wrapper).

Run:
  uvicorn parquet_mcp_server:app --host 127.0.0.1 --port 8000 --reload
POST to:
  http://127.0.0.1:8000/mcp
"""

import time, re, uuid, json, shutil
from typing import Any, Dict, List
from starlette.responses import PlainTextResponse
from mcp.server.fastmcp import FastMCP, Context
from pathlib import Path

# ---- local imports ----
from ilec_r_lib import ILECREnvironment as REnv, AgentRCommands as RCmd
from app_shared import Database, AppSession
from env_vars import DEFAULT_DDB_ROW_LIMIT

# get tool descriptions
from prompt import SQL_SCHEMA_DESC, SQL_RUN_DESC, CMD_INIT_DESC, CMD_CREATE_DATASET_DESC,\
    CMD_RUN_INFERENCE_DESC, CMD_RPART_DESC, CMD_GLMNET_DESC, CMD_FINALIZE_DESC

# for finalize()
from audit import AuditLogReader

# ---- Default R environment setup ----
def create_REnv(workspace_id, no_cmd=False):
            
    with Database.get_session_conn() as con:
        session = AppSession(con)
        workspace_dir = Path(session["MCP_WORK_DIR"])

        # check if this workspace dir has been finalized already
        # via a call to cmd_finalize()
        check_finalize = workspace_dir / "final.json"

        if (check_finalize.exists()):
            raise Exception("cmd_finalize() has already been called")

        return REnv(
            work_dir=workspace_dir,
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
def sql_run(desc : str, sql: str, ctx: Context) -> Dict[str, Any]:

    DUCKDB_SELECT_STMT = re.compile(r'(?is)^\s*select\b[^;]*\blimit\s+\d+\s*(?:offset\s+\d+\s*)?;?\s*$')

    if not DUCKDB_SELECT_STMT.match(sql):
        return {
            "success" : False,
            "message" : "'query' not a valid duckdb select sql w/ a limit clause."
        }
    
    # helper func for running sql query
    def run_query(ddb_con, desc, sql):
        t0 = time.time()
        clean_sql = sql.strip().rstrip(";")
        try:
            query_res = ddb_con.execute(clean_sql)
            cols = [d[0] for d in query_res.description]
            rows = query_res.fetchmany(DEFAULT_DDB_ROW_LIMIT + 1)
            res = {
                "success" : True,
                "desc" : desc,
                "sql" : sql,
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
                "sql" : sql,
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
            query_res = run_query(ddb_con, desc, sql)
            log_entry = {
                "success" : query_res["success"],                
                "desc" : desc,
                "sql" : sql
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

    # can select without a limit   
    DUCKDB_SELECT_STMT = re.compile(r'(?is)^\s*select\b.*;?\s*$')

    if not DUCKDB_SELECT_STMT.match(sql):
        return {
            "success" : False,
            "message" : "sql must be a valid duckdb select statement."
        }    

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


@mcp.tool(description=CMD_FINALIZE_DESC)
def cmd_finalize(workspace_id) -> Dict[str , Any]:
    
    final_workspace_id = None
    renv = create_REnv(workspace_id, no_cmd=True)
    with renv:
        final_workspace_id = renv.workspace_id

    # create final modeling log
    with Database.get_session_conn() as con:
        
        session = AppSession(con)
        workspace_dir = Path(session["MCP_WORK_DIR"])
    
        audit_reader = AuditLogReader(workspace_dir)

        sql_log = audit_reader.traverse_sql_audit_log()
        final_model_log, all_model_logs, all_by_time = audit_reader.traverse_model_audit_log(workspace_id)

        finalize_data = {
            "workspace_id": final_workspace_id,
            "sql_log" : sql_log,
            "final_model_log" : final_model_log,
            "all_model_logs" : all_model_logs,
            "all_model_logs_by_time" : all_by_time
        }

        with open(workspace_dir / Path("final.json"), "w") as fh:
            json.dump(finalize_data, fh)
    
    # gather PNGs / plots
    img_dir = workspace_dir / "plots"

    if not img_dir.exists():
        img_dir.mkdir(exist_ok=True)

    for i in list(workspace_dir.rglob("*.png")):

        ws_dir = i.parent.name
        _, ws_id = ws_dir.split("_")

        img_path = img_dir / f"{ws_id}.png"
        shutil.copy(i, img_path)
    
    return {
        "workspace_id": final_workspace_id, 
        "result": {
            "success" : True,
            "message" : "workspace finalized, no further calls to cmd_*() allowed, your modeling work is done."
        }}


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
