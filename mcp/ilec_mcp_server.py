#!/usr/bin/env python3
"""
ILEC MCP server (Streamable HTTP) + simple /health endpoint (ASGI wrapper).

Run:
  uvicorn parquet_mcp_server:app --host 127.0.0.1 --port 8000 --reload
POST to:
  http://127.0.0.1:8000/mcp
"""
import os, time
from typing import Any, Dict, List

import duckdb
from starlette.responses import PlainTextResponse
from mcp.server.fastmcp import FastMCP, Context

import asyncio
import anyio
from contextlib import asynccontextmanager
from pathlib import Path

from ilec_r_lib import ILECREnvironment as REnv, AgentRCommands as RCmd

# ---- Max Work Processes ----
MAX_WORKERS = 4
anyio.to_thread.current_default_thread_limiter().total_tokens = MAX_WORKERS

# ---- Worker directory ----
WORKER_DIR = "/home/mike/workspace/soa-ilec/soa-ilec/mcp_agent_work/"

# ---- Config ----
PARQUET_PATH = os.environ.get("PARQUET_PATH", "/home/mike/workspace/soa-ilec/soa-ilec/data/ilec_perm_historical.parquet")
#PARQUET_PATH = os.environ.get("PARQUET_PATH", "/home/mike/workspace/soa-ilec/soa-ilec/data/ilec_2009_19_20210528.parquet")
ROW_LIMIT    = int(os.environ.get("ROW_LIMIT", "5000"))

# ---- DuckDB ----
con = duckdb.connect(config={"threads": os.cpu_count() or 4})

SETUP_DDB = [    
    "PRAGMA disable_progress_bar",
    "PRAGMA memory_limit='16GB'",
    f"PRAGMA threads={os.cpu_count() or 4}",
    f"CREATE VIEW ILEC_DATA AS SELECT * FROM read_parquet('{PARQUET_PATH}')"
]
for sql in SETUP_DDB:
    con.execute(sql)

# ---- Default R environment setup ----
def create_REnv(session_guid, no_cmd=False):
    return REnv(WORKER_DIR, PARQUET_PATH, SETUP_DDB, session_guid, no_cmd=no_cmd)

# ---- MCP server + tools ----
mcp = FastMCP("ilec")

@mcp.tool(description="Returns schema information for the ILEC_DATA table.")
def sql_schema(ctx: Context) -> Dict[str, str]:
    rows = con.execute("PRAGMA table_info('ILEC_DATA')").fetchall()
    return {str(r[1]): str(r[2]).upper() for r in rows}

@mcp.tool(description="Run a single ANSI-SQL compatible query on the ILEC_DATA table. Can only select from ILEC_DATA.  Must include LIMIT (enforced).")
def sql_run(query: str, ctx: Context) -> Dict[str, Any]:

    # logging
    t0 = time.time()
    with open(Path(WORKER_DIR) / Path("sql_run.log"), "a+") as fh:
        fh.write("\nRunning query...\n")
        fh.write(query + "\n")         
        q = (query or "").strip().rstrip(";")    
        try:        
            res = con.execute(q)
            fh.write("\n\tquery done")
        except:
            fh.write("\n\tquery error")
            raise
    
    cols = [d[0] for d in res.description]
    rows = res.fetchmany(ROW_LIMIT + 1)
    return {
        "columns": cols,
        "rows": rows[:ROW_LIMIT],
        "truncated": len(rows) > ROW_LIMIT,
        "elapsed_s": round(time.time() - t0, 3),
    }
    
CMD_INIT_DESC = """
Initializes a session for calls to cmd_* methods, returns a session_id. session_ids represent immutable workspaces.
each call to a cmd_* method produces a new session_id to be used in subsequent calls if they depend on some
change in state that occured with the method call, like fitting a model, creating a dataset, or running inference."
"This allows back-tracking to a previous state without side-effects from subsequent method calls.""".strip()
@mcp.tool(description=CMD_INIT_DESC)
def cmd_init() -> Dict[str, Any]:
    session_id = None
    renv = create_REnv(None, no_cmd=True)
    with renv:
        session_id = renv.session_guid
    return {
        "session_id": session_id, 
        "result": {
            "success" : True,
            "message" : "workspace created"
        }}

@mcp.tool(description="creates a new dataset. executes R code for cmd_create_dataset(). Called prior to cmd_rpart(), cmd_glmnet() and cmd_run_inference().")
def cmd_create_dataset(session_id, dataset_name, sql) -> Dict[str, Any]:    
    r_env = create_REnv(session_id)
    new_session_id = r_env.session_guid
    dataset_res = RCmd.run_command(
        RCmd.cmd_create_dataset,
        (
            dataset_name,
            sql
        ),
        r_env
    )
    return {
        "session_id": new_session_id,
        "result": dataset_res
    }

@mcp.tool(description="executes R code for cmd_run_inference()."
          "must be called after cmd_glmnet() in a chain of session_ids." \
          "runs on an existing dataset (dataset_in)."\
          "creates a new dataset (dataset_out) with predictions stored in the MODEL_PRED column.")
def cmd_run_inference(session_id, dataset_in, dataset_out) -> Dict[str, Any]:
    r_env = create_REnv(session_id)
    new_session_id = r_env.session_guid
    dataset_res = RCmd.run_command(
        RCmd.cmd_run_inference,
        (
            dataset_in, 
            dataset_out            
        ),
        r_env
    )
    return {
        "session_id": new_session_id,
        "result": dataset_res
    }

@mcp.tool(description="executes R code for cmd_rpart(), returns session_id reflecting updated workspace."\
          "does not cause side-effects when called, so can be called as many times as necessary in a session_id chain."\
          "limit x_vars to a maximum of 5 variables (enforced) and max_depth to 4.")
def cmd_rpart(session_id: str, dataset: str, x_vars: List[str], offset: str, y_var: str, max_depth : int, cp : float, ctx: Context) -> Dict[str, Any]:    
    
    r_env = create_REnv(session_id)
    new_session_id = r_env.session_guid

    if len(x_vars) > 5:
        return {
            "session_id": new_session_id,
            "result": {
                "success": False,
                "message": "more than 5 x_vars passed, maximum of 5"
            }
        }
    if max_depth > 4:
        return {
            "session_id": new_session_id,
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
        "session_id": new_session_id,
        "result": dataset_res
    }

@mcp.tool(description="executes R code for cmd_glmnet(), any subsequent calls to cmd_run_inference() will"\
          "use this model. can only be called once in a chain of session_ids.  If it needs to be called again,"
          "back-track to the session_id before cmd_glmnet() was called.")
def cmd_glmnet(session_id, dataset : str, x_vars : List[str], design_matrix_vars : List[str], \
              factor_vars_levels: dict, num_var_clip : dict, offset_var : str, y_var : str, lambda_strat : str):
    r_env = create_REnv(session_id)
    new_session_id = r_env.session_guid
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
        "session_id": new_session_id,
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
