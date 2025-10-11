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

# ---- Config ----
PARQUET_PATH = os.environ.get("PARQUET_PATH", "/home/mike/workspace/soa-ilec/soa-ilec/data/ilec_2009_19_20210528.parquet")
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

# ---- MCP server + tools ----
mcp = FastMCP("ilec")

@mcp.tool(description="Returns schema information for the ILEC_DATA table.")
def schema(ctx: Context) -> Dict[str, str]:
    rows = con.execute("PRAGMA table_info('ILEC_DATA')").fetchall()
    return {str(r[1]): str(r[2]).upper() for r in rows}

@mcp.tool(description="Run a single ANSI-SQL compatible query on the ILEC_DATA table. Can only select from ILEC_DATA.  Must include LIMIT (enforced).")
def sql(query: str, ctx: Context) -> Dict[str, Any]:
    print(query)    
    t0 = time.time()
    q = (query or "").strip().rstrip(";")    
    try:        
        res = con.execute(q)
        print("\n\tquery done")
    except:
        print("\n\tquery error")
        raise
    
    cols = [d[0] for d in res.description]
    rows = res.fetchmany(ROW_LIMIT + 1)
    return {
        "columns": cols,
        "rows": rows[:ROW_LIMIT],
        "truncated": len(rows) > ROW_LIMIT,
        "elapsed_s": round(time.time() - t0, 3),
    }

RPART_POISSON_DESC = \
    "Runs a Poisson decision tree using rpart against the result of a sql query on ILEC_DATA table."\
    "The where_clause argument will be used in the query SELECT * FROM ILEC_DATA WHERE <where_clause>."\
    "all other arguments are must be valid column names from the schema of ILEC_data."\
    "pass x_vars as a string array of column names OR an array of SQL expressions referencing valid column names. x_vars has a maximum size of 6 elements."\
    "pass y_var as a string and a valid column name, and offset as a string and a valid column name."\
    "The x_vars are used for GROUP BY in the sql query, and the SUM(offset) and SUM(y_var) are also computed."\
    "The rpart formula is then 'cbind(SUM(offset),SUM(y_var)) ~ x_vars'."\
    "max_depth and cp parameters are passed to rpart(control=rpart.control(...))."\
    "max_depth can be a maximum of 5 (enforced)."\
    "The function returns the output of print() on the decision tree."\
    "yval in the output can be interpreted as an actual-to-expected ratio."\
    

@mcp.tool(description=RPART_POISSON_DESC)
def rpart_poisson(where_clause: str, x_vars: List[str], offset: str, y_var: str, max_depth, cp, ctx: Context) -> Dict[str, Any]:
    
    print(f"rpart called with arguments: {where_clause}, {x_vars}, {offset}, {y_var}, {max_depth:d}, {cp}")

    from rpy2.robjects import r, globalenv
    from rpy2.robjects.packages import importr
    
    rduckdb = importr("duckdb")
    rDBI = importr("DBI")

    rconn = rDBI.dbConnect(rduckdb.duckdb(), ":memory:")
    
    max_depth = max(1, min(max_depth, 5))
    cp = min(1., max(0.000001, cp))

    print("running rpart()...\n")

    if len(x_vars) > 6:
        return {
            "error" : "x_vars supports a maximum of 6 columns"
        }

    try:
        for sql in SETUP_DDB:
            rDBI.dbExecute(rconn, sql)                
        
        group_by_vars = ",".join(x_vars)
        sum_vars = ",".join([
            f"SUM(coalesce({offset}, 0)) as EXPECTED_EVENTS",
            f"SUM(coalesce({y_var}, 0)) AS ACTUAL_EVENTS"
        ])

        data_sql = f"select {group_by_vars},{sum_vars} from ILEC_DATA where {where_clause} group by {group_by_vars}"
        data_sql = f"with x as ({data_sql}) select * from x where EXPECTED_EVENTS > 0"

        print("\n" + data_sql + "\n")

        globalenv["df_results"] = rDBI.dbGetQuery(rconn, data_sql)

        importr("rpart")
        
        formula_xvars = "+".join(x_vars)        

        rpart_call = "rpart("\
            """as.matrix(df_results[, c("EXPECTED_EVENTS","ACTUAL_EVENTS")]) ~ """\
            f"{formula_xvars},"\
            """data=df_results, method="poisson","""\
            f"control=rpart.control(max_depth={max_depth:d}, cp={cp:.8f}))"        

        rpart_res = r(f"capture.output(print({rpart_call}))")

        print(rpart_call)

    finally:
        rDBI.dbDisconnect(rconn)

    return {
        "print(rpart(...))" : "\n".join(rpart_res)
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
