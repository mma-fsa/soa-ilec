#!/usr/bin/env python3
"""
ILEC MCP server (Streamable HTTP) + simple /health endpoint (ASGI wrapper).

Run:
  uvicorn parquet_mcp_server:app --host 127.0.0.1 --port 8000 --reload
POST to:
  http://127.0.0.1:8000/mcp
"""
import os, time
from typing import Any, Dict

import duckdb
from starlette.responses import PlainTextResponse
from mcp.server.fastmcp import FastMCP, Context

# ---- Config ----
PARQUET_PATH = os.environ.get("PARQUET_PATH", "/home/mike/workspace/soa-ilec/soa-ilec/data/ilec_2009_19_20210528.parquet")
ROW_LIMIT    = int(os.environ.get("ROW_LIMIT", "5000"))

# ---- DuckDB ----
con = duckdb.connect(config={"threads": os.cpu_count() or 4})
con.execute(f"CREATE VIEW ILEC_DATA AS SELECT * FROM read_parquet('{PARQUET_PATH}')")
con.execute("PRAGMA disable_progress_bar")
con.execute("PRAGMA memory_limit='8GB'")
con.execute(f"PRAGMA threads={os.cpu_count() or 4}")

# ---- MCP server + tools ----
mcp = FastMCP("ilec")

@mcp.tool(description="Returns schema information for the ILEC_DATA table.")
def schema(ctx: Context) -> Dict[str, str]:
    rows = con.execute("PRAGMA table_info('ILEC_DATA')").fetchall()
    return {str(r[1]): str(r[2]).upper() for r in rows}

@mcp.tool(description="Run a SQL query on the ILEC_DATA table. Can only select from ILEC_DATA.  Must include LIMIT (enforced).")
def sql(query: str, ctx: Context) -> Dict[str, Any]:
    print(query)
    
    t0 = time.time()
    q = (query or "").strip().rstrip(";")
    ql = " " + q.lower() + " "    
    if " ilec_data " not in ql:
        return {"error": "Query must reference ilec_data in FROM clause."}
    if " limit " not in ql:
        q = f"{q} LIMIT {ROW_LIMIT}"
    res = con.execute(q)
    print("\n\tquery done")
    cols = [d[0] for d in res.description]
    rows = res.fetchmany(ROW_LIMIT + 1)
    return {
        "columns": cols,
        "rows": rows[:ROW_LIMIT],
        "truncated": len(rows) > ROW_LIMIT,
        "elapsed_s": round(time.time() - t0, 3),
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
