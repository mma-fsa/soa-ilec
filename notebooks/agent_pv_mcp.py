#!/usr/bin/env python3
"""
ILEC MCP server (Streamable HTTP) + simple /health endpoint (ASGI wrapper).

Run:
  uvicorn parquet_mcp_server:app --host 127.0.0.1 --port 8000 --reload
POST to:
  http://127.0.0.1:8000/mcp
"""

from typing import Any, Dict, List
from starlette.responses import PlainTextResponse
from mcp.server.fastmcp import FastMCP
import numpy as np

mcp = FastMCP("pv_calc")

@mcp.tool(description="Calculate the present value of cashflows using the 1-year discount rates @ t")
def pv_calc(cashflows: List[float], int_rates_at_t: List[float]) -> Dict[str , Any]:        

    dt = 1.0 /(1.0 + np.array(int_rates_at_t))
    vt = np.cumprod(dt)
    cf_t = np.array(cashflows)
    pv = np.sum(cf_t * vt)

    return {
        "present_value": pv,
        "result": {
            "success" : True,
            "message" : "calculation successful"
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
