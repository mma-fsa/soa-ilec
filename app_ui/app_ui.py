import os
from starlette.applications import Starlette
from starlette.responses import HTMLResponse, JSONResponse
from starlette.requests import Request
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader
import httpx
import uvicorn

# --- Config ---
MCP_URL = os.environ.get("MCP_URL", "http://127.0.0.1:9090/mcp")

# --- Templates ---
templates = Environment(loader=FileSystemLoader("templates"))

def render(template_name, **ctx):
    template = templates.get_template(template_name)
    return HTMLResponse(template.render(**ctx))

# --- Routes ---
async def homepage(request: Request):
    return render("index.html", title="ILEC Agent Interface")

async def data(request: Request):
    return render("data.html", title="ILEC Agent Interface")

async def run_query(request: Request):
    data = await request.form()
    query = data.get("query", "")
    async with httpx.AsyncClient() as client:
        res = await client.post(MCP_URL, json={
            "method": "sql_run",
            "params": {"query": query},
            "id": 1,
            "jsonrpc": "2.0"
        })
    mcp_result = res.json()
    return render("result.html", title="Results", result=mcp_result)

routes = [
    Route("/", data, methods=["GET"]),
    Route("/query", data, methods=["GET"]),
    Route("/run_query", run_query, methods=["POST"]),
    Mount("/static", app=StaticFiles(directory="static"), name="static"),
]

app = Starlette(debug=True, routes=routes)

if __name__ == "__main__":
    uvicorn.run("app_ui:app", host="127.0.0.1", port=8086, reload=True)
