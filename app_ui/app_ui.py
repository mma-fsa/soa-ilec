import os
from starlette.applications import Starlette
from starlette.responses import HTMLResponse, FileResponse, JSONResponse
from starlette.requests import Request
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader
import sqlglot
from sqlglot import expressions as exp
import httpx
import uvicorn

from app_shared import Database
from vwmodel import DataViewModel, AgentViewModel
from pathlib import Path

# --- Config ---
MCP_URL = os.environ.get("MCP_URL", "http://127.0.0.1:9090/mcp")

# --- Templates ---
templates = Environment(loader=FileSystemLoader("templates"))

def render(template_name, **ctx):
    template = templates.get_template(template_name)
    return HTMLResponse(template.render(**ctx))

# --- Routes ---
async def data(request: Request):
    
    form_data = await request.form()

    # check if this is a read-only view
    if request.method == "POST" and "read_only" in form_data.keys():
        read_only = form_data["read_only"] == "on"
    else:
        read_only = False

    # populate the view data
    with Database.get_duckdb_conn(read_only = read_only) as conn:
    
        dvm = DataViewModel(conn)

        view_names = list(map(lambda x: x[0], dvm.get_views()["rows"]))
        
        # initialize data to be rendered
        query_results = {}
        view_data = {            
            "view_names" : view_names,
            "query_message" : "Run a query to see results here.",
            "query" : "SELECT * FROM ILEC_DATA LIMIT 50;"
        }
                        
        if request.method == "POST":     
            if "run_query" in form_data.keys():
                query = form_data["query"]
                view_data["query"] = query
                query_limit = 500
                try:                    
                    query_res = dvm.run_query(query, limit=query_limit)
                    view_data["query_results"] = query_res
                    view_data["query_limit"] = query_limit
                    view_data["query_success"] = True
                except Exception as e:
                    view_data["query_success"] = False
                    view_data["query_message"] = str(e)

            elif "load_vw" in form_data.keys():
                try:
                    raw_sql = dvm.get_view_definition(form_data["load_vw"])["rows"][0][0]                    
                except Exception as e:
                    view_data["query_success"] = False
                    view_data["query_message"] = str(e)

                query = sqlglot.transpile(raw_sql, read="duckdb", write="duckdb", pretty=True)[0]
            
            elif "export_results" in form_data.keys():
                
                export_type = form_data["export_results"]
                query = form_data["query"]                
             
                if export_type == "excel":
                    file_path = Path(dvm.export_xlsx(query)[1])
                    content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                elif export_type == "csv":
                    file_path = Path(dvm.export_csv(query))
                    content_type = "text/csv"
                elif export_type == "parquet":
                    file_path = Path(dvm.export_parquet(query))
                    content_type = "application/octet-stream"
                else:
                    raise Exception(f"Invalid export_type: {export_type}")

                # --- Return file for download ---            
                return FileResponse(
                    path=file_path,
                    media_type=content_type,
                    filename=file_path.name,
                )
        
    return render(
        "data.html", 
        view_data = view_data,
        query_results = query_results
    )

async def agent(request: Request):

    form_data = await request.form()
    
    with Database.get_duckdb_conn() as conn:
        
        avm = AgentViewModel(conn)
        
        mv_opts = list(map(
            lambda x: x[0], 
            avm.get_views()["rows"]))
        
        view_data = {
            "model_view_data_options" : mv_opts,
            "model_view_data_cols" : [],
            "param_selected_view" : "",
            "param_selected_target" : "",
            "param_selected_offset" : ""
        }
        
        if request.method == "POST":            
            selected_view = str(form_data["model_view"]).strip()            
            if selected_view != "":
                view_columns = avm.get_columns(selected_view)
                col_name_idx = view_columns["rows"].index("name")
                view_data["model_view_data_cols"] = list(map(
                    lambda x: x[col_name_idx],
                    view_columns["rows"]))
            
            if "run_agent" in form_data.keys():
                pass
            
    return render("agent.html", view_data=view_data)

async def audit(request: Request):
    return render("audit.html", title="ILEC Agent Interface")

routes = [
    Route("/", data, methods=["GET", "POST"]),
    Route("/data", data, methods=["GET", "POST"]),
    Route("/agent", agent, methods=["GET", "POST"]),
    Route("/audit", audit, methods=["GET", "POST"]),
    Mount("/static", app=StaticFiles(directory="static"), name="static"),
]

app = Starlette(debug=True, routes=routes)

if __name__ == "__main__":
    uvicorn.run("app_ui:app", host="127.0.0.1", port=8086, reload=True)
