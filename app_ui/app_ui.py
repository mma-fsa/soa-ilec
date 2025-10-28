import os, re
from starlette.applications import Starlette
from starlette.responses import HTMLResponse, FileResponse, JSONResponse
from starlette.requests import Request
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader
import sqlglot
import mistune
import uvicorn

from app_shared import Database, AppSession
from vwmodel import DataViewModel, AgentViewModel
from agent import AssumptionsAgent
from prompt import ModelingPrompt
from pathlib import Path

# --- Config ---
MCP_URL = os.environ.get("MCP_URL", "http://127.0.0.1:9090/mcp")

# --- Templates ---
templates = Environment(loader=FileSystemLoader("templates"))

def render(template_name, **ctx):
    template = templates.get_template(template_name)
    return HTMLResponse(template.render(**ctx))

# --- Routes that render views / handle posts ---

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
            
    return render("agent.html", view_data=view_data)

async def audit(request: Request):
    return render("audit.html", title="ILEC Agent Interface")

async def agent_response(request: Request):
    return render("audit.html", title="ILEC Agent Interface")

# --- Routes for js async (XmlHttpRequest) ---
async def start_agent(request: Request):
    
    # read GET params
    qp = request.query_params
    agent_name = qp.get("agent_name", "").strip()
    model_data_view = qp.get("model_data_view", "").strip()
    target_var = qp.get("target_var", "").strip()
    offset_var = qp.get("offset_var", "").strip()

    # helpers
    def get_error_json(message = "error", invalid = None):
        invalid = [] if invalid is None else invalid
        return JSONResponse({
                "success": False,
                "message": message,
                "invalid": invalid,
            },
            status_code=400)

    # check missing
    missing = [
        name for name, val in (
            ("agent_name", agent_name),
            ("model_data_view", model_data_view),
            ("target_var", target_var),
            ("offset_var", offset_var),
        ) if not val
    ]

    if missing:
        return get_error_json(
            message = f"Missing required parameters: {', '.join(missing)}",
            invalid = missing
        )
    
    # check valid name    
    if not AssumptionsAgent.is_valid_agent_name(agent_name):
        return get_error_json(
            message = f"Invalid agent_name: '{agent_name}'",
            invalid = ["agent_name"]
        )
    
    cols = None
    with Database.get_duckdb_conn() as conn:
        avm = AgentViewModel(conn)
        cols = None
        try:
            cols = avm.get_columns(model_data_view)
        except:
            return get_error_json(
                message = f"Invalid model_data_view: '{model_data_view}', error fetching columns",
                invalid = ["model_data_view"]
            )

        cols = set(cols)
        if not offset_var in cols:
            return get_error_json(
                message = f"Invalid offset_var: '{offset_var}', does not exist in model_data_view." ,
                invalid = ["offset_var"]
            )
    
        if not target_var in cols:
            return get_error_json(
                message = f"Invalid target_var: '{target_var}', does not exist in model_data_view." ,
                invalid = ["target_var"]
            )

    predictor_cols = list(cols.difference(set([offset_var, target_var])))

    # create the prompt
    modeling_prompt = ModelingPrompt(
        model_data_vw = model_data_view,
        predictors=predictor_cols,
        target_var=target_var,
        offset_var=offset_var
    )
    
    # create an assumptions agent
    assump_agent = AssumptionsAgent()
    agent_response = await assump_agent.prompt_async(
        agent_name,
        str(modeling_prompt)
    )

    # save the response markdown + html
    work_dir = None
    with Database.get_session_conn() as conn:
        app_session = AppSession(conn)
        work_dir = Path(app_session["MCP_WORK_DIR"])

    with open(work_dir / "response.md", "w") as fh:
        fh.write(agent_response)

    md_render = mistune.create_markdown()
    agent_response_html = md_render(agent_response)
    with open(work_dir / "response.html", "w") as fh:
        fh.write(agent_response_html)

    return JSONResponse({
        "agent_name": agent_name,
        "model_data_view": model_data_view,
        "target_var": target_var,
        "offset_var": offset_var,
    })

async def poll_agent(request: Request):
    pass

routes = [
    Route("/", data, methods=["GET", "POST"]),
    Route("/data", data, methods=["GET", "POST"]),
    Route("/agent", agent, methods=["GET", "POST"]),
    Route("/start_agent", start_agent, methods=["GET"]),
    Route("/poll_agent", poll_agent, methods=["GET"]),
    Route("/agent_response", agent_response, methods=["GET"]),
    Route("/audit", audit, methods=["GET", "POST"]),
    Mount("/static", app=StaticFiles(directory="static"), name="static"),
]

app = Starlette(debug=True, routes=routes)

if __name__ == "__main__":
    uvicorn.run("app_ui:app", host="127.0.0.1", port=8086, reload=True)
