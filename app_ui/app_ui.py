import os, re, json, base64, mimetypes
from starlette.applications import Starlette
from starlette.responses import HTMLResponse, FileResponse, JSONResponse, PlainTextResponse, RedirectResponse
from starlette.requests import Request
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader
import sqlglot
import mistune
import uvicorn

from app_shared import Database, AppSession
from audit import AuditLogRenderer
from vwmodel import DataViewModel, AgentViewModel
from agent import AssumptionsAgent
from prompt import ModelingPrompt
from pathlib import Path

from env_vars import DEFAULT_AGENT_WORK_DIR

# --- Config ---
MCP_URL = os.environ.get("MCP_URL", "http://127.0.0.1:9090/mcp")

# --- Templates ---
templates = Environment(loader=FileSystemLoader("templates"))

def render(template_name, **ctx):
    template = templates.get_template(template_name)
    return HTMLResponse(template.render(**ctx))

# --- Routes that render views / handle posts ---
async def default_handler(request: Request):
    return RedirectResponse("/agent", status_code=302)

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
        view_names = dvm.get_views()
        
        # initialize data to be rendered
        query_results = {}
        view_data = {            
            "view_names" : view_names,
            "query_message" : "Run a query to see results here.",
            "query" : "SELECT * FROM ILEC_DATA LIMIT 50;"
        }

        clear_q = False
        if "q" in request.query_params:
            view_data["query"] = sqlglot.transpile(request.query_params["q"], read="duckdb", write="duckdb", pretty=True)[0]                                    
            clear_q = True
        
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
                    raw_sql = dvm.get_view_definition(form_data["load_vw"])
                    view_data["query"] = sqlglot.transpile(raw_sql, read="duckdb", write="duckdb", pretty=True)[0]
                except Exception as e:
                    view_data["query_success"] = False
                    view_data["query_message"] = str(e)
                            
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
        query_results = query_results,
        clear_q = clear_q
    )

async def agent(request: Request):

    form_data = await request.form()
    
    with Database.get_duckdb_conn() as conn:
        
        avm = AgentViewModel(conn)
        
        mv_opts = avm.get_views()
        
        view_data = {
            "model_view_data_options" : mv_opts,
            "model_view_data_cols" : [],
            "param_selected_view" : "",
            "param_selected_target" : "",
            "param_selected_offset" : "",
            "param_agent_name" : "",            
            "previous_agents" : avm.get_previous_agents()
        }

        agent_data = {}
        load_agent_data = False

        if "param_load_agent" in request.query_params:
            load_agent_data = True            
            
            agent_name = request.query_params["param_load_agent"]
            view_data["param_agent_name"] = agent_name

            # load the previous agent data
            agent_data = avm.get_agent_data(agent_name)
            
            # set agent parameters
            agent_params = agent_data["agent_params"]            
            selected_view = agent_params["model_data_vw"]
            view_data["model_view_data_cols"] = avm.get_columns(selected_view)            
            view_data["param_selected_view"] = selected_view
            view_data["param_selected_target"] = agent_params["target_var"]
            view_data["param_selected_offset"] = agent_params["offset_var"]
            view_data["param_custom_prompt"] = agent_params["custom_prompt"]

            # set agent response
            view_data["agent_response"] = agent_data["agent_response"]
            view_data["audit_response"] = agent_data["audit_response"]
            
            artifact_links = agent_data["artifact_links"]            
            view_data["link_model_factors"] = artifact_links["model_factors"]
            view_data["link_agent_response"] = artifact_links["agent_response"]
            view_data["link_audit_response"] = artifact_links["audit_response"]
            view_data["link_model_pred"] = artifact_links["model_pred"]

        if request.method == "POST":
            selected_view = str(form_data["param_selected_view"]).strip()            
            if selected_view != "":
                view_columns = avm.get_columns(selected_view)                
                view_data["model_view_data_cols"] = view_columns
        
            view_data["param_selected_view"] = selected_view
        
    return render(
        "agent.html", 
        load_agent_data=load_agent_data,
        view_data=view_data)

async def audit(request: Request):
    return render("audit.html", title="ILEC Agent Interface")

async def agent_response(request: Request):
    return render("audit.html", title="ILEC Agent Interface")

async def plots(request):
    
    agent_name = request.path_params["agent_name"]
    workspace_id = request.path_params["workspace_id"]
    
    workdir_path = Path(DEFAULT_AGENT_WORK_DIR) / agent_name

    if not workdir_path.exists():
        return PlainTextResponse(f"'{agent_name}' does not exist", status_code=400)
    
    plot_dir_path = workdir_path / "plots"

    if not plot_dir_path.exists():
        return PlainTextResponse(f"'{agent_name}' does not have any plots.", status_code=400)

    plot_path = plot_dir_path / f"{workspace_id}.png"

    if not plot_path.exists():
        return PlainTextResponse(f"'{plot_path}' does not exist.", status_code=400)
    
    mime, _ = mimetypes.guess_type(str(plot_path))
    mime = mime or "application/octet-stream"
    
    img_bytes = plot_path.read_bytes()
    b64 = base64.b64encode(img_bytes).decode("ascii")
    data_uri = f"data:{mime};base64,{b64}"

    return render("plot.html", data_uri=data_uri, agent=agent_name, wid = workspace_id)

async def export_artifact(request):

    allowed_artifacts = {
        "model_factors": "model_factors.xlsx",
        "agent_response": "response.md",
        "audit_response": "audit.html",
    }

    agent_name = request.path_params["agent_name"]
    artifact_name_raw: str = request.path_params["artifact_name"]
    artifact_key = (artifact_name_raw or "").lower().strip()

    # Validate artifact key
    if artifact_key not in allowed_artifacts:
        return PlainTextResponse(f"'{artifact_name_raw}' is not a valid artifact.", status_code=400)

    # Resolve path
    filename = allowed_artifacts[artifact_key]
    artifact_path = (Path(DEFAULT_AGENT_WORK_DIR) / agent_name / filename).resolve()

    # Ensure the resolved path is within the agents work dir
    base_dir = Path(DEFAULT_AGENT_WORK_DIR).resolve()
    try:
        artifact_path.relative_to(base_dir)
    except ValueError:
        return PlainTextResponse("invalid path resolution", status_code=400)

    # Check existence
    if not artifact_path.is_file():
        return PlainTextResponse("artifact not found", status_code=404)

    # Guess content type (override a couple common ones)
    mime, _ = mimetypes.guess_type(str(artifact_path))
    if not mime:
        # sensible defaults
        if artifact_path.suffix.lower() == ".md":
            mime = "text/markdown; charset=utf-8"
        elif artifact_path.suffix.lower() == ".xlsx":
            mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        else:
            mime = "application/octet-stream"

    # Nice download name (include agent prefix)
    download_name = f"{agent_name}-{filename}"

    # Send as an attachment; disable caching if these are sensitive
    headers = {"Cache-Control": "no-store"}

    return FileResponse(
        path=str(artifact_path),
        media_type=mime,
        filename=download_name,                   # Starlette >= 0.27
        content_disposition_type="attachment",
        headers=headers,
    )

# --- Routes for js async (XmlHttpRequest) ---
async def start_agent(request: Request):
    
    # read GET params (this should really be a POST)
    qp = request.query_params
    agent_name = qp.get("agent_name", "").strip()
    model_data_view = qp.get("model_data_view", "").strip()
    target_var = qp.get("target_var", "").strip().upper()
    offset_var = qp.get("offset_var", "").strip().upper()
    custom_prompt = qp.get("custom_prompt", "").strip().lower()

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
    
    # check if already exists
    if (Path(DEFAULT_AGENT_WORK_DIR) / agent_name).exists():
        return get_error_json(
            message = f"Agent already exists: '{agent_name}'. Choose a different name.",
            invalid = ["agent_name"]
        )

    cols = None
    with Database.get_duckdb_conn() as conn:
        avm = AgentViewModel(conn)
        cols = None
        try:
            cols = list(map(
                lambda c: c.upper(),
                avm.get_columns(model_data_view)
            ))
        except:
            return get_error_json(
                message = f"Invalid model_data_view: '{model_data_view}', error fetching columns",
                invalid = ["model_data_view"]
            )

        try:
            ds_idx = cols.index("DATASET")
            cols.pop(ds_idx)
        except ValueError:
            return get_error_json(
                message = f"Column 'DATASET' must be present in '{model_data_view}', case insensitive.",
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

    predictor_cols = list(cols.difference(set([offset_var, target_var, "DATASET"])))

    # create the prompt
    modeling_prompt = ModelingPrompt(
        model_data_vw = model_data_view,
        predictors=predictor_cols,
        target_var=target_var,
        offset_var=offset_var,
        custom_prompt=custom_prompt if len(custom_prompt) > 0 else None
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
        work_dir = Path(app_session["MCP_WORK_DIR"]) # pyright: ignore[reportArgumentType]
        agent_status = app_session["AGENT_STATUS"]

    with open(work_dir / "response.md", "w") as fh:
        fh.write(agent_response)

    md_render = mistune.create_markdown()
    agent_response_html = md_render(agent_response)
    with open(work_dir / "response.html", "w") as fh:
        fh.write(agent_response_html) # pyright: ignore[reportArgumentType]

    # write the agent parameters
    agent_params = {
        "model_data_vw" : model_data_view,
        "predictors" : predictor_cols,
        "target_var" : target_var,
        "offset_var" : offset_var,
        "prompt" : str(modeling_prompt),
        "custom_prompt" : custom_prompt or ""
    }    
    with open(work_dir / "agent_params.json", "w") as fh:
        json.dump(agent_params, fh)

    # prepare the audit log
    audit_render = AuditLogRenderer(work_dir)
    audit_response_html = audit_render.render()
    with open(work_dir / "audit.html", "w") as fh:
        fh.write(audit_response_html)

    return JSONResponse({
        "agent_name": agent_name,
        "agent_status":agent_status,
        "model_data_view": model_data_view,
        "target_var": target_var,
        "offset_var": offset_var,
        "agent_response" : agent_response_html,
        "audit_log" : audit_response_html
    })

async def poll_agent(request: Request):
    
    last_event = "<none>"
    with Database.get_session_conn() as conn:
        last_event = AppSession(conn)["AGENT_LAST_ACTION"]

    return JSONResponse({
        "last_event" : last_event
    })

# --- Configure server routes ---

routes = [
    Route("/", default_handler, methods=["GET", "POST"]),
    Route("/data", data, methods=["GET", "POST"]),
    Route("/agent", agent, methods=["GET", "POST"]),
    Route("/start_agent", start_agent, methods=["GET"]),    
    Route("/poll_agent", poll_agent, methods=["GET"]),
    Route("/agent_response", agent_response, methods=["GET"]),
    Route("/audit", audit, methods=["GET", "POST"]),
    Route("/plots/{agent_name}/{workspace_id}", endpoint=plots, methods=["GET"]),
    Route("/export/{agent_name}/{artifact_name}", endpoint=export_artifact, methods=["GET"]),
    Mount("/static", app=StaticFiles(directory="static"), name="static"),
    Mount("/img", app=StaticFiles(directory="img"), name="img")
]

app = Starlette(debug=True, routes=routes)

if __name__ == "__main__":
    uvicorn.run("app_ui:app", host="127.0.0.1", port=8086, reload=True)
