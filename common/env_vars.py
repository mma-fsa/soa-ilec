from pathlib import Path
import os

# ---- Admin Settings ( move to environment vars) ----
MAX_SERVER_WORKERS = 4

DEFAULT_DDB_WORKERS = os.cpu_count() or 4
DEFAULT_DDB_MEM = "16GB"

DEFAULT_DATA_DIR = Path("/home/mike/workspace/soa-ilec/soa-ilec/data/")
DEFAULT_AGENT_WORK_DIR = DEFAULT_DATA_DIR / Path("workspaces/")

DEFAULT_DDB_PATH = DEFAULT_DATA_DIR / Path("ilec_data.duckdb")
DEFAULT_ILEC_PQ_LOCATION = DEFAULT_DATA_DIR / Path("ilec_2009_19_20210528.parquet")
DEFAULT_DATA_EXPORT_DIR = DEFAULT_DATA_DIR / Path("exports")

if not DEFAULT_DATA_EXPORT_DIR.exists():
    DEFAULT_DATA_EXPORT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_SESS_DB_INIT_SQL = """CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)"""

DEFAULT_DDB_INIT_SQL = "create view if not exists ILEC_DATA as "\
    f"(select * from read_parquet('{DEFAULT_ILEC_PQ_LOCATION}'))"

DEFAULT_DDB_ROW_LIMIT = 1000

AGENT_R_LIB = "/home/mike/workspace/soa-ilec/soa-ilec/mcp/ilec_r_lib.R"
EXPORT_R_LIB = "/home/mike/workspace/soa-ilec/soa-ilec/mcp/model_export_lib.R"

DEFAULT_MCP_URL = "http://127.0.0.1:9090/mcp/"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or Path("/home/mike/workspace/soa-ilec/soa-ilec/.openai_key").read_text().strip()

# ---- Default Session Settings, global state for the UI + MCP ----
DEFAULT_SESSION_SETTINGS = {
    "AGENT_NAME" : "test_agent",
    "AGENT_STATUS" : "NOT_RUNNING",    
    "MCP_WORK_DIR" : DEFAULT_AGENT_WORK_DIR / Path("/test_agent/"),    
    "MODEL_DB": DEFAULT_DDB_PATH,
    "MODEL_DATA_VW" : "ILEC_DATA"
}

COMMON_TEMPLATE_DIR = "/home/mike/workspace/soa-ilec/soa-ilec/common/templates/"