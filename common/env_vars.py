from pathlib import Path
import os

# ---- Admin Settings ( move to environment vars) ----
MAX_SERVER_WORKERS = 4

DEFAULT_DDB_WORKERS = os.cpu_count() or 4
DEFAULT_DDB_MEM = "16GB"

DEFAULT_DATA_DIR = Path("/home/mike/workspace/soa-ilec/soa-ilec/data/")
DEFAULT_DDB_PATH = DEFAULT_DATA_DIR / Path("ilec_data.duckdb")
DEFAULT_ILEC_PQ_LOCATION = DEFAULT_DATA_DIR / Path("ilec_2009_19_20210528.parquet")

DEFAULT_SESS_DB_INIT_SQL = """CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)"""

DEFAULT_DDB_INIT_SQL = "create view if not exists ILEC_DATA as "\
    f"(select * from read_parquet('{DEFAULT_ILEC_PQ_LOCATION}'))"

DEFAULT_DDB_ROW_LIMIT = 1000

AGENT_R_LIB = "/home/mike/workspace/soa-ilec/soa-ilec/mcp/ilec_r_lib.R"

# ---- Default Session Settings, stuff the client can change ----
DEFAULT_SESSION_SETTINGS = {
    "MCP_WORK_DIR" : "/home/mike/workspace/soa-ilec/soa-ilec/mcp_agent_work/",    
    "MODEL_DB": DEFAULT_DDB_PATH,
    "MODEL_DATA_VW" : "ILEC_DATA"
}

