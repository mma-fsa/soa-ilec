from app_shared import Database
from env_vars import DEFAULT_DATA_EXPORT_DIR
import uuid
import pandas as pd
from pathlib import Path
from operator import itemgetter

EXCEL_MAX_EXPORT_ROWS = 1_048_576

def get_query_data(res, limit : int = -1):
    cols = [c[0] for c in res.description]
    rows = res.fetchall() if limit < 0 else res.fetchmany(limit + 1)
    return {
        "truncated" : False if limit < 0 else len(rows) >= (limit + 1),
        "cols" : cols,
        "rows" : rows if limit < 0 else rows[:limit]
    }

def get_columns_from_query_data(query_data, columns):
    if type(columns) == str or len(columns) == 1:
        col_name = columns if type(columns) == str else columns[0]
        col_idx = query_data["cols"].index(col_name)
        return list(map(lambda r: r[col_idx], query_data["rows"]))
    else:
        col_idxs = [query_data["cols"].index(cn) for cn in columns]
        get_cols = itemgetter(*col_idxs)
        return list(
            map(
                lambda r: get_cols(r), query_data["rows"]
            )
        )

class DataViewModel:

    def __init__(self, conn, export_dir=DEFAULT_DATA_EXPORT_DIR):
        self.conn = conn
        self.export_dir = export_dir

    def run_query(self, query, limit=500):        
        return get_query_data(self.conn.execute(query), limit=limit)

    def export_csv(self, query):
        return self._export(query, "csv")
                            
    def export_parquet(self, query):
        return self._export(query, "parquet")
    
    def export_xlsx(self, query):

        file_uuid = uuid.uuid4()                
        export_path = self.export_dir / Path(f"export_{file_uuid}.xlsx")

        df = self.conn.execute(query).df()
        truncated = False
        
        if len(df) > EXCEL_MAX_EXPORT_ROWS:
            df = df.iloc[: EXCEL_MAX_EXPORT_ROWS]
            truncated = True

        df.to_excel(export_path, index=False, engine="openpyxl")

        return truncated, export_path        

    def _export(self, query, filetype : str):
        
        file_uuid = uuid.uuid4()        
        filetype = filetype.lower().strip()
        export_path = self.export_dir / Path(f"export_{file_uuid}.{filetype}")

        if filetype == "parquet":
            export_query = f"copy({query}) to '{export_path}' (FORMAT PARQUET, ROW_GROUP_SIZE 100000)"
        elif filetype == "csv":
            export_query = f"copy({query}) to '{export_path}' "\
                """(HEADER, DELIMITER ',', QUOTE '"', ESCAPE '"', """\
                """NULL '', DATEFORMAT '%Y-%m-%d', TIMESTAMPFORMAT '%Y-%m-%d %H:%M:%S')"""
        else:
            raise Exception(f"Unhandled filetype: {filetype}")
        
        self.conn.execute(export_query)

        return export_path

    def get_views(self):
        query = """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                  and table_type = 'VIEW'
            ORDER BY table_name"""
        res = self.run_query(query)

        return get_columns_from_query_data(res, "table_name")

    def get_view_definition(self, vw_name):
        
        view_data = list(filter(
            lambda vw: vw == vw_name, 
            self.get_views()))
        
        if len(view_data) == 0:
            raise Exception("View {vw_name} does not exist")
        elif len(view_data) > 1:
            raise Exception("Multiple views matched {vw_name}")
        
        res = self.conn.execute("""
            SELECT view_definition
            FROM information_schema.views
            WHERE table_name = ? and table_schema NOT IN ('information_schema', 'pg_catalog')""",
            [vw_name])
            
        return get_columns_from_query_data(
            get_query_data(res),
            "view_definition"
        )[0]
    
class AgentViewModel:
    
    def __init__(self, conn):
        self.conn = conn
        
    def get_views(self):
        return DataViewModel(self.conn).get_views()
    
    def get_columns(self, view_name):
        return get_columns_from_query_data(get_query_data(
            self.conn.execute(f"pragma table_info({view_name})")
        ), ["name"])
    