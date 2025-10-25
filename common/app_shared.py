import threading, sqlite3, duckdb, decimal, datetime

from pathlib import Path
from env_vars import DEFAULT_SESSION_SETTINGS, DEFAULT_DDB_WORKERS,\
    DEFAULT_DDB_MEM, DEFAULT_DDB_PATH, DEFAULT_DATA_DIR,\
    DEFAULT_SESS_DB_INIT_SQL
from collections import defaultdict

class DatabaseConnectionWrapper:
    def __init__(self, con):
        self.con = con    
    def __enter__(self):
        return self.con
    def __exit__(self, exc_type, exc_value, traceback):
        self.con.close()

class Database:

    DDB_PRAGMAS = [
        "PRAGMA disable_progress_bar",
        f"PRAGMA memory_limit='{DEFAULT_DDB_MEM}'"
    ]

    @staticmethod
    def get_session_conn():        
        conn = sqlite3.connect(DEFAULT_DATA_DIR / "settings.db", check_same_thread=True)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(DEFAULT_SESS_DB_INIT_SQL)
        return DatabaseConnectionWrapper(conn)        

    @staticmethod
    def get_duckdb_conn(read_only = True):

        duckdb_conn = duckdb.connect(
            database = DEFAULT_DDB_PATH,
            read_only=read_only,
            config={"threads": DEFAULT_DDB_WORKERS})

        for sql in Database.DDB_PRAGMAS:
            duckdb_conn.execute(sql)
        
        return DatabaseConnectionWrapper(duckdb_conn)
    
    @staticmethod
    def ddb_to_json_safe(rows):    
        """Convert any DuckDB result rows into JSON-serializable Python types."""
        safe_rows = []
        for row in rows:
            safe_row = []
            for v in row:
                if isinstance(v, decimal.Decimal):                    
                    safe_row.append(float(v))
                elif isinstance(v, (datetime.date, datetime.datetime)):
                    safe_row.append(v.isoformat())
                else:
                    safe_row.append(v)
            safe_rows.append(safe_row)
        return safe_rows

class AppSession:

    def __init__(self, conn):
        self.conn = conn        
        
    def _get_data(self):

        settings = defaultdict(lambda: None, DEFAULT_SESSION_SETTINGS)
        
        cur = self.conn.cursor()

        cur.execute("SELECT key, value FROM settings")
        for k,v in cur.fetchall():
            if not k in DEFAULT_SESSION_SETTINGS.keys():
                raise Exception(f"unrecognized session key: {k}")
            settings[k] = v

        return settings        
    
    def _set_data(self, key, value):
        
        if not key in DEFAULT_SESSION_SETTINGS.keys():
            raise Exception(f"Cannot set key '{key}', no default value")

        self.conn.execute("""
            INSERT INTO settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value
        """, (key, value))
        self.conn.commit()

    def _del_data(self, key):
        self.conn.execute("""
            DELETE FROM settings
            where key = ?
        """, (key))
        self.conn.commit()

    def __getitem__(self, key):
        data = self._get_data()
        try:
            return data[key]
        except KeyError:
            raise KeyError(f"{key!r} not found")

    def __setitem__(self, key, value):
        self._set_data(self, key, value)
    
    def __delitem__(self, key):
        self._del_data(key)

    def __contains__(self, key):
        return key in self._get_data()

    def __repr__(self):
        data = self._get_data()
        return f"{type(self).__name__}({data!r})"