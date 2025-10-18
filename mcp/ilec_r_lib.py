from typing import Callable, Iterable
from multiprocessing import Process, Queue

from rpy2.robjects import r, globalenv
from rpy2.robjects.packages import importr

from pathlib import Path

import logging
import uuid
import traceback
import subprocess

logging.basicConfig(level=logging.INFO)

AGENT_R_LIB = "/home/mike/workspace/soa-ilec/soa-ilec/mcp/ilec_r_lib.py"

class ILECREnvironment:

    def __init__(self, work_dir : str, parquet_path : str, db_pragmas : Iterable[str] = None, last_session_guid : str = None):
    
        self.parquet_path = parquet_path
        self.db_pragmas = [] if db_pragmas is None else db_pragmas
        
        # load the last session if applicable
        if last_session_guid is not None and len(last_session_guid) > 0:
            self.last_session_guid = last_session_guid
        else:
            self.last_session_guid = None

        # where to save / load sesions
        self.work_dir = Path(work_dir)
        if not self.work_dir.exists():
            self.work_dir.mkdir(parents=True, exist_ok=True)
        
        # create a session id for this object
        self.session_guid = str(uuid.uuid4())
        self.log = logging.getLogger(__name__)

        # these should get set by enter()
        self.rduckdb = None
        self.rDBI = None
        self.rconn = None        

        self.run_setup = True
        self.disposed = False
    
    def __enter__(self):
        
        self.log.info("setting up environment")
        
        if self.disposed:
            raise Exception("Environment is already disposed.")

        if self.run_setup:

            # init packages
            self.rduckdb = importr("duckdb")
            self.rDBI = importr("DBI")
            
            # setup database connection
            self.rconn = self.rDBI.dbConnect(
                self.rduckdb.duckdb(), ":memory:")

            # run pragmas
            for sql in self.db_pragmas:
                self.rDBI.dbExecute(self.rconn, sql)
            
            # create the session directory
            this_session_path = self.work_dir / f"session_{self.session_guid}/"
            this_session_path.mkdir(parents=True, exist_ok=False)

            # copy the previous working directory
            if self.last_session_guid is not None:
                last_session_path = self.work_dir / f"session_{self.last_session_guid}/"
                if (last_session_path.exists() and last_session_path.is_dir()):
                    self.log.info(f"rsync {last_session_path}->{this_session_path}")
                    subprocess.run([
                        "rsync", "-a", str(last_session_path), str(this_session_path)],
                        check=True)
                else:
                    msg = f"invalid last session guid: {last_session_path}"
                    self.log.error(msg)
                    raise FileNotFoundError(msg)
            
            # set the working directory
            r.setwd(str(this_session_path))


    def __exit__(self, exc_type, exc_val, exc_tb):
        
        if exc_type is not None:            
            nice_tb = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
            self.log.error(nice_tb)            
        
        self.log.info("tearing down environment")
        self.disposed = True
        
        session_path = self.work_dir / f"session_{self.session_guid}.RData"

        self.log.debug(f"saving RData: '{session_path}'...")

        try:
            r["save.image"](session_path)
        except Exception as e:
            tb_str = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            self.log.error("Exception occurred:\n%s", tb_str)
            raise



class AgentRCommands:    

    @staticmethod
    def run_command(target:Callable, args: Iterable, r_env: ILECREnvironment):        

        # fork() entry point
        def run_target(args):            
            log = logging.getLogger(__name__)
            arg_list = list(args)
            q = arg_list.pop()
            try:
                with r_env as env:
                    r.source(AGENT_R_LIB)
                    q.put({
                        "session_guid" : env.session_guid,
                        "result" : target(*arg_list)
                    })
            except Exception as e:
                tb_str = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                log.error("Exception occurred:\n%s", tb_str)
                q.put(None)    
        
        # easiest to run R in a separate process, avoid async headaches
        # with multiple threads / interleaved calls to server, since R
        # intepreter is one instance per Python process
        q = Queue()
        p = Process(target = run_target, args=list(args) + [q])
        p.start()
        res = q.get()
        p.join()
         
        return res
    
    @staticmethod()
    def cmd_rpart(where_clause: str, x_vars: Iterable[str], offset: str, y_var: str, max_depth : int, cp : float):
        return r.cmd_rpart(
            where_clause,
            x_vars,
            offset,
            y_var,
            max_depth
        )