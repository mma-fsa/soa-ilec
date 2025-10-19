from typing import Callable, Iterable, List, Any
from multiprocessing import Process, Queue

from rpy2.robjects import r, globalenv
from rpy2.robjects.vectors import StrVector, FloatVector
from rpy2.robjects import ListVector
from rpy2.robjects.packages import importr

from pathlib import Path

import logging
import uuid
import traceback
import subprocess

logging.basicConfig(level=logging.INFO)

AGENT_R_LIB = "/home/mike/workspace/soa-ilec/soa-ilec/mcp/ilec_r_lib.R"

class ILECREnvironment:

    def __init__(self, work_dir : str, parquet_path : str, db_pragmas : Iterable[str] = None, last_session_guid : str = None, no_cmd=False):
    
        self.parquet_path = parquet_path
        self.db_pragmas = [] if db_pragmas is None else db_pragmas
        
        self.no_cmd = no_cmd

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
            
            # create the session directory
            this_session_path = self.work_dir / f"session_{self.session_guid}/"
            this_session_path.mkdir(parents=True, exist_ok=False)

            # if this is the initial session, do nothing
            if self.no_cmd:
                return

            # init packages
            self.rduckdb = importr("duckdb")
            self.rDBI = importr("DBI")
            
            # setup database connection
            self.rconn = self.rDBI.dbConnect(
                self.rduckdb.duckdb(), ":memory:")

            # run pragmas
            for sql in self.db_pragmas:
                self.rDBI.dbExecute(self.rconn, sql)
            
            # copy the previous working directory
            if self.last_session_guid is not None:
                last_session_path = self.work_dir / f"session_{self.last_session_guid}/"
                if (last_session_path.exists() and last_session_path.is_dir()):
                    self.log.info(f"rsync {last_session_path}->{this_session_path}")
                    subprocess.run([
                        "rsync", "-a", f"{last_session_path}/", f"{this_session_path}/"],
                        check=True)
                    
                else:
                    msg = f"invalid last session guid: {self.last_session_guid}"
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

# fork() entry point
def run_target(*args):            
    log = logging.getLogger(__name__)
    arg_list = list(args)
    target, r_env, q = arg_list[-3:]        
    try:
        with r_env:
            r.source(AGENT_R_LIB)
            arg_list = [r_env.rconn] + arg_list
            q.put({
                "success": True,
                "result": target(*arg_list[:-3])
            })
    except Exception as e:
        tb_str = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        log.error("Exception occurred:\n%s", tb_str)
        q.put({
            "success" : False,
            "message" : str(e)
        })


class AgentRCommands:    

    @staticmethod
    def run_command(target:Callable, args: Iterable, r_env: ILECREnvironment):        
        # easiest to run R in a separate process, avoid async headaches
        # with multiple threads / interleaved calls to server, since R
        # intepreter is one instance per Python process
        q = Queue()
        p = Process(target = run_target, args=list(args) + [target, r_env, q])
        p.start()
        res = q.get()
        p.join()
         
        return res
    
    @staticmethod
    def cmd_create_dataset(conn, dataset_name:str, sql:str):
        res = r.cmd_create_dataset(
            conn,
            dataset_name,
            sql
        )        
        results = {}
        for k, v in res.items():
            results[k] = v[0]
        return results

    
    @staticmethod
    def cmd_run_inference(conn, dataset_in : str, dataset_out : str):
        success = r.cmd_run_inference(
            conn,
            dataset_in,
            dataset_out
        )
        return f"created dataset {dataset_out} with predictions contained in MODEL_PRED column."

    @staticmethod
    def cmd_rpart(conn, dataset: str, x_vars: List[str], offset: str, y_var: str, max_depth : int, cp : float):
        res = r.cmd_rpart(
            conn, 
            dataset,
            StrVector(x_vars), 
            offset,
            y_var, 
            max_depth, 
            cp
        )
        return "\n".join(r["capture.output"](r.print(res)))
    
    @staticmethod
    def cmd_glmnet(conn, dataset : str, x_vars : List[str], design_matrix_vars : List[str], \
              factor_vars_levels: dict, num_var_clip : dict, offset_var : str, y_var : str, lambda_strat : str):                
        
        r_num_var_clip = ListVector({k: FloatVector(v) for k, v in num_var_clip.items()})

        res = r.cmd_glmnet(
            conn, 
            dataset,
            StrVector(x_vars), 
            StrVector(design_matrix_vars),
            ListVector(factor_vars_levels),
            r_num_var_clip,
            offset_var, 
            y_var, 
            lambda_strat
        )

        return {
            "rpart_train" : "\n".join(r["capture.output"](r.print(res[0]))),
            "ae_train" : float(res[1][0])
        }
    
