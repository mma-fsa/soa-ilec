from typing import Callable, Iterable, List, Any
from multiprocessing import Process, Queue

from pathlib import Path

import logging
import uuid
import traceback
import subprocess
import os
import sys

from audit import AuditLogEntry
from env_vars import AGENT_R_LIB, DEFAULT_DDB_PATH, EXPORT_R_LIB, DEFAULT_AGENT_WORK_DIR, R_TMP_DIR

Path(R_TMP_DIR).mkdir(exist_ok=True, parents=True)
os.environ["TMPDIR"] = str(Path(R_TMP_DIR))

logging.basicConfig(level=logging.INFO)

class ILECREnvironment:

    def __init__(self, work_dir : str, db_pragmas : Iterable[str] = None, last_workspace_id : str = None, no_cmd=False):
            
        self.db_pragmas = [] if db_pragmas is None else db_pragmas
        
        self.no_cmd = no_cmd

        # load the last session if applicable
        if last_workspace_id is not None and len(last_workspace_id) > 0:
            self.last_workspace_id = last_workspace_id
        else:
            self.last_workspace_id = None

        # where to save / load sesions
        self.work_dir = Path(work_dir)
        if not self.work_dir.exists():
            self.work_dir.mkdir(parents=True, exist_ok=True)
        
        # create a session id for this object
        self.workspace_id = str(uuid.uuid4())
        self.log = logging.getLogger(__name__)
        self.this_workspace_id = self.work_dir / f"workspace_{self.workspace_id}/"

        # rpy2 complications
        self.rpy2_env = None

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
            this_workspace_id = self.this_workspace_id
            this_workspace_id.mkdir(parents=True, exist_ok=False)

            # create session pointer file
            with open(this_workspace_id / "workspace_pointer.txt", "w") as fh:
                pointer_desc = "root" if self.last_workspace_id is None \
                    else f"\"{self.last_workspace_id}\"->\"{self.workspace_id}\""
                fh.write(pointer_desc)                

            # if this is the initial session, do nothing
            if self.no_cmd:
                return
            
            # these are only inited when we fork()
            importr = self.rpy2_env["importr"]
            r = self.rpy2_env["r"]

            # init packages
            self.rduckdb = importr("duckdb")
            self.rDBI = importr("DBI")
            
            # setup database connection
            self.rconn = self.rDBI.dbConnect(
                self.rduckdb.duckdb(dbdir=str(DEFAULT_DDB_PATH), read_only=True))

            # run pragmas
            for sql in self.db_pragmas:
                self.rDBI.dbExecute(self.rconn, sql)
            
            # copy the previous working directory
            if self.last_workspace_id is not None:
                last_workspace_path = self.work_dir / f"workspace_{self.last_workspace_id}/"
                if (last_workspace_path.exists() and last_workspace_path.is_dir()):
                    self.log.info(f"rsync {last_workspace_path}->{this_workspace_id}")
                    
                    # first pass, symlink parquet files
                    symlink_files = list(last_workspace_path.rglob("*.parquet")) + \
                        list(last_workspace_path.rglob("*.rds"))

                    for f in symlink_files:
                        rel = f.relative_to(last_workspace_path)
                        link_path = this_workspace_id / rel
                        link_path.parent.mkdir(parents=True, exist_ok=True)
                        if link_path.exists():
                            link_path.unlink()
                        os.symlink(f, link_path)
                    
                    for f in last_workspace_path.rglob("*.parquet"):
                        rel = f.relative_to(last_workspace_path)
                        link_path = this_workspace_id / rel
                        link_path.parent.mkdir(parents=True, exist_ok=True)
                        if link_path.exists():
                            link_path.unlink()
                        os.symlink(f, link_path)

                    # second pass, copy files
                    subprocess.run([
                        "rsync", "-a",                        
                        "--exclude=*.parquet", # symlinked, not copied
                        "--exclude=*.rds", # symlinked, not copied
                        "--exclude=*.json", # not copied
                        "--exclude=*.png", # not copied
                        "--exclude=workspace_pointer.txt", # not copied
                        f"{last_workspace_path}/", f"{this_workspace_id}/"],
                        check=True)
                    
                else:
                    msg = f"invalid last session guid: {self.last_workspace_id}"
                    self.log.error(msg)
                    raise FileNotFoundError(msg)
            
            # set the working directory
            r.setwd(str(this_workspace_id))


    def __exit__(self, exc_type, exc_val, exc_tb):                
        if exc_type is not None:            
            nice_tb = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
            self.log.error(nice_tb)            
        
        self.log.info("tearing down environment")
        self.disposed = True    

# fork() entry point
def run_target(*args):                    
        
    res = None    
    log = logging.getLogger(__name__)
    arg_list = list(args)

    if (len(args) >= 3):
        target, r_env, q = arg_list[-3:]
        target_args = arg_list[:-3]
    else:
        err_msg = "Error parsing run_target args"
        log.error(err_msg)
        raise Exception(err_msg)

    try:                
        audit = AuditLogEntry(r_env)
        tool_name = target.__name__

        # --------------------
        # RPy2 fork() complications,
        # basically we don't want to fork() with a running R process already started.
        from rpy2.robjects import r as _r, globalenv as _globalenv
        from rpy2.robjects.vectors import StrVector as _StrVector, FloatVector as _FloatVector
        from rpy2.robjects import ListVector as _ListVector
        from rpy2.robjects.packages import importr as _importr
        from rpy2.rinterface_lib.embedded import endr as _endr
        
        rpy2_env = {
            "r" : _r,
            "globalenv" : _globalenv,
            "StrVector" : _StrVector,
            "FloatVector" : _FloatVector,
            "ListVector" : _ListVector,
            "importr"  : _importr,
            "endr" : _endr
        }
        # --------------------

        r_env.rpy2_env = rpy2_env
        with r_env:                        
            _r.source(AGENT_R_LIB)
            _r.source(EXPORT_R_LIB)
            R_arg_list = [rpy2_env, r_env.rconn] + target_args
            # call tool
            res = {
                "success": True,
                "result": target(*R_arg_list)
            }
        audit.log_tool_call(tool_name, target_args, res)
    
    except Exception as e:
        tb_str = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        log.error(f"Exception occurred in tool call: {tool_name}\n{tb_str}",  )        
        res = {
            "success" : False,
            "message" : str(e)
        }
        audit.log_tool_call(tool_name, target_args, res)
    
    finally:        
        res = res if res is not None else {"success": False, "message": "unknown error"}
        q.put(res)
        _endr(0)
        sys.exit(0)


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
    def cmd_create_dataset(rpy2_env, conn, dataset_name:str, sql:str):
        r = rpy2_env["r"]        
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
    def cmd_run_inference(rpy2_env, conn, dataset_in : str, dataset_out : str):
        
        r = rpy2_env["r"]
        
        success = r.cmd_run_inference(
            conn,
            dataset_in,
            dataset_out
        )
        return f"created dataset {dataset_out} with predictions contained in MODEL_PRED column."

    @staticmethod
    def cmd_rpart(rpy2_env, conn, dataset: str, x_vars: List[str], offset: str, y_var: str, max_depth : int, cp : float):
        
        r = rpy2_env["r"]        
        StrVector = rpy2_env["StrVector"]
        
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
    def cmd_glmnet(rpy2_env, conn, dataset : str, x_vars : List[str], design_matrix_vars : List[str], \
              factor_vars_levels: dict, num_var_clip : dict, offset_var : str, y_var : str, lambda_strat : str):                
        
        r = rpy2_env["r"]
        ListVector = rpy2_env["ListVector"]
        FloatVector = rpy2_env["FloatVector"]
        StrVector = rpy2_env["StrVector"]

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
    
    @staticmethod
    def cmd_export_model(rpy2_env, conn, model_rds_path : str, export_location : str):        
        r = rpy2_env["r"]        
        r.export_factors(
            str(model_rds_path),
            str(export_location)
        )

        return export_location
