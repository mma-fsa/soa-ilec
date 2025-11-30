import json
import os
import re
import copy
import sqlglot
from datetime import date
from pathlib import Path
from collections import defaultdict, deque
from jinja2 import Environment, FileSystemLoader, select_autoescape
from env_vars import COMMON_TEMPLATE_DIR
from urllib.parse import quote
from abc import ABC, abstractmethod

PTR_PATTERN = re.compile(r'^"([A-Za-z0-9-]+)"->"([A-Za-z0-9-]+)"$')

class AuditLogReader:

    NODE_TYPE_ROOT = 1
    NODE_TYPE_CHILD = 2
    MAX_DEPTH = 1000

    def __init__(self, work_dir):
        self.work_dir = Path(str(work_dir))

    def traverse_model_audit_log(self, final_workspace_id=None):

        if final_workspace_id is None:
            final_ws_data = self.work_dir / "final.json"
            with open(final_ws_data, "r") as fh:
                ws_data = json.load(fh)
                final_workspace_id = ws_data["workspace_id"]
        
        final_path = self._traverse_branch(final_workspace_id)
        full_tree, all_entries = self._traverse_tree(final_path["workspace_id"])
        full_by_time = self._traverse_mcp_calls_by_time(all_entries)

        return (final_path, full_tree, full_by_time)

    def _traverse_mcp_calls_by_time(self, all_entries):
        
        by_time_log = []
        
        workspace_dirs = self._scan_dirs_w_ts(self.work_dir)
        workspace_dirs.sort(key=lambda x: x[0])

        for _, dir_name in workspace_dirs:
            _, workspace_id = Path(dir_name).name.split(r'_')
            if workspace_id in all_entries:
                by_time_log.append(all_entries[workspace_id])
            
        return by_time_log

    def traverse_sql_audit_log(self):
        
        base_dir = self.work_dir / "sql_run"
        log_files = self._scan_files_w_ts(base_dir, ".json")            
    
        # show queries in order of execution
        log_files.sort(key=lambda x: x[0])

        # parse query json
        sql_log_entries = []
        for _, p in log_files:
            log_file_path = Path(p)
            with open(str(log_file_path.resolve()), "r") as fh:
                sql_log_entries.append(
                    json.load(fh)
                )

        return sql_log_entries

    def _traverse_tree(self, root_workspace_id):

        root_node_type = self._get_node_type(root_workspace_id)
        if root_node_type != AuditLogReader.NODE_TYPE_ROOT:
            raise Exception(f"{root_workspace_id} is not a root node")
                
        adj_mat = defaultdict(lambda: [])
        all_entries = {}
        
        parent_node = None
        child_node = None

        # build the adjacency matrix
        for path in self.work_dir.rglob("workspace_pointer.txt"):
            ptr_path = path.resolve()
            with open(ptr_path, "r") as fh:
                ptr_data = fh.read().strip()
                is_child_node = (ptr_data != "root")
                if is_child_node:                    
                    m = PTR_PATTERN.search(ptr_data)
                    if not m:
                        raise Exception(f"Invalid workspace_pointer.txt: {ptr_path}")                
                    parent_node, child_node = m.groups()
                    adj_mat[parent_node].append(child_node)
        
        # build the full audit tree        
        nodes_todo = deque([root_workspace_id])
        node_data = {}

        # setup the root entry
        root_entry = self.__init_entry(
            workspace_id=root_workspace_id,
            node_type=AuditLogReader.NODE_TYPE_ROOT
        )

        # initialize the traversal                
        node_data[root_workspace_id] = root_entry
        all_entries[root_workspace_id] = copy.deepcopy(root_entry)
        del all_entries[root_workspace_id]["next"]

        # run a BFS
        while len(nodes_todo) > 0:
            
            curr_sess_id = nodes_todo.popleft()
            curr_traversal = node_data[curr_sess_id]
            
            for child_sess_id in adj_mat[curr_sess_id]:     
                nodes_todo.append(child_sess_id)
                child_data = self.__init_entry(
                    workspace_id=child_sess_id,
                    node_type=AuditLogReader.NODE_TYPE_CHILD
                )
                all_entries[child_sess_id] = copy.deepcopy(child_data)
                del all_entries[child_sess_id]["next"]

                node_data[child_sess_id] = child_data
                curr_traversal["next"].append(child_data)
                
        
        return (node_data[root_workspace_id], all_entries)
    
    def __init_entry(self, workspace_id=None, node_type=None, entry=None):            
        if workspace_id is not None:
            if node_type is None:
                node_type = self._get_node_type(workspace_id)
            if node_type != AuditLogReader.NODE_TYPE_ROOT and entry is None:
                entry = self._get_node_log(workspace_id)            

        return {
            "workspace_id" : workspace_id,
            "type" : node_type,
            "next" : [],
            "entry" : entry
        }

    def _traverse_branch(self, leaf_workspace_id):        
        
        # init traversal vars
        curr_node_type = 0
        curr_workspace_id = leaf_workspace_id
        traversal = {}
        tot_depth = 0 
        
        # traverse log backwards to root
        while (tot_depth < AuditLogReader.MAX_DEPTH) and curr_node_type != AuditLogReader.NODE_TYPE_ROOT:
        
            curr_node_type = self._get_node_type(curr_workspace_id)
            is_child = curr_node_type == AuditLogReader.NODE_TYPE_CHILD
        
            traversal = {                                
                "type" : curr_node_type,
                "next" : None if tot_depth == 0 else traversal,
                "entry" : self._get_node_log(curr_workspace_id) if is_child else None,
                "workspace_id" : curr_workspace_id
            }

            if is_child:
                curr_workspace_id = traversal["entry"]["last_workspace_id"]
            
            tot_depth += 1
        
        if tot_depth >= AuditLogReader.MAX_DEPTH:
            raise Exception("Max depth reached while scanning for root")
        
        return traversal
    
    def _get_node_type(self, workspace_id):
        node_ptr_file = self.work_dir / Path(f"workspace_{workspace_id}") / Path("workspace_pointer.txt")
        with open(node_ptr_file, "r") as fh:
            return AuditLogReader.NODE_TYPE_ROOT if fh.read().strip() == "root"\
                    else AuditLogReader.NODE_TYPE_CHILD

    def _get_node_log(self, workspace_id):
        ws_root = self.work_dir / Path(f"workspace_{workspace_id}")
        log_entry_file = ws_root / Path("tool_call.json")
        ws_ptr_file = ws_root / Path("workspace_pointer.txt")
        tool_call = None
        # check if tool_call.json exists, otherwise create a 
        # stand-in from the workspace_pointer.txt
        if log_entry_file.exists():
            with open(log_entry_file, "r") as fh:
                tool_call = json.load(fh)
        elif ws_ptr_file.exists():
            last_ws_id = None
            this_ws_id = None
            with open(ws_ptr_file, "r") as fh:
                ptr_data = fh.read().strip()
                m = PTR_PATTERN.search(ptr_data)
                if not m:
                    raise Exception(f"Invalid workspace_pointer.txt: {ws_root}")
                last_ws_id, this_ws_id = m.groups()            
            tool_call = AuditLogEntry._create_log_entry(
                last_ws_id,
                this_ws_id,
                "unknown",
                [],
                {}
            )
        else:
            raise Exception("No tool_call or workspace pointer in {}")

        return tool_call
    
    @staticmethod
    def _scan_files_w_ts(base_dir, file_ext=".json"):
        log_files = []
        with os.scandir(base_dir) as it:
            for de in it:
                if not de.is_file():
                    continue
                name = de.name
                if not name.endswith(file_ext):
                    continue
                try:
                    st = de.stat()
                except FileNotFoundError:                    
                    continue
                # Use the creation time for ordering
                t_ns = getattr(st, "st_ctime_ns", int(st.st_ctime))
                log_files.append((t_ns, Path(de.path).resolve()))
        return log_files

    @staticmethod
    def _scan_dirs_w_ts(base_dir, prefix="workspace_"):
        workspace_dirs = []
        with os.scandir(base_dir) as it:
            for de in it:
                if not de.is_dir():
                    continue
                name = de.name
                if not name.startswith(prefix):
                    continue
                try:
                    st = de.stat()
                except FileNotFoundError:
                    continue
                # Use the creation time for ordering
                t_ns = getattr(st, "st_ctime_ns", int(st.st_ctime))
                workspace_dirs.append((t_ns, Path(de.path).resolve()))
        return workspace_dirs


class AuditLogEntry:

    def __init__(self, r_env):        
        self.last_workspace_id = r_env.last_workspace_id \
            if r_env.last_workspace_id is not None else ""        
        self.workspace_id = r_env.workspace_id
        self.audit_log_dir = r_env.this_workspace_id

    def log_tool_call(self, tool_name, args, result):        
        audit_log_entry = self._create_log_entry(
            self.last_workspace_id,
            self.workspace_id,
            tool_name,
            args,
            result
        )
        with open(self.audit_log_dir / "tool_call.json", "w") as fh:
            json.dump(audit_log_entry, fh, indent=2)
    
    @staticmethod
    def _create_log_entry(last_workspace_id, workspace_id, tool_name, args, result):
        return {
            "last_workspace_id": last_workspace_id,
            "workspace_id": workspace_id,
            "tool_name" : tool_name,
            "args" : args,
            "result" : result
        }


class AbstractRenderer(ABC):
    
    def __init__(self, mcp_work_dir):
        self.mcp_work_dir = Path(mcp_work_dir)

        if not self.mcp_work_dir.exists():
            raise FileNotFoundError(f"{self.mcp_work_dir} does not exist")

        self.agent_name = self.mcp_work_dir.name
        self.audit_log_data = self.mcp_work_dir / "final.json"

        if not self.audit_log_data.exists():
            raise FileNotFoundError(
                f"final.json does not exist in {self.mcp_work_dir}"
            )
        
    @abstractmethod
    def render(self):
        pass
        
    @staticmethod
    def _pretty_sql(raw_sql):
        sql = sqlglot.transpile(raw_sql, read="duckdb", write="duckdb", pretty=True)[0]
        return "\n".join(list(map(lambda l: "# " + l, sql.split("\n")))) 
    
class AuditLogRenderer(AbstractRenderer):

    def __init__(self, mcp_work_dir):
        super().__init__(mcp_work_dir)

    def render(self):

        reader = AuditLogReader(self.mcp_work_dir)
        
        sql_log = reader.traverse_sql_audit_log()
        final_cmd_tree, _, full_cmd_log_by_time = reader.traverse_model_audit_log()

        bfs_work = [final_cmd_tree]
        final_cmd_log = []
        while len(bfs_work) > 0:
            curr_node = bfs_work.pop(0)
            final_cmd_log.append(curr_node)
            if curr_node["next"] is not None:
                bfs_work.append(curr_node["next"])

        env = Environment(
            loader=FileSystemLoader(COMMON_TEMPLATE_DIR),
            autoescape=select_autoescape(["html", "xml"])
        )
        
        env.filters["pretty_sql"] = lambda x: self._pretty_sql(x) if x else ""
        
        audit_template = env.get_template("audit_log.html")        
        safe_agent_name = quote(self.agent_name, safe="")

        return audit_template.render(
            agent_name = safe_agent_name,
            sql_log = sql_log,
            cmd_log = [
                ("Final Model Audit Log", final_cmd_log),
                ("All Modeling Commands", full_cmd_log_by_time)
            ]            
        )

class ModelNotebookRenderer(AbstractRenderer):

    def __init__(self, mcp_work_dir):
        super().__init__(mcp_work_dir)

    def _prepare_template_data(self):
        
        with open(self.audit_log_data, "r") as fh:
            audit_log_data = json.load(fh)
        
        final_model_data = audit_log_data["final_model_log"]
        curr_node = final_model_data

        # rparts before glmnet() are var_imp, after are model_validation
        is_var_imp = True
        datasets = []
        var_imp = []
        cmd_model = None
        model_validation = []
        model_inference = []

        while curr_node is not None and "next" in curr_node:
            
            is_child_node = (
                curr_node["type"] == AuditLogReader.NODE_TYPE_CHILD
            )
            
            if is_child_node and "entry" in curr_node:
                node_entry = curr_node["entry"]
                cmd_name = node_entry["tool_name"] 
                node_args = node_entry["args"]
                if cmd_name == "cmd_create_dataset":
                    cmd = {
                        "name": node_args[0], 
                        "sql" : self._pretty_sql(node_args[1])
                    }
                    datasets.append(cmd)
                elif cmd_name == "cmd_rpart":
                    cmd = {
                        "dataset" : node_args[0],
                        "x_vars" : node_args[1],
                        "offset_var" : node_args[2],
                        "y_var" : node_args[3],
                        "max_depth" : node_args[4],
                        "cp" : node_args[5]
                    }
                    if is_var_imp:
                        var_imp.append(cmd)
                    else:
                        model_validation.append(cmd)
                elif cmd_name == "cmd_run_inference":
                    cmd = {
                        "in_dataset" : node_args[0],
                        "out_dataset" : node_args[1]
                    }
                    model_inference.append(cmd)
                elif cmd_name == "cmd_glmnet":
                    is_var_imp = False
                    cmd_model = {
                        "dataset": node_args[0],
                        "x_vars" : node_args[1],
                        "design_matrix_vars" : node_args[2],
                        "factor_vars_levels": node_args[3],
                        "num_var_clip" : node_args[4],
                        "offset_var" : node_args[5],
                        "y_var" : node_args[6],
                        "lambda_strat" : node_args[7]
                    }
            
            curr_node = curr_node["next"]

        doc = {
            "title": "Model Output",
            "date" : date.today().strftime("%Y-%m-%d")
        }

        cmds = {
            "datasets": datasets,
            "cmd_model" : cmd_model,
            "var_imp" : var_imp,
            "model_validation" : model_validation,
            "model_inference" : model_inference
        }

        return doc, cmds

    def render(self):
        
        env = Environment(
            loader=FileSystemLoader(COMMON_TEMPLATE_DIR),
            autoescape=select_autoescape()
        )
        
        template = env.get_template("model_output.Rmd")

        doc, cmds = self._prepare_template_data()

        tmpl_out = template.render(
            doc = doc,
            cmds = cmds
        )
        
        return tmpl_out
