import json
import re
from pathlib import Path
from collections import defaultdict, deque

class AuditLogReader:

    NODE_TYPE_ROOT = 1
    NODE_TYPE_CHILD = 2
    MAX_DEPTH = 1000

    def __init__(self, work_dir):
        self.work_dir = Path(str(work_dir))

    def traverse_audit_log(self, final_workspace_id):
        
        final_path = self._traverse_final(final_workspace_id)
        full_tree = self._traverse_full(final_path["workspace_id"])

        return (final_path, full_tree)

    def _traverse_tree(self, root_workspace_id):

        root_node_type = self._get_node_type(root_workspace_id)
        if root_node_type != AuditLogReader.NODE_TYPE_ROOT:
            raise Exception(f"{root_workspace_id} is not a root node")
                
        adj_mat = defaultdict(lambda: [])
        parent_node = None
        child_node = None

        ptr_pattern = re.compile(r'^"([A-Za-z0-9-]+)"->"([A-Za-z0-9-]+)"$')

        # build the adjacency matrix
        for path in self.work_dir.rglob("workspace_pointer.txt"):
            ptr_path = path.resolve()
            with open(ptr_path, "r") as fh:
                ptr_data = fh.read().strip()
                is_child_node = (ptr_data != "root")
                if is_child_node:                    
                    m = ptr_pattern.search(ptr_data)
                    if not m:
                        raise Exception(f"Invalid workspace_pointer.txt: {ptr_path}")                
                    parent_node, child_node = m.groups()
                    adj_mat[parent_node].append(child_node)
        
        # build the full audit tree        
        nodes_todo = deque([root_workspace_id])
        node_data = {}

        # initialize the traversal                
        node_data[root_workspace_id] = self.__init_entry(
            workspace_id=root_workspace_id,
            node_type=AuditLogReader.NODE_TYPE_ROOT
        )

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
                node_data[child_sess_id] = child_data
                curr_traversal["next"].append(child_data)
        
        return node_data[root_workspace_id]
    
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
        log_entry_file = self.work_dir / Path(f"workspace_{workspace_id}") / Path("tool_call.json")
        with open(log_entry_file, "r") as fh:
            return json.load(fh)

class AuditLogEntry:

    def __init__(self, r_env):        
        self.last_workspace_id = r_env.last_workspace_id \
            if r_env.last_workspace_id is not None else ""        
        self.workspace_id = r_env.workspace_id
        self.audit_log_dir = r_env.this_workspace_id

    def log_tool_call(self, tool_name, args, result):        
        audit_log_entry = {
            "last_workspace_id": self.last_workspace_id,
            "workspace_id": self.workspace_id,
            "tool_name" : tool_name,
            "args" : args,
            "result" : result
        }
        with open(self.audit_log_dir / "tool_call.json", "w") as fh:
            json.dump(audit_log_entry, fh, indent=2)
    
