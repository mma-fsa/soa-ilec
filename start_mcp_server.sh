#!/bin/bash
PYTHONPATH=./common uvicorn --app-dir ./mcp ilec_mcp_server:app --host 127.0.0.1 --port 9090
