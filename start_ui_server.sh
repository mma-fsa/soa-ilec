#!/bin/bash
PYTHONPATH=./common uvicorn --app-dir ./app_ui app_ui:app --host 0.0.0.0 --port 8085
