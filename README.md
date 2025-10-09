
## agent_context_tool.py

### dry run

```
python agent_context_tool.py instructions.txt -i notes.md data.xlsx \
  --mcp-url http://127.0.0.1:8000/mcp --mode agents --budget-tokens 120000 --reserve-output 4000 --plan-only
```

### normal run (agents + local MCP)

```
python agent_context_tool.py instructions.txt -i notes.md \
  --mcp-url http://127.0.0.1:8000/mcp --mode agents --stream
```