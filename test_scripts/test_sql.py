import asyncio
from fastmcp import Client

async def call_mcp_tool():    
    async with Client("http://localhost:9090/mcp") as client:
        try:
            # Call the 'add' tool with specific parameters
            tool_name = "sql_run"
            params = {"desc" : "test query", "sql": "select * from ILEC_DATA limit 1"}
            print(f"Calling tool '{tool_name}' with parameters: {params}")

            # Use the call_tool() method to execute the tool
            result = await client.call_tool(tool_name, params)

            # The result is a streamable object, so you can access its content
            sum_result = result.content[0].text
            print(f"Result from the MCP tool: {sum_result}")

        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(call_mcp_tool())