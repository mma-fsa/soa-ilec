import asyncio
from fastmcp import Client

async def call_mcp_tool():    
    async with Client("http://localhost:9090/mcp") as client:
        try:
            # Call the 'add' tool with specific parameters
            tool_name = "cmd_finalize"
            params = {
                "workspace_id" : "5dfed9ed-26a4-4cd9-9fb1-80f22e35988b"
            }
            
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