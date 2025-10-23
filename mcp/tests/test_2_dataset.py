import asyncio
from fastmcp import Client

async def call_mcp_tool():    
    async with Client("http://localhost:9090/mcp") as client:
        try:
            # Call the 'add' tool with specific parameters
            tool_name = "cmd_create_dataset"
            params = {
                "session_id" : "3d6c3312-ff87-4294-851d-01465bb94959",
                "dataset_name": "ul_train_data", 
                "sql": "select * from ILEC_DATA where Insurance_Plan = 'UL' and coalesce(Expected_Death_QX2015VBT_by_Policy, 0) > 0 and Smoker_Status in ('Smoker', 'NonSmoker')"}
            
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