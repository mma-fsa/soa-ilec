import asyncio
from fastmcp import Client


async def call_mcp_tool():    
    async with Client("http://localhost:9090/mcp") as client:
        try:
            # Call the 'add' tool with specific parameters
            tool_name = "cmd_rpart"
            params = {
                "workspace_id" : "488d8c35-8f1a-4e08-b28c-97f68e1e7bed",
                "dataset": "ul_train_data", 
                "x_vars": ["Gender", "Attained_Age", "Smoker_Status", "Face_Amount_Band"],
                "offset": "Expected_Death_QX2015VBT_by_Policy",
                "y_var": "Number_Of_Deaths",
                "max_depth": 3,
                "cp" : 0.001
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