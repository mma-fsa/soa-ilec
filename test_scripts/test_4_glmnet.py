import asyncio
from fastmcp import Client

async def call_mcp_tool():    
    async with Client("http://localhost:9090/mcp") as client:
        try:
            # Call the 'add' tool with specific parameters
            tool_name = "cmd_glmnet"
            params = {
                "workspace_id" : "202ebe6b-fae6-41de-97f3-d87d2f8f3c5e",
                "dataset": "ul_train_data", 
                "x_vars": ["Gender", "Attained_Age", "Smoker_Status", "Face_Amount_Band"],
                "design_matrix_vars" : [
                    "Gender * splines::ns(Attained_Age, knots=c(30, 40, 50, 60, 70, 80), Boundary.knots=c(17, 95))",
                    "Smoker_Status",
                    "Face_Amount_Band"
                ],
                "factor_vars_levels" : {
                    "Gender" : "Male",
                    "Smoker_Status" : "NonSmoker" 
                },
                "num_var_clip" : {
                    "Attained_Age" : [17, 95]
                },
                "offset_var": "Expected_Death_QX2015VBT_by_Policy",
                "y_var": "Number_Of_Deaths",
                "lambda_strat" : "1se"
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