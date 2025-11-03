import asyncio
from fastmcp import Client

async def call_mcp_tool():    
    async with Client("http://localhost:9090/mcp") as client:
        try:
            # Call the 'add' tool with specific parameters
            tool_name = "cmd_glmnet"
            params = {
                "workspace_id" : "19c22862-0dc2-4993-a807-f84432921f59",
                "dataset": "model_data_train", 
                "x_vars": [ 
                    "Number_Of_Preferred_Classes", 
                    "Preferred_Indicator", 
                    "Issue_Age", 
                    "Preferred_Class", 
                    "Face_Amount_Band", 
                    "Gender", 
                    "Attained_Age", 
                    "Duration", 
                    "Smoker_Status", 
                    "Issue_Year", 
                    "Age_Basis" 
                ],
                "design_matrix_vars" : [ 
                    "splines::ns(Issue_Age, knots=c(25,45,65), Boundary.knots=c(0,97))", 
                    "splines::ns(Attained_Age, knots=c(50,65,80), Boundary.knots=c(0,120))", 
                    "splines::ns(Duration, knots=c(1,5,10,20), Boundary.knots=c(1,95))", 
                    "Number_Of_Preferred_Classes", 
                    "Preferred_Indicator", 
                    "Preferred_Class", 
                    "Face_Amount_Band", 
                    "Gender", 
                    "Smoker_Status", 
                    "Issue_Year", 
                    "Age_Basis"
                ],
                "factor_vars_levels" : { 
                    "Gender": "Male", 
                    "Smoker_Status": "NonSmoker", 
                    "Preferred_Class": "1", 
                    "Preferred_Indicator" : "0",
                    "Face_Amount_Band" : "100000-249999",
                    "Age_Basis" : "ALB"
                },
                "num_var_clip" : {
                    "Issue_Age" : [ 0, 97 ],
                    "Attained_Age" : [ 0, 120 ],
                    "Duration" : [ 1, 30 ],
                    "Issue_Year" : [ 1985, 2018 ],
                    "Number_Of_Preferred_Classes" : [ 0, 4 ]
                },
                "offset_var": "EXPDEATHQX2015VBTWMI_BYPOL",
                "y_var": "NUMBER_OF_DEATHS",
                "lambda_strat" : "1se"
            }
            
            print(f"Calling tool '{tool_name}' with parameters: {params}")

            # Use the call_tool() method to execute the tool
            result = await client.call_tool(tool_name, params, timeout=3600)

            # The result is a streamable object, so you can access its content
            sum_result = result.content[0].text
            print(f"Result from the MCP tool: {sum_result}")

        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(call_mcp_tool())