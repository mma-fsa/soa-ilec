import os
import duckdb

# Create a directory named 'data' if it doesn't exist
if not os.path.exists('data'):
    os.makedirs('data')

# Connect to DuckDB (or create a new database file in the 'data' directory)
con = duckdb.connect(database='/content/soa-ilec/data/ilec_data.duckdb', read_only=False)

# Create the table with the specified schema
create_table_query = """
CREATE OR REPLACE TABLE ilec_mortality_raw (
    Observation_Year        INT,
    Age_Ind STRING,
    Sex     STRING,
    Smoker_Status   STRING,
    Insurance_Plan  STRING,
    Issue_Age       DOUBLE,
    Duration        DOUBLE,
    Face_Amount_Band        STRING,
    Issue_Year      DOUBLE,
    Attained_Age    DOUBLE,
    SOA_Antp_Lvl_TP STRING,
    SOA_Guar_Lvl_TP STRING,
    SOA_Post_Lvl_Ind        STRING,
    Slct_Ult_Ind    STRING,
    Preferred_Indicator     STRING,
    Number_of_Pfd_Classes   STRING,
    Preferred_Class STRING,
    Amount_Exposed  DOUBLE,
    Policies_Exposed        DOUBLE,
    Death_Claim_Amount      DOUBLE,
    Death_Count     DOUBLE,
    ExpDth_VBT2015_Cnt      DOUBLE,
    ExpDth_VBT2015_Amt      DOUBLE,
    ExpDth_VBT2015wMI_Cnt   DOUBLE,
    ExpDth_VBT2015wMI_Amt   DOUBLE,
    Cen2MomP1wMI_Amt        DOUBLE,
    Cen2MomP2wMI_Amt        DOUBLE,
    Cen3MomP1wMI_Amt        DOUBLE,
    Cen3MomP2wMI_Amt        DOUBLE,
    Cen3MomP3wMI_Amt        DOUBLE
);
"""

con.execute(create_table_query)

print("Directory 'data' created and 'ilec_mortality' table created in data/ilec_mortality.duckdb")

print("Importing ILEC data into duckdb")
copy_command = """
COPY ilec_mortality_raw FROM '/content/soa-ilec/raw_data/ILEC_2012_19 - 20240429.txt'
WITH (DELIMITER '\t', HEADER TRUE);
"""

try:
    con.execute(copy_command)
    print("Data copied successfully into ilec_mortality table.")
except Exception as e:
    print(f"An error occurred: {e}")

# Close the connection
con.close()