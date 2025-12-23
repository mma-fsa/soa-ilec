import os
import duckdb
import sys
from pathlib import Path
import urllib.request
import subprocess
from tqdm import tqdm
from common.env_vars import DEFAULT_DATA_DIR, DEFAULT_DDB_PATH, ILEC_IMPORT_FILE_NAME, ILEC_DATA_URL


def get_ilec_data(
    base_dir: Path | str = DEFAULT_DATA_DIR,
    url: str = ILEC_DATA_URL) -> None:
    """
    Download and extract SOA ILEC mortality text data.

    Parameters
    ----------
    base_dir : Path or str
        Directory where the data should be downloaded and extracted.
    url : str
        URL of the ILEC zip file.

    Raises
    ------
    RuntimeError
        If download or extraction fails.
    """
    base_dir = Path(base_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    zip_path = base_dir / Path(url).name

    # --- Download with progress bar ---
    if not zip_path.exists():
        print(f"Downloading {url}")

        with urllib.request.urlopen(url) as response:
            total = int(response.headers.get("Content-Length", 0))

            with open(zip_path, "wb") as f, tqdm(
                total=total,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc="Downloading ILEC data",
            ) as pbar:
                while True:
                    chunk = response.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
                    pbar.update(len(chunk))
    else:
        print(f"Zip file already exists: {zip_path}")

    # --- Extract ---
    print(f"Extracting {zip_path}")
    try:
      subprocess.run(
          ["unzip", "-o", zip_path.name],
          cwd=base_dir,
          check=True,
      )
    except FileNotFoundError:
        raise RuntimeError(
            "`unzip` not found. Please install unzip or p7zip."
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Unzip failed: {e}") from e

    print("download successful")

###################
## Script Start  ##
###################

# Create a data directory if it doesn't exist
if not os.path.exists(DEFAULT_DATA_DIR):
    os.makedirs(DEFAULT_DATA_DIR)

# Connect to DuckDB (or create a new database file in the 'data' directory)
con = duckdb.connect(database=DEFAULT_DDB_PATH, read_only=False)

# Create the table with the specified schema
create_table_query = create_table_query = """
CREATE OR REPLACE TABLE ilec_mortality_raw (
    Observation_Year                    INT,
    Age_Ind                             STRING,
    Gender                              STRING,
    Smoker_Status                       STRING,
    Insurance_Plan                      STRING,
    Issue_Age                           DOUBLE,
    Duration                            DOUBLE,
    Face_Amount_Band                    STRING,
    Issue_Year                          DOUBLE,
    Attained_Age                        DOUBLE,
    SOA_Anticipated_Level_Term_Period   STRING,
    SOA_Guaranteed_Level_Term_Period    STRING,
    SOA_Post_level_Term_Indicator       STRING,
    Select_Ultimate_Indicator           STRING,
    Preferred_Indicator                 STRING,
    Number_Of_Preferred_Classes         STRING,
    Preferred_Class                     STRING,
    Amount_Exposed                      DOUBLE,
    Policies_Exposed                    DOUBLE,
    Death_Claim_Amount                  DOUBLE,
    Number_Of_Deaths                    DOUBLE,
    Expected_Death_QX2015VBT_by_Policy  DOUBLE,
    Expected_Death_QX2015VBT_by_Amount  DOUBLE,
    ExpDeathQx2015VBTwMI_byPol          DOUBLE,
    ExpDeathQx2015VBTwMI_byAmt          DOUBLE,
    Cen2MomP1wMI_Amt                    DOUBLE,
    Cen2MomP2wMI_Amt                    DOUBLE,
    Cen3MomP1wMI_Amt                    DOUBLE,
    Cen3MomP2wMI_Amt                    DOUBLE,
    Cen3MomP3wMI_Amt                    DOUBLE
);
"""

try:
  con.execute(create_table_query)
  print(f"Directory '{DEFAULT_DATA_DIR}' created and table 'ilec_mortality_raw' created in '{DEFAULT_DDB_PATH}'")
except Exception as e:
  print(f"An error occurred with creating ilec_mortality_raw table in {DEFAULT_DDB_PATH} :\n{e}")
  sys.exit(-1)

ilec_data_import_path = Path(DEFAULT_DATA_DIR) / Path(ILEC_IMPORT_FILE_NAME)

if not ilec_data_import_path.exists():
  print(f"ILEC data does not exist, attempting to download and unzip in '{str(DEFAULT_DATA_DIR)}'")
  get_ilec_data()

if not ilec_data_import_path.exists():
  print(f"Cannot import data, ILEC data file not found: '{str(ilec_data_import_path)}'")
  sys.exit(-1)

print("Importing ILEC data into duckdb")
copy_command = f"""
COPY ilec_mortality_raw FROM '{ilec_data_import_path}'
WITH (DELIMITER '\t', HEADER TRUE);
"""

try:
    con.execute(copy_command)
    print("Data copied successfully into ilec_mortality table.")
except Exception as e:
    print(f"An error occurred while importing the data: {e}")
    sys.exit(-1)

try:
    con.execute("create or replace view ILEC_DATA as (select * from ilec_mortality_raw)")
    print("Created ILEC_DATA")
except Exception as e:
    print(f"An error occurred while creating the view: {e}")
    sys.exit(-1)

## Sample View Query
sample_vw_query = """
CREATE OR REPLACE VIEW UL_MODEL_DATA_SMALL AS
WITH obs_data AS (
  SELECT
    Gender,
    Attained_Age,
    Smoker_Status,
    Number_Of_Deaths,
    ExpDeathQx2015VBTwMI_byPol,
    CASE WHEN (
      (
        Observation_Year < 2016
      )
    ) THEN (
      'TRAIN'
    ) ELSE 'TEST' END AS DATASET
  FROM ILEC_DATA
  WHERE
    (
      Insurance_Plan = 'UL'
    )
)
SELECT
  DATASET,
  Gender,
  Attained_Age,
  Smoker_Status,
  SUM(COALESCE(Number_Of_Deaths, 0)) AS Number_Of_Deaths,
  SUM(COALESCE(ExpDeathQx2015VBTwMI_byPol, 0)) AS ExpDeathQx2015VBTwMI_byPol
FROM obs_data
GROUP BY
  DATASET,
  Gender,
  Attained_Age,
  Smoker_Status
"""

try:
    con.execute(sample_vw_query)
    print("Created UL_MODEL_DATA_SMALL")
except Exception as e:
    print(f"An error occurred: {e}")


# Close the connection
con.close()