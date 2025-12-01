Witside Production Floor DWH Pipeline  
#####################################

## Information and Instructions ##

This repository contains an Extract, Transform, Load (ETL) and Data Warehouse (DWH) pipeline.

The pipeline ingests production event data from a CSV flat file, applies sequential logic validation,  
loads the data into a Star Schema (specifically targeting SQL Server), and performs key floor-level operational analytics.

The solution is written in Python (using Pandas, SQLAlchemy) and SQL, orchestrated by the single entry point: main_runner.py.
The SQL scripts are written in T-SQL and it's meant to use MS SQL Server.

1. Project Structure

The key files in this repository are:

DATA/dataset.csv           : Input source file (Raw production events).  
dwh_config.py              : Configuration settings for DB credentials and analysis parameters.  
dwh_documentation.md       : Technical documentation on the DWH schema and ETL logic.  
dwh_requirements.txt       : Python dependency list (pandas, SQLAlchemy, pyodbc).  
dwh_main_runner.py         : The main script that runs the entire pipeline (setup, ETL, reporting).  
dwh_sql_analytics-views.sql: SQL script defining the analytical views (business logic).  
dwh_sql_schema-tables.sql  : SQL script defining the table DDL.  
test_queries.sql           : SQL script where the user can interact with the created Witside_Production_Floor_DWH for any testing

2. Quick-Start Execution (Local Setup)

This pipeline requires a running SQL Server instance and the Microsoft ODBC driver.

Step 1: Install External Dependency (The ODBC Driver)

The Python library pyodbc requires the official Microsoft driver to connect to SQL Server.  
I recommend using the latest version, ODBC Driver 18 for SQL Server.  
Note: ODBC Driver 17 for SQL Server also works - but you have manually change the setting in DB_CONFIG dictionary in dwh_config.py

Windows: Download and install the appropriate 64-bit version of the Microsoft ODBC Driver 18 for SQL Server.

Linux (Required for pyodbc):  
Install the ODBC Driver Manager, typically unixODBC.  
Follow Microsoft's documentation to install the official Microsoft ODBC Driver 18 for SQL Server for your distribution (e.g., Ubuntu, RHEL).

Note: If you use a different driver version, you must update the driver key in config.py to match the exact name of the installed driver.

Step 2: Set Up Python Environment

Clone the repository.  
Create and activate a virtual environment (Highly Recommended):

python -m venv venv

On Windows:
.\venv\Scripts\activate

On Linux/macOS:
source venv/bin/activate

Install dependencies:  
pip install -r requirements.txt

Step 3: Configure Database Access (dwh_config.py)

In the DB_CONFIGS['SQL_SERVER'] section:

Set Server: The pipeline attempts to read the server name from the DB_SERVER environment variable first.  
If this variable is not set, it defaults to 'localhost'.  
Only update the 'server' key manually if the default 'localhost' is incorrect for your setup  
or if you prefer not to use environment variables.  

Ensure the 'driver' key is set to 'ODBC Driver 18 for SQL Server' (or the version you installed).  
Adjust 'trusted_connection' based on your authentication method.

Step 4: Configure file paths (dwh_config.py)

Check section: --- A. FILE PATHS AND DIRECTORIES ---

Default paths are set.  
If you want to change them or use a different OS and the paths must be re-written, do so in these flags:

path for input data file:  
INPUT_DATAFILE_PATH = BASE_DIR / 'DATA' / 'dataset.csv' 

path for .sql file location (default is base dir, together with .py):  
SQL_DIR = BASE_DIR # Assuming SQL files are in the root or 'sql' subfolder

path for output files (report and quarantined events):  
OUTPUT_REPORT_FILE = BASE_DIR / 'analytics_report.txt'  
QUARANTINE_FILE = BASE_DIR / 'quarantined_events.csv'

Step 5: Configure Global pipeline settings (dwh_config.py)

Four flag variables control what the pipeline will do.  
These are in the part --- B. DWH SETUP AND CONTROL FLAGS ---

i) SETUP_DB = True  
If True, the DB will be created.  
Necessary to be True the first time the code runs.  
If True and the DB is already created, the code reports that the DB exists and continues.  
Set to False if the DB is already set up to save time/resources

ii) SETUP_TABLES = True  
If True, Tables are created.  
Necessary to be True the first time the code runs.  
If True and the Tables exist, they are deleted and recreated.  
Set to False if Tables exist to protect data or save time/resources if nothing needs to change.

iii) SETUP_VIEWS = True  
If True, the Views are created.  
Necessary to be True the first time the code runs.  
Views creation .sql uses CREATE OR ALTER VIEW  
=> If True and the Views exist, they will either remain as is or updated.  
You can set to False if you want to keep the as is and want to save time/resources.

iv) SHOULD_LOAD_DATA = True  
If True, data are Loaded.  
Necessary the first time the code runs.  
Necessary if new data needs to be added.  
So if True and the data (given csv data) are the same, nothing changes  
If True and the data are different, any new rows will be added to the DB.  
Set to False to skip the ETL loading step when  
you want to use the already loaded data and just focus  
on analytics to save time/resources.  

Step 6: Run the Pipeline

Execute the main orchestrator script from the project root directory.  
Run the terminal command:

python main_runner.py

Pipeline Output

Upon successful execution, the following files will be generated in the root directory:

i) analytics_report.txt: Contains the final analytical results (Q1, Q2, Q3).

ii) quarantined_events.csv: A log file containing all source rows that failed the ETL's data quality checks.  
For the included data csv file, quarantined_events.csv will return empty (no problematic data exist) 
