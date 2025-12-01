# dwh_config.py
####################################################################
## MODULE TO SET THE PIPELINE ANALYSIS PARAMETERS (CONFIGURATION) ##
####################################################################
import os
from pathlib import Path


# --- A. FILE PATHS AND DIRECTORIES ---
BASE_DIR = Path(__file__).resolve().parent

# Input data file (relative to this directory)
# Ensure the folder 'data' exists or adjust this path to where your CSV actually is.
# If your CSV is in the same folder as the scripts, remove / 'data'
INPUT_DATAFILE_PATH = BASE_DIR / 'DATA' / 'dataset.csv' 
SQL_DIR = BASE_DIR # Assuming SQL files are in the root or 'sql' subfolder

# Output files
OUTPUT_REPORT_FILE = BASE_DIR / 'analytics_report.txt'
QUARANTINE_FILE = BASE_DIR / 'quarantined_events.csv'


# --- B. DWH SETUP AND CONTROL FLAGS ---

# Global setting to determine if DDL scripts should be run
# Use False if the DB is already set up to save time/avoid errors
SETUP_DB = True      

# Control for destructive Table DDL (DROP/CREATE). Set False to protect data.
SETUP_TABLES = True

# Control for non-destructive View DDL (CREATE OR ALTER VIEW). 
# Set True to easily update business logic views.
SETUP_VIEWS = True

SHOULD_LOAD_DATA = True # Set to False to skip the ETL loading step


# --- C. DATABASE CONNECTION SETTINGS ---

DB_TYPE = 'SQL_SERVER' 
DB_NAME = 'Witside_Production_Floor_DWH'

DB_CONFIGS = {
    'SQL_SERVER': {
        'server': os.getenv('DB_SERVER', 'localhost'), # Replace with your server name if env var not set
        'database': DB_NAME,
        'driver': os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server'),
        'username': None, 
        'password': None,
        'trusted_connection': True,
        'encrypt': True,     # Required for some newer SQL Server configurations
        'trust_cert': True   # Bypasses SSL certificate validation errors
    },
    'POSTGRESQL': {
        'server': os.getenv('PG_HOST', 'localhost'),
        'port': os.getenv('PG_PORT', 5432),
        'database': DB_NAME,
        'username': 'postgres', 
        'password': 'your_postgres_password_here' 
    }
}

# --- D. ETL SETTINGS ---

# Normalization map: Map csv status string to integer status_id
# This MUST match the values inserted into Dim_Status table
# !!! THIS WAS MISSING CAUSING YOUR ERROR !!!
STATUS_MAP = {
    'START': 1,
    'ON': 2,
    'STOP': 3
}


# --- E. ANALYTICS PARAMETERS ---

LINE_ID_Q1 = 'gr-np-47'
TOP_LINES_Q3 = 1 

# --- F. HELPERS ---

def get_active_credentials():
    """Returns the credentials dictionary for the currently selected DB_TYPE."""
    if DB_TYPE not in DB_CONFIGS:
        raise ValueError(f"DB_TYPE '{DB_TYPE}' is not defined in DB_CONFIGS.")
    return DB_CONFIGS[DB_TYPE]


