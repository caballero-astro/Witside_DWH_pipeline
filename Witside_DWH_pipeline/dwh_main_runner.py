# dwh_main_runner.py
#####################################
##MAIN MODULE FOR EXECUTION OF CODE##
#####################################

import sys
import time
import traceback
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Import configuration settings and file paths
import dwh_config 
from dwh_config import get_active_credentials

# Import core pipeline modules
import dwh_db_setup
import dwh_sql_connection
import dwh_etl_pipeline
import dwh_analytics

# --- Main Execution Functions ---

def execute_pipeline():
    """
    Main function to run the DWH pipeline from setup to reporting.
    """
    start_time = time.time()
    successful_run = False # <-- ADDED: Flag to track successful completion
    
    # 1. Configuration Setup
    credentials = get_active_credentials()
    db_type = dwh_config.DB_TYPE
    
    print("====================================================")
    print(f"DWH PIPELINE STARTING ({db_type} Backend)")
    print("====================================================")
    print(f"Target DB: {dwh_config.DB_NAME}")
    print(f"Data File: {dwh_config.INPUT_DATAFILE_PATH.name}")

    # Initialize the engine to the target DWH (non-master connection)
    dwh_engine: Engine = None
    
    try:
        # --- PHASE 1: INFRASTRUCTURE SETUP ---
        
        # db_setup.initialize_dwh handles connection to 'master'/'postgres' 
        # and runs DDL scripts based on SETUP_DB and SETUP_SCHEMA flags.
        dwh_db_setup.initialize_dwh(dwh_config)
        
        # Now, create the main engine connected directly to the target DWH
        print("\n--- Connecting to Target DWH ---")
        dwh_engine = dwh_sql_connection.get_db_engine(
            db_type=db_type,
            credentials=credentials,
            database_only=False # Connect to the named database
        )
        print("Connection successful.")

        # --- PHASE 2: ETL EXECUTION ---
        
        if dwh_config.SHOULD_LOAD_DATA:
            print("\n====================================================")
            print("PHASE III: ETL PIPELINE EXECUTION")
            print("====================================================")
            
            dwh_etl_pipeline.run_etl_pipeline(
                input_file_path=dwh_config.INPUT_DATAFILE_PATH,
                engine=dwh_engine,
                load_data=dwh_config.SHOULD_LOAD_DATA
            )
        else:
            print("\n--- ETL LOADING SKIPPED (SHOULD_LOAD_DATA=False) ---")


        # --- PHASE 3: ANALYTICS AND REPORTING ---
        
        print("\n====================================================")
        print("PHASE IV: ANALYTICS AND REPORTING")
        print("====================================================")
        
        dwh_analytics.generate_analytics_report(
            engine=dwh_engine,
            output_file=dwh_config.OUTPUT_REPORT_FILE
        )
        
        # <-- SET FLAG HERE: Only reached if no exceptions were raised
        successful_run = True 

    except FileNotFoundError as e:
        print(f"\n!!! FATAL ERROR: Required file not found. Check path: {e}", file=sys.stderr)
        sys.exit(1)
        
    except SQLAlchemyError as e:
        print(f"\n!!! FATAL DATABASE ERROR: Could not connect or execute SQL. Check credentials/server status.", file=sys.stderr)
        print(f"Details: {e}", file=sys.stderr)
        sys.exit(1)
        
    except Exception as e:
        # This catches the 'module 'db_setup' has no attribute 'initialize_dwh'' error
        print(f"\n!!! AN UNEXPECTED FATAL ERROR OCCURRED: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        if dwh_engine:
            dwh_engine.dispose()
            
        end_time = time.time()
        duration = end_time - start_time
        
        # <-- CONDITIONAL PRINTING: Only print success on actual success
        if successful_run:
            print("\n====================================================")
            print(f"PIPELINE COMPLETED SUCCESSFULLY in {duration:.2f} seconds.")
            print("====================================================")
        else:
            print(f"\nPIPELINE EXECUTION HALTED due to error after {duration:.2f} seconds.")


if __name__ == '__main__':
    execute_pipeline()