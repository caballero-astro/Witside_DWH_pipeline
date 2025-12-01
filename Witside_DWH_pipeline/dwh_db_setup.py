# dwh_db_setup.py
#################################################################
## MODULE FOR AUTOMATICALLY SETTING UP THE DWH INFRASTRUCTURE S##
#################################################################

import os
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import text
from typing import Dict, Any

from dwh_sql_connection import get_db_engine
from dwh_config import get_active_credentials, DB_TYPE, DB_NAME, SQL_DIR

def run_sql_ddl(engine: Engine, file_path: str, print_output: bool = True):
    """
    Reads an SQL file and executes its contents against the DWH.
    Handles SQL Server 'GO' batches by splitting commands and committing each batch.
    """
    try:
        if print_output:
            print(f"-> Executing DDL script: {os.path.basename(file_path)}")
            
        with open(file_path, 'r') as f:
            sql_script = f.read()
        
        # We use 'GO' in SQL Server to delineate batches/transactions.
        if DB_TYPE == 'SQL_SERVER':
            # Split the script into separate statements using 'GO'. 
            commands = sql_script.split('GO')
        else:
            # Most other databases (PostgreSQL) handle the entire script as one execution block
            commands = [sql_script]
        
        # Explicitly manage the connection and transaction for DDL to ensure 
        # tables are created and committed before they are needed by the ETL process.
        with engine.begin() as conn:
            for command in commands:
                command = command.strip()
                if command:
                    conn.execute(text(command))
        
        if print_output:
            print(f"-> Successfully executed {os.path.basename(file_path)}")
            
    except FileNotFoundError:
        print(f"!!! ERROR: SQL file not found at path: {file_path}")
        raise
    except Exception as e:
        print(f"!!! ERROR executing SQL DDL in {os.path.basename(file_path)}: {e}")
        raise


def create_database_if_not_exists(master_engine: Engine, db_name: str, db_type: str):
    """
    Checks if the target database exists on the server and creates it if missing.
    Must be run against a system-level database (like 'master' in SQL Server).
    """
    print(f"-> Checking for database '{db_name}'...")
    
    if db_type == 'SQL_SERVER':
        check_query = f"SELECT name FROM sys.databases WHERE name = '{db_name}'"
    elif db_type == 'POSTGRESQL':
        # PostgreSQL uses pg_database for system catalog
        check_query = f"SELECT datname FROM pg_database WHERE datname = '{db_name}'"
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

    
    with master_engine.connect() as conn:
        # Use simple try-except structure to catch errors without stopping the flow if DB exists
        try:
            result = conn.execute(text(check_query)).fetchone()
            
            if result:
                print(f"-> Database '{db_name}' exists.")
                return # Database already exists, we are done
            else:
                print(f"-> Database '{db_name}' not found. Creating database...")
                # Execution requires AUTOCOMMIT isolation level, handled in get_db_engine
                conn.execute(text(f"CREATE DATABASE {db_name}"))
                print(f"-> Database '{db_name}' created successfully.")

        except ProgrammingError as e:
            # This often happens if the user doesn't have privileges to run the check query
            print(f"!!! WARNING: Could not reliably check or create database. Privilege issue? Details: {e}")
            # If we couldn't check, we assume it exists or rely on the next connection to fail.
        except Exception as e:
            print(f"!!! FATAL ERROR during database check/creation: {e}")
            raise


def initialize_dwh(config: Any):
    """
    Orchestrates the entire DWH setup process: DB creation, table schema, and views.
    Returns the final DWH engine connection.
    """
    credentials = get_active_credentials()
    dwh_engine: Engine = None
    
    # ----------------------------------------------------------
    # 1. DATABASE CREATION (If config.SETUP_DB is True)
    # ----------------------------------------------------------
    if config.SETUP_DB:
        print("\n==================================================")
        print("PHASE I: DATABASE INITIALIZATION (DB CHECK/CREATE)")
        print("==================================================")
        
        # Get connection to the system database (master/postgres)
        master_engine = get_db_engine(
            db_type=config.DB_TYPE, 
            credentials=credentials,
            database_only=True # Connects to the system database
        )
        
        # Attempt to create the target database
        create_database_if_not_exists(master_engine, config.DB_NAME, config.DB_TYPE)
        
        # Dispose of the master connection
        master_engine.dispose()
        
    # -------------------------------------------------------------------------------------
    # 2. SCHEMA CREATION (TABLES & VIEWS - Now split by control flags)
    # -------------------------------------------------------------------------------------
    if config.SETUP_TABLES or config.SETUP_VIEWS:
        print("\n=======================================================")
        print("PHASE II: DWH SCHEMA INITIALIZATION (TABLES & VIEWS)")
        print("=======================================================")
        
        # Get connection to the target DWH
        dwh_engine = get_db_engine(
            db_type=config.DB_TYPE, 
            credentials=credentials,
            database_only=False # Connects to the target DWH
        )

        try:
            # 2a. TABLE CREATION/RESET (Controlled by SETUP_TABLES)
            if config.SETUP_TABLES:
                print("-> ATTENTION: SETUP_TABLES is True. Tables will be dropped and recreated.")
                run_sql_ddl(dwh_engine, config.SQL_DIR / 'dwh_sql_schema-tables.sql')
            else:
                print("-> Skipping table schema setup (SETUP_TABLES is False).")
            
            # 2b. VIEWS CREATION/ALTERATION (Controlled by SETUP_VIEWS)
            if config.SETUP_VIEWS:
                # Execute SQL script for views (analytics-views.sql)
                run_sql_ddl(dwh_engine, config.SQL_DIR / 'dwh_sql_analytics-views.sql')
            else:
                print("-> Skipping analytical views setup (SETUP_VIEWS is False).")


        except Exception as e:
            print("!!! FATAL ERROR during schema setup. Pipeline stopping.")
            dwh_engine.dispose()
            raise
    
    # If the setup was skipped, we still need to initialize the engine for the next phase
    if not dwh_engine:
        print("\n=======================================================")
        print("PHASE II: DWH SCHEMA CHECK (Setup Skipped)")
        print("=======================================================")
        print("-> Establishing DWH connection for subsequent steps...")
        dwh_engine = get_db_engine(
            db_type=config.DB_TYPE, 
            credentials=credentials,
            database_only=False
        )
        
    return dwh_engine