# dwh_sql_connection.py
######################################################
## MODULE TO CREATE SQL DATABASE ENGINE CONNECTIONS ##
######################################################


from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from typing import Dict, Any, Optional
import urllib.parse
import os

# Base configuration (imported from the config module during runtime)

def build_sqlserver_url(credentials: Dict[str, Any], database_name: str, master: bool) -> str:
    """
    Builds the SQLAlchemy connection URL for SQL Server (MSSQL).
    Handles both standard and trusted (Windows Integrated) authentication.
    """
    
    server = credentials.get('server')
    driver = credentials.get('driver')
    
    # Use 'master' database for operations that require higher privilege (like creating a new DB)
    db = 'master' if master else database_name
    
    # 1. Build the ODBC parameters dictionary
    params = {
        'driver': driver,
        'server': server,
        'database': db,
        # Default options for robustness (always included)
        'Timeout': 30, 
        'Port': 1433, # Default SQL Server port

        # Security/Authentication settings from config
        'Encrypt': 'yes' if credentials.get('encrypt', False) else 'no',
        
        
        # This tells the driver to trust the self-signed certificate, 
        'TrustServerCertificate': 'yes' if credentials.get('trust_cert', False) else 'no',
    }
    
    # 2. Add Authentication Method
    if credentials.get('trusted_connection', False):
        # Trusted Connection (Windows Integrated Security)
        params['Trusted_Connection'] = 'yes'
        auth_url = "" # No username/password in the URL path
    else:
        # Standard SQL Login (Username/Password)
        username = credentials.get('username')
        password = credentials.get('password')
        # Note: We keep the username/password check simple here, 
        # relying on the caller (main_runner) to configure credentials correctly.
        if not username or not password:
             # This check is disabled when trusted_connection is True
             pass 
        
        # URL-encode the password to handle special characters (e.g., #, @)
        encoded_password = urllib.parse.quote_plus(password or "")
        auth_url = f"{username or ''}:{encoded_password}@"

    # 3. Format ODBC parameters for the URL query string
    odbc_params = urllib.parse.urlencode(params).replace('+', '%20')
    
    # 4. Construct the final SQLAlchemy URL
    # When connecting to the 'master' database for DDL (master=True), 
    # we omit the database name from the URL path for safer execution context.
    path_component = f"/{db}" if not master else "" 
    
    # Format: mssql+pyodbc://<username>:<password>@<server>/<database>?<params>
    # Note: path_component is empty when master=True
    url = f"mssql+pyodbc://{auth_url}{server}{path_component}?{odbc_params}"
    
    return url


def build_postgresql_url(credentials: Dict[str, Any], database_name: str, master: bool) -> str:
    """
    Builds the SQLAlchemy connection URL for PostgreSQL.
    """
    
    server = credentials.get('server')
    port = credentials.get('port')
    username = credentials.get('username')
    password = credentials.get('password')
    
    # Use 'postgres' database for operations that require higher privilege
    db = 'postgres' if master else database_name
    
    # URL-encode the password
    encoded_password = urllib.parse.quote_plus(password)

    # Format: postgresql+psycopg2://<user>:<password>@<host>:<port>/<dbname>
    url = f"postgresql+psycopg2://{username}:{encoded_password}@{server}:{port}/{db}"
    
    return url


def get_db_engine(db_type: str, credentials: Dict[str, Any], database_only: bool = False) -> Engine:
    """
    Creates a SQLAlchemy Engine connected to the specified DWH.
    
    Args:
        db_type (str): The type of database ('SQL_SERVER' or 'POSTGRESQL').
        credentials (Dict): The configuration dictionary for the database type.
        database_only (bool): If True, connects to the master database 
                              (e.g., 'master' for SQL Server, 'postgres' for PostgreSQL) 
                              instead of the target DWH defined by config.DB_NAME.
                              This is used for creating the target DWH itself.
    
    Returns:
        Engine: The SQLAlchemy engine instance.
    """
    # Import config dynamically to avoid circular dependencies during setup
    import dwh_config 
    
    db_name = dwh_config.DB_NAME
    
    if db_type == 'SQL_SERVER':
        url = build_sqlserver_url(credentials, db_name, database_only)
        #When performing high-level DDL (like CREATE DATABASE),
        #the engine must be set to isolation_level='AUTOCOMMIT' to prevent 
        # the database from raising "CREATE DATABASE statement not allowed within multi-statement transaction."
        isolation_level = 'AUTOCOMMIT' if database_only else None #important
    elif db_type == 'POSTGRESQL':
        url = build_postgresql_url(credentials, db_name, database_only)
        # PostgreSQL handles CREATE DATABASE within transactions fine, but we can set 
        # AUTOCOMMIT for consistency if we are in the 'master' context.
        isolation_level = 'AUTOCOMMIT' if database_only else None
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

    # Use a pool size of 5 connections and allow 10 overflows
    # Apply isolation_level only when necessary
    engine = create_engine(url, pool_size=5, max_overflow=10, isolation_level=isolation_level)
    
    return engine