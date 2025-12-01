# dwh_etl_pipeline.py
###################################################################
## MODULE FOR ETL PIPELINE OPERATIONS (Extract, Transform, Load) ##
###################################################################

from sqlalchemy.engine import Engine
import dwh_config # to import the parameters from config.py
import pandas as pd
from typing import Tuple # added to allow (unnecessary, totally optional) tuple type annotation


def check_and_insert_dimension(engine: Engine, fact_df: pd.DataFrame, column_name: str, table_name: str):
    """
    Checks for new unique values in a dimension column and inserts them 
    into the corresponding dimension table if they don't exist.
    """
    unique_values = fact_df[column_name].unique()
    
    if unique_values.size == 0:
        print(f"No data to check for {table_name}.")
        return

    # Create a DataFrame of the new unique values
    new_dim_df = pd.DataFrame(unique_values, columns=[column_name])
    
    # Check what already exists in the dimension table
    existing_values = pd.read_sql(f"SELECT {column_name} FROM {table_name}", con=engine)[column_name].tolist()
    
    # Filter for values that are not yet in the dimension table
    to_insert_df = new_dim_df[~new_dim_df[column_name].isin(existing_values)].reset_index(drop=True)

    if not to_insert_df.empty:
        # Load new dimension keys
        to_insert_df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            schema='dbo' 
        )
        print(f"Successfully inserted {len(to_insert_df)} new entries into {table_name}.")
    else:
        print(f"No new entries found for {table_name}.")


def transform_raw_data(raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Performs data cleaning, validation, and transformation.
    Separates clean records from records requiring quarantine.
    """
    # 1. Initial Raw Data Check
    # Check if the CSV has the expected raw headers
    expected_raw_cols = ['production_line_id', 'status', 'timestamp']
    for col in expected_raw_cols:
        if col not in raw_df.columns:
            raise ValueError(f"Required raw column '{col}' missing from input CSV.")

    # 2. Transformation (Renaming & Mapping)
    # --------------------------------------
    # Rename columns to match DWH schema
    df = raw_df.rename(columns={
        'production_line_id': 'production_line_id', # Keeps name
        'status': 'status_name',
        'timestamp': 'event_time'
    })
    
    # Map status string to ID
    df['status_id'] = df['status_name'].map(dwh_config.STATUS_MAP)

    # Convert timestamp to datetime (coercing errors to NaT)
    df['event_time'] = pd.to_datetime(df['event_time'], errors='coerce')

    # 3. Validation & Separation
    # --------------------------------------
    # Now we validate the TRANSFORMED columns
    required_cols = ['production_line_id', 'status_id', 'event_time']
    
    # Check for nulls in the required columns (This catches unmapped statuses or bad dates)
    is_valid = df[required_cols].notna().all(axis=1)
    
    # Separate clean vs dirty data
    clean_fact_df = df[is_valid].copy()
    quarantined_df = df[~is_valid].copy()
    
    # Sort data chronologically (crucial for incremental loading checks)
    clean_fact_df.sort_values(by='event_time', inplace=True)
    
    print(f"Raw rows: {len(raw_df)} | Clean rows: {len(clean_fact_df)} | Quarantined rows: {len(quarantined_df)}")
    
    return clean_fact_df, quarantined_df


def run_etl_pipeline(input_file_path: str, engine: Engine, load_data: bool):
    """
    Runs the entire ETL process (Extract, Transform, Validate, Load).
    """
    
    # 1. Extraction (E)
    print("\n--- Starting Data Extraction ---")
    try:
        raw_df = pd.read_csv(input_file_path)
    except FileNotFoundError:
        print(f"!!! ERROR: Input file not found at {input_file_path}")
        raise
    
    # 2. Transformation & Validation (T)
    print("\n--- Starting Data Transformation and Validation ---")
    clean_fact_df, quarantined_df = transform_raw_data(raw_df)
    
    # 3. Quarantine (If needed)
    if not quarantined_df.empty:
        quarantine_file_path = dwh_config.QUARANTINE_FILE 
        quarantined_df.to_csv(quarantine_file_path, index=False)
        print(f"Quarantined {len(quarantined_df)} invalid records to: {quarantine_file_path}")

    # 4. Loading (L)
    ###############################################
    if load_data:
        print("\n--- Starting Data Loading to DWH ---")
        
        # a) Load Dim_ProductionLine 
        check_and_insert_dimension(
            engine, 
            clean_fact_df,
            column_name='production_line_id', 
            table_name='Dim_ProductionLine'
        )

        # b) Load Fact_ProcessEvents
        TARGET_TABLE = 'Fact_ProcessEvents'
        max_time_query = f"SELECT MAX(event_time) FROM {TARGET_TABLE};"

        try:
            # Find the latest timestamp already loaded
            max_time_result = pd.read_sql(max_time_query, con=engine).iloc[0, 0]
            latest_event_time = pd.to_datetime(max_time_result) if pd.notna(max_time_result) else pd.NaT 

        except Exception as e:
            print(f"Warning: Could not determine max event time. Proceeding with caution. Details: {e}")
            latest_event_time = pd.NaT

        # Filter the incoming data
        if pd.isna(latest_event_time):
            data_to_load = clean_fact_df
            print(f"{TARGET_TABLE} is empty. Loading all {len(data_to_load)} clean rows.")
        else:
            data_to_load = clean_fact_df[clean_fact_df['event_time'] > latest_event_time]
            print(f"Latest event in DB: {latest_event_time}. Found {len(data_to_load)} new rows.")

        # Load filtered data
        if not data_to_load.empty:
            data_to_load[['production_line_id', 'status_id', 'event_time']].to_sql(
                name=TARGET_TABLE,
                con=engine,
                if_exists='append',
                index=False,
                schema='dbo'
            )
            print(f"Successfully loaded {len(data_to_load)} new rows into {TARGET_TABLE}.")
        else:
            print(f"No new data found for {TARGET_TABLE} in the current batch.")
            
    else: 
        print("\n--- Data Loading SKIPPED (load_data=False) ---")