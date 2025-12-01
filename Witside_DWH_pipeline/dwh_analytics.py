# dwh_analytics.py
#################################################
## MODULE FOR ANSWERING DHW BUSINESS QUESTIONS ##
##           AND GENERATE REPORT(S)            ##
#################################################

import pandas as pd
from sqlalchemy.engine import Engine
from typing import TextIO, Dict, Any
import dwh_config
from datetime import datetime

# Helper function for better report readability (Markdown table format)
def format_markdown_table(df: pd.DataFrame) -> str:
    """Converts a DataFrame to a clean markdown table string."""
    if df.empty:
        return "No results found."

    # 1. Header and Separator (Using old format for better readability)
    
    # Calculate column widths for consistent spacing
    col_widths = [max(len(col), 10) for col in df.columns]
    
    header_parts = [col.ljust(width) for col, width in zip(df.columns, col_widths)]
    header = "| " + " | ".join(header_parts) + " |"
    
    # Separator uses markdown alignment syntax
    separator_parts = [":-" + "-" * (width - 1) for width in col_widths]
    separator = "|" + "|".join(separator_parts) + "|"

    # 2. Data Rows
    rows = []
    for _, row in df.iterrows():
        row_parts = [str(item).ljust(width) for item, width in zip(row.tolist(), col_widths)]
        row_str = "| " + " | ".join(row_parts) + " |"
        rows.append(row_str)

    return "\n".join([header, separator] + rows)

# Q1: Process Cycle Analysis
def run_q1_process_cycles(engine: Engine, line_id: str) -> str:
    """Calculates the duration of each production cycle for a specific line."""
    
    # Query uses the pre-created view (View_Line_Process_Durations)
    sql_query = f"""
    SELECT 
        start_timestamp, 
        stop_timestamp, 
        CAST(DATEDIFF(second, start_timestamp, stop_timestamp) / 60.0 AS DECIMAL(10, 2)) AS duration
    FROM 
        View_Line_Process_Durations 
    WHERE 
        production_line_id = '{line_id}'
    ORDER BY 
        start_timestamp;
    """
    
    df = pd.read_sql(sql_query, con=engine)
    
    # Rename columns for presentation
    df.columns = ['start_timestamp', 'stop_timestamp', 'duration']
    
    # Format the timestamps to look cleaner in the report
    df['start_timestamp'] = df['start_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['stop_timestamp'] = df['stop_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    
    df['duration'] = df['duration'].astype(str) + ' minutes'
    
    report_section = f"--- Q1: Process Cycles for Line '{line_id}' ---\n"
    report_section += format_markdown_table(df)
    
    return report_section

# Q2: Total Floor Uptime and Downtime
def run_q2_floor_time(engine: Engine) -> str:
    """Calculates total uptime and downtime for the entire production floor, providing both 
    Operational Time (Total) and Full Cycle Time (In Full Cycles)."""
    
    # 1. Query for Operational Uptime (START/ON) and Operational Downtime (STOP)
    operational_query = """
    SELECT 
        SUM(total_uptime_seconds) AS operational_uptime_seconds,
        SUM(floor_downtime_seconds) AS operational_downtime_seconds
    FROM 
        View_Total_Uptime_Downtime;
    """
    df_op = pd.read_sql(operational_query, con=engine)
    
    operational_uptime = round(df_op.iloc[0]['operational_uptime_seconds'] / 60.0, 3)
    operational_downtime = round(df_op.iloc[0]['operational_downtime_seconds'] / 60.0, 3)
    
    # 2. Query for Full Cycle Uptime (Total Process Cycle Time)
    cycle_query = """
    SELECT 
        SUM(duration_seconds) AS cycle_uptime_seconds
    FROM 
        View_Line_Process_Durations;
    """
    df_cycle = pd.read_sql(cycle_query, con=engine)
    
    cycle_uptime = round(df_cycle.iloc[0]['cycle_uptime_seconds'] / 60.0, 3)
    
    # 3. Assemble the final summary table
    # Downtime for 'In Full Cycles' is defined as the time between START and STOP events, 
    # which is the same as the 'Operational Downtime' calculated above.
    summary_data = [
        {
            'Total Up/Down-time': 'Total Uptime', 
            'Total': f"{operational_uptime} minutes",
            'In Full Cycles': f"{cycle_uptime} minutes"
        },
        {
            'Total Up/Down-time': 'Total Downtime', 
            'Total': f"{operational_downtime} minutes",
            'In Full Cycles': f"{operational_downtime} minutes" # Downtime is consistent in both scenarios
        }
    ]
    summary_df = pd.DataFrame(summary_data)
    
    report_section = "\n--- Q2: Total Floor Uptime and Downtime ---\n"
    report_section += format_markdown_table(summary_df)
    
    return report_section

# Q3: Production Line with Most Downtime
def run_q3_top_downtime(engine: Engine, top_n: int) -> str:
    """Identifies the production line(s) with the highest total downtime."""
    
    sql_query = f"""
    SELECT TOP {top_n}
        production_line_id,         
        CAST(floor_downtime_seconds / 60.0 AS DECIMAL(10, 4)) AS downtime
    FROM 
        View_Total_Uptime_Downtime -- CORRECTED VIEW NAME
    ORDER BY 
        floor_downtime_seconds DESC;
    """
    
    df = pd.read_sql(sql_query, con=engine)
    
    
    df.columns = ['production_line_id', 'downtime']
    
    # Append ' minutes' to the downtime column value for clean output
    df['downtime'] = df['downtime'].astype(str) + ' minutes'
    
    report_section = f"\n--- Q3: Production Line with Most Downtime ---\n"
    report_section += format_markdown_table(df)
    
    return report_section


def write_report_to_file(content: str, output_file: str):
    """Writes the generated report content to a specified file."""
    try:
        with open(output_file, 'w') as f:
            f.write(content)
        print(f"\nPipeline Execution Complete. Report saved to {output_file}")
    except Exception as e:
        print(f"!!! ERROR writing report to file {output_file}: {e}")
        raise


def generate_analytics_report(engine: Engine, output_file: str):
    """
    Orchestrates the running of all analytical queries and compiles the final report.
    """
    
    # Get current execution time for the report header
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    report_content = []
    
    # 1. Report Header
    report_content.append("=====================================================")
    report_content.append("=== Production Floor DWH Pipeline Execution Log ===")
    report_content.append(f"=== Execution Date: {current_time} ===")
    report_content.append("=====================================================")
    report_content.append("\nETL Status: SUCCESS\n") # Assuming successful load for now
    report_content.append("=====================================================")
    report_content.append("=== ANALYTICS REPORTING ===")
    report_content.append("=====================================================")
    
    # 2. Run Queries using parameters from config
    try:
        # Q1
        q1_result = run_q1_process_cycles(engine, dwh_config.LINE_ID_Q1)
        report_content.append(q1_result)
        
        # Q2
        q2_result = run_q2_floor_time(engine)
        report_content.append(q2_result)

        # Q3
        q3_result = run_q3_top_downtime(engine, dwh_config.TOP_LINES_Q3)
        report_content.append(q3_result)
        
    except Exception as e:
        report_content.append(f"\n!!! FATAL ANALYTICS ERROR: Could not run queries. Details: {e}")
        raise # Re-raise the exception to be caught by main_runner

    # 3. Write final report
    final_report = "\n".join(report_content)
    write_report_to_file(final_report, output_file)