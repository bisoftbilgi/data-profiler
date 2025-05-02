import streamlit as st
from database.analysis import get_all_tables_and_views
from database.summary import show_all_tables_summary
from database.utils import load_db_config
from decimal import Decimal
import pandas as pd
import io
from datetime import datetime
import openpyxl

def generate_detailed_statistics(conn, schema):
    """Generate detailed statistics for all tables and columns"""
    stats = []
    
    # Get all tables
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{schema}'
        AND table_type = 'BASE TABLE'
    """)
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        
        # Get column information with more detailed data type information
        cursor.execute(f"""
            SELECT 
                column_name, 
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                udt_name,
                character_set_name,
                collation_name
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = '{table_name}'
        """)
        columns = cursor.fetchall()
        
        # Get row count
        cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
        row_count = cursor.fetchone()[0]
        
        # Get column statistics
        for col in columns:
            col_name = col[0]
            data_type = col[1]
            
            # Format data type with restrictions
            formatted_data_type = data_type
            if col[2] is not None:  # character_maximum_length
                formatted_data_type = f"{data_type}({col[2]})"
            elif col[3] is not None:  # numeric_precision
                if col[4] is not None:  # numeric_scale
                    formatted_data_type = f"{data_type}({col[3]},{col[4]})"
                else:
                    formatted_data_type = f"{data_type}({col[3]})"
            
            # Add character set and collation if available
            if col[7] is not None:  # character_set_name
                formatted_data_type += f" CHARACTER SET {col[7]}"
            if col[8] is not None:  # collation_name
                formatted_data_type += f" COLLATE {col[8]}"
            
            try:
                # Get distinct values count
                cursor.execute(f"""
                    SELECT COUNT(DISTINCT "{col_name}")
                    FROM "{schema}"."{table_name}"
                """)
                distinct_count = cursor.fetchone()[0]
                
                # Get null count
                cursor.execute(f"""
                    SELECT COUNT(*)
                    FROM "{schema}"."{table_name}"
                    WHERE "{col_name}" IS NULL
                """)
                null_count = cursor.fetchone()[0]
                
                # Calculate null percentage
                null_percentage = (null_count / row_count * 100) if row_count > 0 else 0
                
                stats.append({
                    'Table Name': table_name,
                    'Column Name': col_name,
                    'Data Type': formatted_data_type,
                    'Base Type': data_type,
                    'Row Count': row_count,
                    'Distinct Values': distinct_count,
                    'Null Count': null_count,
                    'Null Percentage': round(null_percentage, 2),
                    'Max Length': col[2],
                    'Numeric Precision': col[3],
                    'Numeric Scale': col[4],
                    'Is Nullable': col[5],
                    'UDT Name': col[6],
                    'Character Set': col[7],
                    'Collation': col[8]
                })
            except Exception as e:
                # If there's an error with a specific column, log it and continue
                st.warning(f"Error processing column {col_name} in table {table_name}: {str(e)}")
                continue
    
    return pd.DataFrame(stats)

def main():
    st.set_page_config(layout="wide")
    st.title("Database Schema Explorer")

    # Load config
    db_config = load_db_config()
    db_type = db_config.pop("type", "postgres")
    schema = db_config.pop("schema", "public")
    conn = None
    objects = None

    try:
        if db_type == "postgres":
            import psycopg2
            conn = psycopg2.connect(**db_config)
        elif db_type == "mysql":
            import pymysql
            conn = pymysql.connect(**db_config)
        elif db_type == "mssql":
            import pyodbc
            conn = pyodbc.connect(
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={db_config['host']},{db_config['port']};"
                f"DATABASE={db_config['dbname']};"
                f"UID={db_config['user']};PWD={db_config['password']}"
            )
        elif db_type == "oracle":
            import oracledb
            dsn = f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
            conn = oracledb.connect(
                user=db_config['user'],
                password=db_config['password'],
                dsn=dsn
            )
        else:
            st.error(f"Unsupported database type: {db_type}")
            st.stop()

        # ✅ Fetch objects after connecting
        objects = get_all_tables_and_views(conn, schema)

        if not objects:
            st.warning(f"No tables/views found in schema '{schema}'")
            return

        # Sidebar: Add an option to switch between Table Analysis and Summary
        st.sidebar.header("Database Objects")
        app_mode = st.sidebar.radio("Choose Mode", ["Table Analysis", "Summary Statistics", "Detailed Statistics"])

        if app_mode == "Summary Statistics":
            show_all_tables_summary(conn, schema)
        elif app_mode == "Detailed Statistics":
            st.header("Detailed Database Statistics")
            
            if st.button("Generate Detailed Statistics"):
                with st.spinner("Generating detailed statistics..."):
                    df = generate_detailed_statistics(conn, schema)
                    
                    # Display the statistics
                    st.dataframe(df)
                    
                    # Create Excel file
                    buffer = io.BytesIO()
                    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                        df.to_excel(writer, sheet_name='Database Statistics', index=False)
                    
                    # Add download button
                    st.download_button(
                        label="Download Excel Report",
                        data=buffer.getvalue(),
                        file_name=f"database_statistics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
        else:  # Table Analysis
            selected = st.sidebar.selectbox(
                "Select table/view:",
                [obj[0] for obj in objects],
                format_func=lambda x: f"{x} ({next(obj[1] for obj in objects if obj[0] == x)})"
            )

            if st.sidebar.button("Analyze"):
                object_type = next(obj[1] for obj in objects if obj[0] == selected)
                from database.analysis import analyze_table  # Lazy import
                analyze_table(conn, schema, selected, object_type)

    except Exception as e:
        st.error(f"Database error: {str(e)}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main() 