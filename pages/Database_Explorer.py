import streamlit as st
<<<<<<< HEAD
from database.summary import show_all_tables_summary
from database.utils import load_db_config, check_connection
from database.quality import show_quality_tests_page
from database.db_factory import DatabaseFactory
=======
from database.analysis import get_all_tables_and_views
from database.summary import show_all_tables_summary
from database.utils import load_db_config
from database.quality import show_quality_tests_page
from decimal import Decimal
>>>>>>> 0eb268a0608e7d53dccb44eb4326c005e3f13709
import pandas as pd
import io
from datetime import datetime
import openpyxl

<<<<<<< HEAD
# Set page config must be the first Streamlit command
st.set_page_config(layout="wide")

def generate_detailed_statistics(connector, schema):
    """Generate detailed statistics for all tables and columns using connector methods only"""
    stats = []
    
    # Get all tables

    tables = connector.get_all_tables_and_views(schema)

=======
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
>>>>>>> 0eb268a0608e7d53dccb44eb4326c005e3f13709
    
    for table in tables:
        table_name = table[0]
        
<<<<<<< HEAD
        table_stats = connector.get_table_analysis(schema, table_name)
        
        if table_stats and 'columns' in table_stats:
            primary_keys = set(connector.get_primary_keys(schema, table_name))
            foreign_keys = connector.get_foreign_keys(schema, table_name)
            for col in table_stats['columns']:
                col_name = col[0]  # column_name
                data_type = col[1]  # data_type
                is_nullable = col[2]  # is_nullable
                max_length = col[3]  # max_length
                precision = col[4]  # precision
                scale = col[5]  # scale
                
                # Format data type with additional information
                formatted_type = data_type
                if max_length and max_length > 0:
                    formatted_type += f"({max_length})"
                elif precision and scale:
                    formatted_type += f"({precision},{scale})"
                
                # Get detailed column metrics
                col_details = connector.get_column_details(schema, table_name, col_name)
                metrics = col_details.get('metrics', {}) if col_details else {}
                def fmt(val):
                    from decimal import Decimal
                    if isinstance(val, (float, Decimal)):
                        return f"{val:.2f}"
                    return val
                is_pk = 'Yes' if col_name in primary_keys else 'No'
                is_fk = 'Yes' if col_name in foreign_keys else 'No'
                references = foreign_keys.get(col_name) if col_name in foreign_keys else None
                stats.append({
                    'Table Name': table_name,
                    'Column Name': col_name,
                    'Data Type': formatted_type,
                    'Is Nullable': 'Yes' if is_nullable else 'No',
                    'Is Primary Key': is_pk,
                    'Is Foreign Key': is_fk,
                    'References': references,
                    'Row Count': table_stats['row_count'],
                    'Total Size (MB)': table_stats['total_size'],
                    'Table Size (MB)': table_stats['table_size'],
                    'Index Size (MB)': table_stats['index_size'],
                    'Avg Row Width (bytes)': table_stats['avg_row_width'],
                    'Last Analyzed': table_stats['last_analyzed'] if table_stats['last_analyzed'] else 'Never',
                    'Distinct Count': col_details.get('distinct_count') if col_details else None,
                    'Null Count': col_details.get('null_count') if col_details else None,
                    'Unique Count': col_details.get('unique_count') if col_details else None,
                    'Min': fmt(metrics.get('min')) if 'min' in metrics else None,
                    'Max': fmt(metrics.get('max')) if 'max' in metrics else None,
                    'Avg': fmt(metrics.get('avg')) if 'avg' in metrics else None,
                    'Std Dev': fmt(metrics.get('std_dev')) if 'std_dev' in metrics else None,
                    'Median': fmt(metrics.get('median')) if 'median' in metrics else None,
                    'Min Length': fmt(metrics.get('min_length')) if 'min_length' in metrics else None,
                    'Max Length': fmt(metrics.get('max_length')) if 'max_length' in metrics else None,
                    'Avg Length': fmt(metrics.get('avg_length')) if 'avg_length' in metrics else None,
                    'Min Date': metrics.get('min_date') if 'min_date' in metrics else None,
                    'Max Date': metrics.get('max_date') if 'max_date' in metrics else None,
                })
        else:
            st.write(f"Debug - No columns found for table {table_name} or table_stats is None")
    
=======
        # Get column information with more detailed data type information
        cursor.execute(f"""
            SELECT 
                column_name, 
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                udt_name
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
                    'Max Length': col[2],
                    'Numeric Precision': col[3],
                    'Row Count': row_count,
                    'Distinct Values': distinct_count,
                    'Null Count': null_count,
                    'Null Percentage': round(null_percentage, 2),
                    'Numeric Scale': col[4],
                    'Is Nullable': col[5],
                    'UDT Name': col[6]
                })
            except Exception as e:
                # If there's an error with a specific column, log it and continue
                st.warning(f"Error processing column {col_name} in table {table_name}: {str(e)}")
                continue
>>>>>>> 0eb268a0608e7d53dccb44eb4326c005e3f13709
    
    return pd.DataFrame(stats)

def main():
<<<<<<< HEAD
    st.title("Database Schema Explorer")


    # Check connection and redirect if needed
    if not check_connection():
        return


    # Load config
    db_config = load_db_config()
    db_type = db_config.pop("type")
    schema = db_config.pop("schema")
    
    try:
        # Create database connector
        connector = DatabaseFactory.create_connector(db_type)
        connector.connect(db_config)
   
        
        # Get all tables and views
        objects = connector.get_all_tables_and_views(schema)
=======
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
>>>>>>> 0eb268a0608e7d53dccb44eb4326c005e3f13709

        if not objects:
            st.warning(f"No tables/views found in schema '{schema}'")
            return

        # Sidebar: Add an option to switch between Table Analysis and Summary
        st.sidebar.header("Tables / Views")
        app_mode = st.sidebar.radio("Choose Mode", ["Table Analysis", "Summary Statistics", "Detailed Statistics", "Quality Tests"])

        if app_mode == "Summary Statistics":
<<<<<<< HEAD
            show_all_tables_summary(connector, schema)
=======
            show_all_tables_summary(conn, schema)
>>>>>>> 0eb268a0608e7d53dccb44eb4326c005e3f13709
        elif app_mode == "Detailed Statistics":
            st.header("Detailed Database Statistics")
            
            if st.button("Generate Detailed Statistics"):
                with st.spinner("Generating detailed statistics..."):
<<<<<<< HEAD
                    df = generate_detailed_statistics(connector, schema)
=======
                    df = generate_detailed_statistics(conn, schema)
>>>>>>> 0eb268a0608e7d53dccb44eb4326c005e3f13709
                    
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
        elif app_mode == "Quality Tests":
<<<<<<< HEAD
            show_quality_tests_page(connector, schema)
=======
            show_quality_tests_page(conn, schema)
>>>>>>> 0eb268a0608e7d53dccb44eb4326c005e3f13709
        else:  # Table Analysis
            selected = st.sidebar.selectbox(
                "Select table/view:",
                [obj[0] for obj in objects],
                format_func=lambda x: f"{x} ({next(obj[1] for obj in objects if obj[0] == x)})"
            )

            if st.sidebar.button("Analyze"):
                object_type = next(obj[1] for obj in objects if obj[0] == selected)
<<<<<<< HEAD
                # Use connector-based analysis
                from database.analysis import analyze_table
                analyze_table(connector, schema, selected, object_type)
=======
                from database.analysis import analyze_table  # Lazy import
                analyze_table(conn, schema, selected, object_type)
>>>>>>> 0eb268a0608e7d53dccb44eb4326c005e3f13709

    except Exception as e:
        st.error(f"Database error: {str(e)}")
    finally:
<<<<<<< HEAD
        if 'connector' in locals():
            connector.close()
=======
        if conn:
            conn.close()
>>>>>>> 0eb268a0608e7d53dccb44eb4326c005e3f13709

if __name__ == "__main__":
    main() 