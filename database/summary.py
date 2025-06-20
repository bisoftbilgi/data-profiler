import streamlit as st
import pandas as pd
import psycopg2
from decimal import Decimal
import plotly.express as px
from database.utils import load_db_config, decimal_to_float
import io
from datetime import datetime


def get_table_row_width(conn, schema, table):
    """Calculate the average row width for a table"""
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT pg_size_pretty(AVG(pg_column_size(t.*))::bigint) as avg_row_width
            FROM "{schema}"."{table}" t
        """)
        return cursor.fetchone()[0]


def get_table_summary(conn, schema):
    """Get summary statistics for all tables"""
    summary_data = []

    with conn.cursor() as cursor:
        # Get all tables
        cursor.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """, (schema,))
        tables = [row[0] for row in cursor.fetchall()]

        for table in tables:
            # Get basic table info
            cursor.execute(f"""
                SELECT 
                    COUNT(*) AS row_count,
                    pg_size_pretty(pg_total_relation_size(%s)) AS table_size,
                    COUNT(*) AS column_count
                FROM "{schema}"."{table}"
            """, (f'"{schema}"."{table}"',))
            row_count, table_size, column_count = cursor.fetchone()

            # Get numeric column count
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = %s 
                AND table_name = %s
                AND data_type IN ('integer','numeric','real','double precision','bigint')
            """, (schema, table))
            numeric_columns = cursor.fetchone()[0]

            # Get average row width
            avg_row_width = get_table_row_width(conn, schema, table)

            summary_data.append({
                "Table": table,
                "Rows": row_count,
                "Size": table_size,
                "Columns": column_count,
                "Numeric Columns": numeric_columns,
                "Avg Row Width": avg_row_width
            })

    return pd.DataFrame(summary_data)


def show_all_tables_summary(connector, schema):
    """Show summary statistics for all tables in the database"""
    try:
        # Get all tables
        tables = connector.get_all_tables_and_views(schema)
        
        # Create a list to store table statistics
        table_stats = []
        
        # Get statistics for each table
        for table in tables:
            table_name = table[0]
            table_type = table[1]
            
            # Get table analysis
            analysis = connector.get_table_analysis(schema, table_name)
            
            if analysis:
                table_stats.append({
                    'Table Name': table_name,
                    'Type': table_type,
                    'Row Count': analysis['row_count'],
                    'Total Size (MB)': analysis['total_size'],
                    'Table Size (MB)': analysis['table_size'],
                    'Index Size (MB)': analysis['index_size'],
                    'Avg Row Width (bytes)': analysis['avg_row_width'],
                    'Last Analyzed': analysis['last_analyzed'] if analysis['last_analyzed'] else 'Never'
                })
        
        # Convert to DataFrame
        if table_stats:
            df = pd.DataFrame(table_stats)
            
            # Display summary
            st.header("Database Summary")
            
            # Overall statistics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Tables", len(df))
                st.metric("Total Rows", df['Row Count'].sum())
            with col2:
                st.metric("Total Size", f"{df['Total Size (MB)'].sum():.2f} MB")
                st.metric("Average Table Size", f"{df['Table Size (MB)'].mean():.2f} MB")
            with col3:
                st.metric("Average Row Count", f"{df['Row Count'].mean():.0f}")
                st.metric("Average Row Width", f"{df['Avg Row Width (bytes)'].mean():.0f} bytes")
            
            # Table statistics
            st.subheader("Table Statistics")
            st.dataframe(df)
            
            # Create Excel file
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Table Statistics', index=False)
            
            # Add download button
            st.download_button(
                label="Download Excel Report",
                data=buffer.getvalue(),
                file_name=f"table_statistics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("No tables found in the database.")
            
    except Exception as e:
        st.error(f"Error getting table summary: {str(e)}")


# Main execution
if __name__ == "__main__":
    db_config = load_db_config()
    schema = db_config.pop("schema", "public")

    try:
        conn = psycopg2.connect(**db_config)
        show_all_tables_summary(conn, schema)
    except Exception as e:
        st.error(f"Database error: {str(e)}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()