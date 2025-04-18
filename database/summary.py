import configparser
from decimal import Decimal
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px







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
                    pg_size_pretty(pg_total_relation_size(%s)) AS table_size
                FROM "{schema}"."{table}"
            """, (f'"{schema}"."{table}"',))

            row_count, table_size = cursor.fetchone()

            # Get column count
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """, (schema, table))

            column_count = cursor.fetchone()[0]

            # Get numeric column count
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = %s 
                AND table_name = %s
                AND data_type IN ('integer','numeric','real','double precision','bigint')
            """, (schema, table))
            numeric_columns = cursor.fetchone()[0]

            summary_data.append({
                "Table": table,
                "Rows": row_count,
                "Size": table_size,
                "Columns": column_count,
                "Numeric Columns": numeric_columns
            })

    return pd.DataFrame(summary_data)


def show_all_tables_summary(conn, schema):
    st.title("Database Summary Report")

    with st.spinner("Generating database summary..."):
        # Get summary data
        summary_df = get_table_summary(conn, schema)

        # Show main summary table
        st.subheader("📋 All Tables Overview")
        # Calculate summary metrics
        total_size = summary_df["Size"].sum()
        total_rows = summary_df["Rows"].sum()
        avg_columns = summary_df["Columns"].mean()

        # Display metrics
        metric1, metric2, metric3 = st.columns(3)
        metric1.metric("Total Tables", len(summary_df))
        metric2.metric("Total Rows", f"{total_rows:,}")
        metric3.metric("Average Columns/Table", round(avg_columns, 1))
        st.dataframe(summary_df.style.format({
            "Rows": "{:,}",
            "Numeric Columns": "{:,}"
        }))

        # Show visualizations
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("📈 Tables by Size")
            fig = px.bar(summary_df.sort_values("Size", ascending=False),
                         x="Table", y="Rows",
                         color="Size",
                         title="Table Sizes (Rows)")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("🧮 Numeric Columns Distribution")
            fig = px.pie(summary_df, names="Table",
                         values="Numeric Columns",
                         title="Numeric Columns per Table")
            st.plotly_chart(fig, use_container_width=True)

        # Add expandable detailed metrics
        with st.expander("📊 Detailed Database Metrics"):
            st.subheader("Database Statistics")





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
