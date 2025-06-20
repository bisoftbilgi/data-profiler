import streamlit as st
import pandas as pd
import psycopg2
from decimal import Decimal
from Home_Page import load_db_config, decimal_to_float


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
        st.write("Debug: Summary DF bos degil", summary_df)

        # Show main summary table
        st.subheader("📋 All Tables Overview")
        st.dataframe(summary_df.style.format({
            "Rows": "{:,}",
            "Numeric Columns": "{:,}"
        }))

        # Show visualizations
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("📈 Tables by Size")
            # Sort and prepare data for top 5
            sorted_df = summary_df.sort_values("Rows", ascending=False)
            top_5 = sorted_df.head(5)
            other_rows = sorted_df.iloc[5:]["Rows"].sum()
            other_size = sorted_df.iloc[5:]["Size"].iloc[0]  # Take first size as representative
            
            # Create new dataframe for visualization
            viz_df = pd.DataFrame({
                "Table": list(top_5["Table"]) + ["Diğer"],
                "Rows": list(top_5["Rows"]) + [other_rows],
                "Size": list(top_5["Size"]) + [other_size]
            })
            
            fig = px.bar(viz_df,
                         x="Table", y="Rows",
                         color="Size",
                         title="Table Sizes (Rows) - Top 5 + Diğer")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("🧮 Numeric Columns Distribution")
            # Sort and prepare data for top 5
            sorted_df = summary_df.sort_values("Numeric Columns", ascending=False)
            top_5 = sorted_df.head(5)
            other_columns = sorted_df.iloc[5:]["Numeric Columns"].sum()
            
            # Create new dataframe for visualization
            viz_df = pd.DataFrame({
                "Table": list(top_5["Table"]) + ["Diğer"],
                "Numeric Columns": list(top_5["Numeric Columns"]) + [other_columns]
            })
            
            fig = px.pie(viz_df, 
                         names="Table",
                         values="Numeric Columns",
                         title="Numeric Columns per Table - Top 5 + Diğer")
            st.plotly_chart(fig, use_container_width=True)

        # Add expandable detailed metrics
        with st.expander("📊 Detailed Database Metrics"):
            st.subheader("Database Statistics")

            # Calculate summary metrics
            total_size = summary_df["Size"].sum()
            total_rows = summary_df["Rows"].sum()
            avg_columns = summary_df["Columns"].mean()

            # Display metrics
            metric1, metric2, metric3 = st.columns(3)
            metric1.metric("Total Tables", len(summary_df))
            metric2.metric("Total Rows", f"{total_rows:,}")
            metric3.metric("Average Columns/Table", round(avg_columns, 1))

            # Add new metrics for extremes
            st.subheader("Table Extremes")

            # Most/Least Columns
            most_cols = summary_df.loc[summary_df['Columns'].idxmax()]
            least_cols = summary_df.loc[summary_df['Columns'].idxmin()]
            col1, col2 = st.columns(2)
            col1.metric("Table with Most Columns",
                        f"{most_cols['Table']} ({most_cols['Columns']} columns)")
            col2.metric("Table with Least Columns",
                        f"{least_cols['Table']} ({least_cols['Columns']} columns)")

            # Most/Least Rows
            most_rows = summary_df.loc[summary_df['Rows'].idxmax()]
            least_rows = summary_df.loc[summary_df['Rows'].idxmin()]
            col3, col4 = st.columns(2)
            col3.metric("Table with Most Rows",
                        f"{most_rows['Table']} ({most_rows['Rows']:,} rows)")
            col4.metric("Table with Least Rows",
                        f"{least_rows['Table']} ({least_rows['Rows']:,} rows)")

            # Most/Least Size
            # Convert size to bytes for comparison
            summary_df['Size_Bytes'] = summary_df['Size'].apply(lambda x: float(x.split()[0]) * (
                1024 if 'KB' in x else (1024 ** 2 if 'MB' in x else (1024 ** 3 if 'GB' in x else 1))))
            most_size = summary_df.loc[summary_df['Size_Bytes'].idxmax()]
            least_size = summary_df.loc[summary_df['Size_Bytes'].idxmin()]
            col5, col6 = st.columns(2)
            col5.metric("Largest Table",
                        f"{most_size['Table']} ({most_size['Size']})")
            col6.metric("Smallest Table",
                        f"{least_size['Table']} ({least_size['Size']})")

            # Show raw data
            st.write("Raw summary data:")
            st.dataframe(summary_df)


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