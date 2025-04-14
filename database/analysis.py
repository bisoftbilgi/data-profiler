import streamlit as st
import pandas as pd
import plotly.express as px
from database.utils import decimal_to_float



def get_all_tables_and_views(conn, schema):
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT table_name, 'Table' as object_type 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            UNION
            SELECT table_name, 'View' as object_type 
            FROM information_schema.views 
            WHERE table_schema = %s
            ORDER BY table_name
        """, (schema, schema))
        return cursor.fetchall()

def analyze_table(conn, schema, table, object_type):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        columns_info = cursor.fetchall()

        st.header(f"{object_type}: {schema}.{table}")
        with st.expander("📊 Table Metadata"):
            if object_type == 'Table':
                cursor.execute(f"""
                    SELECT COUNT(*) AS row_count, 
                           pg_size_pretty(pg_total_relation_size(%s)) AS table_size
                    FROM "{schema}"."{table}"
                """, (f'"{schema}"."{table}"',))
                row_count, table_size = cursor.fetchone()
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Rows", row_count)
                col2.metric("Size", table_size)
                col3.metric("Columns", len(columns_info))
            else:
                cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                row_count = cursor.fetchone()[0]
                col1, col2 = st.columns(2)
                col1.metric("Total Rows", row_count)
                col2.metric("Columns", len(columns_info))
        st.write("---")
        for column, data_type in columns_info:
            col_analysis(conn, schema, table, column, data_type)
            st.write("---")
    finally:
        cursor.close()

def col_analysis(conn, schema, table, column, data_type):
    cursor = conn.cursor()
    st.subheader(f"Column: {column} ({data_type})")
    tab1, tab2 = st.tabs(["📈 Statistics", "🔍 Data Distribution"])

    with tab1:
        if data_type in ["integer", "numeric", "real", "double precision", "bigint"]:
            query = f"""
                SELECT 
                    COUNT(*),
                    SUM(CASE WHEN "{column}" IS NULL THEN 1 ELSE 0 END)::float/COUNT(*),
                    COUNT(DISTINCT "{column}"),
                    COUNT(DISTINCT "{column}")::float/NULLIF(COUNT("{column}"),0)*100,
                    (SELECT COUNT(*) FROM (
                        SELECT "{column}" FROM "{schema}"."{table}"
                        WHERE "{column}" IS NOT NULL
                        GROUP BY "{column}" HAVING COUNT("{column}") = 1
                    ) AS unique_subquery)::float/COUNT(*)*100,
                    MIN("{column}"), MAX("{column}"), AVG("{column}"),
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{column}"),
                    STDDEV("{column}")
                FROM "{schema}"."{table}"
            """
        else:
            query = f"""
                SELECT 
                    COUNT(*),
                    SUM(CASE WHEN "{column}" IS NULL THEN 1 ELSE 0 END)::float/COUNT(*),
                    COUNT(DISTINCT "{column}"),
                    COUNT(DISTINCT "{column}")::float/NULLIF(COUNT("{column}"),0)*100,
                    (SELECT COUNT(*) FROM (
                        SELECT "{column}" FROM "{schema}"."{table}"
                        WHERE "{column}" IS NOT NULL
                        GROUP BY "{column}" HAVING COUNT("{column}") = 1
                    ) AS unique_subquery)::float/COUNT(*)*100,
                    NULL, NULL, NULL, NULL, NULL
                FROM "{schema}"."{table}"
            """

        cursor.execute(query)
        stats = [decimal_to_float(val) if val is not None else None for val in cursor.fetchone()]

        cols = st.columns(4)
        with cols[0]:
            st.metric("Total Rows", int(stats[0]))
            st.metric("Null Ratio", f"{stats[1]*100:.2f}%" if stats[1] else "N/A")
        with cols[1]:
            st.metric("Distinct Values", int(stats[2]))
            st.metric("Distinct Ratio", f"{stats[3]:.2f}%" if stats[3] else "N/A")
        with cols[2]:
            st.metric("Unique Ratio", f"{stats[4]:.2f}%" if stats[4] else "N/A")
            if data_type in ["integer", "numeric", "real", "double precision", "bigint"]:
                st.metric("Std Dev", f"{stats[9]:.2f}" if stats[9] else "N/A")
        with cols[3]:
            if data_type in ["integer", "numeric", "real", "double precision", "bigint"]:
                st.metric("Min", f"{stats[5]}" if stats[5] else "N/A")
                st.metric("Max", f"{stats[6]}" if stats[6] else "N/A")
                st.metric("Mean", f"{stats[7]:.2f}" if stats[7] else "N/A")
                st.metric("Median", f"{stats[8]:.2f}" if stats[8] else "N/A")

    with tab2:
        if data_type in ["integer", "numeric", "real", "double precision", "bigint"]:
            cursor.execute(f'SELECT "{column}" FROM "{schema}"."{table}" WHERE "{column}" IS NOT NULL')
            data = [decimal_to_float(row[0]) for row in cursor.fetchall()]
            if data:
                df = pd.DataFrame(data, columns=[column])
                viz_tab1, viz_tab2 = st.tabs(["📊 Histogram", "📦 Box Plot"])
                with viz_tab1:
                    fig = px.histogram(df, x=column, nbins=5, title=f"Distribution of {column}")
                    st.plotly_chart(fig, use_container_width=True)
                with viz_tab2:
                    fig = px.box(df, y=column, points="all", title=f"Value Distribution of {column}")
                    st.plotly_chart(fig, use_container_width=True)
                st.write("### Value Composition")
                st.dataframe(df[column].value_counts().reset_index().head(5))
        else:
            cursor.execute(f"""
                SELECT "{column}", COUNT(*) as count 
                FROM "{schema}"."{table}" 
                WHERE "{column}" IS NOT NULL
                GROUP BY "{column}" 
                ORDER BY count DESC
                LIMIT 5
            """)
            top5 = pd.DataFrame(cursor.fetchall(), columns=["Value", "Count"])
            if not top5.empty:
                cursor.execute(f"""
                    SELECT COUNT(DISTINCT "{column}") - 5,
                           SUM(count) FROM (
                        SELECT "{column}", COUNT(*) as count 
                        FROM "{schema}"."{table}"
                        WHERE "{column}" IS NOT NULL
                        GROUP BY "{column}"
                        ORDER BY count DESC
                        OFFSET 5
                    ) t
                """)
                others_count, others_sum = cursor.fetchone()
                if others_count > 0:
                    others_row = pd.DataFrame({
                        "Value": [f"Other {others_count} values"],
                        "Count": [others_sum]
                    })
                    df = pd.concat([top5, others_row])
                else:
                    df = top5
                viz_tab1, viz_tab2 = st.tabs(["📊 Bar Chart", "📋 Value Table"])
                with viz_tab1:
                    fig = px.bar(df, x="Value", y="Count", title=f"Top Values in {column}")
                    st.plotly_chart(fig, use_container_width=True)
                with viz_tab2:
                    st.dataframe(df)
                if len(df) > 1:
                    st.write("### Value Distribution")
                    fig = px.pie(df, names="Value", values="Count", title=f"Distribution of {column}")
                    st.plotly_chart(fig, use_container_width=True)
