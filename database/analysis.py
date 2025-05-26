import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from decimal import Decimal
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
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        columns_info = cursor.fetchall()

        st.header(f"{object_type}: {schema}.{table}")
        with st.expander("📊 Table Metadata"):
            if object_type == 'Table':
                cursor.execute(f"""
                    SELECT 
                        COUNT(*) AS row_count, 
                        pg_size_pretty(pg_total_relation_size(%s)) AS table_size,
                        pg_size_pretty(AVG(pg_column_size(t.*))::bigint) as avg_row_width
                    FROM "{schema}"."{table}" t
                """, (f'"{schema}"."{table}"',))
                row_count, table_size, avg_row_width = cursor.fetchone()
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Rows", row_count)
                col2.metric("Size", table_size)
                col3.metric("Columns", len(columns_info))
                col4.metric("Avg Row Width", avg_row_width)
            else:
                cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                row_count = cursor.fetchone()[0]
                col1, col2 = st.columns(2)
                col1.metric("Total Rows", row_count)
                col2.metric("Columns", len(columns_info))
        st.write("---")
        for col_info in columns_info:
            col_analysis(conn, schema, table, col_info)
            st.write("---")
    finally:
        cursor.close()

def col_analysis(conn, schema, table, col_info):
    cursor = conn.cursor()
    column = col_info[0]
    data_type = col_info[1]
    
    # Format data type with restrictions
    formatted_data_type = data_type
    if col_info[2] is not None:  # character_maximum_length
        formatted_data_type = f"{data_type}({col_info[2]})"
    elif col_info[3] is not None:  # numeric_precision
        if col_info[4] is not None:  # numeric_scale
            formatted_data_type = f"{data_type}({col_info[3]},{col_info[4]})"
        else:
            formatted_data_type = f"{data_type}({col_info[3]})"
    
    # Add character set and collation if available
    if col_info[7] is not None:  # character_set_name
        formatted_data_type += f" CHARACTER SET {col_info[7]}"
    if col_info[8] is not None:  # collation_name
        formatted_data_type += f" COLLATE {col_info[8]}"
    
    st.subheader(f"Column: {column} ({formatted_data_type})")
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
                    # Regular histogram
                    fig1 = px.histogram(df, x=column, nbins=10, title=f"Regular Histogram of {column}")
                    fig1.update_layout(bargap=0.2)
                    st.plotly_chart(fig1, use_container_width=True)
                    
                    # Hyper-balanced histogram
                    data = df[column].values
                    # Calculate optimal number of bins using Freedman-Diaconis rule
                    iqr = np.percentile(data, 75) - np.percentile(data, 25)
                    bin_width = 2 * iqr * len(data) ** (-1/3)
                    num_bins = int(np.ceil((data.max() - data.min()) / bin_width))
                    
                    # Create balanced bins
                    bins = np.linspace(data.min(), data.max(), num_bins + 1)
                    hist, bin_edges = np.histogram(data, bins=bins)
                    
                    # Create balanced histogram
                    balanced_hist = np.zeros_like(hist)
                    total_samples = len(data)
                    samples_per_bin = total_samples / num_bins
                    
                    for i in range(len(hist)):
                        if hist[i] > 0:
                            balanced_hist[i] = samples_per_bin
                    
                    # Create the plot
                    fig2 = px.bar(
                        x=bin_edges[:-1],
                        y=balanced_hist,
                        title=f"Hyper-Balanced Histogram of {column}",
                        labels={'x': column, 'y': 'Balanced Count'}
                    )
                    fig2.update_layout(
                        showlegend=False,
                        xaxis_title=column,
                        yaxis_title="Balanced Count"
                    )
                    st.plotly_chart(fig2, use_container_width=True)
                    
                    st.write("""
                    **Hyper-Balanced Histogram Explanation:**
                    - This visualization shows a balanced distribution of data points
                    - Each bin contains an equal number of samples
                    - Useful for identifying patterns in skewed data
                    - Helps visualize the true distribution without being affected by outliers
                    """)
                with viz_tab2:
                    fig = px.box(df, y=column, points="all", title=f"Value Distribution of {column}")
                    st.plotly_chart(fig, use_container_width=True)
                st.write("### Value Composition")
                st.dataframe(df[column].value_counts().reset_index().head(10))
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
