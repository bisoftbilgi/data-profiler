import psycopg2
import configparser
import streamlit as st
import pandas as pd
import plotly.express as px
from decimal import Decimal
from database_summary import show_all_tables_summary  # Assuming you have this in a separate file

#bu dosya yoksa bir sayfaya gidilmeli ve bu profile.cfg sayfası doldurulmalı,
#profile.cfg dosyası varsa edit profile şeklinde configi editleyebilmeliyiz
#mysql
#postgres sql server mysql oracle
# Function to load database configuration from the .cfg file
def load_db_config(filename="profile.cfg", section="database"):
    config = configparser.ConfigParser()
    config.read(filename)

    if section in config:
        return {key: value for key, value in config[section].items()}
    else:
        raise Exception(f"Section '{section}' not found in the {filename} file.")


# Helper function to convert Decimal to float safely
def decimal_to_float(value):
    if isinstance(value, Decimal):
        return float(value)
    return value


# Function to get all tables and views in the schema
def get_all_tables_and_views(conn, schema):
    with conn.cursor() as cursor:
        # Get regular tables
        cursor.execute("""
            SELECT table_name, 'Table' as object_type 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """, (schema,))
        tables = cursor.fetchall()

        # Get views
        cursor.execute("""
            SELECT table_name, 'View' as object_type 
            FROM information_schema.views 
            WHERE table_schema = %s
            ORDER BY table_name
        """, (schema,))
        views = cursor.fetchall()

        return tables + views


# Function to analyze a specific table or view
def analyze_table(conn, schema, table_name, object_type):
    cursor = conn.cursor()

    try:
        # Display object header with type
        st.header(f"{object_type}: {schema}.{table_name}")
        st.markdown(f"**Object Type:** {object_type}")

        # Retrieve column info for the selected table/view
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table_name))

        columns_info = cursor.fetchall()

        # Create expandable section for table metadata
        with st.expander("📊 Object Metadata"):
            if object_type == 'Table':
                cursor.execute(f"""
                    SELECT COUNT(*) AS row_count, 
                           pg_size_pretty(pg_total_relation_size(%s)) AS table_size
                    FROM "{schema}"."{table_name}"
                """, (f'"{schema}"."{table_name}"',))
                row_count, table_size = cursor.fetchone()

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Rows", row_count)
                with col2:
                    st.metric("Table Size", table_size)
                with col3:
                    st.metric("Total Columns", len(columns_info))
            else:
                cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
                row_count = cursor.fetchone()[0]

                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Rows", row_count)
                with col2:
                    st.metric("Total Columns", len(columns_info))

        st.write("---")

        # Process each column
        for column, data_type in columns_info:
            st.subheader(f"Column: {column} ({data_type})")

            # Create tabs for different views
            tab1, tab2 = st.tabs(["📈 Statistics", "🔍 Data Distribution"])

            with tab1:
                # Get comprehensive statistics for all columns
                if data_type in ["integer", "numeric", "real", "double precision", "bigint"]:
                    query = f"""
                        SELECT 
                            COUNT(*) AS total_rows,
                            SUM(CASE WHEN "{column}" IS NULL THEN 1 ELSE 0 END)::float / COUNT(*) AS null_ratio,
                            COUNT(DISTINCT "{column}") AS distinct_count,
                            COUNT(DISTINCT "{column}")::float / NULLIF(COUNT("{column}"), 0) * 100 AS distinct_ratio,
                            (
                                SELECT COUNT(*)
                                FROM (
                                    SELECT "{column}"
                                    FROM "{schema}"."{table_name}"
                                    WHERE "{column}" IS NOT NULL
                                    GROUP BY "{column}"
                                    HAVING COUNT("{column}") = 1
                                ) AS unique_subquery  
                            )::float / COUNT(*) * 100 AS unique_count_ratio,
                            MIN("{column}") AS min_value,
                            MAX("{column}") AS max_value,
                            AVG("{column}") AS mean_value,
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{column}") AS median_value,
                            STDDEV("{column}") AS stddev_value
                        FROM "{schema}"."{table_name}"
                    """
                else:
                    query = f"""
                        SELECT 
                            COUNT(*) AS total_rows,
                            SUM(CASE WHEN "{column}" IS NULL THEN 1 ELSE 0 END)::float / COUNT(*) AS null_ratio,
                            COUNT(DISTINCT "{column}") AS distinct_count,
                            COUNT(DISTINCT "{column}")::float / NULLIF(COUNT("{column}"), 0) * 100 AS distinct_ratio,
                            (
                                SELECT COUNT(*)
                                FROM (
                                    SELECT "{column}"
                                    FROM "{schema}"."{table_name}"
                                    WHERE "{column}" IS NOT NULL
                                    GROUP BY "{column}"
                                    HAVING COUNT("{column}") = 1
                                ) AS unique_subquery  
                            )::float / COUNT(*) * 100 AS unique_count_ratio,
                            NULL AS min_value,
                            NULL AS max_value,
                            NULL AS mean_value,
                            NULL AS median_value,
                            NULL AS stddev_value
                        FROM "{schema}"."{table_name}"
                    """

                cursor.execute(query)
                stats = cursor.fetchone()

                # Convert Decimal values to float for display
                stats = [decimal_to_float(val) if val is not None else None for val in stats]

                # Create metrics display
                cols = st.columns(4)
                with cols[0]:
                    st.metric("Total Rows", int(stats[0]))
                    st.metric("Null Ratio", f"{stats[1] * 100:.2f}%" if stats[1] is not None else "N/A")
                with cols[1]:
                    st.metric("Distinct Values", int(stats[2]))
                    st.metric("Distinct Ratio", f"{stats[3]:.2f}%" if stats[3] is not None else "N/A")
                with cols[2]:
                    st.metric("Unique Value Ratio", f"{stats[4]:.2f}%" if stats[4] is not None else "N/A")
                    if data_type in ["integer", "numeric", "real", "double precision", "bigint"]:
                        st.metric("Std Dev", f"{stats[9]:.2f}" if stats[9] is not None else "N/A")
                with cols[3]:
                    if data_type in ["integer", "numeric", "real", "double precision", "bigint"]:
                        st.metric("Min Value", f"{stats[5]}" if stats[5] is not None else "N/A")
                        st.metric("Max Value", f"{stats[6]}" if stats[6] is not None else "N/A")
                        st.metric("Mean", f"{stats[7]:.2f}" if stats[7] is not None else "N/A")
                        st.metric("Median", f"{stats[8]:.2f}" if stats[8] is not None else "N/A")

            with tab2:
                # Get data for visualization
                if data_type in ["integer", "numeric", "real", "double precision", "bigint"]:
                    # For numeric columns
                    query = f'SELECT "{column}" FROM "{schema}"."{table_name}" WHERE "{column}" IS NOT NULL'
                    cursor.execute(query)
                    data = [decimal_to_float(row[0]) for row in cursor.fetchall()]

                    if data:
                        df = pd.DataFrame(data, columns=[column])

                        # Create tabs for different visualizations
                        viz_tab1, viz_tab2 = st.tabs(["📊 Histogram", "📦 Box Plot"])

                        with viz_tab1:
                            fig = px.histogram(df, x=column, nbins=10,
                                               title=f"Distribution of {column}",
                                               labels={column: column},
                                               color_discrete_sequence=['#1f77b4'])
                            fig.update_layout(bargap=0.1)
                            st.plotly_chart(fig, use_container_width=True)

                        with viz_tab2:
                            fig = px.box(df, y=column, points="all",
                                         title=f"Value Distribution of {column}")
                            st.plotly_chart(fig, use_container_width=True)

                        # Show value composition
                        st.write("### Value Composition")
                        value_counts = df[column].value_counts().reset_index()
                        value_counts.columns = ['Value', 'Count']
                        st.dataframe(value_counts.sort_values('Count', ascending=False).head(20))

                else:
                    # For categorical columns
                    query = f"""
                        SELECT "{column}", COUNT(*) as count 
                        FROM "{schema}"."{table_name}" 
                        WHERE "{column}" IS NOT NULL
                        GROUP BY "{column}" 
                        ORDER BY count DESC
                        LIMIT 50
                    """
                    cursor.execute(query)
                    value_counts = cursor.fetchall()

                    if value_counts:
                        df = pd.DataFrame(value_counts, columns=["Value", "Count"])
                        df = df.applymap(decimal_to_float)

                        if len(df) > 5:
                            top5 = df.head(5)
                            others = pd.DataFrame({
                                "Value": ["Others"],
                                "Count": [df["Count"][5:].sum()]
                            })
                            df = pd.concat([top5, others])

                            # Create tabs for different views
                        viz_tab1, viz_tab2 = st.tabs(["📊 Bar Chart", "📋 Value Table"])

                        with viz_tab1:
                            fig = px.bar(df, x="Value", y="Count",
                                         title=f"Top Values in {column}",
                                         labels={"Value": column, "Count": "Frequency"},
                                         color="Count",
                                         color_continuous_scale='Blues')
                            fig.update_layout(xaxis={'categoryorder': 'total descending'})
                            st.plotly_chart(fig, use_container_width=True)

                        with viz_tab2:
                            st.dataframe(df)

                        # Show value distribution pie chart
                        if len(df) > 0:
                            st.write("### Value Distribution")
                            fig = px.pie(df, names="Value", values="Count",
                                         title=f"Value Distribution of {column}",
                                         hover_data=['Count'])
                            st.plotly_chart(fig, use_container_width=True)

    finally:
        cursor.close()


# Main application
def main():
    st.set_page_config(layout="wide")
    st.title("Database Schema Explorer")

    # Load database config
    db_config = load_db_config()
    schema = db_config.pop("schema", "public")  # Default schema is 'public'

    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(**db_config)

        # Get all tables and views in the schema
        objects = get_all_tables_and_views(conn, schema)

        if not objects:
            st.warning(f"No tables or views found in schema '{schema}'")
            return

        # Create a DataFrame for better display
        objects_df = pd.DataFrame(objects, columns=["Name", "Type"])

        # Table selection
        st.sidebar.header("Database Objects")

        # Search/filter functionality
        search_term = st.sidebar.text_input("Search objects:")
        if search_term:
            objects_df = objects_df[objects_df['Name'].str.contains(search_term, case=False)]

        # Show object count
        st.sidebar.markdown(f"**Total Objects:** {len(objects_df)}")

        # Display objects in sidebar with type indicators
        selected_object = st.sidebar.selectbox(
            "Select an object to analyze:",
            objects_df['Name'],
            format_func=lambda x: f"{x} ({objects_df[objects_df['Name'] == x]['Type'].values[0]})",
            index=0
        )

        # ====== NEW SUMMARY FUNCTIONALITY ======
        # Add summary button above the analyze button
        if st.sidebar.button("📊 Show All Tables Summary"):
            st.session_state['show_summary'] = True

        # Get the selected object's type
        object_type = objects_df[objects_df['Name'] == selected_object]['Type'].values[0]

        if st.sidebar.button("Analyze Object"):
            st.session_state['show_summary'] = False

        # Check which view to show
        if st.session_state.get('show_summary', False):
            show_all_tables_summary(conn, schema)
        else:
            with st.spinner(f"Analyzing {object_type.lower()} {selected_object}..."):
                analyze_table(conn, schema, selected_object, object_type)
        # ====== END NEW FUNCTIONALITY ======

    except Exception as e:
        st.error(f"Database error: {str(e)}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()


if __name__ == "__main__":
    main()