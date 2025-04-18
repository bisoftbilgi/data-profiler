import streamlit as st
from database.analysis import get_all_tables_and_views
from database.summary import show_all_tables_summary
from database.utils import load_db_config
from decimal import Decimal



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
        app_mode = st.sidebar.radio("Choose Mode", ["Table Analysis", "Summary Statistics"])

        if app_mode == "Summary Statistics":
            show_all_tables_summary(conn, schema)
        else:
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
