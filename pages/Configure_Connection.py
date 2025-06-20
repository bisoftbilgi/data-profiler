import os
from decimal import Decimal

import configparser
import streamlit as st

<<<<<<< HEAD
def test_connection(db_type, host, port, dbname, user, password):
    try:
        if db_type == "postgres":
            import psycopg2
            conn = psycopg2.connect(
                host=host,
                port=int(port),
                dbname=dbname,
                user=user,
                password=password
            )
            print("connection successful")
        elif db_type == "mysql":
            import pymysql
            conn = pymysql.connect(
                host=host,
                port=int(port),
                database=dbname,
                user=user,
                password=password
            )
        elif db_type == "mssql":
            import pyodbc
            conn = pyodbc.connect(
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={host},{port};"
                f"DATABASE={dbname};"
                f"UID={user};PWD={password}"
            )
        elif db_type == "oracle":
            import oracledb
            dsn = f"{host}:{port}/{dbname}"
            conn = oracledb.connect(
                user=user,
                password=password,
                dsn=dsn
            )
        else:
            return False, f"Unsupported database type: {db_type}"
        
        conn.close()
        return True, "Connection successful!"
    except Exception as e:
        return False, f"Connection failed: {str(e)}"
=======
>>>>>>> 0eb268a0608e7d53dccb44eb4326c005e3f13709

# Example config fields
st.title("Configure Connection Profile")

config_path = "profile.cfg"
config = configparser.ConfigParser()

<<<<<<< HEAD
db_type_options = ["postgres", "mysql", "mssql", "oracle"]

=======
>>>>>>> 0eb268a0608e7d53dccb44eb4326c005e3f13709
# Check if config exists
if os.path.exists(config_path):
    st.success("✅ Config file exists — you can edit it below.")
    config.read(config_path)
    db_config = config["database"]

<<<<<<< HEAD
    db_type_value = db_config.get("type", "")
    if db_type_value in db_type_options:
        db_type_index = db_type_options.index(db_type_value)
    else:
        db_type_index = -1
    db_type = st.selectbox(
        "Database Type",
        db_type_options,
        index=db_type_index if db_type_index >= 0 else 0,
        key="db_type_select"
    )
=======
    db_type = st.selectbox("Database Type", ["postgres", "mysql", "mssql", "oracle"])
>>>>>>> 0eb268a0608e7d53dccb44eb4326c005e3f13709
    host = st.text_input("Host", db_config.get("host", ""))
    port = st.text_input("Port", db_config.get("port", ""))
    dbname = st.text_input("Database Name", db_config.get("dbname", ""))
    user = st.text_input("Username", db_config.get("user", ""))
    password = st.text_input("Password", db_config.get("password", ""), type="password")
    schema = st.text_input("Schema", db_config.get("schema", ""))
<<<<<<< HEAD
else:
    st.warning("⚠️ Config file not found. Please create one.")
    db_type = st.selectbox(
        "Database Type",
        [""] + db_type_options,
        index=0,
        key="db_type_select"
    )
=======
    # Load and edit config (your logic here)
else:
    st.warning("⚠️ Config file not found. Please create one.")
    db_type = st.selectbox("Database Type", ["postgres", "mysql", "mssql", "oracle"])
>>>>>>> 0eb268a0608e7d53dccb44eb4326c005e3f13709
    host = st.text_input("Host")
    port = st.text_input("Port")
    dbname = st.text_input("Database Name")
    user = st.text_input("Username")
    password = st.text_input("Password", type="password")
    schema = st.text_input("Schema")

# Form to save
with st.form("save_config_form"):
    st.markdown("### 💾 Save & Test Connection")
    submitted = st.form_submit_button("Save and Continue")

    if submitted:
        config["database"] = {
            "type": db_type,
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
            "schema": schema,
        }

        with open(config_path, "w") as configfile:
            config.write(configfile)

        st.success("✅ Configuration saved successfully!")

<<<<<<< HEAD
# Test Connection Button
if st.button("🔍 Test Connection"):
    if not all([host, port, dbname, user, password]):
        st.error("Please fill in all connection details first.")
    else:
        with st.spinner("Testing connection..."):
            success, message = test_connection(db_type, host, port, dbname, user, password)
            if success:
                st.success(message)
            else:
                st.error(message)
=======
        # Optionally, test the connection here...
>>>>>>> 0eb268a0608e7d53dccb44eb4326c005e3f13709
