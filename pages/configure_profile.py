import os

import configparser
import streamlit as st


# Example config fields
st.title("Configure Connection Profile")

config_path = "profile.cfg"
config = configparser.ConfigParser()

# Check if config exists
if os.path.exists(config_path):
    st.success("✅ `profile.cfg` exists — you can edit it below.")
    config.read(config_path)
    db_config = config["database"]

    db_type = st.selectbox("Database Type", ["postgres", "mysql", "mssql", "oracle"])
    host = st.text_input("Host", db_config.get("host", ""))
    port = st.text_input("Port", db_config.get("port", ""))
    dbname = st.text_input("Database Name", db_config.get("dbname", ""))
    user = st.text_input("Username", db_config.get("user", ""))
    password = st.text_input("Password", db_config.get("password", ""), type="password")
    schema = st.text_input("Schema", db_config.get("schema", ""))
    # Load and edit config (your logic here)
else:
    st.warning("⚠️ `profile.cfg` not found. Please create one.")
    db_type = st.selectbox("Database Type", ["postgres", "mysql", "mssql", "oracle"])
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

        # Optionally, test the connection here...
