import streamlit as st

# Set page config must be the first Streamlit command
st.set_page_config(layout="wide")

from database.utils import check_connection

def main():
    # Check connection and redirect if needed
    if check_connection():
        # If everything is OK, redirect to Database Explorer
        st.switch_page("pages/Database_Explorer.py")
    else:
        # Redirect to Configure_Connection page
        st.switch_page("pages/Configure_Connection.py")

if __name__ == "__main__":
    main()
