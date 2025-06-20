import streamlit as st
<<<<<<< HEAD
st.set_page_config(layout="wide")

from database.utils import check_connection

def main():
    # Check connection and redirect if needed
    if check_connection():
        # If everything is OK, redirect to Database Explorer
        st.switch_page("pages/Database_Explorer.py")
=======

def main():
    st.set_page_config(layout="wide")

    # Redirect to Configure_Connection page
    st.switch_page("pages/Configure_Connection.py")
>>>>>>> 0eb268a0608e7d53dccb44eb4326c005e3f13709
    return

if __name__ == "__main__":
    main()
