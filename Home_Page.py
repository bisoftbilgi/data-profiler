import streamlit as st
st.set_page_config(layout="wide")

from database.utils import check_connection

def main():
    # Check connection and redirect if needed
    if check_connection():
        # If everything is OK, redirect to Database Explorer
        st.switch_page("pages/Database_Explorer.py")
    return

if __name__ == "__main__":
    main()
