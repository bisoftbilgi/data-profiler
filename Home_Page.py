import streamlit as st

def main():
    st.set_page_config(layout="wide")

    # Redirect to Configure_Connection page
    st.switch_page("pages/Configure_Connection.py")
    return

if __name__ == "__main__":
    main()
