import streamlit as st

st.set_page_config(
    page_title="Snowflake Usage Insights",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Snowflake Usage Insights")
st.markdown("Navigate using the sidebar to explore your Snowflake consumption data.")

with st.sidebar:
    st.markdown("---")
    st.caption("Data from SNOWFLAKE.ACCOUNT_USAGE")
    st.caption("Latency: up to 3 hours")
