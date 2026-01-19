import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta

session = get_active_session()

st.title("Query Performance")

days_back = st.selectbox("Time Period", [7, 14, 30], index=0, format_func=lambda x: f"Last {x} days")

end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)

@st.cache_data(ttl=3600, show_spinner=False)
def get_query_metrics(_session, start, end):
    query = f"""
    SELECT 
        COUNT(*) as TOTAL_QUERIES,
        COUNT_IF(EXECUTION_STATUS = 'SUCCESS') as SUCCESSFUL,
        COUNT_IF(EXECUTION_STATUS != 'SUCCESS') as FAILED,
        ROUND(AVG(TOTAL_ELAPSED_TIME) / 1000, 2) as AVG_DURATION_SECS,
        ROUND(MEDIAN(TOTAL_ELAPSED_TIME) / 1000, 2) as MEDIAN_DURATION_SECS,
        ROUND(MAX(TOTAL_ELAPSED_TIME) / 1000, 2) as MAX_DURATION_SECS,
        ROUND(SUM(BYTES_SCANNED) / POWER(1024, 4), 2) as TB_SCANNED
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= '{start}' AND START_TIME < '{end}'
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=3600, show_spinner=False)
def get_daily_query_volume(_session, start, end):
    query = f"""
    SELECT 
        DATE_TRUNC('DAY', START_TIME)::DATE as QUERY_DATE,
        COUNT(*) as QUERY_COUNT,
        COUNT_IF(EXECUTION_STATUS = 'SUCCESS') as SUCCESS_COUNT,
        COUNT_IF(EXECUTION_STATUS != 'SUCCESS') as FAILED_COUNT
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= '{start}' AND START_TIME < '{end}'
    GROUP BY 1
    ORDER BY 1
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=3600, show_spinner=False)
def get_expensive_queries(_session, start, end):
    query = f"""
    SELECT 
        QUERY_ID,
        USER_NAME,
        WAREHOUSE_NAME,
        QUERY_TYPE,
        ROUND(TOTAL_ELAPSED_TIME / 1000, 1) as DURATION_SECS,
        ROUND(BYTES_SCANNED / POWER(1024, 3), 2) as GB_SCANNED,
        ROUND(CREDITS_USED_CLOUD_SERVICES, 4) as CS_CREDITS,
        LEFT(QUERY_TEXT, 100) as QUERY_PREVIEW
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= '{start}' AND START_TIME < '{end}'
        AND BYTES_SCANNED > 0
    ORDER BY BYTES_SCANNED DESC
    LIMIT 20
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=3600, show_spinner=False)
def get_slow_queries(_session, start, end, threshold_secs):
    query = f"""
    SELECT 
        QUERY_ID,
        USER_NAME,
        WAREHOUSE_NAME,
        QUERY_TYPE,
        ROUND(TOTAL_ELAPSED_TIME / 1000, 1) as DURATION_SECS,
        ROUND(COMPILATION_TIME / 1000, 1) as COMPILE_SECS,
        ROUND(EXECUTION_TIME / 1000, 1) as EXEC_SECS,
        LEFT(QUERY_TEXT, 100) as QUERY_PREVIEW
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= '{start}' AND START_TIME < '{end}'
        AND TOTAL_ELAPSED_TIME > {threshold_secs * 1000}
        AND EXECUTION_STATUS = 'SUCCESS'
    ORDER BY TOTAL_ELAPSED_TIME DESC
    LIMIT 20
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=3600, show_spinner=False)
def get_failed_queries(_session, start, end):
    query = f"""
    SELECT 
        QUERY_ID,
        USER_NAME,
        WAREHOUSE_NAME,
        ERROR_CODE,
        CASE
            WHEN ERROR_MESSAGE ILIKE 'Statement reached its statement or warehouse timeout%' THEN 'Timeout'
            WHEN ERROR_MESSAGE ILIKE 'SQL execution canceled%' THEN 'Canceled'
            WHEN ERROR_MESSAGE ILIKE 'SQL compilation error%' THEN 'Compilation'
            WHEN ERROR_MESSAGE ILIKE '%access control%' THEN 'Access Control'
            ELSE 'Other'
        END as ERROR_TYPE,
        LEFT(ERROR_MESSAGE, 100) as ERROR_PREVIEW,
        START_TIME
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= '{start}' AND START_TIME < '{end}'
        AND EXECUTION_STATUS != 'SUCCESS'
    ORDER BY START_TIME DESC
    LIMIT 50
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=3600, show_spinner=False)
def get_query_by_type(_session, start, end):
    query = f"""
    SELECT 
        QUERY_TYPE,
        COUNT(*) as QUERY_COUNT,
        ROUND(AVG(TOTAL_ELAPSED_TIME) / 1000, 2) as AVG_DURATION_SECS,
        ROUND(SUM(BYTES_SCANNED) / POWER(1024, 3), 2) as TOTAL_GB_SCANNED
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= '{start}' AND START_TIME < '{end}'
    GROUP BY 1
    ORDER BY 2 DESC
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=3600, show_spinner=False)
def get_query_by_warehouse(_session, start, end):
    query = f"""
    SELECT 
        COALESCE(WAREHOUSE_NAME, 'Cloud Services') as WAREHOUSE_NAME,
        COUNT(*) as QUERY_COUNT,
        ROUND(AVG(TOTAL_ELAPSED_TIME) / 1000, 2) as AVG_DURATION_SECS
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= '{start}' AND START_TIME < '{end}'
    GROUP BY 1
    ORDER BY 2 DESC
    """
    return _session.sql(query).to_pandas()

with st.spinner("Loading query metrics..."):
    metrics = get_query_metrics(session, start_date, end_date)
    daily_volume = get_daily_query_volume(session, start_date, end_date)
    by_type = get_query_by_type(session, start_date, end_date)
    by_warehouse = get_query_by_warehouse(session, start_date, end_date)

if not metrics.empty:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Queries", f"{metrics['TOTAL_QUERIES'].iloc[0]:,}")
    with col2:
        success_rate = (metrics['SUCCESSFUL'].iloc[0] / metrics['TOTAL_QUERIES'].iloc[0] * 100) if metrics['TOTAL_QUERIES'].iloc[0] > 0 else 0
        st.metric("Success Rate", f"{success_rate:.1f}%")
    with col3:
        st.metric("Avg Duration", f"{metrics['AVG_DURATION_SECS'].iloc[0]:.1f}s")
    with col4:
        st.metric("Data Scanned", f"{metrics['TB_SCANNED'].iloc[0]:.2f} TB")

st.markdown("---")

st.subheader("Daily Query Volume")
if not daily_volume.empty:
    volume_melted = daily_volume.melt(id_vars=['QUERY_DATE'], value_vars=['SUCCESS_COUNT', 'FAILED_COUNT'], var_name='Status', value_name='Count')
    volume_melted['Status'] = volume_melted['Status'].map({'SUCCESS_COUNT': 'Success', 'FAILED_COUNT': 'Failed'})
    chart = alt.Chart(volume_melted).mark_line(strokeWidth=2).encode(
        x=alt.X('QUERY_DATE:T', title='Date', axis=alt.Axis(format='%b %d')),
        y=alt.Y('Count:Q', title='Query Count'),
        color=alt.Color('Status:N', scale=alt.Scale(domain=['Success', 'Failed'], range=['#29B5E8', '#E74C3C']))
    ).properties(height=250)
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("No query data")

st.markdown("---")

col1, col2 = st.columns(2)

with col1:
    st.subheader("By Query Type")
    if not by_type.empty:
        chart = alt.Chart(by_type.head(10)).mark_bar(color='#29B5E8').encode(
            x=alt.X('QUERY_COUNT:Q', title='Query Count'),
            y=alt.Y('QUERY_TYPE:N', title='', sort='-x')
        ).properties(height=300)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No data")

with col2:
    st.subheader("By Warehouse")
    if not by_warehouse.empty:
        chart = alt.Chart(by_warehouse.head(10)).mark_bar(color='#29B5E8').encode(
            x=alt.X('QUERY_COUNT:Q', title='Query Count'),
            y=alt.Y('WAREHOUSE_NAME:N', title='', sort='-x')
        ).properties(height=300)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No data")

st.markdown("---")

tab1, tab2, tab3 = st.tabs(["Expensive Queries", "Slow Queries", "Failed Queries"])

with tab1:
    st.caption("Top queries by data scanned")
    with st.spinner("Loading expensive queries..."):
        expensive = get_expensive_queries(session, start_date, end_date)
    if not expensive.empty:
        st.dataframe(expensive, use_container_width=True)
    else:
        st.info("No expensive queries found")

with tab2:
    threshold = st.slider("Duration threshold (seconds)", 10, 300, 60)
    with st.spinner("Loading slow queries..."):
        slow = get_slow_queries(session, start_date, end_date, threshold)
    if not slow.empty:
        st.dataframe(slow, use_container_width=True)
    else:
        st.info(f"No queries slower than {threshold}s")

with tab3:
    with st.spinner("Loading failed queries..."):
        failed = get_failed_queries(session, start_date, end_date)
    if not failed.empty:
        error_counts = failed.groupby('ERROR_TYPE').size().reset_index(name='COUNT')
        chart = alt.Chart(error_counts).mark_bar(color='#E74C3C').encode(
            x=alt.X('COUNT:Q', title='Count'),
            y=alt.Y('ERROR_TYPE:N', title='', sort='-x')
        ).properties(height=150)
        st.altair_chart(chart, use_container_width=True)
        st.dataframe(failed, use_container_width=True)
    else:
        st.success("No failed queries!")

st.markdown("---")

st.subheader("Queries by Warehouse")
if not by_warehouse.empty:
    st.dataframe(by_warehouse, use_container_width=True)
