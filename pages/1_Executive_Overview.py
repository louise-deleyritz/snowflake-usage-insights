import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta

session = get_active_session()

st.title("Executive Overview")

days_back = st.selectbox("Time Period", [7, 14, 30, 60, 90], index=2, format_func=lambda x: f"Last {x} days")

end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
prev_start = start_date - timedelta(days=days_back)
prev_end = start_date

@st.cache_data(ttl=3600, show_spinner=False)
def get_credit_summary(_session, start, end, prev_start, prev_end):
    query = f"""
    WITH current_period AS (
        SELECT COALESCE(SUM(CREDITS_USED), 0) as total_credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE START_TIME >= '{start}' AND START_TIME < '{end}'
    ),
    previous_period AS (
        SELECT COALESCE(SUM(CREDITS_USED), 0) as total_credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE START_TIME >= '{prev_start}' AND START_TIME < '{prev_end}'
    )
    SELECT 
        c.total_credits as current_credits,
        p.total_credits as previous_credits
    FROM current_period c, previous_period p
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=3600, show_spinner=False)
def get_daily_credits(_session, start, end):
    query = f"""
    SELECT 
        DATE_TRUNC('DAY', START_TIME)::DATE as USAGE_DATE,
        ROUND(SUM(CREDITS_USED), 2) as CREDITS,
        ROUND(SUM(CREDITS_USED_CLOUD_SERVICES), 2) as CLOUD_SERVICES_CREDITS
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= '{start}' AND START_TIME < '{end}'
    GROUP BY 1
    ORDER BY 1
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=3600, show_spinner=False)
def get_warehouse_breakdown(_session, start, end):
    query = f"""
    SELECT 
        WAREHOUSE_NAME,
        ROUND(SUM(CREDITS_USED), 2) as CREDITS
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= '{start}' AND START_TIME < '{end}'
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=3600, show_spinner=False)
def get_query_summary(_session, start, end):
    query = f"""
    SELECT 
        COUNT(*) as TOTAL_QUERIES,
        COUNT_IF(EXECUTION_STATUS = 'SUCCESS') as SUCCESSFUL,
        COUNT_IF(EXECUTION_STATUS != 'SUCCESS') as FAILED,
        ROUND(AVG(TOTAL_ELAPSED_TIME) / 1000, 2) as AVG_DURATION_SECS
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= '{start}' AND START_TIME < '{end}'
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=3600, show_spinner=False)
def get_storage_summary(_session):
    query = """
    SELECT 
        ROUND(AVG(STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES) / POWER(1024, 4), 2) as TOTAL_TB
    FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
    WHERE USAGE_DATE >= DATEADD('day', -7, CURRENT_DATE())
    """
    return _session.sql(query).to_pandas()

def get_warehouse_usage_summary(_session, start, end):
    query = f"""
    SELECT 
        WAREHOUSE_NAME,
        ROUND(SUM(CREDITS_USED), 2) as CREDITS_USED,
        COUNT(DISTINCT DATE_TRUNC('HOUR', START_TIME)) as ACTIVE_HOURS,
        ROUND(SUM(CREDITS_USED) / NULLIF(COUNT(DISTINCT DATE_TRUNC('HOUR', START_TIME)), 0), 2) as CREDITS_PER_HOUR
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= '{start}' AND START_TIME < '{end}'
    GROUP BY 1
    ORDER BY 2 DESC
    """
    return _session.sql(query).to_pandas()

with st.spinner("Loading overview..."):
    summary = get_credit_summary(session, start_date, end_date, prev_start, prev_end)
    daily = get_daily_credits(session, start_date, end_date)
    warehouses = get_warehouse_breakdown(session, start_date, end_date)
    queries = get_query_summary(session, start_date, end_date)
    storage = get_storage_summary(session)
    wh_usage = get_warehouse_usage_summary(session, start_date, end_date)

current = summary['CURRENT_CREDITS'].iloc[0] if not summary.empty else 0
previous = summary['PREVIOUS_CREDITS'].iloc[0] if not summary.empty else 0
delta = ((current - previous) / previous * 100) if previous > 0 else 0

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Credits", f"{current:,.0f}", f"{delta:+.1f}% vs prev period")
with col2:
    total_queries = queries['TOTAL_QUERIES'].iloc[0] if not queries.empty else 0
    st.metric("Total Queries", f"{total_queries:,}")
with col3:
    avg_duration = queries['AVG_DURATION_SECS'].iloc[0] if not queries.empty else 0
    st.metric("Avg Query Duration", f"{avg_duration:.1f}s")
with col4:
    total_tb = storage['TOTAL_TB'].iloc[0] if not storage.empty else 0
    st.metric("Storage", f"{total_tb:.2f} TB")

st.markdown("---")

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Daily Credit Consumption")
    if not daily.empty:
        chart = alt.Chart(daily).mark_area(
            color='#29B5E8',
            opacity=0.7,
            line={'color': '#29B5E8'}
        ).encode(
            x=alt.X('USAGE_DATE:T', title='Date', axis=alt.Axis(format='%b %d')),
            y=alt.Y('CREDITS:Q', title='Credits')
        ).properties(height=300)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No data for selected period")

with col2:
    st.subheader("Top Warehouses")
    if not warehouses.empty:
        chart = alt.Chart(warehouses).mark_bar(color='#29B5E8').encode(
            x=alt.X('CREDITS:Q', title='Credits'),
            y=alt.Y('WAREHOUSE_NAME:N', title='', sort='-x')
        ).properties(height=300)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No warehouse data")

st.markdown("---")
st.subheader("Credit Breakdown")
col1, col2 = st.columns(2)
with col1:
    if not daily.empty:
        total_compute = daily['CREDITS'].sum()
        total_cs = daily['CLOUD_SERVICES_CREDITS'].sum()
        breakdown_df = pd.DataFrame({
            'Category': ['Compute', 'Cloud Services'],
            'Credits': [total_compute, total_cs]
        })
        st.dataframe(breakdown_df, use_container_width=True)
with col2:
    if not queries.empty:
        success = queries['SUCCESSFUL'].iloc[0]
        failed = queries['FAILED'].iloc[0]
        success_rate = (success / (success + failed) * 100) if (success + failed) > 0 else 0
        st.metric("Query Success Rate", f"{success_rate:.1f}%")

st.markdown("---")
st.subheader("Warehouse Usage Summary")
if not wh_usage.empty:
    display_df = wh_usage.copy()
    display_df.columns = ['Warehouse', 'Credits Used', 'Active Hours', 'Credits/Hour']
    st.dataframe(display_df, use_container_width=True)
else:
    st.info("No warehouse usage data")
