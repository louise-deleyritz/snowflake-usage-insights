import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta

session = get_active_session()

st.title("Warehouse Analysis")

days_back = st.selectbox("Time Period", [7, 14, 30, 60, 90], index=0, format_func=lambda x: f"Last {x} days")

end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)

@st.cache_data(ttl=3600, show_spinner=False)
def get_warehouses(_session, start, end):
    query = f"""
    SELECT DISTINCT WAREHOUSE_NAME
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= '{start}' AND START_TIME < '{end}'
    ORDER BY WAREHOUSE_NAME
    """
    return _session.sql(query).to_pandas()

warehouses_df = get_warehouses(session, start_date, end_date)
warehouse_list = warehouses_df['WAREHOUSE_NAME'].tolist() if not warehouses_df.empty else []

selected_warehouse = st.selectbox("Select Warehouse", warehouse_list if warehouse_list else ["No warehouses found"])

if warehouse_list and selected_warehouse != "No warehouses found":
    
    @st.cache_data(ttl=3600, show_spinner=False)
    def get_daily_credits(_session, warehouse, start, end):
        query = f"""
        SELECT 
            DATE_TRUNC('DAY', START_TIME)::DATE as USAGE_DATE,
            ROUND(SUM(CREDITS_USED), 2) as CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE WAREHOUSE_NAME = '{warehouse}'
            AND START_TIME >= '{start}' AND START_TIME < '{end}'
        GROUP BY 1
        ORDER BY 1
        """
        return _session.sql(query).to_pandas()

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_hourly_credits(_session, warehouse, start, end):
        query = f"""
        SELECT 
            DATE_TRUNC('HOUR', START_TIME)::TIMESTAMP_NTZ as USAGE_HOUR,
            ROUND(SUM(CREDITS_USED), 4) as CREDITS,
            ROUND(SUM(CREDITS_USED_CLOUD_SERVICES), 4) as GS_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE WAREHOUSE_NAME = '{warehouse}'
            AND START_TIME >= '{start}' AND START_TIME < '{end}'
        GROUP BY 1
        ORDER BY 1
        """
        return _session.sql(query).to_pandas()

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_warehouse_events(_session, warehouse, start, end):
        query = f"""
        SELECT 
            TIMESTAMP,
            EVENT_NAME,
            CLUSTER_NUMBER
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY
        WHERE WAREHOUSE_NAME = '{warehouse}'
            AND TIMESTAMP >= '{start}' AND TIMESTAMP < '{end}'
            AND EVENT_NAME IN ('RESUME_WAREHOUSE', 'SUSPEND_WAREHOUSE')
        ORDER BY TIMESTAMP
        """
        return _session.sql(query).to_pandas()

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_warehouse_size_history(_session, warehouse, start, end):
        query = f"""
        SELECT 
            DATE_TRUNC('HOUR', START_TIME)::TIMESTAMP_NTZ as USAGE_HOUR,
            WAREHOUSE_SIZE,
            COUNT(*) as QUERY_COUNT
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE WAREHOUSE_NAME = '{warehouse}'
            AND START_TIME >= '{start}' AND START_TIME < '{end}'
            AND WAREHOUSE_SIZE IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 1
        """
        return _session.sql(query).to_pandas()

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_cluster_usage(_session, warehouse, start, end):
        query = f"""
        SELECT 
            DATE_TRUNC('HOUR', START_TIME)::TIMESTAMP_NTZ as USAGE_HOUR,
            COALESCE(CLUSTER_NUMBER, 0) as CLUSTER_NUMBER,
            COUNT(*) as QUERY_COUNT
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE WAREHOUSE_NAME = '{warehouse}'
            AND START_TIME >= '{start}' AND START_TIME < '{end}'
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
        return _session.sql(query).to_pandas()

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_query_type_breakdown(_session, warehouse, start, end):
        query = f"""
        SELECT 
            QUERY_TYPE,
            COUNT(*) as QUERY_COUNT,
            ROUND(SUM(TOTAL_ELAPSED_TIME) / 60000, 2) as DURATION_MINS
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE WAREHOUSE_NAME = '{warehouse}'
            AND START_TIME >= '{start}' AND START_TIME < '{end}'
        GROUP BY 1
        ORDER BY 2 DESC
        """
        return _session.sql(query).to_pandas()

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_duration_breakdown(_session, warehouse, start, end):
        query = f"""
        SELECT 
            DATE_TRUNC('HOUR', START_TIME)::TIMESTAMP_NTZ as USAGE_HOUR,
            ROUND(AVG(COMPILATION_TIME) / 1000, 2) as AVG_COMPILE_SECS,
            ROUND(AVG(QUEUED_PROVISIONING_TIME + QUEUED_REPAIR_TIME + QUEUED_OVERLOAD_TIME) / 1000, 2) as AVG_QUEUE_SECS,
            ROUND(AVG(EXECUTION_TIME) / 1000, 2) as AVG_EXEC_SECS
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE WAREHOUSE_NAME = '{warehouse}'
            AND START_TIME >= '{start}' AND START_TIME < '{end}'
        GROUP BY 1
        ORDER BY 1
        """
        return _session.sql(query).to_pandas()

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_cache_usage(_session, warehouse, start, end):
        query = f"""
        SELECT 
            DATE_TRUNC('HOUR', START_TIME)::TIMESTAMP_NTZ as USAGE_HOUR,
            ROUND(AVG(PERCENTAGE_SCANNED_FROM_CACHE), 2) as PCT_FROM_CACHE
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE WAREHOUSE_NAME = '{warehouse}'
            AND START_TIME >= '{start}' AND START_TIME < '{end}'
            AND BYTES_SCANNED > 0
        GROUP BY 1
        ORDER BY 1
        """
        return _session.sql(query).to_pandas()

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_spilling(_session, warehouse, start, end):
        query = f"""
        SELECT 
            COUNT_IF(BYTES_SPILLED_TO_LOCAL_STORAGE > 0) as JOBS_SPILLED_LOCAL,
            COUNT_IF(BYTES_SPILLED_TO_REMOTE_STORAGE > 0) as JOBS_SPILLED_REMOTE,
            COUNT(*) as TOTAL_JOBS
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE WAREHOUSE_NAME = '{warehouse}'
            AND START_TIME >= '{start}' AND START_TIME < '{end}'
        """
        return _session.sql(query).to_pandas()

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_errors(_session, warehouse, start, end):
        query = f"""
        SELECT 
            CASE
                WHEN ERROR_MESSAGE ILIKE 'Statement reached its statement or warehouse timeout%' THEN 'Compute timeout'
                WHEN ERROR_MESSAGE ILIKE 'Statement reached its statement or warehouse queuing timeout%' THEN 'Queuing timeout'
                WHEN ERROR_MESSAGE ILIKE 'SQL execution canceled%' THEN 'Execution canceled'
                WHEN ERROR_MESSAGE ILIKE 'SQL compilation error%' THEN 'Compilation error'
                WHEN ERROR_MESSAGE ILIKE '%access control error%' THEN 'Access control error'
                ELSE 'Other'
            END as ERROR_CATEGORY,
            COUNT(*) as ERROR_COUNT
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE WAREHOUSE_NAME = '{warehouse}'
            AND START_TIME >= '{start}' AND START_TIME < '{end}'
            AND ERROR_CODE IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
        """
        return _session.sql(query).to_pandas()

    with st.spinner("Loading warehouse data..."):
        daily_credits = get_daily_credits(session, selected_warehouse, start_date, end_date)
        hourly_credits = get_hourly_credits(session, selected_warehouse, start_date, end_date)
        events = get_warehouse_events(session, selected_warehouse, start_date, end_date)
        size_history = get_warehouse_size_history(session, selected_warehouse, start_date, end_date)
        cluster_usage = get_cluster_usage(session, selected_warehouse, start_date, end_date)
        query_types = get_query_type_breakdown(session, selected_warehouse, start_date, end_date)
        duration_breakdown = get_duration_breakdown(session, selected_warehouse, start_date, end_date)
        cache_usage = get_cache_usage(session, selected_warehouse, start_date, end_date)
        spilling = get_spilling(session, selected_warehouse, start_date, end_date)
        errors = get_errors(session, selected_warehouse, start_date, end_date)

    total_credits = daily_credits['CREDITS'].sum() if not daily_credits.empty else 0
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Credits", f"{total_credits:,.1f}")
    with col2:
        resume_count = len(events[events['EVENT_NAME'] == 'RESUME_WAREHOUSE']) if not events.empty else 0
        st.metric("Resume Events", resume_count)
    with col3:
        avg_cache = cache_usage['PCT_FROM_CACHE'].mean() if not cache_usage.empty else 0
        st.metric("Avg Cache Hit %", f"{avg_cache:.1f}%")

    st.markdown("---")

    st.subheader("Daily Credits")
    if not daily_credits.empty:
        chart = alt.Chart(daily_credits).mark_area(
            color='#29B5E8',
            opacity=0.7,
            line={'color': '#29B5E8'}
        ).encode(
            x=alt.X('USAGE_DATE:T', title='Date', axis=alt.Axis(format='%b %d')),
            y=alt.Y('CREDITS:Q', title='Credits')
        ).properties(height=250)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No credit data")

    st.subheader("Hourly Credits")
    st.caption("Spot consumption spikes within the day")
    if not hourly_credits.empty:
        hourly_melted = hourly_credits.melt(id_vars=['USAGE_HOUR'], value_vars=['CREDITS', 'GS_CREDITS'], var_name='Type', value_name='Credits')
        chart = alt.Chart(hourly_melted).mark_line(strokeWidth=2).encode(
            x=alt.X('USAGE_HOUR:T', title='Time', axis=alt.Axis(format='%b %d %H:%M')),
            y=alt.Y('Credits:Q', title='Credits'),
            color=alt.Color('Type:N', scale=alt.Scale(domain=['CREDITS', 'GS_CREDITS'], range=['#29B5E8', '#1f84b3']))
        ).properties(height=250)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No hourly data")

    st.markdown("---")

    st.subheader("Suspend/Resume Timeline")
    st.caption("Is the warehouse suspending properly or left running?")
    if not events.empty:
        events['EVENT_COLOR'] = events['EVENT_NAME'].map({
            'RESUME_WAREHOUSE': 'Resume',
            'SUSPEND_WAREHOUSE': 'Suspend'
        })
        chart = alt.Chart(events).mark_circle(size=100).encode(
            x=alt.X('TIMESTAMP:T', title='Time', axis=alt.Axis(format='%b %d %H:%M')),
            y=alt.Y('EVENT_COLOR:N', title=''),
            color=alt.Color('EVENT_COLOR:N', 
                scale=alt.Scale(domain=['Resume', 'Suspend'], range=['#29B5E8', '#71D3DC']),
                legend=alt.Legend(title='Event')
            ),
            tooltip=['TIMESTAMP:T', 'EVENT_NAME:N', 'CLUSTER_NUMBER:Q']
        ).properties(height=150)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No events found - warehouse may be running 24x7 or no activity")

    st.markdown("---")

    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Warehouse Size Over Time")
        st.caption("Size changes can cause credit spikes")
        if not size_history.empty:
            chart = alt.Chart(size_history).mark_bar().encode(
                x=alt.X('USAGE_HOUR:T', title='Time'),
                y=alt.Y('QUERY_COUNT:Q', title='Query Count'),
                color=alt.Color('WAREHOUSE_SIZE:N', title='Size')
            ).properties(height=250)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No size data")

    with col2:
        st.subheader("Cluster Usage")
        st.caption("Multi-cluster behavior (higher = more scaling)")
        if not cluster_usage.empty:
            cluster_usage['CLUSTER_NUMBER'] = cluster_usage['CLUSTER_NUMBER'].astype(str)
            chart = alt.Chart(cluster_usage).mark_bar().encode(
                x=alt.X('USAGE_HOUR:T', title='Time'),
                y=alt.Y('QUERY_COUNT:Q', title='Query Count'),
                color=alt.Color('CLUSTER_NUMBER:N', title='Cluster')
            ).properties(height=250)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No cluster data")

    st.markdown("---")

    st.subheader("Query Type Breakdown")
    st.caption("What is the warehouse doing?")
    if not query_types.empty:
        chart = alt.Chart(query_types.head(10)).mark_bar(color='#29B5E8').encode(
            x=alt.X('QUERY_COUNT:Q', title='Query Count'),
            y=alt.Y('QUERY_TYPE:N', title='', sort='-x')
        ).properties(height=300)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No query type data")

    st.markdown("---")

    st.subheader("Query Duration Breakdown")
    st.caption("Where is time being spent? (Compile vs Queue vs Execute)")
    if not duration_breakdown.empty:
        duration_melted = duration_breakdown.melt(
            id_vars=['USAGE_HOUR'], 
            value_vars=['AVG_COMPILE_SECS', 'AVG_QUEUE_SECS', 'AVG_EXEC_SECS'], 
            var_name='Phase', 
            value_name='Seconds'
        )
        duration_melted['Phase'] = duration_melted['Phase'].map({
            'AVG_COMPILE_SECS': 'Compile',
            'AVG_QUEUE_SECS': 'Queue',
            'AVG_EXEC_SECS': 'Execute'
        })
        chart = alt.Chart(duration_melted).mark_area(opacity=0.7).encode(
            x=alt.X('USAGE_HOUR:T', title='Time'),
            y=alt.Y('Seconds:Q', title='Avg Seconds', stack='zero'),
            color=alt.Color('Phase:N', scale=alt.Scale(domain=['Compile', 'Queue', 'Execute'], range=['#71D3DC', '#1f84b3', '#29B5E8']))
        ).properties(height=250)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No duration data")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Cache Hit Ratio")
        st.caption("Higher = better (reading from cache vs remote storage)")
        if not cache_usage.empty:
            chart = alt.Chart(cache_usage).mark_line(color='#29B5E8', strokeWidth=2).encode(
                x=alt.X('USAGE_HOUR:T', title='Time'),
                y=alt.Y('PCT_FROM_CACHE:Q', title='% from Cache', scale=alt.Scale(domain=[0, 100]))
            ).properties(height=200)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No cache data")

    with col2:
        st.subheader("Data Spilling")
        st.caption("Spilling = memory pressure, consider larger warehouse")
        if not spilling.empty:
            total_jobs = spilling['TOTAL_JOBS'].iloc[0]
            local_pct = (spilling['JOBS_SPILLED_LOCAL'].iloc[0] / total_jobs * 100) if total_jobs > 0 else 0
            remote_pct = (spilling['JOBS_SPILLED_REMOTE'].iloc[0] / total_jobs * 100) if total_jobs > 0 else 0
            
            spill_df = pd.DataFrame({
                'Type': ['Spilling Locally', 'Spilling Remotely'],
                'Percentage': [local_pct, remote_pct]
            })
            chart = alt.Chart(spill_df).mark_bar().encode(
                x=alt.X('Percentage:Q', title='% of Jobs', scale=alt.Scale(domain=[0, 100])),
                y=alt.Y('Type:N', title=''),
                color=alt.Color('Type:N', scale=alt.Scale(domain=['Spilling Locally', 'Spilling Remotely'], range=['#1f84b3', '#71D3DC']), legend=None)
            ).properties(height=100)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No spilling data")

    st.markdown("---")

    st.subheader("Errors by Category")
    st.caption("Timeouts, cancellations, compilation errors")
    if not errors.empty:
        chart = alt.Chart(errors).mark_bar(color='#E74C3C').encode(
            x=alt.X('ERROR_COUNT:Q', title='Error Count'),
            y=alt.Y('ERROR_CATEGORY:N', title='', sort='-x')
        ).properties(height=200)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.success("No errors found!")

else:
    st.warning("No warehouses found for the selected period")
