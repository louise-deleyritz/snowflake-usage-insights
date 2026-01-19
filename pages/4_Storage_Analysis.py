import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("Storage Analysis")

@st.cache_data(ttl=3600, show_spinner=False)
def get_storage_overview(_session):
    query = """
    SELECT 
        USAGE_DATE,
        ROUND(STORAGE_BYTES / POWER(1024, 4), 4) as STORAGE_TB,
        ROUND(STAGE_BYTES / POWER(1024, 4), 4) as STAGE_TB,
        ROUND(FAILSAFE_BYTES / POWER(1024, 4), 4) as FAILSAFE_TB,
        ROUND((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES) / POWER(1024, 4), 4) as TOTAL_TB
    FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
    WHERE USAGE_DATE >= DATEADD('day', -90, CURRENT_DATE())
    ORDER BY USAGE_DATE
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=3600, show_spinner=False)
def get_database_storage(_session):
    query = """
    SELECT 
        DATABASE_NAME,
        ROUND(AVG(AVERAGE_DATABASE_BYTES) / POWER(1024, 3), 2) as AVG_DB_GB,
        ROUND(AVG(AVERAGE_FAILSAFE_BYTES) / POWER(1024, 3), 2) as AVG_FAILSAFE_GB
    FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
    WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 20
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=3600, show_spinner=False)
def get_database_growth(_session):
    query = """
    SELECT 
        DATABASE_NAME,
        USAGE_DATE,
        ROUND(AVERAGE_DATABASE_BYTES / POWER(1024, 3), 2) as DB_GB
    FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
    WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
    ORDER BY USAGE_DATE
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=3600, show_spinner=False)
def get_table_storage(_session):
    query = """
    SELECT 
        TABLE_CATALOG as DATABASE_NAME,
        TABLE_SCHEMA as SCHEMA_NAME,
        TABLE_NAME,
        ROUND(ACTIVE_BYTES / POWER(1024, 3), 4) as ACTIVE_GB,
        ROUND(TIME_TRAVEL_BYTES / POWER(1024, 3), 4) as TIME_TRAVEL_GB,
        ROUND(FAILSAFE_BYTES / POWER(1024, 3), 4) as FAILSAFE_GB,
        ROUND((ACTIVE_BYTES + TIME_TRAVEL_BYTES + FAILSAFE_BYTES) / POWER(1024, 3), 4) as TOTAL_GB,
        CLONE_GROUP_ID
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
    WHERE ACTIVE_BYTES > 0
        AND DELETED IS NULL
    ORDER BY ACTIVE_BYTES DESC
    LIMIT 50
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=3600, show_spinner=False)
def get_storage_by_type(_session):
    query = """
    WITH latest AS (
        SELECT *
        FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
        WHERE USAGE_DATE = (SELECT MAX(USAGE_DATE) FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE)
    )
    SELECT 
        ROUND(STORAGE_BYTES / POWER(1024, 4), 4) as DATABASE_TB,
        ROUND(STAGE_BYTES / POWER(1024, 4), 4) as STAGE_TB,
        ROUND(FAILSAFE_BYTES / POWER(1024, 4), 4) as FAILSAFE_TB
    FROM latest
    """
    return _session.sql(query).to_pandas()

with st.spinner("Loading storage data..."):
    storage_overview = get_storage_overview(session)
    db_storage = get_database_storage(session)
    db_growth = get_database_growth(session)
    table_storage = get_table_storage(session)
    storage_by_type = get_storage_by_type(session)

if not storage_overview.empty:
    latest = storage_overview.iloc[-1]
    earliest = storage_overview.iloc[0]
    growth = latest['TOTAL_TB'] - earliest['TOTAL_TB']
    growth_pct = (growth / earliest['TOTAL_TB'] * 100) if earliest['TOTAL_TB'] > 0 else 0
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Storage", f"{latest['TOTAL_TB']:.2f} TB")
    with col2:
        st.metric("Database Storage", f"{latest['STORAGE_TB']:.2f} TB")
    with col3:
        st.metric("Stage Storage", f"{latest['STAGE_TB']:.2f} TB")
    with col4:
        st.metric("90-Day Growth", f"{growth:+.2f} TB", f"{growth_pct:+.1f}%")

st.markdown("---")

st.subheader("Storage Trend (90 days)")
if not storage_overview.empty:
    storage_melted = storage_overview.melt(
        id_vars=['USAGE_DATE'], 
        value_vars=['STORAGE_TB', 'STAGE_TB', 'FAILSAFE_TB'], 
        var_name='Type', 
        value_name='TB'
    )
    storage_melted['Type'] = storage_melted['Type'].map({
        'STORAGE_TB': 'Database',
        'STAGE_TB': 'Stage',
        'FAILSAFE_TB': 'Failsafe'
    })
    chart = alt.Chart(storage_melted).mark_area(opacity=0.7).encode(
        x=alt.X('USAGE_DATE:T', title='Date', axis=alt.Axis(format='%b %d')),
        y=alt.Y('TB:Q', title='Storage (TB)', stack='zero'),
        color=alt.Color('Type:N', scale=alt.Scale(domain=['Database', 'Stage', 'Failsafe'], range=['#29B5E8', '#1f84b3', '#71D3DC']))
    ).properties(height=300)
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("No storage data")

st.markdown("---")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Storage by Database")
    if not db_storage.empty:
        chart = alt.Chart(db_storage.head(10)).mark_bar(color='#29B5E8').encode(
            x=alt.X('AVG_DB_GB:Q', title='Avg Storage (GB)'),
            y=alt.Y('DATABASE_NAME:N', title='', sort='-x')
        ).properties(height=300)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No database storage data")

with col2:
    st.subheader("Storage Breakdown")
    if not storage_by_type.empty:
        breakdown_df = pd.DataFrame({
            'Type': ['Database', 'Stage', 'Failsafe'],
            'TB': [
                storage_by_type['DATABASE_TB'].iloc[0],
                storage_by_type['STAGE_TB'].iloc[0],
                storage_by_type['FAILSAFE_TB'].iloc[0]
            ]
        })
        chart = alt.Chart(breakdown_df).mark_arc(innerRadius=50).encode(
            theta=alt.Theta('TB:Q'),
            color=alt.Color('Type:N', scale=alt.Scale(domain=['Database', 'Stage', 'Failsafe'], range=['#29B5E8', '#1f84b3', '#71D3DC'])),
            tooltip=['Type:N', 'TB:Q']
        ).properties(height=250)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No breakdown data")

st.markdown("---")

st.subheader("Database Growth (30 days)")
if not db_growth.empty:
    top_dbs = db_storage['DATABASE_NAME'].head(5).tolist()
    filtered_growth = db_growth[db_growth['DATABASE_NAME'].isin(top_dbs)]
    if not filtered_growth.empty:
        chart = alt.Chart(filtered_growth).mark_line(strokeWidth=2).encode(
            x=alt.X('USAGE_DATE:T', title='Date', axis=alt.Axis(format='%b %d')),
            y=alt.Y('DB_GB:Q', title='Storage (GB)'),
            color=alt.Color('DATABASE_NAME:N', title='Database')
        ).properties(height=250)
        st.altair_chart(chart, use_container_width=True)
else:
    st.info("No growth data")

st.markdown("---")

st.subheader("Largest Tables")
st.caption("Top 50 tables by active storage")
if not table_storage.empty:
    col1, col2 = st.columns([1, 2])
    with col1:
        db_filter = st.selectbox("Filter by Database", ["All"] + table_storage['DATABASE_NAME'].unique().tolist())
    
    filtered_tables = table_storage if db_filter == "All" else table_storage[table_storage['DATABASE_NAME'] == db_filter]
    
    st.dataframe(
        filtered_tables[['DATABASE_NAME', 'SCHEMA_NAME', 'TABLE_NAME', 'ACTIVE_GB', 'TIME_TRAVEL_GB', 'FAILSAFE_GB', 'TOTAL_GB']],
        use_container_width=True,
        
    )
    
    with st.expander("Time Travel Analysis"):
        st.caption("Tables with significant Time Travel storage")
        high_tt = table_storage[table_storage['TIME_TRAVEL_GB'] > 0.1].sort_values('TIME_TRAVEL_GB', ascending=False)
        if not high_tt.empty:
            st.dataframe(
                high_tt[['DATABASE_NAME', 'TABLE_NAME', 'ACTIVE_GB', 'TIME_TRAVEL_GB']],
                use_container_width=True,
                
            )
        else:
            st.info("No tables with significant Time Travel storage")
else:
    st.info("No table storage data")
