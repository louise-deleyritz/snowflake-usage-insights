# Snowflake Usage Insights

A Streamlit in Snowflake (SiS) application for monitoring and analyzing your Snowflake account consumption - credits, warehouse performance, query metrics, and storage trends.

## Features

- **Executive Overview**: High-level consumption summary with credit trends and top warehouses
- **Warehouse Analysis**: Deep-dive into individual warehouse performance including credit consumption, suspend/resume events, size changes, query duration breakdown, cache efficiency, and data spilling analysis
- **Query Performance**: Identify expensive, slow, and failed queries with detailed metrics
- **Storage Analysis**: Track storage trends at account, database, and table levels

## Quick Start

### Prerequisites

1. A Snowflake account with ACCOUNTADMIN role (or a role with access to `SNOWFLAKE.ACCOUNT_USAGE`)
2. Snowflake CLI installed ([Installation Guide](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation))

### Installation

**Option 1: Using Snowflake CLI (Recommended)**

```bash
# Clone the repository
git clone https://github.com/louise-deleyritz/snowflake-usage-insights.git
cd snowflake-usage-insights

# Update snowflake.yml with your warehouse name
# Then deploy
snow streamlit deploy --database USAGE_INSIGHTS --schema APP
```

**Option 2: Manual Deployment**

```sql
-- 1. Grant yourself access to ACCOUNT_USAGE (requires ACCOUNTADMIN)
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_role>;

-- 2. Create database, schema, and stage
CREATE DATABASE IF NOT EXISTS USAGE_INSIGHTS;
CREATE SCHEMA IF NOT EXISTS USAGE_INSIGHTS.APP;
CREATE STAGE IF NOT EXISTS USAGE_INSIGHTS.APP.STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- 3. Upload files via Snowsight (Data > Databases > USAGE_INSIGHTS > APP > Stages > STREAMLIT_STAGE)
--    Upload: streamlit_app.py, environment.yml
--    Create 'pages' folder and upload all .py files from pages/

-- 4. Create the Streamlit app
CREATE STREAMLIT IF NOT EXISTS USAGE_INSIGHTS.APP.USAGE_INSIGHTS_APP
    ROOT_LOCATION = '@USAGE_INSIGHTS.APP.STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = '<your_warehouse>';

-- 5. Open the app in Snowsight: Projects > Streamlit > USAGE_INSIGHTS_APP
```

### Grant Access to Other Users

```sql
GRANT USAGE ON DATABASE USAGE_INSIGHTS TO ROLE <role_name>;
GRANT USAGE ON SCHEMA USAGE_INSIGHTS.APP TO ROLE <role_name>;
GRANT USAGE ON STREAMLIT USAGE_INSIGHTS.APP.USAGE_INSIGHTS_APP TO ROLE <role_name>;
```

## Data Sources

All data is sourced from `SNOWFLAKE.ACCOUNT_USAGE` views:

| View | Purpose |
|------|---------|
| WAREHOUSE_METERING_HISTORY | Credit consumption by warehouse |
| WAREHOUSE_EVENTS_HISTORY | Warehouse suspend/resume events |
| QUERY_HISTORY | Query execution metrics |
| STORAGE_USAGE | Account-level storage |
| DATABASE_STORAGE_USAGE_HISTORY | Per-database storage |
| TABLE_STORAGE_METRICS | Table-level storage details |

Note: ACCOUNT_USAGE data has up to 3 hours of latency.

## File Structure

```
snowflake-usage-insights/
├── streamlit_app.py              # Main entry point
├── environment.yml               # Python dependencies
├── snowflake.yml                 # Snowflake CLI configuration
├── README.md
└── pages/
    ├── 1_Executive_Overview.py   # Credit summary and trends
    ├── 2_Warehouse_Analysis.py   # Warehouse deep-dive
    ├── 3_Query_Performance.py    # Query metrics
    └── 4_Storage_Analysis.py     # Storage breakdown
```

## Requirements

- Snowflake account with access to ACCOUNT_USAGE views
- A warehouse for running queries

## License

MIT License
