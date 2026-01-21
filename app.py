import streamlit as st
from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import os

# Page Configuration
st.set_page_config(
    page_title="Pulex Bucket Analysis Report",
    page_icon="🪣",
    layout="wide"
)

# Title and Header
st.title("🪣 Pulex Bucket: Search & Product Analysis")
st.markdown("""
This interactive report analyzes search volume and product views for "Pulex Bucket" items.

### Data Source
*   **BigQuery Project:** `analytics-473719`
*   **Dataset:** `analytics_306941895`
""")

# Methodology Section
col1, col2 = st.columns(2)

with col1:
    st.info("""
    **What this data IS:**
    *   **Internal Site Search Volume:** Count of searches containing "pulex bucket".
    *   **Specific Product Views:** Page views for items with "pulex bucket" in the name, filtered by color (Red, Blue, Green, Gray).
    """)

with col2:
    st.warning("""
    **What this data IS NOT:**
    *   **Not a Sales Report:** Tracks interest (searches/views), not purchases.
    *   **Not Comprehensive:** Limited to "Pulex Bucket" items.
    """)

st.divider()

# Sidebar for Controls
st.sidebar.header("Configuration")

# Date Range
date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(pd.to_datetime("2024-01-01"), pd.to_datetime("2026-02-01")),
    min_value=pd.to_datetime("2023-01-01"),
    max_value=pd.to_datetime("2026-12-31")
)

# Granularity Selector
granularity = st.sidebar.selectbox(
    "Time Granularity",
    options=["Week", "Month"],
    index=0
)

# Mapping options to SQL parts
time_trunc_map = {
    "Week": "WEEK",
    "Month": "MONTH"
}
trunc_val = time_trunc_map[granularity]

if len(date_range) == 2:
    start_date, end_date = date_range
    start_date_str = start_date.strftime('%Y%m%d')
    end_date_str = end_date.strftime('%Y%m%d')
else:
    st.error("Please select a valid start and end date.")
    st.stop()

from google.oauth2 import service_account

# ... (existing imports)

# BigQuery Client Setup
@st.cache_resource
def get_bigquery_client():
    # Check if running in Streamlit Cloud with secrets
    if "gcp_service_account" in st.secrets:
        try:
            creds = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            return bigquery.Client(credentials=creds, project="analytics-473719")
        except Exception as e:
            st.error(f"Error initializing client from secrets: {e}")
            return None
    
    # Fallback to local environment (Application Default Credentials)
    return bigquery.Client(project="analytics-473719")

client = get_bigquery_client()
DATASET = "analytics_306941895"

# --- Section 1: Search Volume ---
st.header("1. Search Volume Trends")
st.write(f"Trend of internal site searches for \"pulex bucket\" aggregated by **{granularity}**.")

@st.cache_data(ttl=3600)
def load_search_data(start, end, time_trunc):
    sql_search = f"""
        SELECT
            DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), {time_trunc}) as date_period,
            COUNT(*) as search_count
        FROM
            `analytics-473719.{DATASET}.events_*`
        WHERE
            event_name = 'view_search_results'
            AND LOWER((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'search_term')) LIKE '%pulex bucket%'
            AND _TABLE_SUFFIX BETWEEN '{start}' AND '{end}'
        GROUP BY
            date_period
        ORDER BY
            date_period ASC
    """
    df = client.query(sql_search).to_dataframe()
    df['date_period'] = pd.to_datetime(df['date_period'])
    return df

with st.spinner('Loading search volume data...'):
    try:
        df_search = load_search_data(start_date_str, end_date_str, trunc_val)
        
        if not df_search.empty:
            fig1 = px.line(
                df_search, 
                x='date_period', 
                y='search_count',
                title=f'Search Volume ({granularity}ly)',
                labels={'date_period': 'Date', 'search_count': 'Searches'},
                markers=True,
                line_shape='spline' # Makes the line smooth
            )
            fig1.update_traces(line_color='#636EFA', hovertemplate='<b>Date:</b> %{x|%b %d, %Y}<br><b>Searches:</b> %{y}')
            fig1.update_layout(hovermode="x unified")
            st.plotly_chart(fig1, use_container_width=True)
            
            with st.expander("View Raw Search Data"):
                st.dataframe(df_search)
        else:
            st.warning("No search data found for this date range.")

    except Exception as e:
        st.error(f"Error loading search data: {e}")

st.divider()

# --- Section 2: Product Views ---
st.header("2. Product View Analysis")
st.write(f"Tracking when users **click** to view specific \"Pulex Bucket\" product pages. Unlike search volume, this represents direct interest in a specific item.")

@st.cache_data(ttl=3600)
def load_product_data(start, end, time_trunc):
    sql_items = f"""
        SELECT
            DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), {time_trunc}) as date_period,
            item.item_name,
            COUNT(*) as views
        FROM
            `analytics-473719.{DATASET}.events_*`,
            UNNEST(items) as item
        WHERE
            event_name = 'view_item'
            AND LOWER(item.item_name) LIKE '%pulex bucket%'
            AND (
                item.item_name LIKE '%Red%'
                OR item.item_name LIKE '%Blue%'
                OR item.item_name LIKE '%Green%'
                OR item.item_name LIKE '%Gray%'
            )
            AND _TABLE_SUFFIX BETWEEN '{start}' AND '{end}'
        GROUP BY 1, 2
        ORDER BY 1 ASC, 3 DESC
    """
    df = client.query(sql_items).to_dataframe()
    df['date_period'] = pd.to_datetime(df['date_period'])
    return df

with st.spinner('Loading product view data...'):
    try:
        df_items = load_product_data(start_date_str, end_date_str, trunc_val)
        
        if not df_items.empty:
            fig2 = px.line(
                df_items, 
                x='date_period', 
                y='views', 
                color='item_name',
                title=f'Product Views by Color ({granularity}ly)',
                labels={'date_period': 'Date', 'views': 'Page Views', 'item_name': 'Product Variant'},
                markers=True
            )
            fig2.update_layout(hovermode="x unified")
            st.plotly_chart(fig2, use_container_width=True)
            
            with st.expander("View Raw Product Data"):
                st.dataframe(df_items)
        else:
            st.warning("No product view data found for this date range.")

    except Exception as e:
        st.error(f"Error loading product data: {e}")

# --- Section 3: Google Search Console (GSC) Data ---
st.divider()
st.header("3. Google Search Console Analysis")
st.write("Analysis of organic Google Search performance from local export data.")

# Paths to local data (Relatively referenced for deployment)
# Using generic paths that work in both local (if cwd is correct) and cloud environments
PATH_DIRECT = os.path.join(os.path.dirname(__file__), "Pulex-Bucket-Direct")
PATH_COLLECTION = os.path.join(os.path.dirname(__file__), "Bucket-Collection")

@st.cache_data(ttl=3600)
def load_gsc_data(directory_path, label):
    data = {}
    
    def safe_load(filename):
        path = os.path.join(directory_path, filename)
        if os.path.exists(path):
            try:
                df = pd.read_csv(path, encoding='utf-8-sig')
            except Exception:
                # Fallback to latin1 if utf-8 fails
                df = pd.read_csv(path, encoding='latin1')
            df.columns = df.columns.str.strip()
            df['Source'] = label
            return df
        return None

    # Load Chart.csv
    df_chart = safe_load('Chart.csv')
    if df_chart is not None:
        if 'Date' in df_chart.columns:
            df_chart['Date'] = pd.to_datetime(df_chart['Date'])
        data['chart'] = df_chart
        
    # Load Queries and Pages
    data['queries'] = safe_load('Queries.csv')
    data['pages'] = safe_load('Pages.csv')
        
    return data

if os.path.exists(PATH_DIRECT) and os.path.exists(PATH_COLLECTION):
    with st.spinner('Loading GSC data...'):
        # Load Data
        data_direct = load_gsc_data(PATH_DIRECT, "Direct")
        data_collection = load_gsc_data(PATH_COLLECTION, "Collection")
        
        # Combine Chart Data
        df_chart_all = pd.DataFrame()
        if data_direct.get('chart') is not None and data_collection.get('chart') is not None:
            df_chart_all = pd.concat([data_direct['chart'], data_collection['chart']], ignore_index=True)
            
        # Combine Queries Data
        df_queries_all = pd.DataFrame()
        if data_direct.get('queries') is not None and data_collection.get('queries') is not None:
            df_queries_all = pd.concat([data_direct['queries'], data_collection['queries']], ignore_index=True)

        # Combine Pages Data
        df_pages_all = pd.DataFrame()
        if data_direct.get('pages') is not None and data_collection.get('pages') is not None:
            df_pages_all = pd.concat([data_direct['pages'], data_collection['pages']], ignore_index=True)

        # Visualizations
        if not df_chart_all.empty:
            # Clicks Trend
            fig_gsc_clicks = px.line(
                df_chart_all, x='Date', y='Clicks', color='Source', 
                title='GSC Clicks Trend', markers=True
            )
            fig_gsc_clicks.update_layout(hovermode="x unified")
            st.plotly_chart(fig_gsc_clicks, use_container_width=True)
            
            # Impressions Trend
            fig_gsc_imps = px.line(
                df_chart_all, x='Date', y='Impressions', color='Source', 
                title='GSC Impressions Trend', markers=True
            )
            fig_gsc_imps.update_layout(hovermode="x unified")
            st.plotly_chart(fig_gsc_imps, use_container_width=True)

        # Top Queries Table
        if not df_queries_all.empty:
            st.subheader("Top Queries (by Clicks)")
            col_q1, col_q2 = st.columns(2)
            
            with col_q1:
                st.caption("Source: Direct")
                st.dataframe(
                    df_queries_all[df_queries_all['Source'] == 'Direct']
                    .sort_values(by='Clicks', ascending=False)
                    .head(10)[['Top queries', 'Clicks', 'Impressions', 'CTR', 'Position']],
                    use_container_width=True,
                    hide_index=True
                )
                
            with col_q2:
                st.caption("Source: Collection")
                st.dataframe(
                    df_queries_all[df_queries_all['Source'] == 'Collection']
                    .sort_values(by='Clicks', ascending=False)
                    .head(10)[['Top queries', 'Clicks', 'Impressions', 'CTR', 'Position']],
                    use_container_width=True,
                    hide_index=True
                )

        # Top Pages Table
        if not df_pages_all.empty:
            st.subheader("Top Landing Pages (by Clicks)")
            col_p1, col_p2 = st.columns(2)
            
            with col_p1:
                st.caption("Source: Direct")
                st.dataframe(
                    df_pages_all[df_pages_all['Source'] == 'Direct']
                    .sort_values(by='Clicks', ascending=False)
                    .head(10)[['Top pages', 'Clicks', 'Impressions', 'CTR', 'Position']],
                    use_container_width=True,
                    hide_index=True
                )
                
            with col_p2:
                st.caption("Source: Collection")
                st.dataframe(
                    df_pages_all[df_pages_all['Source'] == 'Collection']
                    .sort_values(by='Clicks', ascending=False)
                    .head(10)[['Top pages', 'Clicks', 'Impressions', 'CTR', 'Position']],
                    use_container_width=True,
                    hide_index=True
                )

else:
    st.warning(f"GSC Data directories not found. Expected at: `{PATH_DIRECT}` and `{PATH_COLLECTION}`")
