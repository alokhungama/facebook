import os
from dotenv import load_dotenv
load_dotenv()
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time

from facebook_api import FacebookAPI
from database import DatabaseManager
from gemini_query import GeminiQueryEngine
from utils import format_currency, format_percentage, validate_account_id

# Page configuration
st.set_page_config(
    page_title="Facebook Ads Analytics",
    page_icon="📊",
    layout="wide"
)

# Initialize session state
if 'data_fetched' not in st.session_state:
    st.session_state.data_fetched = False
if 'campaigns_df' not in st.session_state:
    st.session_state.campaigns_df = None
if 'adsets_df' not in st.session_state:
    st.session_state.adsets_df = None
if 'ads_df' not in st.session_state:
    st.session_state.ads_df = None
if 'insights_df' not in st.session_state:
    st.session_state.insights_df = None

def main():
    st.title("📊 Facebook Ads Analytics Platform")
    st.markdown("### AI-Powered Facebook Ads Data Analysis")
    
    # Initialize components
    try:
        db_manager = DatabaseManager()
        facebook_api = FacebookAPI()
        gemini_engine = GeminiQueryEngine(db_manager)
    except Exception as e:
        st.error(f"Failed to initialize components: {str(e)}")
        st.stop()
    
    # Sidebar for account input and data fetching
    with st.sidebar:
        st.header("🔧 Configuration")
        
        # Account ID input
        account_id = st.text_input(
            "Facebook Ads Account ID",
            placeholder="act_1234567890",
            help="Enter your Facebook Ads account ID (e.g., act_1234567890)"
        )
        
        # Validate account ID
        if account_id and not validate_account_id(account_id):
            st.error("Invalid account ID format. Should start with 'act_' followed by numbers.")
        
        # Data fetching section
        st.subheader("📥 Data Management")
        
        fetch_button = st.button(
            "🔄 Fetch Fresh Data",
            disabled=not account_id or not validate_account_id(account_id),
            help="Fetch latest data from Facebook Marketing API"
        )
        
        if fetch_button:
            fetch_facebook_data(account_id, facebook_api, db_manager)
        
        # Load existing data button
        load_button = st.button(
            "📂 Load Existing Data",
            help="Load previously fetched data from database"
        )
        
        if load_button:
            load_existing_data(db_manager)
        
        # Data status
        if st.session_state.data_fetched:
            st.success("✅ Data loaded successfully")
            if st.session_state.campaigns_df is not None:
                st.metric("Campaigns", len(st.session_state.campaigns_df))
            if st.session_state.adsets_df is not None:
                st.metric("Ad Sets", len(st.session_state.adsets_df))
            if st.session_state.ads_df is not None:
                st.metric("Ads", len(st.session_state.ads_df))
        else:
            st.info("💡 Fetch or load data to begin analysis")
    
    # Main content area
    if not st.session_state.data_fetched:
        show_welcome_screen()
    else:
        show_analytics_dashboard(gemini_engine)

def fetch_facebook_data(account_id, facebook_api, db_manager):
    """Fetch data from Facebook Marketing API and store in database"""
    with st.spinner("Fetching data from Facebook Marketing API..."):
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            # Fetch campaigns
            status_text.text("Fetching campaigns...")
            campaigns = facebook_api.fetch_campaigns(account_id)
            progress_bar.progress(25)
            
            # Fetch ad sets
            status_text.text("Fetching ad sets...")
            adsets = facebook_api.fetch_adsets(account_id)
            progress_bar.progress(50)
            
            # Fetch ads
            status_text.text("Fetching ads...")
            ads = facebook_api.fetch_ads(account_id)
            progress_bar.progress(75)
            
            # Fetch insights
            status_text.text("Fetching insights...")
            insights = facebook_api.fetch_insights(account_id)
            progress_bar.progress(90)
            
            # Store in database
            status_text.text("Storing data in database...")
            db_manager.store_campaigns(campaigns)
            db_manager.store_adsets(adsets)
            db_manager.store_ads(ads)
            db_manager.store_insights(insights)
            
            # Update session state
            st.session_state.campaigns_df = pd.DataFrame(campaigns)
            st.session_state.adsets_df = pd.DataFrame(adsets)
            st.session_state.ads_df = pd.DataFrame(ads)
            st.session_state.insights_df = pd.DataFrame(insights)
            st.session_state.data_fetched = True
            
            progress_bar.progress(100)
            status_text.text("✅ Data fetched and stored successfully!")
            
            time.sleep(1)
            st.rerun()
            
        except Exception as e:
            st.error(f"Error fetching data: {str(e)}")
            progress_bar.empty()
            status_text.empty()

def load_existing_data(db_manager):
    """Load existing data from database"""
    try:
        with st.spinner("Loading data from database..."):
            campaigns = db_manager.get_campaigns()
            adsets = db_manager.get_adsets()
            ads = db_manager.get_ads()
            insights = db_manager.get_insights()
            
            if campaigns:
                st.session_state.campaigns_df = pd.DataFrame(campaigns)
                st.session_state.adsets_df = pd.DataFrame(adsets)
                st.session_state.ads_df = pd.DataFrame(ads)
                st.session_state.insights_df = pd.DataFrame(insights)
                st.session_state.data_fetched = True
                st.success("Data loaded successfully from database!")
                st.rerun()
            else:
                st.warning("No data found in database. Please fetch fresh data first.")
                
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")

def show_welcome_screen():
    """Display welcome screen with instructions"""
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
        ## 🚀 Welcome to Facebook Ads Analytics
        
        Get started by:
        1. **Enter your Facebook Ads Account ID** in the sidebar
        2. **Fetch fresh data** from Facebook Marketing API, or
        3. **Load existing data** from the database
        
        Once data is loaded, you can:
        - 📊 View comprehensive analytics dashboards
        - 🤖 Ask natural language questions about your data
        - 📈 Explore interactive charts and visualizations
        
        ### Features:
        - 🔄 Real-time data fetching with pagination
        - 💾 PostgreSQL database storage
        - 🧠 AI-powered querying with Google Gemini
        - 📱 Interactive dashboards and charts
        """)

def show_analytics_dashboard(gemini_engine):
    """Display the main analytics dashboard"""
    
    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🤖 AI Query", "📈 Performance", "📋 Raw Data"])
    
    with tab1:
        show_overview_tab()
    
    with tab2:
        show_ai_query_tab(gemini_engine)
    
    with tab3:
        show_performance_tab()
    
    with tab4:
        show_raw_data_tab()

def show_overview_tab():
    """Display overview metrics and charts"""
    st.header("📊 Campaign Overview")
    
    if st.session_state.insights_df is not None and not st.session_state.insights_df.empty:
        insights_df = st.session_state.insights_df
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        total_spend = insights_df['spend'].astype(float).sum()
        total_impressions = insights_df['impressions'].astype(int).sum()
        total_clicks = insights_df['clicks'].astype(int).sum()
        avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
        
        with col1:
            st.metric("Total Spend", format_currency(total_spend))
        with col2:
            st.metric("Total Impressions", f"{total_impressions:,}")
        with col3:
            st.metric("Total Clicks", f"{total_clicks:,}")
        with col4:
            st.metric("Average CTR", format_percentage(avg_ctr))
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            # Spend by campaign
            if st.session_state.campaigns_df is not None:
                campaign_spend = insights_df.groupby('campaign_id')['spend'].sum().reset_index()
                campaign_spend = campaign_spend.merge(
                    st.session_state.campaigns_df[['id', 'name']], 
                    left_on='campaign_id', 
                    right_on='id', 
                    how='left'
                )
                
                fig = px.bar(
                    campaign_spend.head(10),
                    x='name',
                    y='spend',
                    title="Top 10 Campaigns by Spend",
                    labels={'spend': 'Spend (₹)', 'name': 'Campaign Name'}
                )
                fig.update_layout(xaxis_tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # CTR by campaign
            campaign_metrics = insights_df.groupby('campaign_id').agg({
                'clicks': 'sum',
                'impressions': 'sum'
            }).reset_index()
            campaign_metrics['ctr'] = (campaign_metrics['clicks'] / campaign_metrics['impressions'] * 100)
            campaign_metrics = campaign_metrics.merge(
                st.session_state.campaigns_df[['id', 'name']], 
                left_on='campaign_id', 
                right_on='id', 
                how='left'
            )
            
            fig = px.bar(
                campaign_metrics.head(10),
                x='name',
                y='ctr',
                title="Top 10 Campaigns by CTR",
                labels={'ctr': 'CTR (%)', 'name': 'Campaign Name'}
            )
            fig.update_layout(xaxis_tickangle=45)
            st.plotly_chart(fig, use_container_width=True)

def show_ai_query_tab(gemini_engine):
    """Display AI-powered query interface"""
    st.header("🤖 AI-Powered Analytics")
    st.markdown("Ask questions about your Facebook Ads data in natural language!")
    
    # Example queries
    with st.expander("💡 Example Queries"):
        st.markdown("""
        **Simple Data Queries:**
        - "Show me top 5 campaigns by daily budget"
        - "How many active ads do I have?"
        - "List all paused campaigns"
        
        **Advanced Analytics (AI-powered):**
        - "What's the ROAS, CAC, and CTR trend over the past 7 days vs 7 days prior?"
        - "Which campaign has the best performance today? Which one is burning budget?"
        - "Are there any significant drops or spikes in spend today?"
        - "What's the total spend today vs yesterday?"
        - "Which campaigns are over or under-spending against daily budgets?"
        """)
    
    # Query input
    user_query = st.text_area(
        "Enter your question:",
        placeholder="e.g., Show me top 5 campaigns by spend",
        height=100
    )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        query_button = st.button("🔍 Analyze", disabled=not user_query.strip())
    
    if query_button and user_query.strip():
        with st.spinner("Analyzing your query..."):
            try:
                result = gemini_engine.process_query(user_query)
                
                if result:
                    # Check if this is an analytical query
                    if result.get('query_type') == 'analytical':
                        st.subheader("🧠 AI Performance Analysis")
                        st.markdown(result['insights'])
                        
                        # Show data summary if available
                        if result['data'] is not None and not result['data'].empty:
                            with st.expander("📊 Supporting Data"):
                                st.dataframe(result['data'], use_container_width=True)
                    else:
                        st.subheader("📋 Query Results")
                        
                        # Display SQL query
                        with st.expander("🔍 Generated SQL Query"):
                            st.code(result['sql_query'], language='sql')
                        
                        # Display results
                        if result['data'] is not None and not result['data'].empty:
                            st.dataframe(result['data'], use_container_width=True)
                            
                            # Generate chart if appropriate
                            if len(result['data']) > 1 and len(result['data'].columns) >= 2:
                                numeric_cols = result['data'].select_dtypes(include=['number']).columns
                                if len(numeric_cols) > 0:
                                    st.subheader("📈 Visualization")
                                    chart_type = st.selectbox("Chart Type", ["Bar Chart", "Line Chart"])
                                    
                                    if chart_type == "Bar Chart":
                                        fig = px.bar(result['data'], x=result['data'].columns[0], y=numeric_cols[0])
                                    else:
                                        fig = px.line(result['data'], x=result['data'].columns[0], y=numeric_cols[0])
                                    
                                    st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No data returned for this query.")
                        
                        # Display AI insights
                        if result.get('insights'):
                            st.subheader("🧠 AI Insights")
                            st.markdown(result['insights'])
                            
                else:
                    st.error("Failed to process query. Please try rephrasing your question.")
                    
            except Exception as e:
                st.error(f"Error processing query: {str(e)}")

def show_performance_tab():
    """Display performance analytics"""
    st.header("📈 Performance Analytics")
    
    if st.session_state.insights_df is not None and not st.session_state.insights_df.empty:
        insights_df = st.session_state.insights_df
        
        # Performance over time
        if 'date_start' in insights_df.columns:
            insights_df['date'] = pd.to_datetime(insights_df['date_start'])
            daily_performance = insights_df.groupby('date').agg({
                'spend': 'sum',
                'impressions': 'sum',
                'clicks': 'sum'
            }).reset_index()
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.line(
                    daily_performance,
                    x='date',
                    y='spend',
                    title="Daily Spend Trend",
                    labels={'spend': 'Spend (₹)', 'date': 'Date'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                daily_performance['ctr'] = (daily_performance['clicks'] / daily_performance['impressions'] * 100)
                fig = px.line(
                    daily_performance,
                    x='date',
                    y='ctr',
                    title="Daily CTR Trend",
                    labels={'ctr': 'CTR (%)', 'date': 'Date'}
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Performance distribution
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.histogram(
                insights_df,
                x='spend',
                title="Spend Distribution",
                labels={'spend': 'Spend (₹)'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            insights_df['ctr'] = (insights_df['clicks'].astype(int) / insights_df['impressions'].astype(int) * 100)
            fig = px.histogram(
                insights_df,
                x='ctr',
                title="CTR Distribution",
                labels={'ctr': 'CTR (%)'}
            )
            st.plotly_chart(fig, use_container_width=True)

def show_raw_data_tab():
    """Display raw data tables"""
    st.header("📋 Raw Data")
    
    # Data selection
    data_type = st.selectbox(
        "Select data to view:",
        ["Campaigns", "Ad Sets", "Ads", "Insights"]
    )
    
    if data_type == "Campaigns" and st.session_state.campaigns_df is not None:
        st.subheader("Campaigns Data")
        st.dataframe(st.session_state.campaigns_df, use_container_width=True)
        
    elif data_type == "Ad Sets" and st.session_state.adsets_df is not None:
        st.subheader("Ad Sets Data")
        st.dataframe(st.session_state.adsets_df, use_container_width=True)
        
    elif data_type == "Ads" and st.session_state.ads_df is not None:
        st.subheader("Ads Data")
        st.dataframe(st.session_state.ads_df, use_container_width=True)
        
    elif data_type == "Insights" and st.session_state.insights_df is not None:
        st.subheader("Insights Data")
        st.dataframe(st.session_state.insights_df, use_container_width=True)
    
    else:
        st.info("No data available. Please fetch or load data first.")

if __name__ == "__main__":
    main()
