import os
from dotenv import load_dotenv
load_dotenv()
import os
import google.generativeai as genai
import pandas as pd
import logging
from typing import Dict, Any, Optional
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GeminiQueryEngine:
    def __init__(self, database_manager):
        """Initialize Gemini query engine"""
        self.db_manager = database_manager
        self.api_key = os.getenv('GEMINI_API_KEY', '')
        
        if not self.api_key:
            logger.warning("Gemini API key not found in environment variables")
        else:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel('gemini-1.5-flash')
        
        self.schema_info = self.db_manager.get_schema_info()
    
    def process_query(self, user_query: str) -> Dict[str, Any]:
        """Process natural language query and return results"""
        try:
            # Check if this is an analytical query requiring calculations/insights
            if self._is_analytical_query(user_query):
                return self._process_analytical_query(user_query)
            
            # Generate SQL query using Gemini
            sql_query = self._generate_sql_query(user_query)
            
            if not sql_query:
                return {
                    'error': 'Failed to generate SQL query',
                    'sql_query': None,
                    'data': None,
                    'insights': None
                }
            
            # Execute SQL query
            data = self._execute_sql_query(sql_query)
            
            # Generate insights
            insights = self._generate_insights(user_query, data, sql_query)
            
            return {
                'sql_query': sql_query,
                'data': data,
                'insights': insights,
                'error': None
            }
            
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            return {
                'error': str(e),
                'sql_query': None,
                'data': None,
                'insights': None
            }
    
    def _generate_sql_query(self, user_query: str) -> Optional[str]:
        """Generate SQL query from natural language using Gemini"""
        try:
            # Create schema context
            schema_context = self._create_schema_context()
            
            prompt = f"""
            You are a PostgreSQL expert. Convert this natural language query to SQL.
            
            Database Schema:
            {schema_context}
            
            User Query: {user_query}
            
            Important Guidelines:
            - Return ONLY the SQL query, no explanations or markdown
            - insights table contains account-level data by date, NOT individual ad/campaign data
            - For ad/campaign specific queries, use the ads/campaigns/adsets tables directly
            - insights table is mainly useful for time-based analysis and account totals
            - Use appropriate aggregations (SUM, AVG, COUNT)
            - Limit results appropriately (5-20 rows for "top" queries)
            - Use clear column aliases
            
            Examples:
            "top 5 ads by name" → SELECT name FROM ads WHERE status = 'ACTIVE' ORDER BY name LIMIT 5;
            "top 5 campaigns by budget" → SELECT name, daily_budget FROM campaigns WHERE daily_budget > 0 ORDER BY daily_budget DESC LIMIT 5;
            "total spend by date" → SELECT date_start, spend FROM insights WHERE spend > 0 ORDER BY date_start;
            "campaign count" → SELECT COUNT(*) as campaign_count FROM campaigns;
            
            SQL Query:
            """
            
            if hasattr(self, 'model'):
                response = self.model.generate_content(prompt)
                sql_query = self._clean_sql_response(response.text)
                logger.info(f"Generated SQL: {sql_query}")
                return sql_query
            else:
                logger.error("Gemini model not available - API key required")
                return None
                
        except Exception as e:
            logger.error(f"Error generating SQL query: {e}")
            # Still try Gemini if it's available, don't fall back to hardcoded queries
            if hasattr(self, 'model'):
                try:
                    # Retry with simpler prompt
                    simple_prompt = f"Convert to SQL: {user_query}\nUse these tables: campaigns, adsets, ads, insights"
                    response = self.model.generate_content(simple_prompt)
                    return self._clean_sql_response(response.text)
                except:
                    pass
            return None
    
    def _create_schema_context(self) -> str:
        """Create schema context for Gemini"""
        context = ""
        for table_name, table_info in self.schema_info.items():
            context += f"\nTable: {table_info['table']}\n"
            context += f"Description: {table_info['description']}\n"
            context += f"Columns: {', '.join(table_info['columns'])}\n"
        return context
    
    def _clean_sql_response(self, response: str) -> str:
        """Clean and extract SQL query from Gemini response"""
        # Remove markdown code blocks
        response = re.sub(r'```sql\n?', '', response)
        response = re.sub(r'```\n?', '', response)
        
        # Remove extra whitespace and newlines
        response = response.strip()
        
        # Ensure query ends with semicolon
        if not response.endswith(';'):
            response += ';'
        
        return response
    
    def _fallback_sql_generation(self, user_query: str) -> str:
        """Generate basic SQL queries when Gemini is not available"""
        query_lower = user_query.lower()
        
        # Top ads by spend
        if 'top' in query_lower and 'ads' in query_lower and 'spend' in query_lower:
            limit = self._extract_number(user_query) or 10
            return f"""
            SELECT a.name, SUM(i.spend) as total_spend
            FROM ads a
            JOIN insights i ON a.id = i.ad_id
            WHERE i.spend > 0
            GROUP BY a.id, a.name
            ORDER BY total_spend DESC
            LIMIT {limit};
            """
        
        # Top campaigns by spend
        if 'top' in query_lower and 'campaign' in query_lower and 'spend' in query_lower:
            limit = self._extract_number(user_query) or 10
            return f"""
            SELECT c.name, SUM(i.spend) as total_spend
            FROM campaigns c
            JOIN insights i ON c.id = i.campaign_id
            WHERE i.spend > 0
            GROUP BY c.id, c.name
            ORDER BY total_spend DESC
            LIMIT {limit};
            """
        
        # Top adsets by spend
        if 'top' in query_lower and ('adset' in query_lower or 'ad set' in query_lower) and 'spend' in query_lower:
            limit = self._extract_number(user_query) or 10
            return f"""
            SELECT ads.name, SUM(i.spend) as total_spend
            FROM adsets ads
            JOIN insights i ON ads.id = i.adset_id
            WHERE i.spend > 0
            GROUP BY ads.id, ads.name
            ORDER BY total_spend DESC
            LIMIT {limit};
            """
        
        # Average CTR
        if 'average' in query_lower and 'ctr' in query_lower:
            return """
            SELECT AVG(ctr) as average_ctr
            FROM insights
            WHERE ctr > 0;
            """
        
        # Campaign performance
        if 'campaign' in query_lower and 'performance' in query_lower:
            return """
            SELECT 
                c.name,
                SUM(i.impressions) as total_impressions,
                SUM(i.clicks) as total_clicks,
                SUM(i.spend) as total_spend,
                AVG(i.ctr) as avg_ctr
            FROM campaigns c
            JOIN insights i ON c.id = i.campaign_id
            WHERE i.spend > 0
            GROUP BY c.id, c.name
            ORDER BY total_spend DESC
            LIMIT 20;
            """
        
        # Default query - show ads with spend
        if 'ads' in query_lower or 'ad' in query_lower:
            return """
            SELECT 
                a.name as ad_name,
                SUM(i.spend) as total_spend,
                SUM(i.impressions) as total_impressions,
                SUM(i.clicks) as total_clicks
            FROM ads a
            JOIN insights i ON a.id = i.ad_id
            WHERE i.spend > 0
            GROUP BY a.id, a.name
            ORDER BY total_spend DESC
            LIMIT 10;
            """
        
        # Default query - campaigns
        return """
        SELECT 
            c.name as campaign_name,
            SUM(i.spend) as total_spend,
            SUM(i.impressions) as total_impressions,
            SUM(i.clicks) as total_clicks
        FROM campaigns c
        JOIN insights i ON c.id = i.campaign_id
        WHERE i.spend > 0
        GROUP BY c.id, c.name
        ORDER BY total_spend DESC
        LIMIT 10;
        """
    
    def _extract_number(self, text: str) -> Optional[int]:
        """Extract number from text"""
        numbers = re.findall(r'\d+', text)
        return int(numbers[0]) if numbers else None
    
    def _execute_sql_query(self, sql_query: str) -> pd.DataFrame:
        """Execute SQL query and return results"""
        try:
            # Security check - only allow SELECT queries
            if not sql_query.strip().upper().startswith('SELECT'):
                raise ValueError("Only SELECT queries are allowed")
            
            # Execute query
            data = self.db_manager.execute_query(sql_query)
            return data
            
        except Exception as e:
            logger.error(f"Error executing SQL query: {e}")
            raise
    
    def _generate_insights(self, user_query: str, data: pd.DataFrame, sql_query: str) -> str:
        """Generate insights from query results using Gemini"""
        try:
            if data is None or data.empty:
                return "No data found for the given query."
            
            # Create data summary
            data_summary = self._create_data_summary(data)
            
            prompt = f"""
            Analyze the following data and provide insights based on the user's question.
            
            User Question: {user_query}
            SQL Query: {sql_query}
            Data Summary: {data_summary}
            
            Provide a concise analysis with:
            1. Key findings from the data
            2. Notable trends or patterns
            3. Actionable insights or recommendations
            
            Keep the response under 200 words and focus on business value.
            
            Analysis:
            """
            
            if hasattr(self, 'model'):
                response = self.model.generate_content(prompt)
                return response.text
            else:
                # Fallback insights
                return self._generate_fallback_insights(data, user_query)
                
        except Exception as e:
            logger.error(f"Error generating insights: {e}")
            return self._generate_fallback_insights(data, user_query)
    
    def _create_data_summary(self, data: pd.DataFrame) -> str:
        """Create a summary of the data for Gemini analysis"""
        summary = f"Rows: {len(data)}, Columns: {len(data.columns)}\n"
        
        # Add column information
        summary += f"Columns: {', '.join(data.columns)}\n"
        
        # Add basic statistics for numeric columns
        numeric_cols = data.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            summary += "Numeric Data Summary:\n"
            for col in numeric_cols[:3]:  # Limit to first 3 numeric columns
                summary += f"{col}: min={data[col].min():.2f}, max={data[col].max():.2f}, mean={data[col].mean():.2f}\n"
        
        # Add sample rows
        if len(data) > 0:
            summary += f"\nSample Data (first 3 rows):\n{data.head(3).to_string()}"
        
        return summary
    
    def _generate_fallback_insights(self, data: pd.DataFrame, user_query: str) -> str:
        """Generate basic insights when Gemini is not available"""
        insights = []
        
        # Basic data description
        insights.append(f"Found {len(data)} records matching your query.")
        
        # Analyze numeric columns
        numeric_cols = data.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            if 'spend' in col.lower():
                total = data[col].sum()
                avg = data[col].mean()
                insights.append(f"Total {col}: ${total:,.2f}, Average: ${avg:,.2f}")
            elif 'ctr' in col.lower():
                avg = data[col].mean()
                insights.append(f"Average {col}: {avg:.2f}%")
            elif col.lower() in ['impressions', 'clicks']:
                total = data[col].sum()
                insights.append(f"Total {col}: {total:,}")
        
        # Top performers
        if len(data) > 1:
            if 'name' in data.columns and len(numeric_cols) > 0:
                top_col = numeric_cols[0]
                top_performer = data.nlargest(1, top_col)['name'].iloc[0]
                insights.append(f"Top performer by {top_col}: {top_performer}")
        
        return " ".join(insights) if insights else "Data retrieved successfully."
    
    def _is_analytical_query(self, user_query: str) -> bool:
        """Check if query requires analytical processing rather than simple SQL"""
        analytical_keywords = [
            'roas', 'cac', 'cpr', 'ctr trend', 'vs', 'compared to', 'comparison',
            'best performing', 'worst performing', 'burning budget', 'low performance',
            'drops', 'spikes', 'anomalies', 'top creatives', 'learning phase',
            'limited by budget', 'audience size', 'impact of changes', 'budget shifts',
            'over-spending', 'under-spending', 'unexpected spike', 'same day last week',
            'yesterday vs today', 'past 7 days', 'trend analysis', 'performance analysis'
        ]
        
        query_lower = user_query.lower()
        return any(keyword in query_lower for keyword in analytical_keywords)
    
    def _process_analytical_query(self, user_query: str) -> Dict[str, Any]:
        """Process complex analytical queries with calculations and insights"""
        try:
            # Get all available data from database
            all_data = self._get_comprehensive_data()
            
            # Generate analytical response using Gemini
            analytical_response = self._generate_analytical_insights(user_query, all_data)
            
            return {
                'sql_query': 'Analytical Query (No SQL Required)',
                'data': all_data.get('summary', pd.DataFrame()),
                'insights': analytical_response,
                'error': None,
                'query_type': 'analytical'
            }
            
        except Exception as e:
            logger.error(f"Error processing analytical query: {e}")
            return {
                'error': str(e),
                'sql_query': None,
                'data': None,
                'insights': None,
                'query_type': 'analytical'
            }
    
    def _get_comprehensive_data(self) -> Dict[str, Any]:
        """Get comprehensive data for analytical processing"""
        try:
            # Get recent insights data with calculations
            insights_query = """
            SELECT 
                date_start,
                spend,
                impressions,
                clicks,
                reach,
                frequency,
                cpm,
                cpc,
                ctr,
                CASE WHEN clicks > 0 THEN spend / clicks ELSE 0 END as actual_cpc,
                CASE WHEN impressions > 0 THEN (clicks::float / impressions::float) * 100 ELSE 0 END as actual_ctr,
                CASE WHEN impressions > 0 THEN (spend / impressions) * 1000 ELSE 0 END as actual_cpm
            FROM insights 
            WHERE spend > 0
            ORDER BY date_start DESC
            LIMIT 30
            """
            
            insights_data = self.db_manager.execute_query(insights_query)
            
            # Get campaign summary
            campaigns_query = """
            SELECT 
                COUNT(*) as total_campaigns,
                COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) as active_campaigns,
                AVG(daily_budget) as avg_daily_budget,
                SUM(daily_budget) as total_daily_budget
            FROM campaigns
            """
            
            campaigns_summary = self.db_manager.execute_query(campaigns_query)
            
            # Get ads summary  
            ads_query = """
            SELECT 
                COUNT(*) as total_ads,
                COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) as active_ads,
                COUNT(CASE WHEN status = 'PAUSED' THEN 1 END) as paused_ads
            FROM ads
            """
            
            ads_summary = self.db_manager.execute_query(ads_query)
            
            return {
                'insights': insights_data,
                'campaigns_summary': campaigns_summary,
                'ads_summary': ads_summary,
                'summary': insights_data  # For display in UI
            }
            
        except Exception as e:
            logger.error(f"Error getting comprehensive data: {e}")
            return {'insights': pd.DataFrame(), 'summary': pd.DataFrame()}
    
    def _generate_analytical_insights(self, user_query: str, data: Dict[str, Any]) -> str:
        """Generate analytical insights using Gemini with comprehensive data analysis"""
        try:
            insights_df = data.get('insights', pd.DataFrame())
            campaigns_df = data.get('campaigns_summary', pd.DataFrame())
            ads_df = data.get('ads_summary', pd.DataFrame())
            
            if insights_df.empty:
                return "No recent insights data available for analysis."
            
            # Create data summary for analysis
            data_summary = self._create_analytical_summary(insights_df, campaigns_df, ads_df)
            
            prompt = f"""
            You are a Facebook Ads performance analyst. Analyze the provided data and answer the user's question with detailed insights, calculations, and recommendations.
            
            User Question: {user_query}
            
            Available Data Summary:
            {data_summary}
            
            Please provide:
            1. Direct answer to the user's question
            2. Relevant calculations (ROAS, CAC, CPR, CTR trends, etc.)
            3. Performance insights and patterns
            4. Actionable recommendations
            5. Any notable anomalies or concerns
            
            Format your response as a comprehensive analysis with clear sections and specific numbers where available.
            If the question asks for comparisons (vs yesterday, last week, etc.), calculate and present the differences.
            If the question asks about trends, analyze the progression over time.
            If the question asks about performance, identify best and worst performers with specific metrics.
            
            Make calculations based on the actual data provided. For metrics not directly available, explain what additional data would be needed.
            """
            
            if hasattr(self, 'model'):
                response = self.model.generate_content(prompt)
                return response.text
            else:
                return self._generate_fallback_analytical_insights(user_query, insights_df)
                
        except Exception as e:
            logger.error(f"Error generating analytical insights: {e}")
            return f"Error analyzing data: {str(e)}"
    
    def _create_analytical_summary(self, insights_df: pd.DataFrame, campaigns_df: pd.DataFrame, ads_df: pd.DataFrame) -> str:
        """Create comprehensive data summary for analytical processing"""
        summary = []
        
        if not insights_df.empty:
            # Recent performance metrics
            recent_spend = insights_df['spend'].sum()
            recent_impressions = insights_df['impressions'].sum()
            recent_clicks = insights_df['clicks'].sum()
            avg_ctr = insights_df['actual_ctr'].mean()
            avg_cpm = insights_df['actual_cpm'].mean()
            avg_cpc = insights_df['actual_cpc'].mean()
            
            summary.append(f"Recent Performance (Last 30 data points):")
            summary.append(f"- Total Spend: ${recent_spend:,.2f}")
            summary.append(f"- Total Impressions: {recent_impressions:,}")
            summary.append(f"- Total Clicks: {recent_clicks:,}")
            summary.append(f"- Average CTR: {avg_ctr:.2f}%")
            summary.append(f"- Average CPM: ${avg_cpm:.2f}")
            summary.append(f"- Average CPC: ${avg_cpc:.2f}")
            
            # Daily trends
            if 'date_start' in insights_df.columns:
                insights_df['date_start'] = pd.to_datetime(insights_df['date_start'])
                daily_spend = insights_df.groupby('date_start')['spend'].sum().sort_index()
                if len(daily_spend) > 1:
                    latest_spend = daily_spend.iloc[-1]
                    previous_spend = daily_spend.iloc[-2] if len(daily_spend) > 1 else 0
                    spend_change = ((latest_spend - previous_spend) / previous_spend * 100) if previous_spend > 0 else 0
                    summary.append(f"- Latest day spend: ${latest_spend:.2f} (Change: {spend_change:+.1f}%)")
        
        if not campaigns_df.empty:
            campaigns_data = campaigns_df.iloc[0]
            summary.append(f"\\nCampaign Overview:")
            summary.append(f"- Total Campaigns: {campaigns_data.get('total_campaigns', 0)}")
            summary.append(f"- Active Campaigns: {campaigns_data.get('active_campaigns', 0)}")
            summary.append(f"- Total Daily Budget: ${campaigns_data.get('total_daily_budget', 0):,.2f}")
        
        if not ads_df.empty:
            ads_data = ads_df.iloc[0]
            summary.append(f"\\nAds Overview:")
            summary.append(f"- Total Ads: {ads_data.get('total_ads', 0)}")
            summary.append(f"- Active Ads: {ads_data.get('active_ads', 0)}")
            summary.append(f"- Paused Ads: {ads_data.get('paused_ads', 0)}")
        
        return "\\n".join(summary)
    
    def _generate_fallback_analytical_insights(self, user_query: str, insights_df: pd.DataFrame) -> str:
        """Generate basic analytical insights when Gemini is not available"""
        if insights_df.empty:
            return "No data available for analysis."
        
        insights = []
        query_lower = user_query.lower()
        
        # Calculate basic metrics
        total_spend = insights_df['spend'].sum()
        total_impressions = insights_df['impressions'].sum()
        total_clicks = insights_df['clicks'].sum()
        avg_ctr = insights_df['actual_ctr'].mean()
        avg_cpm = insights_df['actual_cpm'].mean()
        
        insights.append(f"**Performance Summary:**")
        insights.append(f"Total Spend: ${total_spend:,.2f}")
        insights.append(f"Total Impressions: {total_impressions:,}")
        insights.append(f"Total Clicks: {total_clicks:,}")
        insights.append(f"Average CTR: {avg_ctr:.2f}%")
        insights.append(f"Average CPM: ${avg_cpm:.2f}")
        
        # Trend analysis if date data available
        if 'date_start' in insights_df.columns:
            insights_df['date_start'] = pd.to_datetime(insights_df['date_start'])
            daily_spend = insights_df.groupby('date_start')['spend'].sum().sort_index()
            
            if len(daily_spend) > 1:
                insights.append(f"\\n**Trend Analysis:**")
                latest_spend = daily_spend.iloc[-1]
                previous_spend = daily_spend.iloc[-2]
                change = ((latest_spend - previous_spend) / previous_spend * 100) if previous_spend > 0 else 0
                insights.append(f"Latest day spend: ${latest_spend:.2f}")
                insights.append(f"Previous day spend: ${previous_spend:.2f}")
                insights.append(f"Day-over-day change: {change:+.1f}%")
        
        if 'roas' in query_lower or 'return' in query_lower:
            insights.append(f"\\n**Note:** ROAS calculation requires conversion/revenue data which is not available in current dataset.")
        
        if 'cac' in query_lower:
            insights.append(f"\\n**Note:** CAC calculation requires conversion count data which is not available in current dataset.")
        
        return "\\n".join(insights)
