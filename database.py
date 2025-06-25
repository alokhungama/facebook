import os
from dotenv import load_dotenv
load_dotenv()
import os
import pandas as pd
import json
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer, Float, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import uuid
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        """Initialize database connection and create tables if they don't exist"""
        self.engine = self._create_engine()
        self.metadata = MetaData()
        self._define_tables()
        self._create_tables()
        
        # Create session
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
    
    def _create_engine(self):
        """Create database engine using environment variables"""
        try:
            # Try to get full DATABASE_URL first
            database_url = os.getenv('DATABASE_URL')
            print("DEBUG: Using DATABASE_URL =", database_url)

            
            if database_url:
                # Handle postgres:// vs postgresql:// prefix
                if database_url.startswith('postgres://'):
                    database_url = database_url.replace('postgres://', 'postgresql://', 1)
                return create_engine(database_url)
            
            # Fallback to individual components
            host = os.getenv('PGHOST', 'localhost')
            port = os.getenv('PGPORT', '5432')
            database = os.getenv('PGDATABASE', 'facebookdb')
            username = os.getenv('PGUSER', 'postgres')
            password = os.getenv('PGPASSWORD', '')
            
            connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
            return create_engine(connection_string)
            
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise
    
    def _define_tables(self):
        """Define database table schemas"""
        
        # Campaigns table
        self.campaigns_table = Table(
            'campaigns',
            self.metadata,
            Column('id', String(50), primary_key=True),
            Column('account_id', String(50), nullable=False),
            Column('name', String(255), nullable=False),
            Column('status', String(50)),
            Column('objective', String(100)),
            Column('created_time', DateTime),
            Column('updated_time', DateTime),
            Column('start_time', DateTime),
            Column('stop_time', DateTime),
            Column('budget_remaining', Float),
            Column('daily_budget', Float),
            Column('lifetime_budget', Float),
            Column('data', JSONB),
            Column('inserted_at', DateTime, default=datetime.utcnow)
        )
        
        # Ad Sets table
        self.adsets_table = Table(
            'adsets',
            self.metadata,
            Column('id', String(50), primary_key=True),
            Column('account_id', String(50), nullable=False),
            Column('campaign_id', String(50)),
            Column('name', String(255), nullable=False),
            Column('status', String(50)),
            Column('optimization_goal', String(100)),
            Column('billing_event', String(100)),
            Column('bid_amount', Float),
            Column('daily_budget', Float),
            Column('lifetime_budget', Float),
            Column('start_time', DateTime),
            Column('end_time', DateTime),
            Column('created_time', DateTime),
            Column('updated_time', DateTime),
            Column('data', JSONB),
            Column('inserted_at', DateTime, default=datetime.utcnow)
        )
        
        # Ads table
        self.ads_table = Table(
            'ads',
            self.metadata,
            Column('id', String(50), primary_key=True),
            Column('account_id', String(50), nullable=False),
            Column('campaign_id', String(50)),
            Column('adset_id', String(50)),
            Column('name', String(255), nullable=False),
            Column('status', String(50)),
            Column('created_time', DateTime),
            Column('updated_time', DateTime),
            Column('data', JSONB),
            Column('inserted_at', DateTime, default=datetime.utcnow)
        )
        
        # Insights table
        self.insights_table = Table(
            'insights',
            self.metadata,
            Column('id', String(50), primary_key=True),
            Column('account_id', String(50), nullable=False),
            Column('campaign_id', String(50)),
            Column('adset_id', String(50)),
            Column('ad_id', String(50)),
            Column('date_start', DateTime),
            Column('date_stop', DateTime),
            Column('impressions', Integer),
            Column('clicks', Integer),
            Column('spend', Float),
            Column('reach', Integer),
            Column('frequency', Float),
            Column('cpm', Float),
            Column('cpc', Float),
            Column('ctr', Float),
            Column('cpp', Float),
            Column('actions', JSONB),
            Column('cost_per_action_type', JSONB),
            Column('data', JSONB),
            Column('inserted_at', DateTime, default=datetime.utcnow)
        )
    
    def _create_tables(self):
        """Create tables if they don't exist"""
        try:
            self.metadata.create_all(self.engine)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise
    
    def store_campaigns(self, campaigns_data):
        """Store campaigns data in database"""
        try:
            # Convert to DataFrame for easier handling
            df = pd.DataFrame(campaigns_data)
            
            # Convert 'data' column to JSON string if it exists
            if 'data' in df.columns:
                df['data'] = df['data'].apply(lambda x: json.dumps(x) if x is not None else None)
            
            # Upsert data
            df.to_sql(
                'campaigns',
                self.engine,
                if_exists='replace',
                index=False,
                dtype={'data': JSONB}
            )
            
            logger.info(f"Stored {len(campaigns_data)} campaigns")
            
        except Exception as e:
            logger.error(f"Failed to store campaigns: {e}")
            raise
    
    def store_adsets(self, adsets_data):
        """Store ad sets data in database"""
        try:
            df = pd.DataFrame(adsets_data)
            
            # Convert 'data' column to JSON string if it exists
            if 'data' in df.columns:
                df['data'] = df['data'].apply(lambda x: json.dumps(x) if x is not None else None)
            
            df.to_sql(
                'adsets',
                self.engine,
                if_exists='replace',
                index=False,
                dtype={'data': JSONB}
            )
            
            logger.info(f"Stored {len(adsets_data)} ad sets")
            
        except Exception as e:
            logger.error(f"Failed to store ad sets: {e}")
            raise
    
    def store_ads(self, ads_data):
        """Store ads data in database"""
        try:
            df = pd.DataFrame(ads_data)
            
            # Convert 'data' column to JSON string if it exists
            if 'data' in df.columns:
                df['data'] = df['data'].apply(lambda x: json.dumps(x) if x is not None else None)
            
            df.to_sql(
                'ads',
                self.engine,
                if_exists='replace',
                index=False,
                dtype={'data': JSONB}
            )
            
            logger.info(f"Stored {len(ads_data)} ads")
            
        except Exception as e:
            logger.error(f"Failed to store ads: {e}")
            raise
    
    def store_insights(self, insights_data):
        """Store insights data in database"""
        try:
            df = pd.DataFrame(insights_data)
            
            # Convert JSON columns to JSON strings
            json_columns = ['actions', 'cost_per_action_type', 'data']
            for col in json_columns:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)
            
            # Define dtype mapping for JSONB columns
            dtype_mapping = {}
            for col in json_columns:
                if col in df.columns:
                    dtype_mapping[col] = JSONB
            
            df.to_sql(
                'insights',
                self.engine,
                if_exists='replace',
                index=False,
                dtype=dtype_mapping
            )
            
            logger.info(f"Stored {len(insights_data)} insights")
            
        except Exception as e:
            logger.error(f"Failed to store insights: {e}")
            raise
    
    def get_campaigns(self):
        """Retrieve campaigns from database"""
        try:
            query = "SELECT * FROM campaigns ORDER BY created_time DESC"
            df = pd.read_sql(query, self.engine)
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Failed to retrieve campaigns: {e}")
            return []
    
    def get_adsets(self):
        """Retrieve ad sets from database"""
        try:
            query = "SELECT * FROM adsets ORDER BY created_time DESC"
            df = pd.read_sql(query, self.engine)
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Failed to retrieve ad sets: {e}")
            return []
    
    def get_ads(self):
        """Retrieve ads from database"""
        try:
            query = "SELECT * FROM ads ORDER BY created_time DESC"
            df = pd.read_sql(query, self.engine)
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Failed to retrieve ads: {e}")
            return []
    
    def get_insights(self):
        """Retrieve insights from database"""
        try:
            query = "SELECT * FROM insights ORDER BY date_start DESC"
            df = pd.read_sql(query, self.engine)
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Failed to retrieve insights: {e}")
            return []
    
    def execute_query(self, query):
        """Execute a custom SQL query and return results as DataFrame"""
        try:
            df = pd.read_sql(text(query), self.engine)
            return df
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise
    
    def get_schema_info(self):
        """Get database schema information for AI query generation"""
        schema_info = {
            'campaigns': {
                'table': 'campaigns',
                'columns': [
                    'id', 'account_id', 'name', 'status', 'objective',
                    'created_time', 'updated_time', 'start_time', 'stop_time',
                    'budget_remaining', 'daily_budget', 'lifetime_budget'
                ],
                'description': 'Facebook advertising campaigns data with campaign details'
            },
            'adsets': {
                'table': 'adsets', 
                'columns': [
                    'id', 'account_id', 'campaign_id', 'name', 'status',
                    'optimization_goal', 'billing_event', 'bid_amount',
                    'daily_budget', 'lifetime_budget', 'start_time', 'end_time'
                ],
                'description': 'Facebook ad sets data linked to campaigns'
            },
            'ads': {
                'table': 'ads',
                'columns': [
                    'id', 'account_id', 'campaign_id', 'adset_id', 'name',
                    'status', 'created_time', 'updated_time'
                ],
                'description': 'Individual Facebook ads linked to campaigns and adsets'
            },
            'insights': {
                'table': 'insights',
                'columns': [
                    'id', 'account_id', 'date_start', 'date_stop', 
                    'impressions', 'clicks', 'spend', 'reach', 'frequency', 
                    'cpm', 'cpc', 'ctr', 'cpp'
                ],
                'description': 'Account-level performance metrics by date. NOTE: campaign_id, adset_id, ad_id are mostly NULL - this contains account-level aggregated data'
            }
        }
        return schema_info
    
    def close(self):
        """Close database connection"""
        try:
            self.session.close()
            self.engine.dispose()
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")
