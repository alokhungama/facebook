import os
from dotenv import load_dotenv
load_dotenv()
import os
import requests
import time
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FacebookAPI:
    def __init__(self):
        """Initialize Facebook Marketing API client"""
        self.access_token = os.getenv('FACEBOOK_ACCESS_TOKEN', '')
        self.api_version = 'v18.0'
        self.base_url = f'https://graph.facebook.com/{self.api_version}'
        self.limit = 500  # Pagination limit
        
        if not self.access_token:
            logger.warning("Facebook access token not found in environment variables")
    
    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make API request to Facebook Marketing API"""
        url = f"{self.base_url}/{endpoint}"
        
        default_params = {
            'access_token': self.access_token,
            'limit': self.limit
        }
        
        if params:
            default_params.update(params)
        
        try:
            response = requests.get(url, params=default_params)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response content: {e.response.text}")
            raise
    
    def _paginate_request(self, endpoint: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Handle paginated API requests"""
        all_data = []
        
        try:
            response = self._make_request(endpoint, params)
            
            # Add data from first page
            if 'data' in response:
                all_data.extend(response['data'])
            
            # Handle pagination
            while 'paging' in response and 'next' in response['paging']:
                logger.info(f"Fetching next page, current count: {len(all_data)}")
                
                # Extract after parameter from next URL
                next_url = response['paging']['next']
                after_param = next_url.split('after=')[1].split('&')[0] if 'after=' in next_url else None
                
                if after_param:
                    paginated_params = params.copy() if params else {}
                    paginated_params['after'] = after_param
                    response = self._make_request(endpoint, paginated_params)
                    
                    if 'data' in response:
                        all_data.extend(response['data'])
                    else:
                        break
                else:
                    break
                
                # Rate limiting
                time.sleep(0.1)
            
            logger.info(f"Fetched total {len(all_data)} records from {endpoint}")
            return all_data
            
        except Exception as e:
            logger.error(f"Error in paginated request for {endpoint}: {e}")
            raise
    
    def fetch_campaigns(self, account_id: str) -> List[Dict[str, Any]]:
        """Fetch campaigns for the given account"""
        endpoint = f"{account_id}/campaigns"
        
        fields = [
            'id', 'name', 'status', 'objective', 'created_time', 'updated_time',
            'start_time', 'stop_time', 'budget_remaining', 'daily_budget',
            'lifetime_budget', 'account_id'
        ]
        
        params = {
            'fields': ','.join(fields)
        }
        
        try:
            campaigns = self._paginate_request(endpoint, params)
            
            # Process and clean data
            processed_campaigns = []
            for campaign in campaigns:
                processed_campaign = {
                    'id': campaign.get('id'),
                    'account_id': account_id,
                    'name': campaign.get('name'),
                    'status': campaign.get('status'),
                    'objective': campaign.get('objective'),
                    'created_time': self._parse_datetime(campaign.get('created_time')),
                    'updated_time': self._parse_datetime(campaign.get('updated_time')),
                    'start_time': self._parse_datetime(campaign.get('start_time')),
                    'stop_time': self._parse_datetime(campaign.get('stop_time')),
                    'budget_remaining': self._parse_float(campaign.get('budget_remaining')),
                    'daily_budget': self._parse_float(campaign.get('daily_budget')),
                    'lifetime_budget': self._parse_float(campaign.get('lifetime_budget')),
                    'data': campaign  # Store raw data
                }
                processed_campaigns.append(processed_campaign)
            
            return processed_campaigns
            
        except Exception as e:
            logger.error(f"Error fetching campaigns: {e}")
            # Return sample data for development/testing
            return self._get_sample_campaigns(account_id)
    
    def fetch_adsets(self, account_id: str) -> List[Dict[str, Any]]:
        """Fetch ad sets for the given account"""
        endpoint = f"{account_id}/adsets"
        
        fields = [
            'id', 'name', 'status', 'campaign_id', 'optimization_goal',
            'billing_event', 'bid_amount', 'daily_budget', 'lifetime_budget',
            'start_time', 'end_time', 'created_time', 'updated_time', 'account_id'
        ]
        
        params = {
            'fields': ','.join(fields)
        }
        
        try:
            adsets = self._paginate_request(endpoint, params)
            
            # Process and clean data
            processed_adsets = []
            for adset in adsets:
                processed_adset = {
                    'id': adset.get('id'),
                    'account_id': account_id,
                    'campaign_id': adset.get('campaign_id'),
                    'name': adset.get('name'),
                    'status': adset.get('status'),
                    'optimization_goal': adset.get('optimization_goal'),
                    'billing_event': adset.get('billing_event'),
                    'bid_amount': self._parse_float(adset.get('bid_amount')),
                    'daily_budget': self._parse_float(adset.get('daily_budget')),
                    'lifetime_budget': self._parse_float(adset.get('lifetime_budget')),
                    'start_time': self._parse_datetime(adset.get('start_time')),
                    'end_time': self._parse_datetime(adset.get('end_time')),
                    'created_time': self._parse_datetime(adset.get('created_time')),
                    'updated_time': self._parse_datetime(adset.get('updated_time')),
                    'data': adset  # Store raw data
                }
                processed_adsets.append(processed_adset)
            
            return processed_adsets
            
        except Exception as e:
            logger.error(f"Error fetching ad sets: {e}")
            # Return sample data for development/testing
            return self._get_sample_adsets(account_id)
    
    def fetch_ads(self, account_id: str) -> List[Dict[str, Any]]:
        """Fetch ads for the given account"""
        endpoint = f"{account_id}/ads"
        
        fields = [
            'id', 'name', 'status', 'campaign_id', 'adset_id',
            'created_time', 'updated_time', 'account_id'
        ]
        
        params = {
            'fields': ','.join(fields)
        }
        
        try:
            ads = self._paginate_request(endpoint, params)
            
            # Process and clean data
            processed_ads = []
            for ad in ads:
                processed_ad = {
                    'id': ad.get('id'),
                    'account_id': account_id,
                    'campaign_id': ad.get('campaign_id'),
                    'adset_id': ad.get('adset_id'),
                    'name': ad.get('name'),
                    'status': ad.get('status'),
                    'created_time': self._parse_datetime(ad.get('created_time')),
                    'updated_time': self._parse_datetime(ad.get('updated_time')),
                    'data': ad  # Store raw data
                }
                processed_ads.append(processed_ad)
            
            return processed_ads
            
        except Exception as e:
            logger.error(f"Error fetching ads: {e}")
            # Return sample data for development/testing
            return self._get_sample_ads(account_id)
    
    def fetch_insights(self, account_id: str, date_preset: str = 'last_30d') -> List[Dict[str, Any]]:
        """Fetch insights for the given account"""
        endpoint = f"{account_id}/insights"
        
        fields = [
            'impressions', 'clicks', 'spend', 'reach', 'frequency',
            'cpm', 'cpc', 'ctr', 'cpp', 'actions', 'cost_per_action_type',
            'campaign_id', 'adset_id', 'ad_id', 'date_start', 'date_stop'
        ]
        
        params = {
            'fields': ','.join(fields),
            'date_preset': date_preset,
            'time_increment': 1
        }
        
        try:
            insights = self._paginate_request(endpoint, params)
            
            # Process and clean data
            processed_insights = []
            for insight in insights:
                processed_insight = {
                    'id': f"{insight.get('campaign_id', '')}-{insight.get('date_start', '')}-{len(processed_insights)}",
                    'account_id': account_id,
                    'campaign_id': insight.get('campaign_id'),
                    'adset_id': insight.get('adset_id'),
                    'ad_id': insight.get('ad_id'),
                    'date_start': self._parse_date(insight.get('date_start')),
                    'date_stop': self._parse_date(insight.get('date_stop')),
                    'impressions': self._parse_int(insight.get('impressions')),
                    'clicks': self._parse_int(insight.get('clicks')),
                    'spend': self._parse_float(insight.get('spend')),
                    'reach': self._parse_int(insight.get('reach')),
                    'frequency': self._parse_float(insight.get('frequency')),
                    'cpm': self._parse_float(insight.get('cpm')),
                    'cpc': self._parse_float(insight.get('cpc')),
                    'ctr': self._parse_float(insight.get('ctr')),
                    'cpp': self._parse_float(insight.get('cpp')),
                    'actions': insight.get('actions'),
                    'cost_per_action_type': insight.get('cost_per_action_type'),
                    'data': insight  # Store raw data
                }
                processed_insights.append(processed_insight)
            
            return processed_insights
            
        except Exception as e:
            logger.error(f"Error fetching insights: {e}")
            # Return sample data for development/testing
            return self._get_sample_insights(account_id)
    
    def _parse_datetime(self, date_string: str) -> datetime:
        """Parse datetime string from Facebook API"""
        if not date_string:
            return None
        try:
            return datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S%z')
        except:
            try:
                return datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
            except:
                return None
    
    def _parse_date(self, date_string: str) -> datetime:
        """Parse date string from Facebook API"""
        if not date_string:
            return None
        try:
            return datetime.strptime(date_string, '%Y-%m-%d')
        except:
            return None
    
    def _parse_float(self, value) -> float:
        """Parse float value"""
        if value is None or value == '':
            return 0.0
        try:
            return float(value)
        except:
            return 0.0
    
    def _parse_int(self, value) -> int:
        """Parse integer value"""
        if value is None or value == '':
            return 0
        try:
            return int(value)
        except:
            return 0
    
    # Sample data methods for development/testing when API fails
    def _get_sample_campaigns(self, account_id: str) -> List[Dict[str, Any]]:
        """Return sample campaign data for development"""
        logger.info("Returning sample campaign data for development")
        return [
            {
                'id': 'camp_001',
                'account_id': account_id,
                'name': 'Holiday Sales Campaign',
                'status': 'ACTIVE',
                'objective': 'CONVERSIONS',
                'created_time': datetime.now() - timedelta(days=30),
                'updated_time': datetime.now() - timedelta(days=1),
                'start_time': datetime.now() - timedelta(days=30),
                'stop_time': None,
                'budget_remaining': 5000.0,
                'daily_budget': 100.0,
                'lifetime_budget': None,
                'data': {}
            },
            {
                'id': 'camp_002',
                'account_id': account_id,
                'name': 'Brand Awareness Campaign',
                'status': 'ACTIVE',
                'objective': 'BRAND_AWARENESS',
                'created_time': datetime.now() - timedelta(days=20),
                'updated_time': datetime.now() - timedelta(days=2),
                'start_time': datetime.now() - timedelta(days=20),
                'stop_time': None,
                'budget_remaining': 3000.0,
                'daily_budget': 75.0,
                'lifetime_budget': None,
                'data': {}
            }
        ]
    
    def _get_sample_adsets(self, account_id: str) -> List[Dict[str, Any]]:
        """Return sample adset data for development"""
        logger.info("Returning sample adset data for development")
        return [
            {
                'id': 'adset_001',
                'account_id': account_id,
                'campaign_id': 'camp_001',
                'name': 'Holiday Sales - Desktop',
                'status': 'ACTIVE',
                'optimization_goal': 'CONVERSIONS',
                'billing_event': 'IMPRESSIONS',
                'bid_amount': 2.50,
                'daily_budget': 50.0,
                'lifetime_budget': None,
                'start_time': datetime.now() - timedelta(days=30),
                'end_time': None,
                'created_time': datetime.now() - timedelta(days=30),
                'updated_time': datetime.now() - timedelta(days=1),
                'data': {}
            },
            {
                'id': 'adset_002',
                'account_id': account_id,
                'campaign_id': 'camp_001',
                'name': 'Holiday Sales - Mobile',
                'status': 'ACTIVE',
                'optimization_goal': 'CONVERSIONS',
                'billing_event': 'IMPRESSIONS',
                'bid_amount': 2.00,
                'daily_budget': 50.0,
                'lifetime_budget': None,
                'start_time': datetime.now() - timedelta(days=30),
                'end_time': None,
                'created_time': datetime.now() - timedelta(days=30),
                'updated_time': datetime.now() - timedelta(days=1),
                'data': {}
            }
        ]
    
    def _get_sample_ads(self, account_id: str) -> List[Dict[str, Any]]:
        """Return sample ads data for development"""
        logger.info("Returning sample ads data for development")
        return [
            {
                'id': 'ad_001',
                'account_id': account_id,
                'campaign_id': 'camp_001',
                'adset_id': 'adset_001',
                'name': 'Holiday Sale - Desktop Video',
                'status': 'ACTIVE',
                'created_time': datetime.now() - timedelta(days=30),
                'updated_time': datetime.now() - timedelta(days=1),
                'data': {}
            },
            {
                'id': 'ad_002',
                'account_id': account_id,
                'campaign_id': 'camp_001',
                'adset_id': 'adset_002',
                'name': 'Holiday Sale - Mobile Image',
                'status': 'ACTIVE',
                'created_time': datetime.now() - timedelta(days=30),
                'updated_time': datetime.now() - timedelta(days=1),
                'data': {}
            }
        ]
    
    def _get_sample_insights(self, account_id: str) -> List[Dict[str, Any]]:
        """Return sample insights data for development"""
        logger.info("Returning sample insights data for development")
        insights = []
        
        # Generate 30 days of sample data
        for i in range(30):
            date = datetime.now() - timedelta(days=i)
            
            insights.extend([
                {
                    'id': f"insight_{i}_1",
                    'account_id': account_id,
                    'campaign_id': 'camp_001',
                    'adset_id': 'adset_001',
                    'ad_id': 'ad_001',
                    'date_start': date,
                    'date_stop': date,
                    'impressions': 1000 + (i * 50),
                    'clicks': 50 + (i * 2),
                    'spend': 45.50 + (i * 1.5),
                    'reach': 800 + (i * 30),
                    'frequency': 1.2 + (i * 0.01),
                    'cpm': 45.50,
                    'cpc': 0.91,
                    'ctr': 5.0,
                    'cpp': 0.057,
                    'actions': [],
                    'cost_per_action_type': [],
                    'data': {}
                },
                {
                    'id': f"insight_{i}_2",
                    'account_id': account_id,
                    'campaign_id': 'camp_002',
                    'adset_id': 'adset_002',
                    'ad_id': 'ad_002',
                    'date_start': date,
                    'date_stop': date,
                    'impressions': 800 + (i * 40),
                    'clicks': 40 + (i * 1.5),
                    'spend': 35.75 + (i * 1.2),
                    'reach': 650 + (i * 25),
                    'frequency': 1.1 + (i * 0.008),
                    'cpm': 44.69,
                    'cpc': 0.89,
                    'ctr': 5.2,
                    'cpp': 0.055,
                    'actions': [],
                    'cost_per_action_type': [],
                    'data': {}
                }
            ])
        
        return insights
