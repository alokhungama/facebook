import re
from typing import Optional

def validate_account_id(account_id: str) -> bool:
    """Validate Facebook Ads account ID format"""
    if not account_id:
        return False
    
    # Facebook account IDs should start with 'act_' followed by digits
    pattern = r'^act_\d+$'
    return bool(re.match(pattern, account_id))

def format_currency(amount: float) -> str:
    """Format currency amount"""
    if amount is None:
        return "$0.00"
    return f"${amount:,.2f}"

def format_percentage(percentage: float) -> str:
    """Format percentage value"""
    if percentage is None:
        return "0.00%"
    return f"{percentage:.2f}%"

def format_number(number: int) -> str:
    """Format large numbers with commas"""
    if number is None:
        return "0"
    return f"{number:,}"

def truncate_text(text: str, max_length: int = 50) -> str:
    """Truncate text to specified length"""
    if not text:
        return ""
    
    if len(text) <= max_length:
        return text
    
    return text[:max_length-3] + "..."

def safe_divide(numerator: float, denominator: float) -> float:
    """Safely divide two numbers, return 0 if denominator is 0"""
    if denominator == 0 or denominator is None:
        return 0.0
    
    if numerator is None:
        return 0.0
    
    return numerator / denominator

def calculate_ctr(clicks: int, impressions: int) -> float:
    """Calculate click-through rate"""
    if impressions == 0 or impressions is None:
        return 0.0
    
    if clicks is None:
        clicks = 0
    
    return (clicks / impressions) * 100

def calculate_cpc(spend: float, clicks: int) -> float:
    """Calculate cost per click"""
    if clicks == 0 or clicks is None:
        return 0.0
    
    if spend is None:
        spend = 0.0
    
    return spend / clicks

def calculate_cpm(spend: float, impressions: int) -> float:
    """Calculate cost per mille (thousand impressions)"""
    if impressions == 0 or impressions is None:
        return 0.0
    
    if spend is None:
        spend = 0.0
    
    return (spend / impressions) * 1000

def parse_date_range(date_range: str) -> tuple:
    """Parse date range string into start and end dates"""
    # This is a simplified parser - in production you'd want more robust parsing
    if date_range == "last_7d":
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        return start_date, end_date
    
    elif date_range == "last_30d":
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        return start_date, end_date
    
    # Add more date range parsing as needed
    return None, None

def clean_campaign_name(name: str) -> str:
    """Clean campaign name for display"""
    if not name:
        return "Unknown Campaign"
    
    # Remove common prefixes/suffixes
    cleaned = name.strip()
    
    # Remove timestamp patterns
    cleaned = re.sub(r'\d{4}-\d{2}-\d{2}', '', cleaned)
    cleaned = re.sub(r'\d{2}/\d{2}/\d{4}', '', cleaned)
    
    # Clean up extra spaces
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    return cleaned if cleaned else "Unknown Campaign"

def get_status_color(status: str) -> str:
    """Get color for status display"""
    status_colors = {
        'ACTIVE': 'green',
        'PAUSED': 'orange',
        'DELETED': 'red',
        'ARCHIVED': 'gray',
        'PENDING_REVIEW': 'blue',
        'DISAPPROVED': 'red',
        'PREAPPROVED': 'blue',
        'PENDING_BILLING_INFO': 'orange',
        'CAMPAIGN_PAUSED': 'orange',
        'ADSET_PAUSED': 'orange'
    }
    
    return status_colors.get(status.upper(), 'gray')

def extract_insights_metrics(insights_data: list) -> dict:
    """Extract key metrics from insights data"""
    if not insights_data:
        return {
            'total_spend': 0,
            'total_impressions': 0,
            'total_clicks': 0,
            'average_ctr': 0,
            'average_cpc': 0,
            'average_cpm': 0
        }
    
    total_spend = sum(float(item.get('spend', 0)) for item in insights_data)
    total_impressions = sum(int(item.get('impressions', 0)) for item in insights_data)
    total_clicks = sum(int(item.get('clicks', 0)) for item in insights_data)
    
    average_ctr = calculate_ctr(total_clicks, total_impressions)
    average_cpc = calculate_cpc(total_spend, total_clicks)
    average_cpm = calculate_cpm(total_spend, total_impressions)
    
    return {
        'total_spend': total_spend,
        'total_impressions': total_impressions,
        'total_clicks': total_clicks,
        'average_ctr': average_ctr,
        'average_cpc': average_cpc,
        'average_cpm': average_cpm
    }

def generate_chart_config(chart_type: str, data: dict) -> dict:
    """Generate chart configuration for different chart types"""
    base_config = {
        'responsive': True,
        'maintainAspectRatio': False,
        'plugins': {
            'legend': {
                'position': 'top',
            },
            'title': {
                'display': True,
                'text': data.get('title', 'Chart')
            }
        }
    }
    
    if chart_type == 'line':
        base_config['scales'] = {
            'y': {
                'beginAtZero': True
            }
        }
    
    elif chart_type == 'bar':
        base_config['scales'] = {
            'y': {
                'beginAtZero': True
            }
        }
    
    elif chart_type == 'pie':
        base_config['responsive'] = True
        base_config['maintainAspectRatio'] = True
    
    return base_config
