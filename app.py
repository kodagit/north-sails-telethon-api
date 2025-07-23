# app.py - Flask API for North Sails Telethon Scanner with Notion Integration
import asyncio
import json
import os
import requests
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from telethon import TelegramClient
import logging
from collections import Counter
import re

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# API credentials from environment
API_ID = int(os.getenv('TELEGRAM_API_ID', '29481789'))
API_HASH = os.getenv('TELEGRAM_API_HASH', '59f4a8346e712a5509ff700bc1da9b5d')
NOTION_TOKEN = os.getenv('NOTION_TOKEN', '')
NOTION_DATABASE_ID = os.getenv('NOTION_DATABASE_ID', '3405e3cc-485f-4281-beaa-2e138bb8fd29')

class NotionClient:
    def __init__(self):
        self.token = NOTION_TOKEN
        self.base_url = "https://api.notion.com/v1"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
    
    def get_channels(self):
        """Notion database'inden aktif kanalları çek"""
        try:
            url = f"{self.base_url}/databases/{NOTION_DATABASE_ID}/query"
            
            # Only get active channels
            payload = {
                "filter": {
                    "property": "Status",
                    "status": {
                        "equals": "Active"
                    }
                }
            }
            
            response = requests.post(url, headers=self.headers, json=payload)
            
            if response.status_code == 200:
                data = response.json()
                channels = []
                
                for page in data['results']:
                    props = page['properties']
                    
                    channel_data = {
                        'id': page['id'],
                        'channel': props.get('Channel', {}).get('title', [{}])[0].get('text', {}).get('content', ''),
                        'channel_title': props.get('Channel Title', {}).get('rich_text', [{}])[0].get('text', {}).get('content', ''),
                        'category': props.get('Category', {}).get('select', {}).get('name', 'Unknown'),
                        'priority': props.get('Priority', {}).get('select', {}).get('name', 'Medium'),
                        'language': props.get('Language', {}).get('select', {}).get('name', 'Russian'),
                        'content_types': [item['name'] for item in props.get('Content Type', {}).get('multi_select', [])],
                        'subscribers': props.get('Subscribers', {}).get('number', 0),
                        'last_scanned': props.get('Last Scanned', {}).get('date', {}).get('start', None)
                    }
                    
                    if channel_data['channel']:  # Only add if channel name exists
                        channels.append(channel_data)
                
                logger.info(f"📊 Fetched {len(channels)} active channels from Notion")
                return channels
                
            else:
                logger.error(f"❌ Notion API error: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Error fetching channels from Notion: {str(e)}")
            return []
    
    def update_channel_stats(self, channel_id, stats):
        """Kanal istatistiklerini Notion'da güncelle"""
        try:
            url = f"{self.base_url}/pages/{channel_id}"
            
            payload = {
                "properties": {
                    "Last Scanned": {
                        "date": {
                            "start": datetime.now().isoformat()
                        }
                    },
                    "Total Posts Found": {
                        "number": stats.get('total_posts', 0)
                    },
                    "Avg North Sails Score": {
                        "number": stats.get('avg_score', 0)
                    }
                }
            }
            
            response = requests.patch(url, headers=self.headers, json=payload)
            
            if response.status_code == 200:
                logger.info(f"✅ Updated stats for channel {channel_id}")
                return True
            else:
                logger.error(f"❌ Failed to update channel {channel_id}: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error updating channel stats: {str(e)}")
            return False

class TelethonScanner:
    def __init__(self):
        self.notion = NotionClient()
        
        # Core brand keywords for initial filtering
        self.core_brand_keywords = [
            'north sails', 'northsails', 'норт сейлс',
            'sailing', 'yacht', 'парус', 'яхта', 'море',
            'fashion', 'мода', 'стиль', 'style', 'бренд',
            'luxury', 'премиум', 'элитный', 'lifestyle'
        ]

    def extract_keywords_from_content(self, posts, min_frequency=3):
        """İçerikten trend keyword'leri otomatik çıkar"""
        all_words = []
        
        for post in posts:
            # Clean and tokenize content
            content = post.get('content', '').lower()
            # Remove URLs, mentions, hashtags
            content = re.sub(r'http\S+|@\w+|#\w+', '', content)
            # Extract words (Cyrillic + Latin)
            words = re.findall(r'[а-яё\w]+', content)
            
            # Filter meaningful words (length > 3)
            meaningful_words = [w for w in words if len(w) > 3]
            all_words.extend(meaningful_words)
        
        # Count frequency
        word_counts = Counter(all_words)
        
        # Get trending keywords
        trending_keywords = [word for word, count in word_counts.items() 
                           if count >= min_frequency]
        
        logger.info(f"🔍 Discovered {len(trending_keywords)} trending keywords")
        return trending_keywords[:50]  # Top 50 keywords

    def calculate_brand_relevance(self, content, trending_keywords):
        """İçeriğin brand relevance'ını hesapla"""
        content_lower = content.lower()
        
        # Core brand keyword scoring
        brand_score = 0
        for keyword in self.core_brand_keywords:
            if keyword in content_lower:
                brand_score += 3
        
        # Trending keyword scoring
        trending_score = 0
        for keyword in trending_keywords:
            if keyword in content_lower:
                trending_score += 1
        
        # Category-specific scoring
        sailing_keywords = ['яхт', 'парус', 'море', 'sail', 'marine', 'yacht', 'регат']
        fashion_keywords = ['мод', 'стиль', 'fashion', 'одежд', 'бренд', 'тренд']
        luxury_keywords = ['премиум', 'luxury', 'элитн', 'VIP', 'lux', 'роскош']
        
        sailing_score = sum(2 for kw in sailing_keywords if kw in content_lower)
        fashion_score = sum(2 for kw in fashion_keywords if kw in content_lower)
        luxury_score = sum(1 for kw in luxury_keywords if kw in content_lower)
        
        total_relevance = min(10, brand_score + trending_score + sailing_score + fashion_score + luxury_score)
        
        return {
            'total_relevance': total_relevance,
            'brand_score': brand_score,
            'trending_score': trending_score,
            'sailing_score': sailing_score,
            'fashion_score': fashion_score,
            'luxury_score': luxury_score
        }

    async def scan_channels(self, hours_back=24, min_score=6.0):
        """Ana tarama fonksiyonu - Notion'dan kanalları çek"""
        
        # Notion'dan aktif kanalları çek
        channels_config = self.notion.get_channels()
        
        if not channels_config:
            logger.warning("⚠️ No active channels found in Notion database")
            return []
        
        collected_posts = []
        channel_stats = {}
        
        try:
            async with TelegramClient('session', API_ID, API_HASH) as client:
                logger.info(f"🔍 Scanning {len(channels_config)} channels from Notion...")
                
                # First pass: collect all content for keyword analysis
                all_raw_posts = []
                
                for channel_config in channels_config:
                    channel = channel_config['channel']
                    
                    if not channel.startswith('@'):
                        channel = f"@{channel}"
                    
                    try:
                        logger.info(f"📱 Scanning: {channel} ({channel_config['category']})")
                        
                        entity = await client.get_entity(channel)
                        since_date = datetime.now() - timedelta(hours=hours_back)
                        messages = await client.get_messages(channel, limit=100, offset_date=since_date)
                        
                        logger.info(f"  📨 Found {len(messages)} messages in last {hours_back}h")
                        
                        channel_posts = []
                        
                        for msg in messages:
                            if not msg.message or len(msg.message) < 50:
                                continue
                            
                            views = getattr(msg, 'views', 0) or 0
                            forwards = getattr(msg, 'forwards', 0) or 0
                            total_engagement = views + (forwards * 10)
                            
                            post_data = {
                                'channel': channel,
                                'channel_config': channel_config,
                                'entity': entity,
                                'message': msg,
                                'content': msg.message,
                                'engagement': {
                                    'views': views,
                                    'forwards': forwards,
                                    'total': total_engagement
                                }
                            }
                            
                            all_raw_posts.append(post_data)
                            channel_posts.append(post_data)
                        
                        # Store channel stats
                        channel_stats[channel_config['id']] = {
                            'total_posts': len(channel_posts),
                            'avg_score': 0  # Will be calculated later
                        }
                        
                        await asyncio.sleep(2)  # Rate limiting
                        
                    except Exception as e:
                        logger.error(f"  ❌ Error scanning {channel}: {str(e)}")
                        continue
                
                # Extract trending keywords from all content
                logger.info("🧠 Analyzing content for trending keywords...")
                trending_keywords = self.extract_keywords_from_content(all_raw_posts)
                
                # Second pass: score posts with trending keywords
                for post_data in all_raw_posts:
                    content = post_data['content']
                    channel_config = post_data['channel_config']
                    
                    # Calculate brand relevance with trending keywords
                    relevance = self.calculate_brand_relevance(content, trending_keywords)
                    
                    # Enhanced engagement scoring
                    engagement = post_data['engagement']
                    engagement_score = min(10, (engagement['total'] // 100) + (len(trending_keywords) * 0.1))
                    
                    # Priority bonus from Notion
                    priority_bonus = {
                        'Critical': 3,
                        'High': 2,
                        'Medium': 1,
                        'Low': 0
                    }.get(channel_config['priority'], 1)
                    
                    # Category relevance bonus
                    category_bonus = {
                        'Sailing': 3,
                        'Fashion': 2,
                        'Lifestyle': 2,
                        'Competitor': 1,
                        'Influencer': 1,
                        'News': 0.5,
                        'Brand': 1
                    }.get(channel_config['category'], 1)
                    
                    # Calculate final North Sails score
                    north_sails_score = min(10, (
                        relevance['total_relevance'] + 
                        engagement_score + 
                        priority_bonus + 
                        category_bonus
                    ) / 4)
                    
                    # Filter by minimum score
                    if north_sails_score < min_score or engagement['total'] < 100:
                        continue
                    
                    # Determine content category
                    if relevance['sailing_score'] >= 4:
                        category = 'sailing'
                    elif relevance['fashion_score'] >= 4:
                        category = 'fashion'
                    elif relevance['luxury_score'] >= 2:
                        category = 'luxury'
                    else:
                        category = 'lifestyle'
                    
                    # Create final post data
                    final_post = {
                        'platform': 'telegram',
                        'channel': post_data['channel'],
                        'channel_title': post_data['entity'].title,
                        'channel_category': channel_config['category'],
                        'channel_priority': channel_config['priority'],
                        'message_id': post_data['message'].id,
                        'content': content,
                        'date': post_data['message'].date.isoformat(),
                        'author': getattr(post_data['message'].from_id, 'user_id', None) if post_data['message'].from_id else None,
                        'engagement': engagement,
                        'media_type': 'photo' if post_data['message'].photo else 'video' if post_data['message'].video else 'text',
                        'trending_keywords_matched': [kw for kw in trending_keywords if kw in content.lower()],
                        'ai_analysis': {
                            'brand_relevance': round(relevance['total_relevance'], 1),
                            'trend_score': round(engagement_score, 1),
                            'viral_potential': min(10, engagement['total'] // 200),
                            'target_audience_fit': round((relevance['sailing_score'] + relevance['fashion_score']) / 2, 1),
                            'content_category': category,
                            'season_relevance': 'year-round',
                            'hashtags': [f'#{category}', '#northsails', '#russiafashion'],
                            'insights': f"High {category} potential - discovered via trending keywords analysis",
                            'campaign_ideas': self.generate_campaign_ideas(category, relevance),
                            'keyword_analysis': {
                                'trending_matched': len([kw for kw in trending_keywords if kw in content.lower()]),
                                'brand_keywords': relevance['brand_score'] > 0,
                                'category_strength': max(relevance['sailing_score'], relevance['fashion_score'], relevance['luxury_score'])
                            }
                        },
                        'north_sails_score': round(north_sails_score, 1),
                        'processed_at': datetime.now().isoformat(),
                        'url': f"https://t.me/{post_data['channel'].replace('@', '')}/{post_data['message'].id}",
                        'scanner_version': 'notion_integrated_v2.0'
                    }
                    
                    collected_posts.append(final_post)
                    logger.info(f"    ✅ High potential: {north_sails_score:.1f}/10 - {content[:50]}...")
                
                # Update Notion channel stats
                for channel_id, stats in channel_stats.items():
                    channel_posts = [p for p in collected_posts if p.get('channel_config', {}).get('id') == channel_id]
                    if channel_posts:
                        avg_score = sum(p['north_sails_score'] for p in channel_posts) / len(channel_posts)
                        stats['avg_score'] = round(avg_score, 1)
                    
                    # Update in Notion
                    self.notion.update_channel_stats(channel_id, stats)
        
        except Exception as e:
            logger.error(f"❌ Fatal error in scan_channels: {str(e)}")
            raise
        
        return collected_posts, trending_keywords

    def generate_campaign_ideas(self, category, relevance):
        """Campaign fikirlerini kategoriye göre üret"""
        ideas = []
        
        if category == 'sailing':
            ideas = [
                "Marina partnership campaigns",
                "Yacht club collaborations", 
                "Sailing lifestyle content series",
                "Technical sailing gear showcases"
            ]
        elif category == 'fashion':
            ideas = [
                "Premium casual wear campaigns",
                "Street style collaborations",
                "Fashion week partnerships", 
                "Influencer brand ambassador programs"
            ]
        elif category == 'luxury':
            ideas = [
                "VIP lifestyle campaigns",
                "Premium brand positioning",
                "Exclusive collection launches",
                "Luxury lifestyle partnerships"
            ]
        else:  # lifestyle
            ideas = [
                "Lifestyle branding campaigns",
                "Outdoor adventure content",
                "Premium lifestyle positioning",
                "Social media influencer partnerships"
            ]
        
        return ideas[:2]

# Initialize scanner
scanner = TelethonScanner()

@app.route('/')
def home():
    """Health check endpoint"""
    return jsonify({
        "status": "active",
        "service": "North Sails Telethon Scanner API",
        "version": "2.0 - Notion Integrated",
        "notion_configured": bool(NOTION_TOKEN and NOTION_DATABASE_ID),
        "timestamp": datetime.now().isoformat

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
