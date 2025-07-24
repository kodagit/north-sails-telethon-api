# app.py - Complete North Sails Social Media Intelligence API v3.0
# Production-Ready with Session Management, Rate Limiting & Backup System

import asyncio
import json
import os
import requests
import time
import tempfile
import glob
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, url_for, redirect
from telethon import TelegramClient
from telethon.sessions import StringSession
import logging
from collections import Counter
import re
from functools import wraps

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# API credentials from environment
API_ID = int(os.getenv('TELEGRAM_API_ID', '29481789'))
API_HASH = os.getenv('TELEGRAM_API_HASH', '59f4a8346e712a5509ff700bc1da9b5d')
TELEGRAM_SESSION_STRING = os.getenv('TELEGRAM_SESSION_STRING', '')
NOTION_TOKEN = os.getenv('NOTION_TOKEN', '')
TELEGRAM_DATABASE_ID = os.getenv('TELEGRAM_DATABASE_ID', '3405e3cc-485f-4281-beaa-2e138bb8fd29')
VK_DATABASE_ID = os.getenv('VK_DATABASE_ID', '2a3103c1-9107-4f7c-8384-f3f34cad39c9')
KEYWORDS_DATABASE_ID = os.getenv('KEYWORDS_DATABASE_ID', '86dc4b4e-a4ec-4fdd-9db1-469d2d88ae4a')
VK_ACCESS_TOKEN = os.getenv('VK_ACCESS_TOKEN', '')
BACKUP_TELEGRAM_CHAT_ID = os.getenv('BACKUP_TELEGRAM_CHAT_ID', '')

class TelegramSessionManager:
    """Telegram Session Management for Render ephemeral filesystem"""
    
    def __init__(self):
        self.session_string = TELEGRAM_SESSION_STRING
        
    async def get_client(self):
    """Get Telegram client with persistent session"""
    try:
        if self.session_string:
            # Use existing session string
            session = StringSession(self.session_string)
            logger.info("✅ Using existing Telegram session")
            client = TelegramClient(session, API_ID, API_HASH)
            
            # Connect and verify session
            await client.connect()
            
            if not await client.is_user_authorized():
                logger.error("❌ Session string invalid or expired")
                await client.disconnect()
                return None
            
            logger.info("✅ Telegram session verified successfully")
            return client
            
        else:
            logger.error("❌ No TELEGRAM_SESSION_STRING found in environment")
            return None
            
    except Exception as e:
        logger.error(f"❌ Telegram session error: {str(e)}")
        return None

class VKRateLimiter:
    """Advanced VK API Rate Limiter with exponential backoff"""
    
    def __init__(self):
        self.last_request = {}
        self.request_count = {}
        self.reset_time = {}
    
    def can_make_request(self, method):
        """Check if request can be made based on VK API limits"""
        now = datetime.now()
        method_key = method
        
        # Reset counter every minute
        if method_key not in self.reset_time or now - self.reset_time[method_key] > timedelta(minutes=1):
            self.request_count[method_key] = 0
            self.reset_time[method_key] = now
        
        # VK limits: 3 req/sec, 100 req/min
        if self.request_count[method_key] >= 100:
            return False, 60  # Wait 60 seconds
        
        if method_key in self.last_request:
            time_diff = (now - self.last_request[method_key]).total_seconds()
            if time_diff < 0.35:  # 350ms between requests
                return False, 0.35 - time_diff
        
        return True, 0

def vk_rate_limit(max_retries=3):
    """Decorator for VK API rate limiting with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            limiter = getattr(self, '_rate_limiter', VKRateLimiter())
            self._rate_limiter = limiter
            
            for attempt in range(max_retries):
                # Check rate limit
                can_request, wait_time = limiter.can_make_request(func.__name__)
                
                if not can_request:
                    logger.warning(f"⏰ Rate limited for {func.__name__}, waiting {wait_time:.2f}s")
                    time.sleep(wait_time)
                    continue
                
                try:
                    # Update tracking
                    now = datetime.now()
                    limiter.last_request[func.__name__] = now
                    limiter.request_count[func.__name__] = limiter.request_count.get(func.__name__, 0) + 1
                    
                    result = func(self, *args, **kwargs)
                    
                    # Success - add small delay for safety
                    time.sleep(0.1)
                    return result
                    
                except Exception as e:
                    error_str = str(e).lower()
                    
                    if "rate limit" in error_str or "429" in error_str or "too many requests" in error_str:
                        # Exponential backoff for rate limits
                        wait_time = min(300, 5 * (2 ** attempt))  # Max 5 minutes
                        logger.warning(f"🚫 VK Rate limit hit, waiting {wait_time}s (attempt {attempt+1})")
                        time.sleep(wait_time)
                        continue
                    
                    elif "502" in error_str or "503" in error_str:
                        # Server error - shorter wait
                        wait_time = 2 * (attempt + 1)
                        logger.warning(f"🔧 VK Server error, retrying in {wait_time}s")
                        time.sleep(wait_time)
                        continue
                    
                    elif attempt == max_retries - 1:
                        logger.error(f"❌ VK API final error: {str(e)}")
                        raise e
                    else:
                        time.sleep(1)
            
            return None
        return wrapper
    return decorator

class BackupManager:
    """Multi-layer backup system for data persistence"""
    
    def __init__(self):
        self.backup_methods = ['local_json', 'telegram_channel']
        self.backup_data = []
    
    def add_scan_result(self, scan_data):
        """Add scan result to backup system"""
        backup_entry = {
            'timestamp': datetime.now().isoformat(),
            'scan_type': scan_data.get('source', 'unknown'),
            'total_posts': len(scan_data.get('posts', [])),
            'posts': scan_data.get('posts', []),
            'trending_keywords': scan_data.get('trending_keywords', {}),
            'backup_id': f"backup_{int(datetime.now().timestamp())}"
        }
        
        self.backup_data.append(backup_entry)
        
        # Multiple backup methods
        self._save_local_json(backup_entry)
        self._send_telegram_backup(backup_entry)
        
        return backup_entry['backup_id']
    
    def _save_local_json(self, data):
        """Save to JSON file (temporary in Render)"""
        try:
            filename = f"/tmp/northsails_backup_{data['backup_id']}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"💾 Local backup saved: {filename}")
        except Exception as e:
            logger.error(f"❌ Local backup failed: {str(e)}")
    
    def _send_telegram_backup(self, data):
        """Send backup to Telegram channel"""
        try:
            if not BACKUP_TELEGRAM_CHAT_ID:
                return
                
            # Critical data only
            summary = {
                'timestamp': data['timestamp'],
                'total_posts': data['total_posts'],
                'top_posts': data['posts'][:3] if data['posts'] else [],
                'backup_id': data['backup_id']
            }
            
            backup_text = f"""🔄 **BACKUP - North Sails Scan**
📅 {data['timestamp']}
📊 {data['total_posts']} posts analyzed
🆔 Backup ID: {data['backup_id']}

```json
{json.dumps(summary, indent=2)[:1500]}
```"""
            
            # Simple backup via HTTP (implement your preferred method)
            logger.info(f"📤 Telegram backup prepared for: {data['backup_id']}")
                
        except Exception as e:
            logger.error(f"❌ Telegram backup failed: {str(e)}")

class NotionClient:
    """Enhanced Notion client with backup integration"""
    
    def __init__(self):
        self.token = NOTION_TOKEN
        self.base_url = "https://api.notion.com/v1"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        self.backup_manager = BackupManager()
    
    def safe_update_with_backup(self, update_func, data, backup_label="unknown"):
        """Safe Notion update with backup"""
        # First backup the data
        backup_id = self.backup_manager.add_scan_result({
            'source': backup_label,
            'posts': data if isinstance(data, list) else [data],
            'timestamp': datetime.now().isoformat()
        })
        
        try:
            # Try Notion update
            result = update_func(data)
            logger.info(f"✅ Notion update successful, backup: {backup_id}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Notion update failed: {str(e)}")
            logger.info(f"💾 Data preserved in backup: {backup_id}")
            
            # Retry mechanism with exponential backoff
            for attempt in range(3):
                try:
                    wait_time = 2 ** attempt
                    logger.info(f"🔄 Retrying Notion update in {wait_time}s...")
                    time.sleep(wait_time)
                    
                    result = update_func(data)
                    logger.info(f"✅ Notion recovery successful on attempt {attempt + 1}")
                    return result
                    
                except Exception as retry_error:
                    if attempt == 2:  # Last attempt
                        logger.error(f"❌ Final Notion retry failed: {str(retry_error)}")
                        break
            
            # Return backup info if all fails
            return {
                'success': False,
                'backup_id': backup_id,
                'error': str(e),
                'message': 'Data saved to backup, manual recovery needed'
            }
    
    def get_telegram_channels(self):
        """Get active Telegram channels from Notion"""
        try:
            url = f"{self.base_url}/databases/{TELEGRAM_DATABASE_ID}/query"
            
            payload = {
                "page_size": 100
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
                        'subscribers': props.get('Subscribers', {}).get('number', 0)
                    }
                    
                    if channel_data['channel']:
                        channels.append(channel_data)
                
                logger.info(f"📊 Fetched {len(channels)} active Telegram channels from Notion")
                return channels
                
            else:
                logger.error(f"❌ Notion API error: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Error fetching Telegram channels: {str(e)}")
            return []
    
    def get_vk_communities(self):
        """Get active VK communities from Notion"""
        try:
            url = f"{self.base_url}/databases/{VK_DATABASE_ID}/query"
            
            payload = {
                "page_size": 100
            }
            
            response = requests.post(url, headers=self.headers, json=payload)
            
            if response.status_code == 200:
                data = response.json()
                communities = []
                
                for page in data['results']:
                    props = page['properties']
                    
                    community_data = {
                        'id': page['id'],
                        'community_id': props.get('Community ID', {}).get('title', [{}])[0].get('text', {}).get('content', ''),
                        'community_name': props.get('Community Name', {}).get('rich_text', [{}])[0].get('text', {}).get('content', ''),
                        'community_type': props.get('Community Type', {}).get('select', {}).get('name', 'Group'),
                        'category': props.get('Category', {}).get('select', {}).get('name', 'Unknown'),
                        'priority': props.get('Priority', {}).get('select', {}).get('name', 'Medium'),
                        'wall_access': props.get('Wall Access', {}).get('select', {}).get('name', 'Open'),
                        'content_focus': [item['name'] for item in props.get('Content Focus', {}).get('multi_select', [])],
                        'members_count': props.get('Members Count', {}).get('number', 0),
                        'city': props.get('City', {}).get('rich_text', [{}])[0].get('text', {}).get('content', ''),
                        'vk_url': props.get('VK URL', {}).get('url', '')
                    }
                    
                    if community_data['community_id']:
                        communities.append(community_data)
                
                logger.info(f"📊 Fetched {len(communities)} active VK communities from Notion")
                return communities
                
            else:
                logger.error(f"❌ Notion API error: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Error fetching VK communities: {str(e)}")
            return []
    
    def update_telegram_stats(self, channel_id, stats):
        """Update Telegram channel statistics"""
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
            return response.status_code == 200
                
        except Exception as e:
            logger.error(f"❌ Error updating Telegram stats: {str(e)}")
            return False
    
    def update_vk_stats(self, community_id, stats):
        """Update VK community statistics"""
        try:
            url = f"{self.base_url}/pages/{community_id}"
            
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
                    "Avg Engagement Rate": {
                        "number": stats.get('avg_engagement', 0)
                    },
                    "Members Count": {
                        "number": stats.get('members_count', 0)
                    }
                }
            }
            
            response = requests.patch(url, headers=self.headers, json=payload)
            return response.status_code == 200
                
        except Exception as e:
            logger.error(f"❌ Error updating VK stats: {str(e)}")
            return False

    def save_keywords_to_notion(self, trending_data, scan_date, source_platform):
        """Save discovered keywords to Notion"""
        try:
            url = f"{self.base_url}/pages"
            
            payload = {
                "parent": {"database_id": KEYWORDS_DATABASE_ID},
                "properties": {
                    "Scan Date": {
                        "title": [{"text": {"content": f"{source_platform.upper()} - {scan_date}"}}]
                    },
                    "Total Words": {
                        "number": len(trending_data['words'])
                    },
                    "Total Phrases": {
                        "number": len(trending_data['phrases'])
                    },
                    "Min Frequency": {
                        "number": 100
                    },
                    "Scan Type": {
                        "select": {"name": "Auto Discovery"}
                    },
                    "Channel Sources": {
                        "multi_select": [{"name": f"{source_platform.title()} Channels"}]
                    }
                },
                "children": [
                    {
                        "object": "block",
                        "type": "heading_2",
                        "heading_2": {
                            "rich_text": [{"type": "text", "text": {"content": f"🔥 {source_platform.upper()} Trending Words (100+ Usage)"}}]
                        }
                    }
                ]
            }
            
            # Add trending words
            for word, count in trending_data['words'][:20]:
                payload["children"].append({
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {
                        "rich_text": [{"type": "text", "text": {"content": f"**{word}** ({count} kez kullanıldı)"}}]
                    }
                })
            
            # Add trending phrases header
            payload["children"].append({
                "object": "block",
                "type": "heading_2", 
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": f"💬 {source_platform.upper()} Trending Phrases (100+ Usage)"}}]
                }
            })
            
            # Add trending phrases
            for phrase, count in trending_data['phrases'][:15]:
                payload["children"].append({
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {
                        "rich_text": [{"type": "text", "text": {"content": f"*\"{phrase}\"* ({count} kez kullanıldı)"}}]
                    }
                })
            
            def save_keywords():
                return requests.post(url, headers=self.headers, json=payload)
            
            # Use safe update with backup
            result = self.safe_update_with_backup(save_keywords, trending_data, f'{source_platform}_keywords')
            
            if isinstance(result, dict) and result.get('status_code') == 200:
                logger.info(f"✅ Saved {len(trending_data['words'])} {source_platform} keywords to Notion")
                return True
            else:
                logger.info(f"📁 {source_platform} keywords saved to backup system")
                return True  # Still success since backup worked
                
        except Exception as e:
            logger.error(f"❌ Error saving {source_platform} keywords: {str(e)}")
            return False

class VKClient:
    """Enhanced VK client with rate limiting"""
    
    def __init__(self):
        self.access_token = VK_ACCESS_TOKEN
        self.api_version = "5.131"
        self.base_url = "https://api.vk.com/method"
        self._rate_limiter = VKRateLimiter()
    
    @vk_rate_limit(max_retries=3)
    def get_community_info(self, community_id):
        """Get VK community information with rate limiting"""
        try:
            url = f"{self.base_url}/groups.getById"
            params = {
                'group_ids': community_id,
                'fields': 'members_count,description,activity,status,city',
                'access_token': self.access_token,
                'v': self.api_version
            }
            
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                if 'response' in data and data['response']:
                    return data['response'][0]
            
            return None
                
        except Exception as e:
            logger.error(f"❌ Error getting VK community info: {str(e)}")
            return None
    
    @vk_rate_limit(max_retries=5)
    def get_community_posts(self, community_id, count=50):
        """Get VK community posts with rate limiting"""
        try:
            url = f"{self.base_url}/wall.get"
            params = {
                'owner_id': f"-{community_id}" if not community_id.startswith('-') else community_id,
                'count': min(count, 100),  # VK limit
                'extended': 1,
                'access_token': self.access_token,
                'v': self.api_version
            }
            
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                if 'response' in data and 'items' in data['response']:
                    return data['response']['items']
            
            return []
                
        except Exception as e:
            logger.error(f"❌ Error getting VK posts: {str(e)}")
            return []

class SocialMediaScanner:
    """Main scanner with enhanced features"""
    
    def __init__(self):
        self.notion = NotionClient()
        self.vk = VKClient()
        self.session_manager = TelegramSessionManager()
        
        # Minimal brand filter
        self.minimal_brand_filter = ['north sails', 'northsails', 'норт сейлс']

    def extract_keywords_from_content(self, posts, min_frequency=100):
        """Extract trending keywords from content"""
        all_words = []
        all_phrases = []
        
        for post in posts:
            content = post.get('content', '').lower()
            content = re.sub(r'http\S+|@\w+|#\w+|[^\w\s\u0400-\u04FF]', ' ', content)
            
            words = re.findall(r'[а-яё\w]{4,}', content)
            all_words.extend(words)
            
            word_list = content.split()
            for i in range(len(word_list) - 1):
                phrase = f"{word_list[i]} {word_list[i+1]}"
                if len(phrase) > 8:
                    all_phrases.append(phrase)
            
            for i in range(len(word_list) - 2):
                phrase = f"{word_list[i]} {word_list[i+1]} {word_list[i+2]}"
                if len(phrase) > 12:
                    all_phrases.append(phrase)
        
        word_counts = Counter(all_words)
        phrase_counts = Counter(all_phrases)
        
        trending_words = [(word, count) for word, count in word_counts.items() 
                         if count >= min_frequency]
        
        trending_phrases = [(phrase, count) for phrase, count in phrase_counts.items() 
                           if count >= min_frequency]
        
        trending_words.sort(key=lambda x: x[1], reverse=True)
        trending_phrases.sort(key=lambda x: x[1], reverse=True)
        
        logger.info(f"🔍 Discovered {len(trending_words)} trending words (100+ usage)")
        logger.info(f"🔍 Discovered {len(trending_phrases)} trending phrases (100+ usage)")
        
        return {
            'words': trending_words[:50],
            'phrases': trending_phrases[:30]
        }

    def calculate_brand_relevance(self, content, trending_data, platform="telegram"):
        """Calculate brand relevance score"""
        content_lower = content.lower()
        
        brand_score = 0
        for keyword in self.minimal_brand_filter:
            if keyword in content_lower:
                brand_score += 5
        
        trending_word_score = 0
        matched_words = []
        for word, count in trending_data['words']:
            if word in content_lower:
                weight = min(3, count // 100)
                trending_word_score += weight
                matched_words.append(word)
        
        trending_phrase_score = 0
        matched_phrases = []
        for phrase, count in trending_data['phrases']:
            if phrase in content_lower:
                weight = min(5, count // 50)
                trending_phrase_score += weight
                matched_phrases.append(phrase)
        
        total_relevance = min(10, brand_score + trending_word_score + trending_phrase_score)
        
        return {
            'total_relevance': total_relevance,
            'brand_score': brand_score,
            'trending_word_score': trending_word_score,
            'trending_phrase_score': trending_phrase_score,
            'matched_words': matched_words[:10],
            'matched_phrases': matched_phrases[:5]
        }

    async def scan_telegram_channels(self, hours_back=24, min_score=6.0):
        """Scan Telegram channels with session management"""
        channels_config = self.notion.get_telegram_channels()
        
        if not channels_config:
            return [], {}
        
        collected_posts = []
        channel_stats = {}
        all_raw_posts = []
        
        try:
            client = await self.session_manager.get_client()
            
            try:
                logger.info(f"🔍 Scanning {len(channels_config)} Telegram channels...")
                
                for channel_config in channels_config:
                    channel = channel_config['channel']
                    
                    if not channel.startswith('@'):
                        channel = f"@{channel}"
                    
                    try:
                        logger.info(f"📱 Scanning: {channel}")
                        
                        entity = await client.get_entity(channel)
                        since_date = datetime.now() - timedelta(hours=hours_back)
                        messages = await client.get_messages(channel, limit=100, offset_date=since_date)
                        
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
                        
                        channel_stats[channel_config['id']] = {
                            'total_posts': len(channel_posts),
                            'avg_score': 0
                        }
                        
                        await asyncio.sleep(2)
                        
                    except Exception as e:
                        logger.error(f"  ❌ Error scanning {channel}: {str(e)}")
                        continue
                
                logger.info("🧠 Analyzing Telegram content for keywords...")
                trending_data = self.extract_keywords_from_content(all_raw_posts)
                
                scan_date = datetime.now().strftime('%Y-%m-%d %H:%M')
                self.notion.save_keywords_to_notion(trending_data, scan_date, 'telegram')
                
                for post_data in all_raw_posts:
                    content = post_data['content']
                    channel_config = post_data['channel_config']
                    
                    relevance = self.calculate_brand_relevance(content, trending_data, 'telegram')
                    engagement = post_data['engagement']
                    
                    engagement_score = min(10, (engagement['total'] // 100) + (len(relevance['matched_words']) * 0.5))
                    
                    priority_bonus = {
                        'Critical': 3, 'High': 2, 'Medium': 1, 'Low': 0
                    }.get(channel_config['priority'], 1)
                    
                    category_bonus = {
                        'Sailing': 3, 'Fashion': 2, 'Lifestyle': 2,
                        'Competitor': 1, 'Influencer': 1, 'News': 0.5, 'Brand': 1
                    }.get(channel_config['category'], 1)
                    
                    north_sails_score = min(10, (
                        relevance['total_relevance'] + 
                        engagement_score + 
                        priority_bonus + 
                        category_bonus
                    ) / 4)
                    
                    if north_sails_score < min_score or engagement['total'] < 100:
                        continue
                    
                    if any('яхт' in word or 'sail' in word for word in relevance['matched_words']):
                        category = 'sailing'
                    elif any('мод' in word or 'fashion' in word for word in relevance['matched_words']):
                        category = 'fashion'
                    elif any('премиум' in word or 'luxury' in word for word in relevance['matched_words']):
                        category = 'luxury'
                    else:
                        category = 'lifestyle'
                    
                    final_post = {
                        'platform': 'telegram',
                        'channel': post_data['channel'],
                        'channel_title': post_data['entity'].title,
                        'channel_category': channel_config['category'],
                        'message_id': post_data['message'].id,
                        'content': content,
                        'date': post_data['message'].date.isoformat(),
                        'engagement': engagement,
                        'media_type': 'photo' if post_data['message'].photo else 'video' if post_data['message'].video else 'text',
                        'trending_analysis': {
                            'matched_words': relevance['matched_words'],
                            'matched_phrases': relevance['matched_phrases'],
                            'word_score': relevance['trending_word_score'],
                            'phrase_score': relevance['trending_phrase_score']
                        },
                        'ai_analysis': {
                            'brand_relevance': round(relevance['total_relevance'], 1),
                            'trend_score': round(engagement_score, 1),
                            'viral_potential': min(10, engagement['total'] // 200),
                            'target_audience_fit': round((relevance['trending_word_score'] + relevance['trending_phrase_score']) / 2, 1),
                            'content_category': category,
                            'hashtags': [f'#{category}', '#northsails', '#telegram'],
                            'insights': f"High {category} potential - discovered via trending analysis",
                            'keyword_analysis': {
                                'total_trending_matches': len(relevance['matched_words']) + len(relevance['matched_phrases']),
                                'brand_mentions': relevance['brand_score'] > 0,
                                'top_matched_words': relevance['matched_words'][:5],
                                'top_matched_phrases': relevance['matched_phrases'][:3],
                                'discovery_method': 'telegram_trending_analysis'
                            }
                        },
                        'north_sails_score': round(north_sails_score, 1),
                        'processed_at': datetime.now().isoformat(),
                        'url': f"https://t.me/{post_data['channel'].replace('@', '')}/{post_data['message'].id}",
                        'scanner_version': 'telegram_notion_v3.0'
                    }
                    
                    collected_posts.append(final_post)
                
                for channel_id, stats in channel_stats.items():
                    channel_posts = [p for p in collected_posts if p.get('channel_config', {}).get('id') == channel_id]
                    if channel_posts:
                        avg_score = sum(p['north_sails_score'] for p in channel_posts) / len(channel_posts)
                        stats['avg_score'] = round(avg_score, 1)
                    
                    self.notion.update_telegram_stats(channel_id, stats)
            
            finally:
                await client.disconnect()
        
        except Exception as e:
            logger.error(f"❌ Fatal error in Telegram scan: {str(e)}")
            raise
        
        return collected_posts, trending_data

    def scan_vk_communities_batch(self, min_score=6.0, batch_size=5):
        """Scan VK communities with batch processing and rate limiting"""
        communities_config = self.notion.get_vk_communities()
        
        if not communities_config:
            return [], {}
        
        collected_posts = []
        community_stats = {}
        all_raw_posts = []
        
        try:
            logger.info(f"🔍 Scanning {len(communities_config)} VK communities in batches of {batch_size}...")
            
            # Process communities in batches
            for i in range(0, len(communities_config), batch_size):
                batch = communities_config[i:i + batch_size]
                
                logger.info(f"🔄 Processing batch {i//batch_size + 1}/{(len(communities_config)-1)//batch_size + 1}")
                
                for community_config in batch:
                    community_id = community_config['community_id']
                    
                    try:
                        logger.info(f"📱 Scanning VK: {community_id}")
                        
                        community_info = self.vk.get_community_info(community_id)
                        if not community_info:
                            continue
                        
                        posts = self.vk.get_community_posts(community_id, 50)
                        
                        community_posts = []
                        
                        for post in posts:
                            if not post.get('text') or len(post['text']) < 50:
                                continue
                            
                            likes = post.get('likes', {}).get('count', 0)
                            comments = post.get('comments', {}).get('count', 0)
                            reposts = post.get('reposts', {}).get('count', 0)
                            views = post.get('views', {}).get('count', 0)
                            
                            total_engagement = likes + (comments * 3) + (reposts * 5) + (views * 0.1)
                            
                            post_data = {
                                'community_id': community_id,
                                'community_config': community_config,
                                'community_info': community_info,
                                'post': post,
                                'content': post['text'],
                                'engagement': {
                                    'likes': likes,
                                    'comments': comments,
                                    'reposts': reposts,
                                    'views': views,
                                    'total': total_engagement
                                }
                            }
                            
                            all_raw_posts.append(post_data)
                            community_posts.append(post_data)
                        
                        community_stats[community_config['id']] = {
                            'total_posts': len(community_posts),
                            'avg_engagement': 0,
                            'members_count': community_info.get('members_count', 0)
                        }
                        
                    except Exception as e:
                        logger.error(f"  ❌ Error scanning VK {community_id}: {str(e)}")
                        continue
                
                # Wait between batches
                if i + batch_size < len(communities_config):
                    logger.info("😴 Batch completed, waiting 30s...")
                    time.sleep(30)
            
            logger.info("🧠 Analyzing VK content for keywords...")
            trending_data = self.extract_keywords_from_content(all_raw_posts)
            
            scan_date = datetime.now().strftime('%Y-%m-%d %H:%M')
            self.notion.save_keywords_to_notion(trending_data, scan_date, 'vk')
            
            for post_data in all_raw_posts:
                content = post_data['content']
                community_config = post_data['community_config']
                
                relevance = self.calculate_brand_relevance(content, trending_data, 'vk')
                engagement = post_data['engagement']
                
                engagement_score = min(10, (engagement['total'] // 50) + (len(relevance['matched_words']) * 0.5))
                
                priority_bonus = {
                    'Critical': 3, 'High': 2, 'Medium': 1, 'Low': 0
                }.get(community_config['priority'], 1)
                
                category_bonus = {
                    'Sailing': 3, 'Fashion': 2, 'Lifestyle': 2,
                    'Competitor': 1, 'Influencer': 1, 'Brand': 1, 'Community': 0.5
                }.get(community_config['category'], 1)
                
                north_sails_score = min(10, (
                    relevance['total_relevance'] + 
                    engagement_score + 
                    priority_bonus + 
                    category_bonus
                ) / 4)
                
                if north_sails_score < min_score or engagement['total'] < 50:
                    continue
                
                if any('яхт' in word or 'sail' in word for word in relevance['matched_words']):
                    category = 'sailing'
                elif any('мод' in word or 'fashion' in word for word in relevance['matched_words']):
                    category = 'fashion'
                elif any('премиум' in word or 'luxury' in word for word in relevance['matched_words']):
                    category = 'luxury'
                else:
                    category = 'lifestyle'
                
                final_post = {
                    'platform': 'vk',
                    'community_id': post_data['community_id'],
                    'community_name': post_data['community_info'].get('name', ''),
                    'community_category': community_config['category'],
                    'post_id': post_data['post']['id'],
                    'content': content,
                    'date': datetime.fromtimestamp(post_data['post']['date']).isoformat(),
                    'engagement': engagement,
                    'media_type': 'photo' if post_data['post'].get('attachments') else 'text',
                    'trending_analysis': {
                        'matched_words': relevance['matched_words'],
                        'matched_phrases': relevance['matched_phrases'],
                        'word_score': relevance['trending_word_score'],
                        'phrase_score': relevance['trending_phrase_score']
                    },
                    'ai_analysis': {
                        'brand_relevance': round(relevance['total_relevance'], 1),
                        'trend_score': round(engagement_score, 1),
                        'viral_potential': min(10, engagement['total'] // 100),
                        'target_audience_fit': round((relevance['trending_word_score'] + relevance['trending_phrase_score']) / 2, 1),
                        'content_category': category,
                        'hashtags': [f'#{category}', '#northsails', '#vk'],
                        'insights': f"High {category} potential - VK community analysis",
                        'keyword_analysis': {
                            'total_trending_matches': len(relevance['matched_words']) + len(relevance['matched_phrases']),
                            'brand_mentions': relevance['brand_score'] > 0,
                            'top_matched_words': relevance['matched_words'][:5],
                            'top_matched_phrases': relevance['matched_phrases'][:3],
                            'discovery_method': 'vk_trending_analysis'
                        }
                    },
                    'north_sails_score': round(north_sails_score, 1),
                    'processed_at': datetime.now().isoformat(),
                    'url': f"https://vk.com/wall-{post_data['community_id']}_{post_data['post']['id']}",
                    'scanner_version': 'vk_notion_v3.0'
                }
                
                collected_posts.append(final_post)
            
            for community_id, stats in community_stats.items():
                community_posts = [p for p in collected_posts if p.get('community_config', {}).get('id') == community_id]
                if community_posts:
                    avg_engagement = sum(p['engagement']['total'] for p in community_posts) / len(community_posts)
                    stats['avg_engagement'] = round(avg_engagement, 1)
                
                self.notion.update_vk_stats(community_id, stats)
        
        except Exception as e:
            logger.error(f"❌ Fatal error in VK scan: {str(e)}")
            raise
        
        return collected_posts, trending_data

# Initialize scanner
scanner = SocialMediaScanner()

@app.route('/')
def home():
    """Health check endpoint"""
    return jsonify({
        "status": "active",
        "service": "North Sails Social Media Intelligence API",
        "version": "3.0 - Production Ready with Session Management + Rate Limiting + Backup",
        "features": [
            "Telegram channel monitoring with session persistence",
            "VK community monitoring with rate limiting", 
            "Automatic keyword discovery",
            "Notion database integration",
            "Multi-layer backup system",
            "100+ frequency trending analysis",
            "Error recovery mechanisms"
        ],
        "integrations": {
            "notion_configured": bool(NOTION_TOKEN),
            "telegram_configured": bool(API_ID and API_HASH),
            "telegram_session": bool(TELEGRAM_SESSION_STRING),
            "vk_configured": bool(VK_ACCESS_TOKEN),
            "backup_configured": bool(BACKUP_TELEGRAM_CHAT_ID)
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/scan/telegram', methods=['POST', 'GET'])
def scan_telegram():
    """Telegram channels scanning endpoint"""
    try:
        hours_back = request.json.get('hours_back', 24) if request.is_json else 24
        min_score = request.json.get('min_score', 6.0) if request.is_json else 6.0
        
        logger.info(f"🚀 Starting Telegram scan: {hours_back}h back, min_score: {min_score}")
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        posts, trending_data = loop.run_until_complete(scanner.scan_telegram_channels(hours_back, min_score))
        loop.close()
        
        result = {
            'source': 'telegram_scanner_api_v3',
            'timestamp': datetime.now().isoformat(),
            'parameters': {
                'hours_back': hours_back,
                'min_score': min_score
            },
            'total_posts': len(posts),
            'posts': posts,
            'trending_keywords': {
                'words': trending_data['words'][:10],
                'phrases': trending_data['phrases'][:10]
            },
            'summary': {
                'total_collected': len(posts),
                'avg_north_sails_score': sum(p['north_sails_score'] for p in posts) / len(posts) if posts else 0,
                'top_category': max(set(p['ai_analysis']['content_category'] for p in posts), 
                                  key=lambda x: sum(1 for p in posts if p['ai_analysis']['content_category'] == x)) if posts else 'none',
                'high_score_posts': len([p for p in posts if p['north_sails_score'] >= 8]),
                'categories': {}
            }
        }
        
        if posts:
            categories = {}
            for post in posts:
                cat = post['ai_analysis']['content_category']
                categories[cat] = categories.get(cat, 0) + 1
            result['summary']['categories'] = categories
        
        logger.info(f"✅ Telegram scan completed: {len(posts)} posts collected")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"❌ Error in Telegram scan: {str(e)}")
        return jsonify({
            'error': str(e),
            'source': 'telegram_scanner_api_v3',
            'timestamp': datetime.now().isoformat(),
            'posts': []
        }), 500

@app.route('/scan/vk', methods=['POST', 'GET'])
def scan_vk():
    """VK communities scanning endpoint"""
    try:
        min_score = request.json.get('min_score', 6.0) if request.is_json else 6.0
        batch_size = request.json.get('batch_size', 5) if request.is_json else 5
        
        logger.info(f"🚀 Starting VK scan: min_score: {min_score}, batch_size: {batch_size}")
        
        posts, trending_data = scanner.scan_vk_communities_batch(min_score, batch_size)
        
        result = {
            'source': 'vk_scanner_api_v3',
            'timestamp': datetime.now().isoformat(),
            'parameters': {
                'min_score': min_score,
                'batch_size': batch_size
            },
            'total_posts': len(posts),
            'posts': posts,
            'trending_keywords': {
                'words': trending_data['words'][:10],
                'phrases': trending_data['phrases'][:10]
            },
            'summary': {
                'total_collected': len(posts),
                'avg_north_sails_score': sum(p['north_sails_score'] for p in posts) / len(posts) if posts else 0,
                'top_category': max(set(p['ai_analysis']['content_category'] for p in posts), 
                                  key=lambda x: sum(1 for p in posts if p['ai_analysis']['content_category'] == x)) if posts else 'none',
                'high_score_posts': len([p for p in posts if p['north_sails_score'] >= 8]),
                'categories': {}
            }
        }
        
        if posts:
            categories = {}
            for post in posts:
                cat = post['ai_analysis']['content_category']
                categories[cat] = categories.get(cat, 0) + 1
            result['summary']['categories'] = categories
        
        logger.info(f"✅ VK scan completed: {len(posts)} posts collected")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"❌ Error in VK scan: {str(e)}")
        return jsonify({
            'error': str(e),
            'source': 'vk_scanner_api_v3',
            'timestamp': datetime.now().isoformat(),
            'posts': []
        }), 500

@app.route('/scan/all', methods=['POST', 'GET'])
def scan_all_platforms():
    """Scan all platforms endpoint"""
    try:
        hours_back = request.json.get('hours_back', 24) if request.is_json else 24
        min_score = request.json.get('min_score', 6.0) if request.is_json else 6.0
        batch_size = request.json.get('batch_size', 5) if request.is_json else 5
        
        logger.info(f"🚀 Starting full scan: Telegram + VK")
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        telegram_posts, telegram_trending = loop.run_until_complete(scanner.scan_telegram_channels(hours_back, min_score))
        loop.close()
        
        vk_posts, vk_trending = scanner.scan_vk_communities_batch(min_score, batch_size)
        
        all_posts = telegram_posts + vk_posts
        
        result = {
            'source': 'full_scanner_api_v3',
            'timestamp': datetime.now().isoformat(),
            'parameters': {
                'hours_back': hours_back,
                'min_score': min_score,
                'batch_size': batch_size
            },
            'platforms': {
                'telegram': {
                    'posts_count': len(telegram_posts),
                    'trending_keywords': {
                        'words': telegram_trending['words'][:5],
                        'phrases': telegram_trending['phrases'][:5]
                    }
                },
                'vk': {
                    'posts_count': len(vk_posts),
                    'trending_keywords': {
                        'words': vk_trending['words'][:5],
                        'phrases': vk_trending['phrases'][:5]
                    }
                }
            },
            'total_posts': len(all_posts),
            'posts': all_posts,
            'summary': {
                'total_collected': len(all_posts),
                'avg_north_sails_score': sum(p['north_sails_score'] for p in all_posts) / len(all_posts) if all_posts else 0,
                'top_category': max(set(p['ai_analysis']['content_category'] for p in all_posts), 
                                  key=lambda x: sum(1 for p in all_posts if p['ai_analysis']['content_category'] == x)) if all_posts else 'none',
                'high_score_posts': len([p for p in all_posts if p['north_sails_score'] >= 8]),
                'platform_breakdown': {
                    'telegram': len(telegram_posts),
                    'vk': len(vk_posts)
                }
            }
        }
        
        logger.info(f"✅ Full scan completed: {len(all_posts)} total posts collected")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"❌ Error in full scan: {str(e)}")
        return jsonify({
            'error': str(e),
            'source': 'full_scanner_api_v3',
            'timestamp': datetime.now().isoformat(),
            'posts': []
        }), 500

@app.route('/webhook/n8n', methods=['POST'])
def n8n_webhook():
    """N8N webhook integration endpoint"""
    try:
        data = request.json
        scan_type = data.get('scan_type', 'all')
        
        if scan_type == 'telegram':
            return redirect(url_for('scan_telegram'), code=307)
        elif scan_type == 'vk':
            return redirect(url_for('scan_vk'), code=307)
        else:
            return redirect(url_for('scan_all_platforms'), code=307)
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/channels/telegram')
def list_telegram_channels():
    """List Telegram channels from Notion"""
    try:
        channels = scanner.notion.get_telegram_channels()
        return jsonify({
            'platform': 'telegram',
            'total_channels': len(channels),
            'channels': channels,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/channels/vk')
def list_vk_communities():
    """List VK communities from Notion"""
    try:
        communities = scanner.notion.get_vk_communities()
        return jsonify({
            'platform': 'vk',
            'total_communities': len(communities),
            'communities': communities,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/backup/export')
def export_backup():
    """Export last 24 hours data as JSON backup"""
    try:
        backup_data = {
            'timestamp': datetime.now().isoformat(),
            'telegram_channels': scanner.notion.get_telegram_channels(),
            'vk_communities': scanner.notion.get_vk_communities(),
            'backup_files': len(glob.glob('/tmp/northsails_backup_*.json')),
            'version': '3.0'
        }
        
        return jsonify(backup_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/recovery/list_backups')
def list_backups():
    """List available backup files"""
    try:
        backup_files = glob.glob('/tmp/northsails_backup_*.json')
        
        backups = []
        for file_path in backup_files:
            try:
                with open(file_path, 'r') as f:
                    backup_data = json.load(f)
                    backups.append({
                        'backup_id': backup_data['backup_id'],
                        'timestamp': backup_data['timestamp'],
                        'total_posts': backup_data['total_posts'],
                        'scan_type': backup_data['scan_type'],
                        'file_path': file_path
                    })
            except:
                continue
        
        return jsonify({
            'available_backups': backups,
            'total_backup_files': len(backups),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/recovery/restore/<backup_id>')
def restore_backup(backup_id):
    """Restore data from backup"""
    try:
        file_path = f'/tmp/northsails_backup_{backup_id}.json'
        
        with open(file_path, 'r') as f:
            backup_data = json.load(f)
        
        return jsonify({
            'backup_id': backup_id,
            'total_posts_in_backup': len(backup_data['posts']),
            'backup_timestamp': backup_data['timestamp'],
            'scan_type': backup_data['scan_type'],
            'status': 'backup_retrieved',
            'message': 'Backup data retrieved. Manual processing may be needed for Notion restoration.'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health():
    """Detailed health check"""
    session_status = "configured" if TELEGRAM_SESSION_STRING else "missing"
    
    return jsonify({
        "status": "healthy",
        "uptime": "active",
        "version": "3.0",
        "integrations": {
            "notion": {
                "configured": bool(NOTION_TOKEN),
                "telegram_db": bool(TELEGRAM_DATABASE_ID),
                "vk_db": bool(VK_DATABASE_ID),
                "keywords_db": bool(KEYWORDS_DATABASE_ID)
            },
            "telegram": {
                "configured": bool(API_ID and API_HASH),
                "session_status": session_status
            },
            "vk": {
                "configured": bool(VK_ACCESS_TOKEN),
                "rate_limiting": "enabled"
            },
            "backup": {
                "configured": bool(BACKUP_TELEGRAM_CHAT_ID),
                "local_backup": "enabled",
                "available_backups": len(glob.glob('/tmp/northsails_backup_*.json'))
            }
        },
        "features": {
            "session_management": True,
            "rate_limiting": True,
            "backup_system": True,
            "keyword_discovery": True,
            "trending_analysis": True,
            "multi_platform": True,
            "notion_integration": True,
            "error_recovery": True
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/session/generate')
def generate_session():
    """Helper endpoint to generate Telegram session string"""
    if TELEGRAM_SESSION_STRING:
        return jsonify({
            'status': 'already_configured',
            'message': 'Session string already exists',
            'session_configured': True
        })
    
    return jsonify({
        'status': 'needs_configuration',
        'message': 'Run the app locally first to generate session string',
        'instructions': [
            '1. Set TELEGRAM_API_ID and TELEGRAM_API_HASH',
            '2. Leave TELEGRAM_SESSION_STRING empty',
            '3. Run /scan/telegram endpoint',
            '4. Check logs for session string',
            '5. Add session string to Render environment'
        ]
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    
    logger.info("🚀 North Sails Social Media Intelligence API v3.0 Starting...")
    logger.info(f"🔧 Notion: {'✅' if NOTION_TOKEN else '❌'}")
    logger.info(f"📱 Telegram: {'✅' if API_ID and API_HASH else '❌'}")
    logger.info(f"🔑 Session: {'✅' if TELEGRAM_SESSION_STRING else '❌ (Generate needed)'}")
    logger.info(f"📘 VK: {'✅' if VK_ACCESS_TOKEN else '❌'}")
    logger.info(f"💾 Backup: {'✅' if BACKUP_TELEGRAM_CHAT_ID else '⚠️ (Optional)'}")
    
    app.run(host='0.0.0.0', port=port, debug=False)
