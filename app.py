import asyncio
import json
import os
import requests
import time
import glob
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, url_for, redirect
from telethon import TelegramClient
from telethon.sessions import StringSession
import logging
from collections import Counter
import re
from functools import wraps

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

API_ID = int(os.getenv('TELEGRAM_API_ID', '29481789'))
API_HASH = os.getenv('TELEGRAM_API_HASH', '59f4a8346e712a5509ff700bc1da9b5d')
TELEGRAM_SESSION_STRING = os.getenv('TELEGRAM_SESSION_STRING', '')
NOTION_TOKEN = os.getenv('NOTION_TOKEN', '')
TELEGRAM_DATABASE_ID = os.getenv('TELEGRAM_DATABASE_ID', '3405e3cc-485f-4281-beaa-2e138bb8fd29')
VK_DATABASE_ID = os.getenv('VK_DATABASE_ID', '2a3103c1-9107-4f7c-8384-f3f34cad39c9')
VK_ACCESS_TOKEN = os.getenv('VK_ACCESS_TOKEN', '')

class TelegramSessionManager:
    def __init__(self):
        self.session_string = TELEGRAM_SESSION_STRING
        self.client = None
        
    async def get_client(self):
        try:
            if not self.session_string:
                logger.error("❌ No TELEGRAM_SESSION_STRING found")
                return None
            
            if len(self.session_string) < 50:
                logger.error("❌ Session string too short")
                return None
            
            logger.info(f"🔑 Session length: {len(self.session_string)}")
            
            session = StringSession(self.session_string)
            self.client = TelegramClient(session, API_ID, API_HASH, device_model="North Sails API")
            
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                logger.error("❌ Session invalid")
                await self.client.disconnect()
                return None
            
            logger.info("✅ Telegram connected")
            return self.client
            
        except Exception as e:
            logger.error(f"❌ Client error: {str(e)}")
            if self.client:
                try:
                    await self.client.disconnect()
                except:
                    pass
            return None
    
    async def disconnect(self):
        if self.client:
            try:
                await self.client.disconnect()
                logger.info("🔌 Disconnected")
            except Exception as e:
                logger.warning(f"⚠️ Disconnect warning: {str(e)}")

class VKRateLimiter:
    def __init__(self):
        self.last_request = {}
        self.request_count = {}
        self.reset_time = {}
    
    def can_make_request(self, method):
        now = datetime.now()
        if method not in self.reset_time or now - self.reset_time[method] > timedelta(minutes=1):
            self.request_count[method] = 0
            self.reset_time[method] = now
        
        if self.request_count[method] >= 100:
            return False, 60
        
        if method in self.last_request:
            time_diff = (now - self.last_request[method]).total_seconds()
            if time_diff < 0.35:
                return False, 0.35 - time_diff
        
        return True, 0

def vk_rate_limit(max_retries=3):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            limiter = getattr(self, '_rate_limiter', VKRateLimiter())
            self._rate_limiter = limiter
            
            for attempt in range(max_retries):
                can_request, wait_time = limiter.can_make_request(func.__name__)
                
                if not can_request:
                    time.sleep(wait_time)
                    continue
                
                try:
                    now = datetime.now()
                    limiter.last_request[func.__name__] = now
                    limiter.request_count[func.__name__] = limiter.request_count.get(func.__name__, 0) + 1
                    
                    result = func(self, *args, **kwargs)
                    time.sleep(0.1)
                    return result
                    
                except Exception as e:
                    if "rate limit" in str(e).lower() or "429" in str(e):
                        wait_time = min(300, 5 * (2 ** attempt))
                        time.sleep(wait_time)
                        continue
                    elif attempt == max_retries - 1:
                        raise e
                    else:
                        time.sleep(1)
            
            return None
        return wrapper
    return decorator

class NotionClient:
    def __init__(self):
        self.token = NOTION_TOKEN
        self.base_url = "https://api.notion.com/v1"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
    
    def get_telegram_channels(self):
        try:
            url = f"{self.base_url}/databases/{TELEGRAM_DATABASE_ID}/query"
            response = requests.post(url, headers=self.headers, json={"page_size": 100})
            
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
                        'subscribers': props.get('Subscribers', {}).get('number', 0)
                    }
                    
                    if channel_data['channel']:
                        channels.append(channel_data)
                
                logger.info(f"📊 Fetched {len(channels)} channels")
                return channels
            else:
                logger.error(f"❌ Notion error: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"❌ Error fetching channels: {str(e)}")
            return []
    
    def get_vk_communities(self):
        try:
            url = f"{self.base_url}/databases/{VK_DATABASE_ID}/query"
            response = requests.post(url, headers=self.headers, json={"page_size": 100})
            
            if response.status_code == 200:
                data = response.json()
                communities = []
                
                for page in data['results']:
                    props = page['properties']
                    community_data = {
                        'id': page['id'],
                        'community_id': props.get('Community ID', {}).get('title', [{}])[0].get('text', {}).get('content', ''),
                        'community_name': props.get('Community Name', {}).get('rich_text', [{}])[0].get('text', {}).get('content', ''),
                        'category': props.get('Category', {}).get('select', {}).get('name', 'Unknown'),
                        'priority': props.get('Priority', {}).get('select', {}).get('name', 'Medium'),
                        'members_count': props.get('Members Count', {}).get('number', 0)
                    }
                    
                    if community_data['community_id']:
                        communities.append(community_data)
                
                logger.info(f"📊 Fetched {len(communities)} communities")
                return communities
            else:
                logger.error(f"❌ Notion error: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"❌ Error fetching communities: {str(e)}")
            return []

class VKClient:
    def __init__(self):
        self.access_token = VK_ACCESS_TOKEN
        self.api_version = "5.131"
        self.base_url = "https://api.vk.com/method"
        self._rate_limiter = VKRateLimiter()
    
    @vk_rate_limit(max_retries=3)
    def get_community_info(self, community_id):
        try:
            url = f"{self.base_url}/groups.getById"
            params = {
                'group_ids': community_id,
                'fields': 'members_count,description',
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
            logger.error(f"❌ Error getting VK info: {str(e)}")
            return None
    
    @vk_rate_limit(max_retries=5)
    def get_community_posts(self, community_id, count=50):
        try:
            url = f"{self.base_url}/wall.get"
            params = {
                'owner_id': f"-{community_id}" if not community_id.startswith('-') else community_id,
                'count': min(count, 100),
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
    def __init__(self):
        self.notion = NotionClient()
        self.vk = VKClient()
        self.session_manager = TelegramSessionManager()
        self.minimal_brand_filter = ['north sails', 'northsails', 'норт сейлс']

    def extract_keywords_from_content(self, posts, min_frequency=100):
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
        
        word_counts = Counter(all_words)
        phrase_counts = Counter(all_phrases)
        
        trending_words = [(word, count) for word, count in word_counts.items() if count >= min_frequency]
        trending_phrases = [(phrase, count) for phrase, count in phrase_counts.items() if count >= min_frequency]
        
        trending_words.sort(key=lambda x: x[1], reverse=True)
        trending_phrases.sort(key=lambda x: x[1], reverse=True)
        
        return {
            'words': trending_words[:50],
            'phrases': trending_phrases[:30]
        }

    def calculate_brand_relevance(self, content, trending_data):
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
        channels_config = self.notion.get_telegram_channels()
        
        if not channels_config:
            logger.warning("❌ No Telegram channels found")
            return [], {}
        
        collected_posts = []
        all_raw_posts = []
        client = None
        
        try:
            client = await self.session_manager.get_client()
            
            if client is None:
                logger.error("❌ Failed to get Telegram client")
                return [], {}
            
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
                    
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.error(f"❌ Error scanning {channel}: {str(e)}")
                    continue
            
            trending_data = self.extract_keywords_from_content(all_raw_posts)
            
            for post_data in all_raw_posts:
                content = post_data['content']
                channel_config = post_data['channel_config']
                
                relevance = self.calculate_brand_relevance(content, trending_data)
                engagement = post_data['engagement']
                
                engagement_score = min(10, (engagement['total'] // 100))
                
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
                
                category = 'lifestyle'
                if any('яхт' in word or 'sail' in word for word in relevance['matched_words']):
                    category = 'sailing'
                elif any('мод' in word or 'fashion' in word for word in relevance['matched_words']):
                    category = 'fashion'
                
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
                        'matched_phrases': relevance['matched_phrases']
                    },
                    'ai_analysis': {
                        'brand_relevance': round(relevance['total_relevance'], 1),
                        'trend_score': round(engagement_score, 1),
                        'content_category': category,
                        'hashtags': [f'#{category}', '#northsails', '#telegram'],
                        'insights': f"High {category} potential"
                    },
                    'north_sails_score': round(north_sails_score, 1),
                    'processed_at': datetime.now().isoformat(),
                    'url': f"https://t.me/{post_data['channel'].replace('@', '')}/{post_data['message'].id}",
                    'scanner_version': 'v3.0'
                }
                
                collected_posts.append(final_post)
        
        except Exception as e:
            logger.error(f"❌ Fatal error in Telegram scan: {str(e)}")
            return [], {}
        
        finally:
            if client:
                await self.session_manager.disconnect()
        
        logger.info(f"✅ Telegram scan completed: {len(collected_posts)} posts")
        return collected_posts, trending_data

    def scan_vk_communities(self, min_score=6.0):
        communities_config = self.notion.get_vk_communities()
        
        if not communities_config:
            return [], {}
        
        collected_posts = []
        all_raw_posts = []
        
        try:
            logger.info(f"🔍 Scanning {len(communities_config)} VK communities...")
            
            for community_config in communities_config:
                community_id = community_config['community_id']
                
                try:
                    logger.info(f"📱 Scanning VK: {community_id}")
                    
                    community_info = self.vk.get_community_info(community_id)
                    if not community_info:
                        continue
                    
                    posts = self.vk.get_community_posts(community_id, 50)
                    
                    for post in posts:
                        if not post.get('text') or len(post['text']) < 50:
                            continue
                        
                        likes = post.get('likes', {}).get('count', 0)
                        comments = post.get('comments', {}).get('count', 0)
                        reposts = post.get('reposts', {}).get('count', 0)
                        
                        total_engagement = likes + (comments * 3) + (reposts * 5)
                        
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
                                'total': total_engagement
                            }
                        }
                        
                        all_raw_posts.append(post_data)
                        
                except Exception as e:
                    logger.error(f"❌ Error scanning VK {community_id}: {str(e)}")
                    continue
            
            trending_data = self.extract_keywords_from_content(all_raw_posts)
            
            for post_data in all_raw_posts:
                content = post_data['content']
                relevance = self.calculate_brand_relevance(content, trending_data)
                engagement = post_data['engagement']
                
                engagement_score = min(10, (engagement['total'] // 50))
                north_sails_score = min(10, (relevance['total_relevance'] + engagement_score) / 2)
                
                if north_sails_score < min_score:
                    continue
                
                final_post = {
                    'platform': 'vk',
                    'community_id': post_data['community_id'],
                    'community_name': post_data['community_info'].get('name', ''),
                    'post_id': post_data['post']['id'],
                    'content': content,
                    'date': datetime.fromtimestamp(post_data['post']['date']).isoformat(),
                    'engagement': engagement,
                    'north_sails_score': round(north_sails_score, 1),
                    'processed_at': datetime.now().isoformat(),
                    'url': f"https://vk.com/wall-{post_data['community_id']}_{post_data['post']['id']}"
                }
                
                collected_posts.append(final_post)
        
        except Exception as e:
            logger.error(f"❌ Fatal error in VK scan: {str(e)}")
            return [], {}
        
        return collected_posts, trending_data

scanner = SocialMediaScanner()

@app.route('/')
def home():
    return jsonify({
        "status": "active",
        "service": "North Sails Social Media Intelligence API",
        "version": "3.0 - FINAL COMPLETE",
        "integrations": {
            "notion_configured": bool(NOTION_TOKEN),
            "telegram_configured": bool(API_ID and API_HASH),
            "telegram_session": bool(TELEGRAM_SESSION_STRING),
            "vk_configured": bool(VK_ACCESS_TOKEN)
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/debug/session', methods=['GET'])
def debug_session():
    session_string = os.getenv('TELEGRAM_SESSION_STRING')
    return jsonify({
        "session_exists": bool(session_string),
        "session_length": len(session_string) if session_string else 0,
        "session_preview": f"{session_string[:20]}...{session_string[-10:]}" if session_string else None,
        "api_configured": bool(os.getenv('TELEGRAM_API_ID') and os.getenv('TELEGRAM_API_HASH')),
        "environment_check": "OK" if session_string and len(session_string) > 50 else "FAILED"
    })

@app.route('/scan/telegram', methods=['POST', 'GET'])
def scan_telegram():
    try:
        hours_back = request.json.get('hours_back', 24) if request.is_json else 24
        min_score = request.json.get('min_score', 6.0) if request.is_json else 6.0
        
        logger.info(f"🚀 Starting Telegram scan: {hours_back}h back, min_score: {min_score}")
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            posts, trending_data = loop.run_until_complete(
                scanner.scan_telegram_channels(hours_back, min_score)
            )
        finally:
            loop.close()
        
        result = {
            'source': 'telegram_scanner_api_v3',
            'timestamp': datetime.now().isoformat(),
            'total_posts': len(posts),
            'posts': posts,
            'trending_keywords': {
                'words': trending_data.get('words', [])[:10],
                'phrases': trending_data.get('phrases', [])[:10]
            }
        }
        
        logger.info(f"✅ Telegram scan completed: {len(posts)} posts")
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
    try:
        min_score = request.json.get('min_score', 6.0) if request.is_json else 6.0
        
        logger.info(f"🚀 Starting VK scan: min_score: {min_score}")
        
        posts, trending_data = scanner.scan_vk_communities(min_score)
        
        result = {
            'source': 'vk_scanner_api_v3',
            'timestamp': datetime.now().isoformat(),
            'total_posts': len(posts),
            'posts': posts,
            'trending_keywords': {
                'words': trending_data.get('words', [])[:10],
                'phrases': trending_data.get('phrases', [])[:10]
            }
        }
        
        logger.info(f"✅ VK scan completed: {len(posts)} posts")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"❌ Error in VK scan: {str(e)}")
        return jsonify({
            'error': str(e),
            'source': 'vk_scanner_api_v3',
            'timestamp': datetime.now().isoformat(),
            'posts': []
        }), 500

@app.route('/channels/telegram')
def list_telegram_channels():
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

@app.route('/health')
def health():
    return jsonify({
        "status": "healthy",
        "version": "3.0 - FINAL COMPLETE",
        "integrations": {
            "notion": bool(NOTION_TOKEN),
            "telegram": bool(API_ID and API_HASH),
            "telegram_session": bool(TELEGRAM_SESSION_STRING),
            "vk": bool(VK_ACCESS_TOKEN)
        },
        "session_info": {
            "configured": bool(TELEGRAM_SESSION_STRING),
            "length": len(TELEGRAM_SESSION_STRING) if TELEGRAM_SESSION_STRING else 0
        },
        "timestamp": datetime.now().isoformat()
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    
    logger.info("🚀 North Sails API v3.0 Starting...")
    logger.info(f"🔧 Notion: {'✅' if NOTION_TOKEN else '❌'}")
    logger.info(f"📱 Telegram: {'✅' if API_ID and API_HASH else '❌'}")
    logger.info(f"🔑 Session: {'✅' if TELEGRAM_SESSION_STRING else '❌'}")
    logger.info(f"📘 VK: {'✅' if VK_ACCESS_TOKEN else '❌'}")
    
    if TELEGRAM_SESSION_STRING:
        logger.info(f"🔍 Session length: {len(TELEGRAM_SESSION_STRING)}")
        if len(TELEGRAM_SESSION_STRING) > 50:
            logger.info("✅ Session format looks valid")
        else:
            logger.error("❌ Session string too short")
    
    app.run(host='0.0.0.0', port=port, debug=False)
