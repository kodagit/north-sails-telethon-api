# app.py - Flask API for North Sails Telethon Scanner
import asyncio
import json
import os
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from telethon import TelegramClient
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# API credentials from environment
API_ID = int(os.getenv('TELEGRAM_API_ID', '29481789'))
API_HASH = os.getenv('TELEGRAM_API_HASH', '59f4a8346e712a5509ff700bc1da9b5d')

class TelethonScanner:
    def __init__(self):
        self.channels_to_monitor = [
            '@elle_ru',           # Working
            '@vogueru',           # Vogue Russia alternative
            '@bazaarrussia',      # Harper's Bazaar Russia alternative  
            '@marieclairerus',    # Marie Claire Russia alternative
            '@fashionweekru',     # Fashion Week Russia
            '@lifestyle_russia',  # Lifestyle content
            '@moscow_fashion',    # Moscow fashion
            '@russian_style',     # Russian style content
            '@luxury_russia',     # Luxury lifestyle
            '@sailing_russia',    # Sailing content
        ]
        
        self.north_sails_keywords = [
            # Fashion & Style (Russian)
            'мода', 'стиль', 'одежда', 'бренд', 'тренд', 'кэжуал', 'premium',
            # Fashion & Style (English) 
            'fashion', 'style', 'brand', 'trend', 'casual', 'clothing',
            # Sailing & Marine (Russian)
            'яхта', 'парус', 'море', 'морской', 'яхтинг', 'регата', 'марина',
            # Sailing & Marine (English)
            'sailing', 'yacht', 'marine', 'sea', 'marina', 'regatta', 'nautical',
            # Lifestyle (Russian)
            'лайфстайл', 'премиум', 'элитный', 'роскошь', 'VIP',
            # Lifestyle (English)
            'lifestyle', 'premium', 'luxury', 'elite', 'exclusive',
            # Outdoor & Sports
            'спорт', 'активный', 'отдых', 'outdoor', 'adventure', 'nature',
            # Brand mentions
            'north sails', 'northsails', 'норт сейлс'
        ]

    async def scan_channels(self, hours_back=24, min_score=6.0):
        """Main scanning function"""
        collected_posts = []
        
        try:
            async with TelegramClient('session', API_ID, API_HASH) as client:
                logger.info(f"🔍 Scanning {len(self.channels_to_monitor)} channels...")
                
                for channel in self.channels_to_monitor:
                    try:
                        logger.info(f"📱 Scanning: {channel}")
                        
                        # Get channel entity
                        entity = await client.get_entity(channel)
                        
                        # Get messages from last X hours
                        since_date = datetime.now() - timedelta(hours=hours_back)
                        messages = await client.get_messages(
                            channel, 
                            limit=50,
                            offset_date=since_date
                        )
                        
                        logger.info(f"  📨 Found {len(messages)} messages in last {hours_back}h")
                        
                        # Process messages
                        for msg in messages:
                            if not msg.message:
                                continue
                                
                            content = msg.message.lower()
                            
                            # Check for keywords
                            matching_keywords = [kw for kw in self.north_sails_keywords 
                                               if kw.lower() in content]
                            
                            if not matching_keywords:
                                continue
                            
                            # Check engagement
                            views = getattr(msg, 'views', 0) or 0
                            forwards = getattr(msg, 'forwards', 0) or 0
                            total_engagement = views + (forwards * 10)
                            
                            if total_engagement < 100:
                                continue
                            
                            # Calculate AI scores
                            sailing_score = sum(1 for kw in ['яхт', 'парус', 'море', 'sail', 'marine', 'yacht'] 
                                              if kw in content)
                            fashion_score = sum(1 for kw in ['мод', 'стиль', 'fashion', 'одежд', 'бренд'] 
                                              if kw in content)
                            luxury_score = sum(1 for kw in ['премиум', 'luxury', 'элитн', 'VIP'] 
                                             if kw in content)
                            
                            brand_relevance = min(10, (sailing_score * 3) + (fashion_score * 2) + luxury_score + 2)
                            trend_score = min(10, max(5, (total_engagement // 100) + len(matching_keywords)))
                            target_audience_fit = min(10, 6 + sailing_score + fashion_score)
                            viral_potential = min(10, max(1, total_engagement // 200))
                            
                            north_sails_score = (brand_relevance + trend_score + target_audience_fit) / 3
                            
                            # Skip low-score posts
                            if north_sails_score < min_score:
                                continue
                            
                            # Determine category
                            if sailing_score >= 2:
                                category = 'sailing'
                            elif fashion_score >= 2:
                                category = 'fashion'
                            else:
                                category = 'lifestyle'
                            
                            # Create post data
                            post_data = {
                                'platform': 'telegram',
                                'channel': channel,
                                'channel_title': entity.title,
                                'message_id': msg.id,
                                'content': msg.message,
                                'date': msg.date.isoformat(),
                                'author': getattr(msg.from_id, 'user_id', None) if msg.from_id else None,
                                'engagement': {
                                    'views': views,
                                    'forwards': forwards,
                                    'total': total_engagement
                                },
                                'media_type': 'photo' if msg.photo else 'video' if msg.video else 'text',
                                'matching_keywords': matching_keywords,
                                'ai_analysis': {
                                    'brand_relevance': round(brand_relevance, 1),
                                    'trend_score': round(trend_score, 1),
                                    'viral_potential': round(viral_potential, 1),
                                    'target_audience_fit': round(target_audience_fit, 1),
                                    'content_category': category,
                                    'season_relevance': 'year-round',
                                    'hashtags': ['#northsails', '#russiafashion', f'#{category}'],
                                    'insights': f"High {category} potential for North Sails Russia - {total_engagement} engagement",
                                    'campaign_ideas': self.generate_campaign_ideas(category, sailing_score, fashion_score)
                                },
                                'north_sails_score': round(north_sails_score, 1),
                                'processed_at': datetime.now().isoformat(),
                                'url': f"https://t.me/{channel.replace('@', '')}/{msg.id}"
                            }
                            
                            collected_posts.append(post_data)
                            logger.info(f"    ✅ High potential: {north_sails_score:.1f}/10 - {content[:50]}...")
                        
                        # Rate limiting
                        await asyncio.sleep(2)
                        
                    except Exception as e:
                        logger.error(f"  ❌ Error scanning {channel}: {str(e)}")
                        continue
        
        except Exception as e:
            logger.error(f"❌ Fatal error in scan_channels: {str(e)}")
            raise
        
        return collected_posts

    def generate_campaign_ideas(self, category, sailing_score, fashion_score):
        """Generate campaign ideas by category"""
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
        "version": "1.0",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/scan', methods=['POST', 'GET'])
def scan_channels():
    """Main scanning endpoint"""
    try:
        # Get parameters
        hours_back = request.json.get('hours_back', 24) if request.is_json else 24
        min_score = request.json.get('min_score', 6.0) if request.is_json else 6.0
        
        logger.info(f"🚀 Starting scan: {hours_back}h back, min_score: {min_score}")
        
        # Run async scan
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        posts = loop.run_until_complete(scanner.scan_channels(hours_back, min_score))
        loop.close()
        
        # Prepare response
        result = {
            'source': 'telethon_scanner_api',
            'timestamp': datetime.now().isoformat(),
            'parameters': {
                'hours_back': hours_back,
                'min_score': min_score
            },
            'total_posts': len(posts),
            'posts': posts,
            'summary': {
                'total_collected': len(posts),
                'avg_north_sails_score': sum(p['north_sails_score'] for p in posts) / len(posts) if posts else 0,
                'top_category': max(set(p['ai_analysis']['content_category'] for p in posts), 
                                  key=lambda x: sum(1 for p in posts if p['ai_analysis']['content_category'] == x)) if posts else 'none',
                'high_score_posts': len([p for p in posts if p['north_sails_score'] >= 8]),
                'categories': {}
            }
        }
        
        # Add category breakdown
        if posts:
            categories = {}
            for post in posts:
                cat = post['ai_analysis']['content_category']
                categories[cat] = categories.get(cat, 0) + 1
            result['summary']['categories'] = categories
        
        logger.info(f"✅ Scan completed: {len(posts)} posts collected")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"❌ Error in scan endpoint: {str(e)}")
        return jsonify({
            'error': str(e),
            'source': 'telethon_scanner_api',
            'timestamp': datetime.now().isoformat(),
            'posts': []
        }), 500

@app.route('/health')
def health():
    """Detailed health check"""
    return jsonify({
        "status": "healthy",
        "uptime": "active",
        "channels_monitored": len(scanner.channels_to_monitor),
        "keywords_tracked": len(scanner.north_sails_keywords),
        "api_configured": bool(API_ID and API_HASH),
        "timestamp": datetime.now().isoformat()
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
