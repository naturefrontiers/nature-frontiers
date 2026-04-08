import os
import json
import logging
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import feedparser
from feedgenerator.django.utils.feedgenerator import Rss201rev2Feed

# --- Configuration ---
BASE_DIR = Path(__file__).parent.parent
DATA_FILE = BASE_DIR / "data" / "state.json"
SOURCES_FILE = BASE_DIR / "src" / "sources.json"
FEED_FILE = BASE_DIR / "feed.xml"
SOCIAL_FILE = BASE_DIR / "output" / "social_queue.md"

# Ensure directories exist
BASE_DIR.joinpath("data").mkdir(exist_ok=True)
BASE_DIR.joinpath("output").mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_json(path, default):
    if not path.exists():
        logger.error(f"File not found: {path}")
        return default
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error reading {path}: {e}")
        return default

def save_json(path, data):
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)

def get_guid(entry, source_id):
    if hasattr(entry, 'id') and entry.id:
        return entry.id
    link = getattr(entry, 'link', '')
    return hashlib.sha256(f"{source_id}:{link}".encode()).hexdigest()

def main():
    logger.info("Starting Aggregator...")
    
    # Load Config
    sources = load_json(SOURCES_FILE, {"youtube_sources": [], "article_sources": []})
    state = load_json(DATA_FILE, {"processed_guids": [], "last_run": None})
    
    if not sources.get("youtube_sources") and not sources.get("article_sources"):
        logger.error("No sources configured. Check src/sources.json")
        return

    all_items = []
    youtube_items = []
    new_count = 0

    # Process All Sources
    all_sources = sources.get("youtube_sources", []) + sources.get("article_sources", [])
    
    for src in all_sources:
        url = src.get('url')
        sid = src.get('id')
        stype = src.get('type')
        
        if not url: continue
        
        logger.info(f"Fetching: {sid}")
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                guid = get_guid(entry, sid)
                
                if guid in state["processed_guids"]:
                    continue
                
                # New Item Found
                new_count += 1
                state["processed_guids"].append(guid)
                
                # Extract Data
                title = getattr(entry, 'title', 'No Title')
                link = getattr(entry, 'link', '')
                summary = getattr(entry, 'summary', '')
                
                # Date
                pub_date = datetime.now(timezone.utc)
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    try:
                        pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                    except: pass
                
                # Thumbnail
                thumb = None
                if 'media_thumbnail' in entry:
                    thumb = entry.media_thumbnail[0]['url']
                
                is_yt = stype == "youtube"
                
                item = {
                    "title": title, "link": link, "description": summary,
                    "pub_date": pub_date, "thumbnail": thumb, "is_youtube": is_yt,
                    "source": sid
                }
                
                all_items.append(item)
                if is_yt:
                    youtube_items.append(item)
                    
        except Exception as e:
            logger.error(f"Failed {sid}: {e}")

    if new_count > 0:
        logger.info(f"Found {new_count} new items.")
        
        # 1. Generate RSS
        feed_gen = Rss201rev2Feed(
            title="Nature Frontiers Aggregator",
            link="https://naturefrontiers.github.io/nature-frontiers/",
            description="Curated wildlife content.",
            language="en"
        )
        for item in sorted(all_items, key=lambda x: x['pub_date'], reverse=True):
            feed_gen.add_item(
                title=item['title'], link=item['link'], description=item['description'],
                pubdate=item['pub_date'], unique_id=get_guid({'link': item['link']}, item['source']),
                enclosures=[item['thumbnail']] if item['thumbnail'] else []
            )
        
        with open(FEED_FILE, 'w', encoding='utf-8') as f:
            feed_gen.write(f, 'utf-8')
        logger.info("RSS Feed saved.")

        # 2. Generate Social Queue
        if youtube_items:
            content = f"# Social Queue ({datetime.now().strftime('%Y-%m-%d')})\n\n"
            for vid in youtube_items:
                content += f"## 🎥 {vid['title']}\n🔗 {vid['link']}\n\n"
            with open(SOCIAL_FILE, 'w', encoding='utf-8') as f:
                f.write(content)
            logger.info("Social Queue updated.")
        
        # 3. Save State
        save_json(DATA_FILE, state)
        print("CHANGES_DETECTED=true")
    else:
        logger.info("No new items.")
        print("CHANGES_DETECTED=false")

if __name__ == "__main__":
    main()
