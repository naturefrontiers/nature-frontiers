import os
import json
import logging
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import feedparser
from feedgenerator.django.utils.feedgenerator import Rss201rev2Feed
from dateutil import parser as date_parser

# --- Configuration ---
BASE_DIR = Path(__file__).parent.parent
DATA_FILE = BASE_DIR / "data" / "state.json"
OUTPUT_DIR = BASE_DIR / "output"
SOURCES_FILE = BASE_DIR / "src" / "sources.json"

# Ensure directories exist
DATA_FILE.parent.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_state():
    """Load the processed GUIDs from state.json."""
    if DATA_FILE.exists():
        with open(DATA_FILE, 'r') as f:
            return json.load(f)
    return {"processed_guids": [], "last_run": None}

def save_state(state):
    """Save the updated state to state.json."""
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    with open(DATA_FILE, 'w') as f:
        json.dump(state, f, indent=2)

def load_sources():
    """Load RSS source URLs from config."""
    with open(SOURCES_FILE, 'r') as f:
        return json.load(f)

def get_unique_guid(entry, source_id):
    """Generate a unique GUID if the feed doesn't provide one."""
    if hasattr(entry, 'id') and entry.id:
        return entry.id
    # Fallback: Hash of link + pubDate
    link = getattr(entry, 'link', '')
    title = getattr(entry, 'title', '')
    unique_str = f"{source_id}:{link}:{title}"
    return hashlib.sha256(unique_str.encode('utf-8')).hexdigest()

def fetch_feed(url, source_id, state):
    """Fetch and parse a single feed, filtering duplicates."""
    logger.info(f"Fetching: {source_id}")
    try:
        feed = feedparser.parse(url)
        new_items = []
        
        for entry in feed.entries:
            guid = get_unique_guid(entry, source_id)
            
            if guid in state["processed_guids"]:
                continue
            
            # Extract data
            item = {
                "guid": guid,
                "title": getattr(entry, 'title', 'No Title'),
                "link": getattr(entry, 'link', ''),
                "description": getattr(entry, 'summary', getattr(entry, 'description', '')),
                "pub_date": None,
                "thumbnail": None,
                "source_id": source_id,
                "is_youtube": False
            }
            
            # Parse Date
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                item["pub_date"] = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                item["pub_date"] = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
            else:
                item["pub_date"] = datetime.now(timezone.utc)

            # Extract Thumbnail (Media RSS or Enclosure)
            if 'media_thumbnail' in entry:
                item["thumbnail"] = entry.media_thumbnail[0]['url']
            elif 'enclosures' in entry and entry.enclosures[0].get('type', '').startswith('image'):
                item["thumbnail"] = entry.enclosures[0]['href']
            
            # Detect YouTube
            if "youtube.com" in url or "youtu.be" in item["link"]:
                item["is_youtube"] = True
                # Ensure clean watch URL
                if "watch?v=" not in item["link"] and "youtu.be/" in item["link"]:
                    vid_id = item["link"].split("/")[-1]
                    item["link"] = f"https://www.youtube.com/watch?v={vid_id}"

            new_items.append(item)
            state["processed_guids"].append(guid)
            
        return new_items
    except Exception as e:
        logger.error(f"Error fetching {source_id}: {str(e)}")
        return []

def generate_social_queue(youtube_items):
    """Create a Markdown file with ready-to-post captions."""
    if not youtube_items:
        return
    
    queue_file = OUTPUT_DIR / "social_queue.md"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    content = f"# Social Media Queue - Generated {timestamp}\n\n"
    content += "> **Instructions:** Copy the content below for each video and post manually to LinkedIn, X, Instagram, and TikTok.\n\n"
    
    for item in youtube_items:
        # Clean description for social (first 200 chars)
        clean_desc = item["description"].replace('<p>', '').replace('</p>', '').replace('<br>', '')
        snippet = clean_desc[:200] + "..." if len(clean_desc) > 200 else clean_desc
        
        content += f"## 🎥 {item['title']}\n\n"
        content += f"**Caption:**\n"
        content += f"🌿 New Discovery! {item['title']}\n\n"
        content += f"{snippet}\n\n"
        content += f"👉 Watch now: {item['link']}\n\n"
        content += f"**Hashtags:**\n"
        content += f"#Wildlife #Nature #Conservation #YouTube #NatureFrontiers\n\n"
        content += f"**Thumbnail URL:** `{item['thumbnail']}`\n"
        content += f"**Direct Link:** {item['link']}\n\n"
        content += "---\n\n"
    
    with open(queue_file, 'w', encoding='utf-8') as f:
        f.write(content)
    logger.info(f"Social queue written to {queue_file}")

def generate_rss_feed(all_items):
    """Generate the unified RSS 2.0 feed."""
    feed = Rss201rev2Feed(
        title="Nature Frontiers Aggregator",
        link="https://nature-frontiers.github.io/nature-frontiers-feed/",
        description="Curated wildlife, nature, and conservation content from top global sources.",
        language="en",
        ttl=60 # Refresh every 60 mins
    )
    
    # Sort by date descending
    sorted_items = sorted(all_items, key=lambda x: x["pub_date"], reverse=True)
    
    for item in sorted_items:
        feed.add_item(
            title=item["title"],
            link=item["link"],
            description=item["description"],
            pubdate=item["pub_date"],
            unique_id=item["guid"],
            enclosures=[item["thumbnail"]] if item["thumbnail"] else [],
            categories=[item["source_id"]]
        )
    
    output_path = OUTPUT_DIR / "feed.xml"
    with open(output_path, 'w', encoding='utf-8') as f:
        feed.write(f, 'utf-8')
    
    logger.info(f"RSS feed generated with {len(sorted_items)} items at {output_path}")

def main():
    logger.info("Starting Nature Frontiers Aggregator...")
    
    state = load_state()
    sources = load_sources()
    
    all_new_items = []
    youtube_new_items = []
    
    # Process YouTube Sources
    for source in sources.get("youtube_sources", []):
        items = fetch_feed(source["url"], source["id"], state)
        for item in items:
            all_new_items.append(item)
            if item["is_youtube"]:
                youtube_new_items.append(item)
    
    # Process Article Sources
    for source in sources.get("article_sources", []):
        items = fetch_feed(source["url"], source["id"], state)
        for item in items:
            all_new_items.append(item)
    
    if all_new_items:
        logger.info(f"Found {len(all_new_items)} new items.")
        
        # Generate Outputs
        generate_rss_feed(all_new_items)
        
        if youtube_new_items:
            generate_social_queue(youtube_new_items)
            logger.info(f"Prepared {len(youtube_new_items)} YouTube videos for social posting.")
        
        # Save State
        save_state(state)
        print("CHANGES_DETECTED=true") # Flag for GitHub Actions
    else:
        logger.info("No new items found.")
        print("CHANGES_DETECTED=false")

if __name__ == "__main__":
    main()
