# youtube_publications_scanner.py (v1.1 - Fixed proxy parser)

import re
import requests
import feedparser
from datetime import datetime
import pytz
from typing import List, Dict, Optional, Tuple


def get_channel_id_from_url(url: str, proxy_dict: Optional[Dict] = None) -> Optional[str]:
    # (no changes)
    if "/channel/" in url:
        match = re.search(r"/channel/([a-zA-Z0-9_-]{24})", url)
        return match.group(1) if match else None
    elif "/@" in url:
        try:
            response = requests.get(url, proxies=proxy_dict, timeout=10)
            response.raise_for_status()
            html = response.text
            match = re.search(r'"channelId":"(UC[\w-]{22})"', html)
            return match.group(1) if match else None
        except requests.RequestException as e:
            print(f"Error trying to get channel_id from {url}: {e}")
            return None
    else:
        return None


def _parse_proxy_string(proxy_str: str) -> Optional[Dict[str, str]]:
    """
    Parses proxy string.
    CHANGED: Now correctly handles 'http://ip:port:user:pass' format.
    """
    if not proxy_str or not isinstance(proxy_str, str):
        return None

    proxy_str = proxy_str.strip()

    # Remove prefix if present
    if proxy_str.startswith("http://"):
        proxy_str = proxy_str[7:]
    elif proxy_str.startswith("https://"):
        proxy_str = proxy_str[8:]

    parts = proxy_str.split(':')

    # ip:port:user:pass
    if len(parts) == 4:
        ip, port, user, password = parts
        return {
            'http': f'http://{user}:{password}@{ip}:{port}',
            'https': f'http://{user}:{password}@{ip}:{port}'
        }
    # ip:port
    elif len(parts) == 2:
        ip, port = parts
        return {
            'http': f'http://{ip}:{port}',
            'https': f'http://{ip}:{port}'
        }

    print(f"Invalid proxy format: {proxy_str}")
    return None


def check_proxy(proxy_str: str) -> Tuple[bool, str]:
    """
    Checks proxy functionality.
    (no changes, will now work with correct parser)
    """
    proxy_dict = _parse_proxy_string(proxy_str)
    if not proxy_dict:
        return False, "Invalid proxy string format"

    try:
        response = requests.get("https://httpbin.org/get", proxies=proxy_dict, timeout=10)
        response.raise_for_status()
        # Check that the response came from the expected IP
        response_ip = response.json().get('origin')
        proxy_ip = proxy_dict['http'].split('@')[-1].split(':')[0]
        if response_ip == proxy_ip:
            return True, "Proxy is working"
        else:
            return False, f"Proxy is working but IP mismatch (expected {proxy_ip}, got {response_ip})"
    except requests.exceptions.ProxyError as e:
        return False, f"Proxy error: {e}"
    except requests.exceptions.Timeout:
        return False, "Request timeout"
    except requests.RequestException as e:
        return False, f"Request error: {e}"


def fetch_channel_videos(channel_id: str, start_date: datetime, proxy_str: Optional[str] = None) -> List[Dict]:
    """
    Scans channel RSS feed.
    (no changes, will now work with correct parser)
    """
    if not channel_id:
        return []

    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    proxy_dict = _parse_proxy_string(proxy_str)

    try:
        response = requests.get(rss_url, proxies=proxy_dict, timeout=15)
        response.raise_for_status()
        feed_content = response.content
    except requests.RequestException as e:
        print(f"Failed to download RSS feed for {channel_id}: {e}")
        return []

    feed = feedparser.parse(feed_content)

    if not feed.entries:
        print(f"Failed to load RSS data for channel {channel_id}.")
        return []

    tz_wroclaw = pytz.timezone("Europe/Warsaw")
    if start_date.tzinfo is None:
        start_date = tz_wroclaw.localize(start_date)

    videos = []
    for entry in feed.entries:
        try:
            published_utc = datetime.strptime(entry.published, "%Y-%m-%dT%H:%M:%S%z")
            published_wroclaw = published_utc.astimezone(tz_wroclaw)

            if published_wroclaw >= start_date:
                videos.append({
                    "title": entry.title,
                    "url": entry.link,
                    "published_time": published_wroclaw
                })
        except (ValueError, KeyError) as e:
            print(f"Error parsing entry for video '{entry.title}': {e}")
            continue

    videos.sort(key=lambda x: x['published_time'])
    return videos