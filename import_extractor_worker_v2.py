# --- import_extractor_worker_v2.py ---

import json
import re
import traceback
import urllib.request
import urllib.error
import html
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
import requests
import random
import time

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
]

try:
    import yt_dlp
except ImportError:
    class yt_dlp:
        class YoutubeDL:
            def __init__(self, *args, **kwargs): pass

            def __enter__(self): return self

            def __exit__(self, exc_type, exc_val, exc_tb): pass

            def extract_info(self, *args, **kwargs): raise ImportError("yt-dlp is not installed")


def _execute_request(url: str) -> dict:
    log_entry = {"url_used": url, "request_headers": {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36', 'Accept-Language': 'en-US,en;q=0.9', 'Accept': 'text/plain,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 'DNT': '1', 'Connection': 'keep-alive', 'Upgrade-Insecure-Requests': '1'}}
    try:
        request = urllib.request.Request(url, headers=log_entry["request_headers"])
        with urllib.request.urlopen(request, timeout=15) as response:
            log_entry["response_status_code"] = response.getcode()
            log_entry["response_headers"] = dict(response.getheaders())
            content = response.read().decode('utf-8', errors='ignore')
            return {"status": "success", "content": content, "log": log_entry}
    except urllib.error.HTTPError as e:
        log_entry["error_type"] = "HTTPError"
        log_entry["response_status_code"] = e.code
        log_entry["response_headers"] = dict(e.headers) if e.headers else {}
        log_entry["error_message"] = str(e.reason) if e.reason else "Unknown HTTP error"
        try:
            error_content = e.read().decode('utf-8', errors='ignore')
            log_entry["error_content"] = error_content[:1000]  # Limit error content size
        except:
            log_entry["error_content"] = "Could not read error response"
        log_entry["full_traceback"] = traceback.format_exc()
        return {"status": "failure", "content": None, "log": log_entry}
    except urllib.error.URLError as e:
        log_entry["error_type"] = "URLError"
        log_entry["error_message"] = str(e.reason) if e.reason else "Unknown URL error"
        log_entry["full_traceback"] = traceback.format_exc()
        return {"status": "failure", "content": None, "log": log_entry}
    except Exception as e:
        log_entry["error_type"] = "GeneralException"
        log_entry["error_message"] = str(e)
        log_entry["full_traceback"] = traceback.format_exc()
        return {"status": "failure", "content": None, "log": log_entry}


def _parse_vtt(vtt_content: str) -> str:
    lines = vtt_content.strip().splitlines()
    raw_text_segments = []
    for line in lines:
        if '-->' in line or line.strip().isdigit() or 'WEBVTT' in line or not line.strip() or line.strip().startswith(
                "Kind:") or line.strip().startswith("Language:"):
            continue
        clean_line = re.sub(r'<[^>]+>', '', line).strip()
        if clean_line:
            raw_text_segments.append(clean_line)
    if not raw_text_segments: return ""
    final_phrases = []
    for i in range(len(raw_text_segments) - 1):
        current_line = raw_text_segments[i]
        next_line = raw_text_segments[i + 1]
        if not next_line.startswith(current_line):
            final_phrases.append(current_line)
    final_phrases.append(raw_text_segments[-1])
    unique_phrases = list(dict.fromkeys(final_phrases))
    full_text = " ".join(unique_phrases)
    full_text = re.sub(r'\[.*?\]', '', full_text)
    full_text = full_text.replace('>>', '')
    full_text = html.unescape(full_text)
    return re.sub(r'\s{2,}', ' ', full_text).strip()


def _fetch_video_metadata(url: str, html_content: str | None) -> dict:
    log = {"attempts": {}}
    try:
        # Completely silence yt-dlp output
        import sys
        import os
        from contextlib import redirect_stderr, redirect_stdout

        import random
        import time

        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
        ]
        selected_ua = random.choice(user_agents)

        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'skip_download': True,
            'logger': None,  # Disable logging
            'logtostderr': False,  # Don't log to stderr
            'sleep_interval': 2,  # Add delay between requests
            'http_headers': {
                'User-Agent': selected_ua,
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            }
        }

        # Capture and suppress all yt-dlp output
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with redirect_stdout(open(os.devnull, 'w')), redirect_stderr(open(os.devnull, 'w')):
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(url, download=False)
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    sleep_time = 2 ** attempt
                    print(f"yt-dlp attempt {attempt + 1} failed, retrying in {sleep_time}s: {e}")
                    time.sleep(sleep_time)
                else:
                    raise e

        meta = {"title": info.get('title'), "description": info.get('description'), "channel": info.get('channel'),
                "channel_id": info.get('channel_id'),
                "video_id": info.get('id'),
                "upload_date": datetime.strptime(info['upload_date'], '%Y%m%d').strftime('%Y-%m-%d') if info.get(
                    'upload_date') else None, "duration": str(timedelta(seconds=int(info.get('duration', 0)))),
                "view_count": info.get('view_count'), }
        thumbnails = info.get('thumbnails', [])
        if thumbnails:
            max_thumbnail = max(thumbnails, key=lambda t: t.get('height', 0))
            if max_thumbnail.get('height', 0) > 480:
                meta['thumbnail_url'] = max_thumbnail.get('url')
                meta['thumbnail_width'] = max_thumbnail.get('width')
                meta['thumbnail_height'] = max_thumbnail.get('height')
            else:
                meta['thumbnail_url'] = info.get('thumbnail')
        else:
            meta['thumbnail_url'] = info.get('thumbnail')
        if not meta.get("title"): raise ValueError("Extracted info is empty")
        log.update({"status": "success", "final_method": "yt-dlp", "data": meta,
                    "attempts": {"yt-dlp": {"status": "success"}}});
        return log
    except Exception as e:
        _force_print_yt_dlp_error(e, f"YouTube Metadata - {url}")

        error_details = {
            "status": "failure",
            "error_type": type(e).__name__,
            "error_message": str(e),
            "full_traceback": traceback.format_exc()
        }
        if hasattr(e, 'exc_info') and e.exc_info:
            error_details["yt_dlp_exc_info"] = str(e.exc_info)
        if "403" in str(e) or "Forbidden" in str(e):
            error_details["likely_cause"] = "YouTube blocking request - try different User-Agent or wait before retrying"
            error_details["response_status_code"] = 403
        elif "429" in str(e) or "Too Many Requests" in str(e):
            error_details["likely_cause"] = "Rate limited by YouTube - wait before retrying"
            error_details["response_status_code"] = 429
        elif "format is not available" in str(e):
            error_details["likely_cause"] = "Video format not available - video may be private, deleted, or region-restricted"
        log["attempts"]["yt-dlp"] = error_details
    if not html_content:
        log.update({"status": "failure", "final_method": "all_methods_failed", "error": "HTML content not available"});
        return log
    try:
        def _find_json(html, var):
            return json.loads(m.group(1)) if (m := re.search(f'var {var} = ({{.*?}});', html, re.DOTALL)) else None

        def _find_key(data, key):
            if isinstance(data, dict):
                if key in data: return data[key]
                for v in data.values():
                    if (found := _find_key(v, key)) is not None: return found
            elif isinstance(data, list):
                for i in data:
                    if (found := _find_key(i, key)) is not None: return found
            return None

        details = _find_key(_find_json(html_content, 'ytInitialPlayerResponse'), 'videoDetails')
        if not details: raise ValueError("Could not find 'videoDetails'")
        meta = {'title': details.get('title'), 'description': details.get('shortDescription'),
                'channel': details.get('author'), 'video_id': details.get('videoId'),
                'view_count': int(details.get('viewCount', 0)),
                'duration': str(timedelta(seconds=int(details.get('lengthSeconds', 0))))}
        import re
        og_image_match = re.search(r'<meta property="og:image" content="([^"]+)"', html_content)
        if og_image_match:
            meta['thumbnail_url'] = og_image_match.group(1)
        else:
            video_id = meta.get('video_id')
            if video_id:
                meta['thumbnail_url'] = f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg"
                meta['thumbnail_width'] = 480
                meta['thumbnail_height'] = 360
        if not meta.get("title"): raise ValueError("Extracted info is empty")
        log.update({"status": "success", "final_method": "json_parser", "data": meta,
                    "attempts": {"json_parser": {"status": "success"}}});
        return log
    except Exception:
        log["attempts"]["json_parser"] = {"status": "failure", "error": traceback.format_exc()}
    try:
        meta = {}
        if m := re.search(r'<title>(.*?) - YouTube</title>', html_content): meta['title'] = html.unescape(
            m.group(1).strip())
        if m := re.search(r'<meta name="description" content="(.*?)">', html_content): meta[
            'description'] = html.unescape(m.group(1).strip())
        if not meta.get("title"): raise ValueError("Could not find title tag")
        log.update({"status": "success", "final_method": "primitive_parser", "data": meta,
                    "attempts": {"primitive_parser": {"status": "success"}}});
        return log
    except Exception:
        log["attempts"]["primitive_parser"] = {"status": "failure", "error": traceback.format_exc()}
    log.update({"status": "failure", "final_method": "all_methods_failed"});
    return log


def _get_transcript_text(url: str, lang: str = "en") -> dict:
    log = {"attempts": {}}
    try:
        import sys
        import os
        from contextlib import redirect_stderr, redirect_stdout

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            output_template = temp_path / "sub"
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'skip_download': True,
                'writeautomaticsub': True,
                'sublangs': [lang],
                'subformat': 'vtt',
                'outtmpl': str(output_template),
                'logger': None,
                'logtostderr': False,
                'sleep_interval': 3,
                'retries': 3,
                'fragment_retries': 3,
                'postprocessors': [{
                    'key': 'FFmpegSubtitlesConvertor',
                    'format': 'vtt',
                }]
            }

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    with redirect_stdout(open(os.devnull, 'w')), redirect_stderr(open(os.devnull, 'w')):
                        with yt_dlp.YoutubeDL(ydl_opts) as ydl: ydl.download([url])
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        sleep_time = 2 ** attempt
                        print(f"Transcript download attempt {attempt + 1} failed, retrying in {sleep_time}s: {e}")
                        time.sleep(sleep_time)
                    else:
                        raise e

            sub_files = list(temp_path.glob("sub.*.vtt"))
            if not sub_files: raise ValueError("yt-dlp did not download a VTT file.")
            vtt_content = sub_files[0].read_text(encoding='utf-8')
            text = _parse_vtt(vtt_content)
            log.update({"status": "success", "final_method": "ytdlp_force_download", "data": text})
            log["attempts"]["ytdlp_force_download"] = {"status": "success", "file_found": str(sub_files[0])};
            return log
    except Exception as e:
        _force_print_yt_dlp_error(e, f"YouTube Transcript Download - {url}")

        error_details = {
            "status": "failure",
            "error_type": type(e).__name__,
            "error_message": str(e),
            "full_traceback": traceback.format_exc()
        }
        if "403" in str(e) or "Forbidden" in str(e):
            error_details["likely_cause"] = "YouTube blocking subtitle download - video may not have auto-generated subtitles or access is restricted"
            error_details["response_status_code"] = 403
        elif "429" in str(e) or "Too Many Requests" in str(e):
            error_details["likely_cause"] = "Rate limited by YouTube - wait before retrying"
            error_details["response_status_code"] = 429
        elif "format is not available" in str(e):
            error_details["likely_cause"] = "Video format not available - video may be private, deleted, or region-restricted"
        log["attempts"]["ytdlp_force_download"] = error_details

    try:
        import sys
        import os
        from contextlib import redirect_stderr, redirect_stdout

        selected_ua = random.choice(user_agents)
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'skip_download': True,
            'logger': None,
            'logtostderr': False,
            'sleep_interval': 2,
            'http_headers': {
                'User-Agent': selected_ua,
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            },
            'retries': 3,
            'fragment_retries': 3
        }

        with redirect_stdout(open(os.devnull, 'w')), redirect_stderr(open(os.devnull, 'w')):
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)

        subs = info.get('automatic_captions', {});
        if lang not in subs:
            raise ValueError(f"No automatic subtitles found for language '{lang}' in metadata.")
        target_url = next((f.get('url') for f in subs[lang] if f.get('ext') == 'vtt'), None)
        if not target_url:
            raise ValueError(f"VTT format not found for automatic subtitles ('{lang}').")
        request_result = _execute_request(target_url)
        if request_result['status'] == 'failure':
            raise ConnectionError("Failed to download VTT file via URL.")
        text = _parse_vtt(request_result['content'])
        log.update({"status": "success", "final_method": "ytdlp_scout_auto", "data": text})
        log["attempts"]["ytdlp_scout_auto"] = {"status": "success"};
        return log
    except Exception as e:
        _force_print_yt_dlp_error(e, f"YouTube Transcript Scout - {url}")

        error_details = {
            "status": "failure",
            "error_type": type(e).__name__,
            "error_message": str(e),
            "full_traceback": traceback.format_exc()
        }
        if "403" in str(e) or "Forbidden" in str(e):
            error_details["likely_cause"] = "YouTube blocking subtitle download - video may not have auto-generated subtitles or access is restricted"
            error_details["response_status_code"] = 403
        elif "429" in str(e) or "Too Many Requests" in str(e):
            error_details["likely_cause"] = "Rate limited by YouTube - wait before retrying"
            error_details["response_status_code"] = 429
        log["attempts"]["ytdlp_scout_auto"] = error_details

    log.update({"status": "failure", "final_method": "all_methods_failed"});
    return log


def _get_next_pid(target_dir: Path) -> str:
    target_dir.mkdir(parents=True, exist_ok=True)
    pids = [int(m.group(1)) for f in target_dir.glob('PID????_*.json') if (m := re.search(r'PID(\d{4})', f.name))]
    return f"{(max(pids) if pids else 0) + 1:04d}"


def _print_detailed_error_to_terminal(log_callback: callable, error_log: dict, context: str):
    print(f"\n{'='*80}")
    print(f"🚨 DETAILED ERROR INFORMATION - {context}")
    print(f"{'='*80}")
    print(f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if "error_type" in error_log:
        print(f"🔍 Error type: {error_log['error_type']}")

    if "response_status_code" in error_log:
        print(f"📊 HTTP status code: {error_log['response_status_code']}")

    if "error_message" in error_log:
        print(f"💬 Error message: {error_log['error_message']}")

    if "likely_cause" in error_log:
        print(f"🎯 Likely cause: {error_log['likely_cause']}")

    if "response_headers" in error_log and error_log["response_headers"]:
        print(f"📋 Server response headers:")
        for key, value in error_log["response_headers"].items():
            print(f"  {key}: {value}")

    if "error_content" in error_log and error_log["error_content"]:
        print(f"📄 Server error content (first 500 characters):")
        print(f"  {error_log['error_content'][:500]}")

    if "url_used" in error_log:
        print(f"🔗 Request URL: {error_log['url_used']}")

    if "request_headers" in error_log:
        print(f"📨 Request headers:")
        for key, value in error_log["request_headers"].items():
            print(f"  {key}: {value}")

    if "full_traceback" in error_log:
        print(f"🔧 Full traceback (last 1000 characters):")
        traceback_text = error_log["full_traceback"]
        print(f"  {traceback_text[-1000:]}")

    print(f"{'='*80}\n")

    log_callback(f"🚨 Detailed error information printed to terminal", is_error=True)


def _force_print_yt_dlp_error(error: Exception, context: str):
    print(f"\n{'='*80}")
    print(f"🚨 YOUTUBE-DLP ERROR - {context}")
    print(f"{'='*80}")
    print(f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔍 Error type: {type(error).__name__}")
    print(f"💬 Message: {str(error)}")

    error_str = str(error)
    if "403" in error_str or "Forbidden" in error_str:
        print(f"📊 HTTP Status: 403 Forbidden")
        print(f"🎯 Likely cause: YouTube is blocking the request. Try another User-Agent or wait before retrying.")
    elif "429" in error_str or "Too Many Requests" in error_str:
        print(f"📊 HTTP Status: 429 Too Many Requests")
        print(f"🎯 Likely cause: Exceeded request limit to YouTube. Wait before retrying.")
    elif "format is not available" in error_str:
        print(f"📊 Format error")
        print(f"🎯 Likely cause: Video format unavailable. The video might be private, deleted, or blocked in your region.")

    print(f"🔧 Traceback: {traceback.format_exc()}")
    print(f"{'='*80}\n")


def process_import_task(url_list: list[str], target_dir: Path, user_pid: str | None, log_callback: callable,
                        status_callback: callable, lang: str = "en"):
    session_log = {"session_start_time": datetime.now().isoformat(), "tasks": []}
    start_pid_num = int(user_pid) if user_pid else int(_get_next_pid(target_dir))
    total_urls = len(url_list)
    for i, url in enumerate(url_list):
        current_pid_str = user_pid if user_pid else f"{start_pid_num + i:04d}"
        task_log = {"url_received": url, "pid": current_pid_str, "status": "processing", "trace": {}}
        log_callback(f"({i + 1}/{total_urls}) Starting PID {current_pid_str} for: {url}")

        status_callback(f"({i + 1}/{total_urls}) Loading page...")
        html_result = _execute_request(url)
        task_log["trace"]["1_html_download"] = html_result['log']

        if html_result['status'] == 'failure':
            _print_detailed_error_to_terminal(log_callback, html_result['log'], f"HTML Download - {url}")

        status_callback(f"({i + 1}/{total_urls}) Extracting metadata...")
        metadata_result = _fetch_video_metadata(url, html_result['content'])
        task_log["trace"]["2_metadata_extraction"] = {k: v for k, v in metadata_result.items() if k != 'data'}
        if metadata_result['status'] == 'failure':
            error_msg = f"ERROR: PID {current_pid_str}: Failed to extract metadata."
            if 'yt-dlp' in metadata_result.get('attempts', {}):
                yt_dlp_error = metadata_result['attempts']['yt-dlp']
                if 'likely_cause' in yt_dlp_error:
                    error_msg += f"\nLikely cause: {yt_dlp_error['likely_cause']}"
                if 'error_message' in yt_dlp_error:
                    error_msg += f"\nError details: {yt_dlp_error['error_message']}"

                try:
                    class MockError(Exception):
                        def __init__(self, message):
                            super().__init__(message)

                    mock_error = MockError(yt_dlp_error.get('error_message', 'Unknown yt-dlp error'))
                    _force_print_yt_dlp_error(mock_error, f"Metadata Extraction - {url}")
                except:
                    _print_detailed_error_to_terminal(log_callback, yt_dlp_error, f"Metadata Extraction - {url}")

            log_callback(error_msg, is_error=True)
            status_callback(f"({i + 1}/{total_urls}) Metadata error")
            task_log['status'] = 'failure';
            session_log['tasks'].append(task_log);
            continue
        metadata = metadata_result['data']
        log_callback(f"-> PID {current_pid_str}: Metadata retrieved using '{metadata_result['final_method']}' method.")

        status_callback(f"({i + 1}/{total_urls}) Extracting transcript... (Method: yt-dlp with automatic subtitles)")
        status_callback(f"({i + 1}/{total_urls}) Preparing yt-dlp for transcript...")
        transcript_result = _get_transcript_text(url, lang)
        status_callback(f"({i + 1}/{total_urls}) Transcript extracted (length: {len(transcript_result.get('data', '')) if transcript_result.get('status') == 'success' else 'error'} characters)")
        task_log["trace"]["3_transcript_extraction"] = {k: v for k, v in transcript_result.items() if k != 'data'}
        if transcript_result['status'] == 'failure':
            status_callback(f"({i + 1}/{total_urls}) Error extracting transcript. Analyzing details...")
            error_msg = f"ERROR: PID {current_pid_str}: Failed to extract transcript. Final method: {transcript_result.get('final_method', 'unknown')}"
            for method in ['ytdlp_force_download', 'ytdlp_scout_auto']:
                if method in transcript_result.get('attempts', {}):
                    method_error = transcript_result['attempts'][method]
                    status_callback(f"({i + 1}/{total_urls}) Error details in method {method}: {method_error.get('error_message', 'unknown')}")
                    if 'likely_cause' in method_error:
                        error_msg += f"\nLikely cause: {method_error['likely_cause']}"
                    if 'error_message' in method_error:
                        error_msg += f"\nError details: {method_error['error_message']}"

                    try:
                        class MockError(Exception):
                            def __init__(self, message):
                                super().__init__(message)

                        mock_error = MockError(method_error.get('error_message', 'Unknown transcript error'))
                        _force_print_yt_dlp_error(mock_error, f"Transcript Extraction - {url}")
                    except:
                        _print_detailed_error_to_terminal(log_callback, method_error, f"Transcript Extraction - {url}")
                    break
            log_callback(error_msg, is_error=True)
            status_callback(f"({i + 1}/{total_urls}) Transcript error")
            task_log['status'] = 'failure';
            session_log['tasks'].append(task_log);
            continue
        transcript = transcript_result['data']
        log_callback(f"-> PID {current_pid_str}: Transcript retrieved using '{transcript_result['final_method']}' method.")

        status_callback(f"({i + 1}/{total_urls}) Processing and saving...")
        json_data = {"PID": current_pid_str, "url": url, **metadata}
        char_count = len(transcript);
        word_count = len(transcript.split())
        duration_str = metadata.get("duration", "0");
        parts = list(map(int, duration_str.split(':')));
        total_seconds = 0
        if len(parts) == 3:
            total_seconds = parts[0] * 3600 + parts[1] * 60 + parts[2]
        elif len(parts) == 2:
            total_seconds = parts[0] * 60 + parts[1]
        elif len(parts) == 1:
            total_seconds = parts[0]
        total_minutes = total_seconds / 60 if total_seconds > 0 else 0
        words_per_minute = round(word_count / total_minutes, 2) if total_minutes > 0 else 0
        chars_per_minute = round(char_count / total_minutes, 2) if total_minutes > 0 else 0
        minutes_per_1k_chars = round(total_minutes / (char_count / 1000), 2) if char_count > 0 else 0
        minutes_per_1k_words = round(total_minutes / (word_count / 1000), 2) if word_count > 0 else 0
        json_data.update(
            {"char_count_with_spaces": char_count, "word_count": word_count, "words_per_minute": words_per_minute,
             "chars_per_minute": chars_per_minute, "minutes_per_1k_chars": minutes_per_1k_chars,
             "minutes_per_1k_words": minutes_per_1k_words})

        txt_filename = f"PID{current_pid_str}_transcript.txt";
        json_filename = f"PID{current_pid_str}_transcript.json"
        txt_filepath = target_dir / txt_filename;
        json_filepath = target_dir / json_filename
        try:
            txt_filepath.write_text(transcript, encoding='utf-8')
            json_filepath.write_text(json.dumps(json_data, ensure_ascii=False, indent=4), encoding='utf-8')
            task_log["trace"]["4_file_save"] = {"status": "success", "json_path": str(json_filepath),
                                                "txt_path": str(txt_filepath)}
            task_log["status"] = "success"
            log_callback(f"SUCCESS: PID {current_pid_str}: Files saved.")

            if 'thumbnail_url' in json_data and json_data['thumbnail_url']:
                try:
                    status_callback(f"({i + 1}/{total_urls}) Downloading thumbnail image... URL: {json_data['thumbnail_url'][:50]}...")
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                        'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8'
                    }
                    response = requests.get(json_data['thumbnail_url'], headers=headers, timeout=15)
                    response.raise_for_status()
                    content_type = response.headers.get('content-type', '').lower()
                    extension = '.jpg' if 'jpeg' in content_type else '.png' if 'png' in content_type else '.jpg'
                    thumbnail_filename = f"{current_pid_str}_source_thumbnail{extension}"
                    thumbnail_dir = target_dir / "source_thumbnails"
                    thumbnail_dir.mkdir(parents=True, exist_ok=True)
                    thumbnail_filepath = thumbnail_dir / thumbnail_filename
                    with open(thumbnail_filepath, 'wb') as f:
                        f.write(response.content)
                    log_callback(f"SUCCESS: PID {current_pid_str}: Thumbnail image saved to {thumbnail_filepath}")
                    task_log["trace"]["5_thumbnail_download"] = {"status": "success", "path": str(thumbnail_filepath)}
                except Exception as e:
                    log_callback(f"ERROR: PID {current_pid_str}: Failed to download thumbnail image: {e}", is_error=True)
                    task_log["trace"]["5_thumbnail_download"] = {"status": "failure", "error": str(e)}
            else:
                log_callback(f"WARNING: PID {current_pid_str}: Thumbnail URL not found in metadata.")
                task_log["trace"]["5_thumbnail_download"] = {"status": "skipped", "reason": "No thumbnail URL"}

        except Exception:
            task_log["trace"]["4_file_save"] = {"status": "failure", "error": traceback.format_exc()}
            task_log["status"] = "failure"
            log_callback(f"ERROR: PID {current_pid_str}: Failed to save files.", is_error=True)
            status_callback(f"({i + 1}/{total_urls}) Saving error")
        session_log['tasks'].append(task_log)

    status_callback("All tasks completed.")
    try:
        log_dir = target_dir.parent / "database";
        log_dir.mkdir(exist_ok=True)
        log_file_path = log_dir / "import_extractor_worker_v2.log.json"
        log_file_path.write_text(json.dumps(session_log, indent=4, ensure_ascii=False), encoding='utf-8')
        log_callback(f"Session report saved to {log_file_path}")
    except Exception as e:
        log_callback(f"Failed to save JSON session log: {e}", is_error=True)
    log_callback("-" * 20 + "\nAll import tasks completed.")