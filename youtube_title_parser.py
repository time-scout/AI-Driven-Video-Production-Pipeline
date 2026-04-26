# youtube_title_parser.py (v2 - with thumbnail URL addition)

import yt_dlp
from datetime import datetime


def fetch_video_metadata(video_url: str) -> dict:
    """
    Extracts video metadata from YouTube using yt-dlp, including thumbnail URL.

    Args:
        video_url: Full URL or Video ID.

    Returns:
        Dictionary with data or dictionary with error.
        Example of successful result:
        {
            'success': True,
            'title': '...',
            'channel': '...',
            'video_id': '...',
            'thumbnail_maxres_url': 'https://.../maxresdefault.jpg',
            'thumbnail_hq_url': 'https://.../hqdefault.jpg',
            ...
        }
    """
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'skip_download': True,
        'force_generic_extractor': False,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = ydl.extract_info(video_url, download=False)

            if 'entries' in info_dict:
                info_dict = info_dict['entries'][0]

            video_id = info_dict.get('id')

            # Collect required data
            metadata = {
                'success': True,
                'title': info_dict.get('title'),
                'description': info_dict.get('description'),
                'channel': info_dict.get('channel'),
                'view_count': info_dict.get('view_count'),
                'upload_date': info_dict.get('upload_date'),
                'duration': info_dict.get('duration'),
                'video_id': video_id,
                # --- NEW: Form preview URL ---
                'thumbnail_maxres_url': f"https://img.youtube.com/vi/{video_id}/maxresdefault.jpg",
                'thumbnail_hq_url': f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg",
            }

            if metadata['upload_date']:
                try:
                    dt_object = datetime.strptime(metadata['upload_date'], '%Y%m%d')
                    metadata['upload_date'] = dt_object.strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    pass

            return metadata

    except yt_dlp.utils.DownloadError as e:
        error_message = str(e)
        if "is not a valid URL" in error_message:
            return {'success': False, 'error': f"Invalid URL or Video ID: {video_url}"}
        if "Video unavailable" in error_message:
            return {'success': False, 'error': "Video unavailable (deleted or private)."}

        simplified_error = str(e).split('\n')[0]
        return {'success': False, 'error': f"Data loading error: {simplified_error}"}

    except Exception as e:
        return {'success': False, 'error': f"An unexpected error occurred: {e}"}


# --- Usage example for verification ---
if __name__ == '__main__':
    test_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    print(f"--- Testing URL: {test_url} ---")
    data = fetch_video_metadata(test_url)
    if data['success']:
        for key, value in data.items():
            if key != 'description':
                print(f"{key}: {value}")
            else:
                print(f"{key}: {str(value)[:100]}...")

        print("\nThumbnail URL verification:")
        print(f"Max-Res: {data['thumbnail_maxres_url']}")
        print(f"HQ: {data['thumbnail_hq_url']}")
    else:
        print(f"Error: {data['error']}")