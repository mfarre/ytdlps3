import os
import sys
import boto3
from yt_dlp import YoutubeDL

def download_youtube_video(video_id, output_path):
    ydl_opts = {
        'format': 'best',
        'writesubtitles': True,
        'subtitleslangs': ['en'],
        'subtitlesformat': 'vtt',
        'writeinfojson': True,
        'skip_download': False,
        'outtmpl': os.path.join(output_path, f'{video_id}.%(ext)s'),
    }
    with YoutubeDL(ydl_opts) as ydl:
        info_dict = ydl.extract_info(video_id, download=True)
        
        # Check if English subtitles are available
        subtitles = info_dict.get('subtitles')
        if subtitles is None or 'en' not in subtitles:
            subtitle_file = os.path.join(output_path, f'{video_id}.vtt')
            if os.path.exists(subtitle_file):
                os.remove(subtitle_file)

def upload_to_s3(local_file_path, s3_bucket, s3_key):
    s3_client = boto3.client('s3')
    s3_client.upload_file(local_file_path, s3_bucket, s3_key)

def main(video_id, s3_bucket, s3_path):
    # Create a temporary directory to store downloaded files
    download_path = '/tmp/youtube_downloads'
    os.makedirs(download_path, exist_ok=True)

    # Download the video, subtitles (if available), and metadata
    download_youtube_video(video_id, download_path)

    # Define file paths
    video_file = os.path.join(download_path, f'{video_id}.mp4')
    metadata_file = os.path.join(download_path, f'{video_id}.info.json')
    subtitle_file = os.path.join(download_path, f'{video_id}.vtt')

    # Upload each file to the specified S3 path if it exists
    if os.path.exists(video_file):
        upload_to_s3(video_file, s3_bucket, os.path.join(s3_path, f'{video_id}.mp4'))
    if os.path.exists(metadata_file):
        upload_to_s3(metadata_file, s3_bucket, os.path.join(s3_path, f'{video_id}.json'))
    if os.path.exists(subtitle_file):
        upload_to_s3(subtitle_file, s3_bucket, os.path.join(s3_path, f'{video_id}.vtt'))

    # Cleanup
    for file_name in os.listdir(download_path):
        os.remove(os.path.join(download_path, file_name))

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python download_and_upload.py <youtube_video_id> <s3_bucket_name> <s3_path>")
        sys.exit(1)

    video_id = sys.argv[1]
    s3_bucket = sys.argv[2]
    s3_path = sys.argv[3]

    main(video_id, s3_bucket, s3_path)
