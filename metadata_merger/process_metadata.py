import pandas as pd
import boto3
import json
import os
from io import BytesIO

# Initialize S3 client
s3 = boto3.client('s3')

def fetch_metadata(video_id, bucket):
    """Fetch JSON metadata from S3 for a given video_id."""
    s3_key = f"videosh/{video_id}.json"
    try:
        response = s3.get_object(Bucket=bucket, Key=s3_key)
        json_data = json.loads(response['Body'].read().decode('utf-8'))
        return json_data
    except Exception as e:
        print(f"Error fetching metadata for video_id {video_id}: {e}")
        return None

def process_dataframe(input_s3_path, output_s3_path, metadata_bucket):
    """Download DataFrame from S3, process metadata, and upload result to S3."""
    # Extract bucket and key from S3 path
    input_bucket, input_key = input_s3_path.replace("s3://", "").split("/", 1)
    output_bucket, output_key = output_s3_path.replace("s3://", "").split("/", 1)

    # Download DataFrame from S3
    response = s3.get_object(Bucket=input_bucket, Key=input_key)
    df = pd.read_pickle(BytesIO(response['Body'].read()))

    # Prepare list of fields for metadata
    metadata_fields = [
        "vcodec", "acodec", "resolution", "duration_string", "title", "description",
        "categories", "tags", "channel", "view_count", "comment_count", "like_count",
        "channel_follower_count", "upload_date", "language", "age_limit"
    ]
    
    # Initialize processed DataFrame with metadata fields
    processed_data = []

    # Process each video_id in the DataFrame
    for _, row in df.iterrows():
        video_id = row['video_id']
        metadata = fetch_metadata(video_id, metadata_bucket)
        if metadata:
            data = {field: metadata.get(field, None) for field in metadata_fields}
            data['video_id'] = video_id
            processed_data.append(data)

    # Create a new DataFrame from the processed data
    processed_df = pd.DataFrame(processed_data)

    # Save processed DataFrame to S3
    with BytesIO() as f:
        processed_df.to_pickle(f)
        f.seek(0)
        s3.upload_fileobj(f, output_bucket, output_key)

    print(f"Processed DataFrame saved to {output_s3_path}")

if __name__ == "__main__":
    # Set environment variables for input/output paths
    input_s3_path = os.environ.get("INPUT_S3_PATH")
    output_s3_path = os.environ.get("OUTPUT_S3_PATH")
    metadata_bucket = os.environ.get("METADATA_BUCKET")

    if not input_s3_path or not output_s3_path or not metadata_bucket:
        raise ValueError("Environment variables INPUT_S3_PATH, OUTPUT_S3_PATH, and METADATA_BUCKET must be set")

    process_dataframe(input_s3_path, output_s3_path, metadata_bucket)
