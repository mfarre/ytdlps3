import boto3
import os

def list_mp4_files_in_s3(bucket_name, prefix):
    s3_client = boto3.client('s3')
    continuation_token = None
    mp4_files = set()

    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                ContinuationToken=continuation_token
            )
        else:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
        
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.mp4'):
                    video_id = os.path.basename(obj['Key']).replace('.mp4', '')
                    mp4_files.add(video_id)
        
        if response.get('IsTruncated'):  # If there are more files to list
            continuation_token = response.get('NextContinuationToken')
        else:
            break

    return mp4_files

def list_failed_files_in_s3(bucket_name, prefix):
    s3_client = boto3.client('s3')
    continuation_token = None
    failed_files = set()

    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                ContinuationToken=continuation_token
            )
        else:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
        
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.txt'):
                    video_id = os.path.basename(obj['Key']).replace('.txt', '')
                    failed_files.add(video_id)
        
        if response.get('IsTruncated'):  # If there are more files to list
            continuation_token = response.get('NextContinuationToken')
        else:
            break

    return failed_files

def read_videoids_from_file(filename):
    with open(filename, 'r') as file:
        return [line.strip() for line in file if line.strip()]

def write_videoids_to_file(videoids, filename):
    with open(filename, 'w') as file:
        for videoid in videoids:
            file.write(f"{videoid}\n")

def generate_videoids_to_process(s3_bucket, s3_prefix, all_file, output_file, N, check_failed=False):
    # Step 1: List MP4 files already in S3
    existing_mp4_ids = list_mp4_files_in_s3(s3_bucket, s3_prefix)
    num_mp4_files = len(existing_mp4_ids)

    # Step 2: Read all video IDs from all.txt
    all_video_ids = read_videoids_from_file(all_file)
    num_all_video_ids = len(all_video_ids)

    # Step 3: Optionally list failed video downloads
    failed_prefix = "failed/"
    failed_ids = list_failed_files_in_s3(s3_bucket, failed_prefix) if check_failed else set()
    num_failed_ids = len(failed_ids)

    # Step 4: Find MP4 files that match video IDs in all.txt
    mp4_ids_in_all = existing_mp4_ids.intersection(all_video_ids)
    num_mp4_in_all = len(mp4_ids_in_all)

    # Step 5: Find MP4 files that do not match any video ID in all.txt
    non_matching_mp4_ids = existing_mp4_ids - mp4_ids_in_all

    # Step 6: Find video IDs that are not in S3 and not marked as failed (if check_failed is True)
    video_ids_to_process = [vid for vid in all_video_ids if vid not in existing_mp4_ids and (not check_failed or vid not in failed_ids)]

    # Step 7: Select video IDs based on N
    if N == -1:
        selected_video_ids = video_ids_to_process  # Select all
    else:
        selected_video_ids = video_ids_to_process[:N]  # Select up to N

    # Step 8: Write selected video IDs to videoids.txt
    write_videoids_to_file(selected_video_ids, output_file)

    # Print summary information
    print(f"Total number of .mp4 files in S3: {num_mp4_files}")
    print(f"Number of attempted videos that failed download: {num_failed_ids}")
    print(f"Number of .mp4 files in S3 that match video IDs in all.txt: {num_mp4_in_all}")
    print(f"Number of .mp4 files in S3 that do not match any video ID in all.txt: {len(non_matching_mp4_ids)}")
    if non_matching_mp4_ids:
        print("List of .mp4 files in S3 that do not match any video ID in all.txt:")
        for non_matching_id in non_matching_mp4_ids:
            print(non_matching_id)
    
    print(f"Selected {len(selected_video_ids)} video IDs for processing, written to {output_file}")

if __name__ == "__main__":
    S3_BUCKET = <your_bucket>
    S3_PREFIX = <path>
    ALL_FILE = "all.txt"
    OUTPUT_FILE = "videoids.txt"
    N = -1  # Adjust the number of video IDs you want to select, or set to -1 to select all
    CHECK_FAILED = True  # Set this to True to check for failed video IDs and exclude them from processing

    generate_videoids_to_process(S3_BUCKET, S3_PREFIX, ALL_FILE, OUTPUT_FILE, N, CHECK_FAILED)
