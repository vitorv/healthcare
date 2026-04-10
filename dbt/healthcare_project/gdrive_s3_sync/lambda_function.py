import json
import os
import boto3
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Environment Variables mapping
S3_BUCKET = os.environ['S3_BUCKET_NAME']
GDRIVE_FOLDER_ID = os.environ['GDRIVE_FOLDER_ID']

# Initialize S3 client
s3_client = boto3.client('s3')

def get_gdrive_service():
    # In production, fetching this JSON from AWS Secrets Manager is best practice 
    # over using an environment variable, but this simpler version uses an env var.
    creds_json = json.loads(os.environ['GDRIVE_SERVICE_ACCOUNT_JSON'])
    credentials = service_account.Credentials.from_service_account_info(creds_json)
    return build('drive', 'v3', credentials=credentials)

def lambda_handler(event, context):
    drive_service = get_gdrive_service()
    
    # 1. Search for CSV files in the specific drive folder.
    query = f"'{GDRIVE_FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false"
    results = drive_service.files().list(q=query, fields="nextPageToken, files(id, name)").execute()
    files = results.get('files', [])

    if not files:
        print('No CSV files found in Drive folder.')
        return {'statusCode': 200, 'body': 'No files found.'}

    for file in files:
        file_id = file['id']
        file_name = file['name']
        print(f"Evaluating {file_name}...")

        # 2. Check if file already exists in S3
        try:
            s3_client.head_object(Bucket=S3_BUCKET, Key=f"raw_data/{file_name}")
            print(f"{file_name} already exists in S3. Skipping.")
            continue
        except s3_client.exceptions.ClientError:
            pass # File doesn't exist in S3, proceed to upload

        # 3. Download the file from Google Drive into memory
        print(f"Downloading {file_name} from Drive...")
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()

        # 4. Upload file's memory stream to Amazon S3
        fh.seek(0)
        s3_client.upload_fileobj(fh, S3_BUCKET, f"raw_data/{file_name}")
        print(f"Successfully uploaded {file_name} to S3!")

    return {
        'statusCode': 200,
        'body': json.dumps('Sync completed successfully!')
    }
