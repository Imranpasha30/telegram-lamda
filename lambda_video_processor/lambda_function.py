import json
import boto3
import os
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import tempfile
import requests

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

def convert_database_url(database_url):
    """Convert SQLAlchemy URL to psycopg2 format"""
    if database_url.startswith('postgresql+asyncpg://'):
        return database_url.replace('postgresql+asyncpg://', 'postgresql://')
    elif database_url.startswith('postgresql://'):
        return database_url
    else:
        raise ValueError(f"Unsupported database URL format: {database_url}")

def lambda_handler(event, context):
    """Main Lambda handler for video processing"""
    try:
        logger.info("=== VIDEO PROCESSOR STARTED ===")
        logger.info(f"Event: {json.dumps(event, default=str)}")
        
        # Extract parameters from Function 1 trigger
        submission_id = event['submission_id']
        volunteer_id = event['volunteer_id']
        s3_key = event['s3_key']
        video_title = event.get('video_title', f'Video from user {volunteer_id}')
        
        logger.info(f"Processing video: {submission_id} | S3: {s3_key}")
        
        # Process the video
        result = process_video_submission(submission_id, volunteer_id, s3_key, video_title)
        
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'result': result
            })
        }
        logger.info(f"✅ Video processing completed: {response}")
        return response
        
    except Exception as e:
        logger.error(f"❌ Video processing failed: {str(e)}", exc_info=True)
        
        # Update submission status to failed in database
        if 'submission_id' in event:
            try:
                update_submission_status(event['submission_id'], 'DECLINED', f"Processing failed: {str(e)}")
            except Exception as db_error:
                logger.error(f"Failed to update database: {str(db_error)}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'message': str(e),
                'submission_id': event.get('submission_id', 'unknown')
            })
        }

def process_video_submission(submission_id: str, volunteer_id: str, s3_key: str, video_title: str):
    """Main video processing logic"""
    temp_file_path = None
    
    try:
        # Step 1: Verify submission exists and get current status
        submission_data = get_submission_data(submission_id)
        if not submission_data:
            raise Exception(f"Submission {submission_id} not found in database")
        
        logger.info(f"📋 Submission verified: {submission_data['status']} | Volunteer: {submission_data['volunteer_id']}")
        
        # Step 2: Validate data integrity
        if submission_data['volunteer_id'] != volunteer_id:
            raise Exception(f"Data mismatch: Expected volunteer {volunteer_id}, got {submission_data['volunteer_id']}")
        
        # Step 3: Download video from S3 to temp file
        logger.info(f"⬇️ Downloading video from S3: {s3_key}")
        temp_file_path = download_video_from_s3(s3_key)
        logger.info(f"✅ Video downloaded to temp file: {temp_file_path}")
        
        # Step 4: Upload video to api.video
        logger.info(f"☁️ Uploading video to api.video: {video_title}")
        api_video_url = upload_to_api_video(temp_file_path, video_title)
        logger.info(f"✅ Video uploaded to api.video: {api_video_url}")
        
        # Step 5: Update database with video URL
        logger.info(f"💾 Updating database with video URL")
        update_result = update_submission_with_video_url(submission_id, api_video_url, volunteer_id)
        logger.info(f"✅ Database updated successfully")
        
        # Step 6: Clean up S3 temp file
        logger.info(f"🧹 Cleaning up S3 temp file: {s3_key}")
        cleanup_s3_file(s3_key)
        
        # Step 7: Trigger response handler (Function 3)
        logger.info(f"📤 Triggering response handler")
        trigger_response_handler(submission_id, volunteer_id, 'success', 
                               'Your video has been processed and is now under review by our team!')
        
        return {
            'submission_id': submission_id,
            'volunteer_id': volunteer_id,
            'api_video_url': api_video_url,
            'status': 'completed',
            'message': 'Video successfully processed and uploaded to api.video'
        }
        
    except Exception as e:
        logger.error(f"❌ Error in process_video_submission: {str(e)}")
        # Update submission to failed status
        update_submission_status(submission_id, 'DECLINED', str(e))
        
        # Trigger failure response to user
        trigger_response_handler(submission_id, volunteer_id, 'error', 
                               'Sorry, there was an error processing your video. Please try again.')
        raise
        
    finally:
        # Clean up temp file
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            logger.info(f"🧹 Cleaned up temp file: {temp_file_path}")

def get_submission_data(submission_id: str) -> dict:
    """Get submission data from database with data integrity check"""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise Exception("DATABASE_URL environment variable not set")
    
    psycopg2_url = convert_database_url(database_url)
    conn = psycopg2.connect(psycopg2_url)
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get submission with volunteer data for verification
            cur.execute("""
                SELECT vs.*, v.first_name, v.last_name, v.username 
                FROM video_submissions vs
                JOIN volunteers v ON vs.volunteer_id = v.id
                WHERE vs.id = %s
            """, (submission_id,))
            
            submission = cur.fetchone()
            if not submission:
                return None
            
            return dict(submission)
            
    finally:
        conn.close()

def download_video_from_s3(s3_key: str) -> str:
    """Download video from S3 to temporary file"""
    s3_bucket = os.environ.get('S3_BUCKET_NAME')
    if not s3_bucket:
        raise Exception("S3_BUCKET_NAME environment variable not set")
    
    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
    temp_file_path = temp_file.name
    temp_file.close()
    
    try:
        # Download from S3
        s3_client.download_file(s3_bucket, s3_key, temp_file_path)
        
        # Verify file was downloaded
        file_size = os.path.getsize(temp_file_path)
        if file_size == 0:
            raise Exception("Downloaded file is empty")
        
        logger.info(f"📁 Downloaded S3 file: {s3_key} ({file_size:,} bytes)")
        return temp_file_path
        
    except Exception as e:
        # Clean up on error
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        raise Exception(f"Failed to download from S3: {str(e)}")

def upload_to_api_video(file_path: str, video_title: str) -> str:
    """Upload video to api.video platform using HTTP API"""
    api_video_key = os.environ.get('API_VIDEO_KEY')
    if not api_video_key:
        raise Exception("API_VIDEO_KEY environment variable not set")
    
    try:
        # Step 1: Create video container
        create_url = "https://ws.api.video/videos"
        headers = {
            "Authorization": f"Bearer {api_video_key}",
            "Content-Type": "application/json"
        }
        
        video_payload = {
            "title": video_title,
            "description": f"Video submission processed at {datetime.utcnow().isoformat()}",
            "public": False,
            "tags": ["telegram-submission", "community-video"]
        }
        
        logger.info(f"🎬 Creating video container: {video_title}")
        response = requests.post(create_url, json=video_payload, headers=headers, timeout=30)
        response.raise_for_status()
        
        video_data = response.json()
        video_id = video_data.get('videoId')
        
        if not video_id:
            raise Exception(f"Failed to get videoId from response: {video_data}")
        
        logger.info(f"📹 Video container created: {video_id}")
        
        # Step 2: Upload video file
        upload_url = f"https://ws.api.video/videos/{video_id}/source"
        
        logger.info(f"⬆️ Uploading video file...")
        
        with open(file_path, 'rb') as video_file:
            files = {'file': video_file}
            upload_headers = {
                "Authorization": f"Bearer {api_video_key}"
            }
            
            upload_response = requests.post(upload_url, files=files, headers=upload_headers, timeout=300)
            upload_response.raise_for_status()
        
        upload_data = upload_response.json()
        
        # Get player URL
        player_url = upload_data.get('assets', {}).get('player')
        if not player_url:
            # Fallback - construct player URL
            player_url = f"https://embed.api.video/vod/{video_id}"
        
        logger.info(f"✅ Video uploaded successfully: {player_url}")
        return player_url
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ HTTP error uploading to api.video: {str(e)}")
        raise Exception(f"api.video HTTP upload failed: {str(e)}")
    except Exception as e:
        logger.error(f"❌ Failed to upload to api.video: {str(e)}")
        raise Exception(f"api.video upload failed: {str(e)}")

def update_submission_with_video_url(submission_id: str, video_url: str, volunteer_id: str) -> dict:
    """Update database with video URL and ensure data integrity"""
    database_url = os.environ.get('DATABASE_URL')
    psycopg2_url = convert_database_url(database_url)
    conn = psycopg2.connect(psycopg2_url)
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Update with data integrity check
            cur.execute("""
                UPDATE video_submissions 
                SET 
                    video_platform_url = %s,
                    status = 'PENDING_REVIEW',
                    updated_at = %s
                WHERE id = %s AND volunteer_id = %s
                RETURNING id, volunteer_id, status
            """, (video_url, datetime.utcnow(), submission_id, volunteer_id))
            
            updated_record = cur.fetchone()
            if not updated_record:
                raise Exception(f"Failed to update submission {submission_id} - record not found or volunteer mismatch")
            
            conn.commit()
            
            logger.info(f"💾 Updated submission: {updated_record['id']} | Status: {updated_record['status']}")
            return dict(updated_record)
            
    finally:
        conn.close()

def update_submission_status(submission_id: str, status: str, reason: str = None):
    """Update submission status (for error handling)"""
    try:
        database_url = os.environ.get('DATABASE_URL')
        psycopg2_url = convert_database_url(database_url)
        conn = psycopg2.connect(psycopg2_url)
        
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE video_submissions 
                    SET status = %s, decline_reason = %s, updated_at = %s
                    WHERE id = %s
                """, (status, reason, datetime.utcnow(), submission_id))
                conn.commit()
                
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Failed to update submission status: {str(e)}")

def cleanup_s3_file(s3_key: str):
    """Delete temporary file from S3"""
    try:
        s3_bucket = os.environ.get('S3_BUCKET_NAME')
        if s3_bucket:
            s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
            logger.info(f"🗑️ Deleted S3 temp file: {s3_key}")
    except Exception as e:
        logger.warning(f"Failed to cleanup S3 file {s3_key}: {str(e)}")

def trigger_response_handler(submission_id: str, volunteer_id: str, status: str, message: str):
    """Trigger Function 3 (Response Handler)"""
    try:
        function_name = os.environ.get('RESPONSE_HANDLER_FUNCTION_NAME')
        if not function_name:
            logger.info("RESPONSE_HANDLER_FUNCTION_NAME not set, skipping response")
            return
        
        payload = {
            'submission_id': submission_id,
            'volunteer_id': volunteer_id,
            'status': status,
            'message': message
        }
        
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',  # Async
            Payload=json.dumps(payload)
        )
        
        logger.info(f"📤 Triggered response handler: {function_name}")
        
    except Exception as e:
        logger.error(f"Failed to trigger response handler: {str(e)}")
