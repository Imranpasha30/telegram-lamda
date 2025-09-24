import json
import boto3
import os
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import uuid
import requests
from botocore.exceptions import ClientError

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
    """Main Lambda handler for Telegram webhook"""
    try:
        logger.info("=== WEBHOOK RECEIVED ===")
        logger.info(f"Event: {json.dumps(event, default=str)}")
        
        # Extract webhook data from API Gateway
        if 'body' in event:
            if isinstance(event['body'], str):
                update = json.loads(event['body'])
            else:
                update = event['body']
        else:
            update = event
        
        logger.info(f"Telegram Update: {json.dumps(update, default=str)}")
        
        # Process the update
        result = process_telegram_update(update)
        
        response = {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'status': 'success', 
                'result': result
            })
        }
        logger.info(f"Response: {response}")
        return response
            
    except Exception as e:
        logger.error(f"❌ Error processing webhook: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'status': 'error', 
                'message': str(e)
            })
        }

def process_telegram_update(update):
    """Process Telegram update with registration flow"""
    try:
        message = update.get('message')
        if not message:
            logger.info("No message found in update")
            return {"status": "no_message"}
        
        # Extract user and chat information
        chat_data = message['chat']
        user_data = message.get('from', {})
        chat_id = str(chat_data['id'])
        
        logger.info(f"Processing message from chat: {chat_id}")
        
        # ===== STEP 1: HANDLE CONTACT SHARING (REGISTRATION) =====
        if 'contact' in message:
            phone_number = message['contact'].get('phone_number')
            contact_user_id = str(message['contact'].get('user_id', chat_id))
            
            logger.info(f"📱 Registration: Phone {phone_number} from user {contact_user_id}")
            
            # Complete registration with phone number
            success = complete_user_registration(contact_user_id, phone_number, chat_data, user_data)
            
            if success:
                send_registration_success_message(chat_id, user_data.get('first_name', 'there'))
                return {
                    "status": "registration_completed",
                    "phone_number": phone_number,
                    "chat_id": contact_user_id
                }
            else:
                send_registration_error_message(chat_id)
                return {"status": "registration_failed"}
        
        # ===== STEP 2: CHECK USER REGISTRATION STATUS =====
        volunteer = check_volunteer_exists(chat_id)
        
        if not volunteer:
            # NEW USER - SEND REGISTRATION REQUEST
            logger.info(f"🆕 New user detected: {chat_id}")
            send_registration_request(chat_id, user_data.get('first_name', 'there'))
            
            return {
                "status": "registration_required",
                "chat_id": chat_id,
                "message": "Registration request sent to new user"
            }
        
        # ===== STEP 3: PROCESS VIDEO (REGISTERED USERS ONLY) =====
        video_data = None
        video_type = None
        
        if 'document' in message and 'video' in message['document'].get('mime_type', ''):
            video_data = message['document']
            video_type = "document"
        elif 'video' in message:
            video_data = message['video']
            video_type = "video"
        
        if video_data:
            logger.info(f"📹 Processing video from registered user: {chat_id}")
            return process_video_from_registered_user(message, volunteer, video_data, video_type)
        
        # ===== STEP 4: HANDLE OTHER MESSAGES FROM REGISTERED USERS =====
        if message.get('text'):
            send_help_message_to_registered_user(chat_id, volunteer.get('first_name', 'there'))
            return {"status": "help_sent"}
        
        return {"status": "unhandled_message"}
        
    except Exception as e:
        logger.error(f"❌ Error in process_telegram_update: {str(e)}", exc_info=True)
        raise

def check_volunteer_exists(chat_id: str) -> dict:
    """Check if volunteer exists in database"""
    try:
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            return None
        
        psycopg2_url = convert_database_url(database_url)
        conn = psycopg2.connect(psycopg2_url)
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM volunteers WHERE id = %s", (chat_id,))
                volunteer = cur.fetchone()
                
                if volunteer:
                    logger.info(f"✅ Registered user found: {chat_id}")
                    return dict(volunteer)
                else:
                    logger.info(f"❌ User not registered: {chat_id}")
                    return None
                    
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Error checking volunteer: {str(e)}")
        return None

def send_registration_request(chat_id: str, first_name: str):
    """Send registration request template with phone button"""
    telegram_token = os.environ.get('TELEGRAM_BOT_TOKEN')
    if not telegram_token:
        return
    
    try:
        send_url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        
        # Custom keyboard with registration button
        keyboard = {
            "keyboard": [
                [
                    {
                        "text": "📱 Complete Registration",
                        "request_contact": True
                    }
                ]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        
        message = f"""🎬 **Welcome to Video Processing Bot!**

Hi {first_name}! 👋

To start using this bot, please complete your registration first.

**📋 Registration Required:**
• Share your contact information
• Verify your phone number  
• Create your account

**🔐 Why Registration?**
• Secure video processing
• Track your submissions
• Account management

**👇 Click the button below to complete registration:**

Once registered, you can start sharing videos immediately! 🎥"""

        payload = {
            "chat_id": chat_id,
            "text": message,
            "reply_markup": keyboard,
            "parse_mode": "Markdown"
        }
        
        response = requests.post(send_url, json=payload, timeout=10)
        logger.info(f"📋 Registration request sent to: {chat_id}")
        
    except Exception as e:
        logger.error(f"Failed to send registration request: {str(e)}")

def complete_user_registration(chat_id: str, phone_number: str, chat_data: dict, user_data: dict) -> bool:
    """Complete user registration with phone number"""
    try:
        database_url = os.environ.get('DATABASE_URL')
        psycopg2_url = convert_database_url(database_url)
        conn = psycopg2.connect(psycopg2_url)
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                
                # Create new volunteer with phone number (REGISTRATION)
                cur.execute("""
                    INSERT INTO volunteers (id, first_name, last_name, username, phone_number, phone_verified, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        phone_number = EXCLUDED.phone_number,
                        phone_verified = EXCLUDED.phone_verified,
                        updated_at = EXCLUDED.updated_at
                """, (
                    chat_id,
                    chat_data.get('first_name') or user_data.get('first_name'),
                    chat_data.get('last_name') or user_data.get('last_name'), 
                    chat_data.get('username') or user_data.get('username'),
                    phone_number,
                    True,
                    datetime.utcnow(),
                    datetime.utcnow()
                ))
                
                conn.commit()
                logger.info(f"✅ Registration completed: {chat_id} | Phone: {phone_number}")
                return True
                
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Registration failed for {chat_id}: {str(e)}")
        return False

def send_registration_success_message(chat_id: str, first_name: str):
    """Send registration success confirmation"""
    message = f"""✅ **Registration Completed!**

Congratulations {first_name}! 🎉

Your account has been successfully created and your phone number is verified.

**🎥 You can now start using the bot:**
• Send video files for processing
• Get instant notifications  
• Track your submissions

**Ready to start?** Just send me a video! 📹"""

    send_simple_message(chat_id, message)

def send_registration_error_message(chat_id: str):
    """Send registration error message"""
    message = """❌ **Registration Failed**

Sorry, there was an error completing your registration.

Please try again by clicking the registration button, or contact support if the problem persists.

Thank you for your patience! 🙏"""

    send_simple_message(chat_id, message)

def send_help_message_to_registered_user(chat_id: str, first_name: str):
    """Send help message to registered users"""
    message = f"""Hi {first_name}! 👋

✅ **You are registered and ready to go!**

🎥 **How to use the bot:**
• Send me any video file
• I'll process it automatically
• You'll get instant notifications
• Track all your submissions

📹 **Supported formats:** MP4, MOV, AVI and more

Just send your video now! 🎬"""

    send_simple_message(chat_id, message)

def process_video_from_registered_user(message: dict, volunteer: dict, video_data: dict, video_type: str):
    """Process video from registered user (existing flow)"""
    try:
        chat_id = volunteer['id']
        
        # Connect to database
        database_url = os.environ.get('DATABASE_URL')
        psycopg2_url = convert_database_url(database_url)
        conn = psycopg2.connect(psycopg2_url)
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                
                # Check for duplicate submission
                cur.execute(
                    "SELECT id FROM video_submissions WHERE telegram_file_id = %s", 
                    (video_data['file_id'],)
                )
                existing = cur.fetchone()
                
                if existing:
                    logger.warning(f"⚠️ Duplicate submission: {video_data['file_id']}")
                    return {"status": "duplicate", "submission_id": str(existing['id'])}
                
                # Generate UUID and create submission
                submission_uuid = str(uuid.uuid4())
                logger.info(f"Generated UUID: {submission_uuid}")
                
                cur.execute("""
                    INSERT INTO video_submissions 
                    (id, volunteer_id, telegram_file_id, status, created_at, updated_at)
                    VALUES (%s, %s, %s, 'PROCESSING', %s, %s)
                    RETURNING id
                """, (submission_uuid, chat_id, video_data['file_id'], datetime.utcnow(), datetime.utcnow()))
                
                submission_record = cur.fetchone()
                submission_id = submission_record['id']
                conn.commit()
                
                logger.info(f"✅ Created submission: {submission_id} for registered user: {chat_id}")
                
                # Process video (existing S3 + Function 2 flow)
                s3_key = download_video_to_s3(video_data['file_id'], submission_id)
                logger.info(f"✅ Video uploaded to S3: {s3_key}")
                
                # Trigger Function 2 (Video Processor)
                video_processor_function = os.environ.get('VIDEO_PROCESSOR_FUNCTION_NAME')
                if video_processor_function:
                    trigger_video_processor(submission_id, chat_id, s3_key, message['chat'])
                    logger.info(f"✅ Triggered video processor")
                
                return {
                    "status": "success",
                    "submission_id": str(submission_id),
                    "volunteer_id": chat_id,
                    "phone_verified": volunteer['phone_verified'],
                    "phone_number": volunteer['phone_number'],
                    "s3_key": s3_key,
                    "message": "Video processing started for registered user"
                }
                
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"❌ Error processing video from registered user: {str(e)}")
        raise

def send_simple_message(chat_id: str, text: str):
    """Send simple message with keyboard removal"""
    telegram_token = os.environ.get('TELEGRAM_BOT_TOKEN')
    if not telegram_token:
        return
    
    try:
        send_url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "reply_markup": {"remove_keyboard": True}
        }
        
        requests.post(send_url, json=payload, timeout=10)
        
    except Exception as e:
        logger.error(f"Failed to send message: {str(e)}")

# Keep existing functions: download_video_to_s3, trigger_video_processor (same as before)
def download_video_to_s3(file_id: str, submission_id: str) -> str:
    """Download video from Telegram and upload to S3"""
    try:
        telegram_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        s3_bucket = os.environ.get('S3_BUCKET_NAME')
        
        if not telegram_token or not s3_bucket:
            raise Exception("Missing environment variables")
        
        logger.info(f"🔍 Getting file info for: {file_id}")
        
        # Get file info from Telegram
        api_url = f"https://api.telegram.org/bot{telegram_token}/getFile"
        response = requests.post(api_url, json={"file_id": file_id}, timeout=30)
        response.raise_for_status()
        
        file_data = response.json()
        if not file_data.get("ok"):
            raise Exception(f"Telegram API error: {file_data}")
        
        file_path = file_data["result"]["file_path"]
        file_size = file_data["result"].get("file_size", 0)
        logger.info(f"📁 File path: {file_path} (Size: {file_size:,} bytes)")
        
        # Download file from Telegram
        file_url = f"https://api.telegram.org/file/bot{telegram_token}/{file_path}"
        logger.info(f"⬇️ Downloading from: {file_url[:50]}...")
        
        download_response = requests.get(file_url, stream=True, timeout=120)
        download_response.raise_for_status()
        
        # Prepare S3 key
        file_extension = file_path.split('.')[-1] if '.' in file_path else 'mp4'
        s3_key = f"temp_videos/{submission_id}.{file_extension}"
        
        # Upload to S3
        logger.info(f"☁️ Uploading to S3: {s3_bucket}/{s3_key}")
        
        video_content = download_response.content
        content_length = len(video_content)
        
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=video_content,
            ContentType='video/mp4',
            Metadata={
                'original_filename': file_path,
                'telegram_file_id': file_id,
                'submission_id': submission_id,
                'uploaded_at': datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"✅ Successfully uploaded to S3: {s3_key} ({content_length:,} bytes)")
        return s3_key
        
    except Exception as e:
        logger.error(f"❌ Video download/upload error: {str(e)}")
        raise

def trigger_video_processor(submission_id: str, volunteer_id: str, s3_key: str, chat_data: dict):
    """Trigger Function 2 (Video Processor)"""
    try:
        function_name = os.environ.get('VIDEO_PROCESSOR_FUNCTION_NAME')
        if not function_name:
            logger.info("VIDEO_PROCESSOR_FUNCTION_NAME not set, skipping trigger")
            return
        
        payload = {
            'submission_id': submission_id,
            'volunteer_id': volunteer_id,
            's3_key': s3_key,
            'video_title': f"Submission from {chat_data.get('username') or chat_data.get('first_name')}"
        }
        
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',  # Async
            Payload=json.dumps(payload)
        )
        
        logger.info(f"✅ Triggered video processor: {function_name}")
        
    except Exception as e:
        logger.error(f"❌ Failed to trigger video processor: {str(e)}")
        # Don't raise - this is optional
