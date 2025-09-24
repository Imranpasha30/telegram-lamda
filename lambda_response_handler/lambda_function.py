import json
import os
import logging
from datetime import datetime
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def convert_database_url(database_url):
    """Convert SQLAlchemy URL to psycopg2 format"""
    if database_url.startswith('postgresql+asyncpg://'):
        return database_url.replace('postgresql+asyncpg://', 'postgresql://')
    elif database_url.startswith('postgresql://'):
        return database_url
    else:
        raise ValueError(f"Unsupported database URL format: {database_url}")

def lambda_handler(event, context):
    """Main Lambda handler for sending responses to Telegram users"""
    try:
        logger.info("=== RESPONSE HANDLER STARTED ===")
        logger.info(f"Event: {json.dumps(event, default=str)}")
        
        # Extract parameters from Function 2 trigger
        submission_id = event['submission_id']
        volunteer_id = event['volunteer_id']
        status = event['status']  # 'success' or 'error'
        message = event['message']
        
        logger.info(f"Processing response: {submission_id} | Status: {status} | User: {volunteer_id}")
        
        # Send response to user
        result = send_telegram_response(volunteer_id, submission_id, status, message)
        
        # Update database with notification status
        update_notification_status(submission_id, result['sent'])
        
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'result': result
            })
        }
        
        logger.info(f"✅ Response handler completed: {response}")
        return response
        
    except Exception as e:
        logger.error(f"❌ Response handler failed: {str(e)}", exc_info=True)
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'message': str(e),
                'submission_id': event.get('submission_id', 'unknown')
            })
        }

def send_telegram_response(volunteer_id: str, submission_id: str, status: str, message: str) -> dict:
    """Send message to Telegram user"""
    try:
        telegram_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        if not telegram_token:
            raise Exception("TELEGRAM_BOT_TOKEN environment variable not set")
        
        # Get submission details for better messaging
        submission_details = get_submission_details(submission_id)
        
        # Prepare message based on status
        if status == 'success':
            formatted_message = format_success_message(submission_details, message)
            emoji = "✅"
        else:
            formatted_message = format_error_message(submission_details, message)
            emoji = "❌"
        
        # Send message via Telegram Bot API
        send_url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        
        payload = {
            "chat_id": volunteer_id,
            "text": formatted_message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": False
        }
        
        logger.info(f"📤 Sending message to user {volunteer_id}: {emoji} {status}")
        
        response = requests.post(send_url, json=payload, timeout=30)
        response.raise_for_status()
        
        response_data = response.json()
        
        if response_data.get('ok'):
            logger.info(f"✅ Message sent successfully to {volunteer_id}")
            return {
                'sent': True,
                'message_id': response_data['result']['message_id'],
                'chat_id': volunteer_id,
                'status': status
            }
        else:
            logger.error(f"❌ Telegram API error: {response_data}")
            return {
                'sent': False,
                'error': response_data.get('description', 'Unknown error'),
                'chat_id': volunteer_id,
                'status': status
            }
            
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Network error sending message: {str(e)}")
        return {
            'sent': False,
            'error': f"Network error: {str(e)}",
            'chat_id': volunteer_id,
            'status': status
        }
    except Exception as e:
        logger.error(f"❌ Error sending Telegram message: {str(e)}")
        return {
            'sent': False,
            'error': str(e),
            'chat_id': volunteer_id,
            'status': status
        }

def get_submission_details(submission_id: str) -> dict:
    """Get submission details from database"""
    try:
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            raise Exception("DATABASE_URL environment variable not set")
        
        psycopg2_url = convert_database_url(database_url)
        conn = psycopg2.connect(psycopg2_url)
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT vs.*, v.first_name, v.last_name, v.username 
                    FROM video_submissions vs
                    JOIN volunteers v ON vs.volunteer_id = v.id
                    WHERE vs.id = %s
                """, (submission_id,))
                
                submission = cur.fetchone()
                return dict(submission) if submission else {}
                
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Error getting submission details: {str(e)}")
        return {}

def format_success_message(submission_details: dict, message: str) -> str:
    """Format success message for user"""
    user_name = submission_details.get('first_name', 'there')
    video_url = submission_details.get('video_platform_url', '')
    created_at = submission_details.get('created_at', datetime.utcnow())
    
    if isinstance(created_at, str):
        try:
            created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
        except:
            created_at = datetime.utcnow()
    
    formatted_message = f"""✅ **Video Processing Complete!**

Hi {user_name}! 👋

Great news! Your video has been successfully processed and is now under review.

📹 **Submission Details:**
• Status: Under Review
• Submitted: {created_at.strftime('%Y-%m-%d %H:%M')}
• Processing: Completed

{message}

🎬 **What's Next?**
Our team will review your video and get back to you soon!

Thank you for your submission! 🙏"""

    # Add video link if available
    if video_url:
        formatted_message += f"\n\n🔗 [View Your Video]({video_url})"
    
    return formatted_message

def format_error_message(submission_details: dict, message: str) -> str:
    """Format error message for user"""
    user_name = submission_details.get('first_name', 'there')
    
    formatted_message = f"""❌ **Video Processing Error**

Hi {user_name}! 👋

We encountered an issue while processing your video submission.

⚠️ **Error Details:**
{message}

🔄 **What You Can Do:**
• Try submitting your video again
• Make sure the video file is not corrupted
• Check that the video format is supported (MP4, MOV, AVI)

If you continue to experience issues, please contact our support team.

We apologize for the inconvenience! 🙏"""
    
    return formatted_message

def update_notification_status(submission_id: str, notification_sent: bool):
    """Update database with notification status"""
    try:
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            return
        
        psycopg2_url = convert_database_url(database_url)
        conn = psycopg2.connect(psycopg2_url)
        
        try:
            with conn.cursor() as cur:
                # Add notification status to video_submissions table
                cur.execute("""
                    UPDATE video_submissions 
                    SET 
                        notification_sent = %s,
                        notification_sent_at = %s,
                        updated_at = %s
                    WHERE id = %s
                """, (notification_sent, datetime.utcnow() if notification_sent else None, 
                     datetime.utcnow(), submission_id))
                
                conn.commit()
                logger.info(f"💾 Updated notification status for {submission_id}: {notification_sent}")
                
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Error updating notification status: {str(e)}")

# Health check endpoint
def health_check():
    """Simple health check for the function"""
    return {
        'statusCode': 200,
        'body': json.dumps({
            'status': 'healthy',
            'function': 'response-handler',
            'timestamp': datetime.utcnow().isoformat()
        })
    }
