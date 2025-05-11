from airflow.decorators import dag, task
from datetime import datetime, timedelta
import boto3
from faker import Faker
import random
import logging
import os
import json
import requests
from linkedin_generate_data import LinkedInGenerateData, retrive_linkedin_urls_s3
from discord_webhook import DiscordWebhook, DiscordEmbed
import traceback
from airflow.operators.python import get_current_context
from send_data_to_aws import send_data_to_aws

os.environ['NO_PROXY'] = '*'

# Constants
BUCKET_NAME = 'linkedin-data-ingestion'
INPUT_FOLDER = 'application_data/'
OUTPUT_FOLDER = 'linkedin_users_data/'
AWS_CONN_ID = 'aws_default'

def send_alert(message):
    webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
    webhook = DiscordWebhook(url=webhook_url, content=message)
    response = webhook.execute()
    if response.status_code != 200:
        logging.error(f"Discord alert failed: {response.text}")

# Discord alert function
def notify_discord(context):
    """Send alert to Discord on task failure"""
    try:
        webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
        webhook=DiscordWebhook(url=webhook_url)
        ti = context['task_instance']
        embed = DiscordEmbed(
            title=f"Task Failed: {ti.task_id}",
            description=f"After {ti.try_number} attempts",
            color="FF0000"
        )
        
        ti = context['task_instance']
        embed.add_embed_field(name="DAG", value=ti.dag_id)
        embed.add_embed_field(name="Task", value=ti.task_id)
        embed.add_embed_field(name="Execution Date", value=str(context['execution_date']))
        embed.add_embed_field(name="Log URL", value=ti.log_url)
        
        webhook.add_embed(embed)
        response = webhook.execute()
        if not response.ok:
            logging.error(f"Discord notification failed: {response.status_code}")
            
    except Exception as e:
        logging.error(f"Notification system error: {str(e)}")

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_discord,  # Global failure callback
}

@dag(
    'batch_linkedin', default_args=default_args, start_date=datetime(2025, 3 , 29),
        description='DAG to scrape user data', tags=['batch_linkedin'],
        schedule='@daily', catchup=False
)

def linkedin_data_generation_dag():
    @task
    def validate_webhook():
        if not os.getenv("DISCORD_WEBHOOK_URL"):
            raise ValueError("Discord webhook URL not set")
        
    @task(task_id='retrieve_linkedin_urls')
    def retrieve_linkedin_urls():
        """Task 1: Retrieve LinkedIn URLs from S3"""
        try:
            session = boto3.Session(profile_name="bdm_group_member")
            s3 = session.client("s3")
            profiles=retrive_linkedin_urls_s3(s3, BUCKET_NAME, INPUT_FOLDER)
            logging.info(f"Retrieved {len(profiles)} profiles from S3")
            return profiles
            
        except Exception as e:
            logging.error("Error retrieving LinkedIn URLs", exc_info=True)
            raise

    @task(task_id='generate_profiles')
    def generate_profiles(profiles):
        """Task 2: Generate User Data and Save to S3"""
        context = get_current_context()
        generator = LinkedInGenerateData()
        generation_failures = []
        success_profiles = []
        
        # Parse input
        lines = profiles.strip().split("\n")
        # lines = [line.replace('\r', '') for line in lines]
        headers = lines[0].split(",")
        rows = [dict(zip(headers, line.split(","))) for line in lines[1:]]

        # Generation Phase
        try:
            for row in rows:
                profile_title = row['name']
                print(f"Generating profile: {profile_title}")
                profile_url = row['url']
                try:
                    profile_data = generator.generate_profile(profile_url)
                    if profile_data:
                        success_profiles.append(profile_data)
                        print(f"Generated: {profile_title}")
                    else:
                        error_msg = f"Empty data for {profile_title} ({profile_url})"
                        generation_failures.append(error_msg)
                        send_alert(error_msg)
                        
                except Exception as e:
                    error_msg = f"""**Profile Generation Failed**
                    - Profile: {profile_title}
                    - URL: {profile_url}
                    - Error: {str(e)}
                    - Traceback: {traceback.format_exc()[:1000]}"""
                    
                    generation_failures.append(error_msg)
                    send_alert(error_msg)
        except FileNotFoundError:
            error_msg=f"Error: No file found at {BUCKET_NAME}"
            send_alert(error_msg)
        except Exception as e:
            error_msg=f"An error occurred while reading the data: {e}"
            send_alert(error_msg)
        # Save Phase
        if success_profiles:
            try:
                timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
                s3_file_name = f"{OUTPUT_FOLDER}{timestamp}_linkedin_profile_data.json"
                send_data_to_aws(success_profiles, BUCKET_NAME, s3_file_name)
                success_msg = f"""**Saved {len(success_profiles)} profiles**
                - Bucket: {BUCKET_NAME}
                - Folder: {OUTPUT_FOLDER}"""
                # notify_discord(
                #             message=success_msg,
                #             context=context,
                #             is_error=False
                #         )
                
            except Exception as e:
                error_msg = f"""**S3 Save Failed**
                - Profiles: {len(success_profiles)}
                - Bucket: {BUCKET_NAME}
                - Error: {str(e)}
                - Traceback: {traceback.format_exc()[:1000]}"""
                
                send_alert(error_msg)
                raise ValueError("Failed to save to S3")

        # Summary Report
        if generation_failures:
            msg=f"**Generation Summary**: {len(generation_failures)} failures\n" + "\n".join(f"• {msg[:200]}..." for msg in generation_failures[:5]) + ("\n...(truncated)" if len(generation_failures) > 5 else "")
            send_alert(msg)
        if len(success_profiles) == 0:
            raise ValueError("All profile generations failed")
        
    try:
        validate=validate_webhook()
        profiles = retrieve_linkedin_urls()
        generate_profiles(profiles)

        validate >> profiles
    except Exception as e:
        logging.error("DAG execution failed", exc_info=True)
        raise

linkedin_dag = linkedin_data_generation_dag()