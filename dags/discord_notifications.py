import os
import logging
from typing import Optional, Dict, Any
from discord_webhook import DiscordWebhook, DiscordEmbed

def get_webhook_url() -> str:
    """
    Retrieves Discord webhook URL from environment variables.
    
    Returns:
        str: Discord webhook URL
        
    Raises:
        ValueError: If webhook URL is not set
    """
    webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
    if not webhook_url:
        raise ValueError("Discord webhook URL not set in environment variables")
    return webhook_url

def send_alert(message: str, color: str = "FFFFFF") -> None:
    """
    Sends a simple message alert to Discord.
    
    Args:
        message (str): The message to send
        color (str): Hex color code for the message (default: white)
    """
    try:
        webhook = DiscordWebhook(url=get_webhook_url())
        embed = DiscordEmbed(description=message, color=color)
        webhook.add_embed(embed)
        response = webhook.execute()
        
        if not response.ok:
            logging.error(f"Discord alert failed: {response.text}")
            
    except Exception as e:
        logging.error(f"Error sending Discord alert: {str(e)}", exc_info=True)

def notify_task_failure(context: Dict[str, Any]) -> None:
    """
    Sends a detailed task failure notification to Discord.
    
    Args:
        context: Airflow task context
    """
    try:
        # Add debug logging
        logging.info("Received task failure context:")
        logging.info(f"DAG ID: {context.get('dag').dag_id if context.get('dag') else 'Not available'}")
        logging.info(f"Task ID: {context.get('task_instance').task_id if context.get('task_instance') else 'Not available'}")
        logging.info(f"Execution Date: {context.get('execution_date', 'Not available')}")
        
        webhook = DiscordWebhook(url=get_webhook_url())
        ti = context['task_instance']
        
        embed = DiscordEmbed(
            title=f"Task Failed: {ti.task_id}",
            description=f"After {ti.try_number} attempts",
            color="FF0000"  # Red
        )
        
        # Add task details
        embed.add_embed_field(name="DAG", value=ti.dag_id)
        embed.add_embed_field(name="Task", value=ti.task_id)
        embed.add_embed_field(name="Execution Date", value=str(context['execution_date']))
        
        # Add error information if available
        if 'exception' in context:
            error_msg = str(context['exception'])[:1000]  # Truncate long error messages
            embed.add_embed_field(name="Error", value=f"```{error_msg}```", inline=False)
            # Add debug logging for exception
            logging.info(f"Exception details: {error_msg}")
        
        # Add log URL if available
        if hasattr(ti, 'log_url'):
            embed.add_embed_field(name="Log URL", value=ti.log_url)
        
        webhook.add_embed(embed)
        response = webhook.execute()
        
        if not response.ok:
            logging.error(f"Discord task failure notification failed: {response.text}")
            
    except Exception as e:
        logging.error(f"Error sending Discord task failure notification: {str(e)}", exc_info=True)

def notify_dag_success(context: Dict[str, Any], message: Optional[str] = None) -> None:
    """
    Sends a DAG success notification to Discord.
    
    Args:
        context: Airflow task context
        message: Optional custom success message
    """
    try:
        webhook = DiscordWebhook(url=get_webhook_url())
        dag_id = context['dag'].dag_id
        
        embed = DiscordEmbed(
            title=f"DAG Completed Successfully: {dag_id}",
            description=message if message else "All tasks completed successfully",
            color="00FF00"  # Green
        )
        
        # Add execution details
        embed.add_embed_field(name="Execution Date", value=str(context['execution_date']))
        
        webhook.add_embed(embed)
        response = webhook.execute()
        
        if not response.ok:
            logging.error(f"Discord success notification failed: {response.text}")
            
    except Exception as e:
        logging.error(f"Error sending Discord success notification: {str(e)}", exc_info=True)

def notify_data_stats(
    stats: Dict[str, Any],
    title: str = "Data Processing Stats",
    color: str = "0000FF"  # Blue
) -> None:
    """
    Sends data processing statistics to Discord.
    
    Args:
        stats: Dictionary of statistics to report
        title: Title for the stats message
        color: Hex color code for the message
    """
    try:
        webhook = DiscordWebhook(url=get_webhook_url())
        embed = DiscordEmbed(title=title, color=color)
        
        # Add each stat as a field
        for key, value in stats.items():
            embed.add_embed_field(name=key, value=str(value))
        
        webhook.add_embed(embed)
        response = webhook.execute()
        
        if not response.ok:
            logging.error(f"Discord stats notification failed: {response.text}")
            
    except Exception as e:
        logging.error(f"Error sending Discord stats notification: {str(e)}", exc_info=True) 