from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import os
import shutil
from connector_neo4j import ConnectorNeo4j
from discord_notifications import notify_task_failure, notify_dag_success
from neo4j_loading_functions import (
    create_constrainsts, load_user, load_alumni, load_languages,
    load_skills, load_education, load_certifications, load_honors,
    load_experiences
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# List of required CSV files
REQUIRED_FILES = [
    'user_basic_information.csv',
    'alumni_basic_information.csv',
    'languages.csv',
    'skills.csv',
    'education.csv',
    'certificates.csv',
    'honors.csv',
    'experiences.csv'
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_task_failure
}

def copy_files_to_neo4j():
    """Copy CSV files from outputs/neo4j to Neo4j import directory"""
    try:
        # Create Neo4j import directory if it doesn't exist
        os.makedirs('/tmp/neo4j_data', exist_ok=True)
        
        # Source directory (your local neo4j data folder)
        source_dir = '/opt/airflow/outputs/neo4j'
        
        # Check if source directory exists
        if not os.path.exists(source_dir):
            raise FileNotFoundError(f"Source directory not found: {source_dir}")
        
        # Copy each required file
        for filename in REQUIRED_FILES:
            source_path = os.path.join(source_dir, filename)
            dest_path = f'/tmp/neo4j_data/{filename}'
            
            if not os.path.exists(source_path):
                raise FileNotFoundError(f"Required file not found: {source_path}")
            
            logger.info(f"Copying {filename} to Neo4j import directory...")
            shutil.copy2(source_path, dest_path)
            logger.info(f"Successfully copied {filename}")
            
        return "/tmp/neo4j_data"
    except Exception as e:
        logger.error(f"Error copying files: {str(e)}")
        raise

def load_neo4j_data():
    """Load data into Neo4j"""
    try:
        # Get Neo4j credentials from Airflow variables
        neo4j_uri = Variable.get("NEO4J_URI")
        neo4j_user = Variable.get("NEO4J_USER")
        neo4j_password = Variable.get("NEO4J_PASSWORD")
        neo4j_db = Variable.get("NEO4J_DATABASE", "neo4j")  # Default to 'neo4j' if not set
        
        # Initialize Neo4j connector
        connector = ConnectorNeo4j(neo4j_uri, neo4j_user, neo4j_password, neo4j_db)
        connector.connect()
        
        # Create session
        session = connector.create_session()
        
        try:
            # Clear existing data
            logger.info("Clearing existing data from Neo4j...")
            connector.clear_session(session)
            
            logger.info("Creating constraints...")
            session.execute_write(create_constrainsts)
            
            # Load all data types
            logger.info("Loading user data...")
            session.execute_write(load_user)
            
            logger.info("Loading alumni data...")
            session.execute_write(load_alumni)
            
            logger.info("Loading languages data...")
            session.execute_write(load_languages)
            
            logger.info("Loading skills data...")
            session.execute_write(load_skills)
            
            logger.info("Loading education data...")
            session.execute_write(load_education)
            
            logger.info("Loading certifications data...")
            session.execute_write(load_certifications)
            
            logger.info("Loading honors data...")
            session.execute_write(load_honors)
            
            logger.info("Loading experiences data...")
            session.execute_write(load_experiences)
            
            logger.info("Data loading completed successfully!")
            
        finally:
            # Clean up
            connector.close()
            
        # Clean up temporary files
        for filename in REQUIRED_FILES:
            file_path = f"/tmp/neo4j_data/{filename}"
            if os.path.exists(file_path):
                os.remove(file_path)
        os.rmdir('/tmp/neo4j_data')
        
    except Exception as e:
        logger.error(f"Error loading data into Neo4j: {str(e)}")
        raise

with DAG(
    'load_user_data_neo4j',
    default_args=default_args,
    description='Load user data into Neo4j database',
    schedule_interval='0 1 * * *',  # Run daily at 1 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['neo4j', 'user_data']
) as dag:
    
    # Task to copy files to Neo4j import directory
    copy_files = PythonOperator(
        task_id='copy_files_to_neo4j',
        python_callable=copy_files_to_neo4j,
        dag=dag
    )
    
    # Task to load data into Neo4j
    load_data = PythonOperator(
        task_id='load_neo4j_data',
        python_callable=load_neo4j_data,
        dag=dag,
        on_success_callback=lambda context: notify_dag_success(
            context,
            message="Successfully loaded user data into Neo4j!"
        )
    )
    
    # Set task dependencies
    copy_files >> load_data 