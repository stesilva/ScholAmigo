from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
from typing import List, Tuple, Optional
from graphdatascience import GraphDataScience
from pinecone import Pinecone
from discord_notifications import notify_task_failure, notify_dag_success

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_task_failure
}

# Constants
EMBEDDING_PROPERTY = "node2vec_emb"
EMBEDDING_DIM = 128
BATCH_SIZE = 100
PINECONE_INDEX_NAME = "person-embeddings"

def get_gds_client() -> GraphDataScience:
    """Connect to Neo4j GDS"""
    try:
        uri = Variable.get("NEO4J_URI")
        user = Variable.get("NEO4J_USER")
        password = Variable.get("NEO4J_PASSWORD")
        db = Variable.get("NEO4J_DATABASE", "neo4j")
        
        gds = GraphDataScience(uri, auth=(user, password), database=db)
        logger.info("Connected to Neo4j GDS.")
        return gds
    except Exception as e:
        logger.error(f"Failed to connect to Neo4j GDS: {e}")
        raise

def generate_embeddings(**context):
    """Generate embeddings using Node2Vec"""
    try:
        gds = get_gds_client()
        graph_name = "person_background_graph"
        
        # Drop existing graph if it exists
        gds.graph.drop(graph_name, failIfMissing=False)
        logger.info(f"Dropped (if exists) and projecting graph '{graph_name}'...")
        
        # Project graph with relationship types
        graph, _ = gds.graph.project(
            graph_name,
            {
                "Person": {"properties": []}  # No node properties needed for now
            },
            {
                "HAS_SKILL": {
                    "type": "HAS_SKILL",
                    "orientation": "NATURAL"
                },
                "HAS_DEGREE": {
                    "type": "HAS_DEGREE",
                    "orientation": "NATURAL"
                },
                "HAS_CERTIFICATION": {
                    "type": "HAS_CERTIFICATION",
                    "orientation": "NATURAL"
                },
                "HAS_HONOR": {
                    "type": "HAS_HONOR",
                    "orientation": "NATURAL"
                },
                "WORKED_AT": {
                    "type": "WORKED_AT",
                    "orientation": "NATURAL"
                },
                "SPEAKS": {
                    "type": "SPEAKS",
                    "orientation": "NATURAL"
                },
                "LIVES_IN": {
                    "type": "LIVES_IN",
                    "orientation": "NATURAL"
                }
            }
        )
        
        # Generate embeddings using Node2Vec
        logger.info("Running Node2Vec for embeddings...")
        gds.node2vec.write(
            graph,
            embeddingDimension=EMBEDDING_DIM,
            writeProperty=EMBEDDING_PROPERTY,
            walkLength=80,
            iterations=15
        )
        logger.info(f"Embeddings written to node property '{EMBEDDING_PROPERTY}'.")
        
        # Clean up
        gds.graph.drop(graph_name)
        
    except Exception as e:
        logger.error(f"Failed to generate embeddings: {e}")
        raise

def extract_embeddings(**context) -> List[Tuple[str, List[float], str, Optional[str]]]:
    """Extract embeddings from Neo4j"""
    try:
        # Get Neo4j connection details from Airflow variables
        uri = Variable.get("NEO4J_URI")
        user = Variable.get("NEO4J_USER")
        password = Variable.get("NEO4J_PASSWORD")
        db = Variable.get("NEO4J_DATABASE", "neo4j")
        
        # Create Neo4j driver
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(uri, auth=(user, password))
        
        logger.info("Executing Cypher query to extract embeddings...")
        with driver.session(database=db) as session:
            result = session.run(f"""
                MATCH (p:Person)
                WHERE p.{EMBEDDING_PROPERTY} IS NOT NULL
                OPTIONAL MATCH (p)-[:HAS_SCHOLARSHIP]->(s:Scholarship)
                WITH p, COLLECT(s.scholarship_name) AS scholarship_ids
                WITH p, scholarship_ids, 
                     CASE WHEN SIZE(scholarship_ids) > 0 THEN 'alumni' ELSE 'user' END AS status
                RETURN p.email AS email, p.{EMBEDDING_PROPERTY} AS embedding, status,
                       CASE WHEN SIZE(scholarship_ids) > 0 THEN scholarship_ids[0] ELSE NULL END AS scholarship_id
            """)
            
            logger.info("Processing query results...")
            embeddings = []
            for record in result:
                email = record.get("email")
                embedding = record.get("embedding")
                status = record.get("status")
                scholarship_id = record.get("scholarship_id")
                
                if embedding is not None and status is not None:
                    embeddings.append((email, embedding, status, scholarship_id))
                    if len(embeddings) == 1:  # Log first record as sample
                        logger.info(f"Sample embedding record - Email: {email}, Status: {status}, "
                                  f"Embedding length: {len(embedding) if embedding else 'N/A'}")
            
        logger.info(f"Extracted {len(embeddings)} embeddings from Neo4j.")
        driver.close()
        
        # Store embeddings in XCom for the next task
        task_instance = context['task_instance']
        task_instance.xcom_push(key='embeddings', value=embeddings)
        return embeddings
        
    except Exception as e:
        logger.error(f"Failed to extract embeddings: {e}")
        raise

def upsert_to_pinecone(**context):
    """Upload embeddings to Pinecone"""
    try:
        # Get embeddings from previous task using XCom
        task_instance = context['task_instance']
        embeddings = task_instance.xcom_pull(task_ids='extract_embeddings_and_metadata', key='embeddings')
        
        if not embeddings:
            raise ValueError("No embeddings received from previous task")
            
        logger.info(f"Retrieved {len(embeddings)} embeddings from previous task")
        
        # Initialize Pinecone
        pinecone_api_key = Variable.get("PINECONE_API_KEY")
        pc = Pinecone(api_key=pinecone_api_key)
        
        # Create index if it doesn't exist
        if PINECONE_INDEX_NAME not in [i.name for i in pc.list_indexes()]:
            pc.create_index(
                name=PINECONE_INDEX_NAME,
                dimension=EMBEDDING_DIM,
                metric="cosine",
                spec=pc.spec.ServerlessSpec(
                    cloud="aws",
                    region="us-east-1"
                )
            )
            logger.info(f"Created Pinecone serverless index '{PINECONE_INDEX_NAME}'.")
        
        # Connect to index using the user-specific host from environment variable
        pinecone_host = Variable.get("PINECONE_USER_HOST")
        index = pc.Index(host=pinecone_host)
        
        # Upsert embeddings in batches
        logger.info(f"Upserting {len(embeddings)} embeddings to Pinecone...")
        for i in range(0, len(embeddings), BATCH_SIZE):
            batch = embeddings[i:i+BATCH_SIZE]
            vectors = []
            for email, embedding, status, scholarship_id in batch:
                metadata = {"status": status}
                if status == "alumni" and scholarship_id:
                    metadata["scholarship_id"] = scholarship_id
                vectors.append({
                    "id": email,
                    "values": embedding,
                    "metadata": metadata
                })
            index.upsert(vectors=vectors)
            logger.info(f"Uploaded batch {i//BATCH_SIZE + 1}")
        
        logger.info("Successfully uploaded embeddings to Pinecone.")
        
    except Exception as e:
        logger.error(f"Failed to upsert embeddings to Pinecone: {e}")
        raise

# Create the DAG
with DAG(
    'user_embeddings_pipeline',
    default_args=default_args,
    description='Generate user embeddings in Neo4j and store them with metadata in Pinecone for recommendations',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['embeddings', 'neo4j', 'pinecone', 'recommendations']
) as dag:
    
    # Task 1: Generate embeddings in Neo4j using Node2Vec
    generate_task = PythonOperator(
        task_id='generate_node2vec_embeddings',
        python_callable=generate_embeddings,
        dag=dag
    )
    
    # Task 2: Extract embeddings and metadata from Neo4j
    extract_task = PythonOperator(
        task_id='extract_embeddings_and_metadata',
        python_callable=extract_embeddings,
        dag=dag
    )
    
    # Task 3: Upload embeddings with metadata to Pinecone for similarity search
    upload_task = PythonOperator(
        task_id='upload_to_pinecone_vectordb',
        python_callable=upsert_to_pinecone,
        dag=dag,
        on_success_callback=lambda context: notify_dag_success(
            context,
            message="Successfully generated and uploaded user embeddings to Pinecone for recommendations!"
        )
    )
    
    # Set task dependencies
    generate_task >> extract_task >> upload_task 