import logging
from typing import List, Tuple, Optional
from graphdatascience import GraphDataScience
from pinecone import Pinecone
import os
from connector_neo4j import ConnectorNeo4j

from dotenv import load_dotenv
load_dotenv()


#Load variables
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USERNAME")
password = os.getenv("NEO4J_PASSWORD")
db_name = os.getenv("NEO4J_DB")

pinecone_api_key = os.getenv("PINECONE_API_KEY")
pinecone_host = os.getenv("PINECONE_HOST")
PINECONE_INDEX_NAME = "person-embeddings"
EMBEDDING_PROPERTY = "node2vec_emb"
EMBEDDING_DIM = 128
BATCH_SIZE = 100

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("user-embedding-pipeline")


#init vector db
def init_pinecone(api_key: str, index_name: str, embedding_dim: int, host: str = None):
    try:
        pc = Pinecone(api_key=api_key)
        #connect to the index
        if host:
            index = pc.Index(host=host)
        else:
            index = pc.Index(index_name)
        return index
    except Exception as e:
        logger.error(f"Failed to initialize Pinecone: {e}")
        raise

#retireve node embedding from neo4j
def get_user_embedding(session, email: str, embedding_property: str) -> Optional[List[float]]:
    try:
        result = session.run(f"""
            MATCH (p:Person {{email: $email}})
            RETURN p.{embedding_property} AS embedding
        """, email=email)
        record = result.single()
        if record and record["embedding"]:
            return record["embedding"]
        else:
            logger.warning(f"No embedding found for {email}")
            return None
    except Exception as e:
        logger.error(f"Failed to fetch embedding for {email}: {e}")
        return None

#query similar user based on the retrived embedding (Cosine Similarity)
def query_pinecone_similarity(index, query_embedding: List[float], query_email, top_k: int = 6):
    try:
        response = index.query(
            vector=query_embedding,
            top_k=top_k,
            include_metadata=False,
            filter={"status": "user"}  #only return users, not alumni
        )
        logger.info("Top similar users (excluding alumni):")
        for match in response["matches"]:
            if match['id'] != query_email: #not the same user
                logger.info(f"Email: {match['id']}, Score: {match['score']}")
        return response["matches"]
    except Exception as e:
        logger.error(f"Failed to query Pinecone: {e}")
        return []

def main():
    connector = ConnectorNeo4j(uri, user, password, db_name)
    connector.connect()
    session = connector.create_session()

    #pinecone setup and upsert
    pinecone_index = init_pinecone(
        pinecone_api_key,
        PINECONE_INDEX_NAME,
        EMBEDDING_DIM,
        pinecone_host
    )
    
    #similarity search example
    query_email = "quinnsherri@example.com"
    query_embedding = get_user_embedding(session, query_email, EMBEDDING_PROPERTY)
    if query_embedding:
        query_pinecone_similarity(pinecone_index, query_embedding, query_email, top_k=6)   
        
    connector.close()

if __name__ == "__main__":
    main()
