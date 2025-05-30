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

#connect to neo4j gds
def get_gds_client(uri: str, user: str, password: str, db: str) -> GraphDataScience:
    try:
        gds = GraphDataScience(uri, auth=(user, password), database=db)
        logger.info("Connected to Neo4j GDS.")
        return gds
    except Exception as e:
        logger.error(f"Failed to connect to Neo4j GDS: {e}")
        raise

#generate embeddings for each node based on each person background (weight the edges by business rules)
def generate_embeddings(gds: GraphDataScience, graph_name: str, embedding_property: str, embedding_dim: int):
    try:
        gds.graph.drop(graph_name, failIfMissing=False)
        logger.info(f"Dropped (if exists) and projecting graph '{graph_name}'...")
        graph, _ = gds.graph.project(
            graph_name,
            node_spec=["Person"],
            relationship_spec={
                "HAS_SKILL":{ 
                    "orientation": "NATURAL",
                    "properties": {
                        "weight": {"defaultValue": 2.0}
                    }
                },
                "HAS_DEGREE":{ 
                    "orientation": "NATURAL",
                    "properties": {
                        "weight": {"defaultValue": 3.0}
                    }
                },
                "HAS_CERTIFICATION":{ 
                    "orientation": "NATURAL",
                    "properties": {
                        "weight": {"defaultValue": 2.0}
                    }
                },
                "HAS_HONOR":{ 
                    "orientation": "NATURAL",
                    "properties": {
                        "weight": {"defaultValue": 1.0}
                    }
                },
                "WORKED_AT":{ 
                    "orientation": "NATURAL",
                    "properties": {
                        "weight": {"defaultValue": 3.0}
                    }
                },
                "SPEAKS":{ 
                    "orientation": "NATURAL",
                    "properties": {
                        "weight": {"defaultValue": 2.0}
                    }
                },
                "LIVES_IN":{ 
                    "orientation": "NATURAL",
                    "properties": {
                        "weight": {"defaultValue": 1.0}
                    }
                },
            }
        )
        logger.info("Running Node2Vec for embeddings...")
        gds.node2vec.write(
            graph,
            embeddingDimension=embedding_dim,
            writeProperty=embedding_property,
            walkLength=80,
            iterations=15
        )
        logger.info(f"Embeddings written to node property '{embedding_property}'.")
    except Exception as e:
        logger.error(f"Failed to generate embeddings: {e}")
        raise
  
"""
Extracts email, embedding, user type (status), and scholarship_id from Neo4j.
Status is 'alumni' if the person has a HAS_SCHOLARSHIP relationship, else 'user'
"""   
def extract_embeddings(session, embedding_property: str) -> List[Tuple[str, List[float], str, Optional[str]]]:
    try:
        result = session.run(f"""
            MATCH (p:Person)
            WHERE p.{embedding_property} IS NOT NULL
            OPTIONAL MATCH (p)-[:HAS_SCHOLARSHIP]->(s:Scholarship)
            WITH p, COLLECT(s.scholarship_name) AS scholarship_ids
            WITH p, scholarship_ids, 
                 CASE WHEN SIZE(scholarship_ids) > 0 THEN 'alumni' ELSE 'user' END AS status
            RETURN p.email AS email, p.{embedding_property} AS embedding, status,
                   CASE WHEN SIZE(scholarship_ids) > 0 THEN scholarship_ids[0] ELSE NULL END AS scholarship_id
        """)
        embeddings = [
            (record["email"], record["embedding"], record["status"], record["scholarship_id"])
            for record in result if record["embedding"] and record["status"]
        ]
        logger.info(f"Extracted {len(embeddings)} embeddings from Neo4j.")
        return embeddings
    except Exception as e:
        logger.error(f"Failed to extract embeddings: {e}")
        raise



#init vector db
def init_pinecone(api_key: str, index_name: str, embedding_dim: int, host: str = None):
    try:
        pc = Pinecone(api_key=api_key)
        #check if index exists, create if not
        if index_name not in [i.name for i in pc.list_indexes()]:
            pc.create_index(
                name=index_name,
                dimension=embedding_dim,
                metric="cosine",
                spec=pc.spec.ServerlessSpec(
                    cloud="aws",
                    region="us-east-1"
                )
            )
            logger.info(f"Created Pinecone serverless index '{index_name}'.")
        else:
            logger.info(f"Pinecone index '{index_name}' already exists.")
        #connect to the index
        if host:
            index = pc.Index(host=host)
        else:
            index = pc.Index(index_name)
        return index
    except Exception as e:
        logger.error(f"Failed to initialize Pinecone: {e}")
        raise

# Insert the embedding to pinecone
def upsert_embeddings_to_pinecone(index, embeddings: List[Tuple[str, List[float], str]], batch_size: int = 100):
    try:
        logger.info(f"Upserting {len(embeddings)} embeddings to Pinecone...")
        for i in range(0, len(embeddings), batch_size):
            batch = embeddings[i:i+batch_size]
            vectors = []
            for email, embedding, status, scholarship_id in batch:
                metadata = {"status": status} #create metadata for easy retriving in pinecone
                if status == "alumni" and scholarship_id:
                    metadata["scholarship_id"] = scholarship_id
                vectors.append({
                    "id": email,
                    "values": embedding,
                    "metadata": metadata
                })
            index.upsert(vectors=vectors)
        logger.info("Upsert to Pinecone completed.")
    except Exception as e:
        logger.error(f"Failed to upsert embeddings to Pinecone: {e}")
        raise

def main():
    connector = ConnectorNeo4j(uri, user, password, db_name)
    connector.connect()
    session = connector.create_session()

    gds = get_gds_client(uri, user, password, db_name)

    #embedding generation
    generate_embeddings(gds, "person_background_graph", EMBEDDING_PROPERTY, EMBEDDING_DIM)
    embeddings = extract_embeddings(session, EMBEDDING_PROPERTY)

    #pinecone setup and upsert
    pinecone_index = init_pinecone(
        pinecone_api_key,
        PINECONE_INDEX_NAME,
        EMBEDDING_DIM,
        pinecone_host
    )
    upsert_embeddings_to_pinecone(pinecone_index, embeddings, BATCH_SIZE)
    connector.close()

if __name__ == "__main__":
    main()
