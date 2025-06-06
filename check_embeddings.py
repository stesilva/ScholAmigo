import os
from pinecone import Pinecone

def check_embeddings():
    # Initialize Pinecone
    api_key = os.getenv('PINECONE_API_KEY')
    if not api_key:
        raise ValueError("PINECONE_API_KEY environment variable not set")
    
    pc = Pinecone(api_key=api_key.strip('"'))
    
    # List all indexes
    print("\nAvailable indexes:")
    indexes = pc.list_indexes().names()
    print(indexes)
    
    if 'scholarship-embeddings' in indexes:
        # Connect to the index
        index = pc.Index('scholarship-embeddings')
        
        # Get index statistics
        stats = index.describe_index_stats()
        print("\nIndex statistics:")
        print(f"Total vectors: {stats.total_vector_count}")
        print(f"Dimension: {stats.dimension}")
        
        # Query for any vectors
        print("\nSample vectors:")
        query_response = index.query(
            vector=[0] * 384,  # dummy vector for querying
            top_k=3,
            include_metadata=True
        )
        
        if query_response.matches:
            for match in query_response.matches:
                print(f"\nScholarship ID: {match.id}")
                print(f"Metadata: {match.metadata}")
        else:
            print("No vectors found in the index.")

if __name__ == "__main__":
    check_embeddings() 