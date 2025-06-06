import pandas as pd
from pinecone import Pinecone
from sentence_transformers import SentenceTransformer
import os
from typing import List, Dict, Any
import numpy as np

class ScholarshipRecommender:
    def __init__(self, api_key: str, environment: str = "gcp-starter"):
        """
        Initialize the recommendation system
        Args:
            api_key: Pinecone API key
            environment: Pinecone environment (default: gcp-starter)
        """
        # Get host from environment
        host = os.getenv('PINECONE_HOST')
        if not host:
            raise ValueError("PINECONE_HOST environment variable not set")
        
        # Remove quotes if present
        host = host.strip('"')
        api_key = api_key.strip('"')
        
        # Initialize Pinecone
        self.pc = Pinecone(api_key=api_key)
        
        self.index_name = "scholarship-embeddings"
        
        # Check if index exists, if not create it
        existing_indexes = self.pc.list_indexes().names()
        if self.index_name not in existing_indexes:
            # Create the index with the correct specifications
            self.pc.create_index(
                name=self.index_name,
                dimension=384,  # dimension for all-MiniLM-L6-v2 model
                metric="cosine",
                spec={"serverless": {"cloud": "aws", "region": "us-east-1"}}
            )
        
        # Connect to the index
        self.index = self.pc.Index(name=self.index_name)
        
        # Initialize the embedding model
        self.model = SentenceTransformer('all-MiniLM-L6-v2')

    def _create_scholarship_text(self, scholarship: Dict[str, Any]) -> str:
        """
        Create a text representation of a scholarship for embedding
        """
        # Combine relevant fields into a single text
        fields = [
            scholarship.get('scholarship_name', ''),
            scholarship.get('description', ''),
            scholarship.get('program_country', ''),
            str(scholarship.get('origin_country', [])),
            scholarship.get('program_level', ''),
            scholarship.get('required_level', ''),
            str(scholarship.get('fields_of_study_code', [])),
            scholarship.get('funding_category', ''),
            scholarship.get('status', '')
        ]
        return ' '.join(str(field).lower() for field in fields if field)

    def create_embeddings(self, scholarships_df: pd.DataFrame):
        """
        Create and store embeddings for all scholarships
        Args:
            scholarships_df: DataFrame containing scholarship data
        """
        batch_size = 100
        for i in range(0, len(scholarships_df), batch_size):
            batch = scholarships_df.iloc[i:i+batch_size]
            
            # Create text representations
            texts = [self._create_scholarship_text(row) for _, row in batch.iterrows()]
            
            # Generate embeddings
            embeddings = self.model.encode(texts)
            
            # Prepare vectors for Pinecone
            vectors = [(
                row['scholarship_id'],
                embedding.tolist(),
                {
                    'name': row['scholarship_name'],
                    'program_country': row['program_country'],
                    'program_level': row['program_level'],
                    'fields_of_study_code': row['fields_of_study_code'].tolist() if isinstance(row['fields_of_study_code'], np.ndarray) else row['fields_of_study_code'],
                    'funding_category': row['funding_category'],
                    'status': row['status']
                }
            ) for embedding, (_, row) in zip(embeddings, batch.iterrows())]
            
            # Upsert to Pinecone
            self.index.upsert(vectors=vectors)

    def get_similar_scholarships(
        self,
        scholarship_id: str,
        top_k: int = 5,
        filter_dict: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Find similar scholarships based on a saved scholarship
        Args:
            scholarship_id: ID of the scholarship to find similar ones for
            top_k: Number of similar scholarships to return
            filter_dict: Optional dictionary for filtering results (e.g., by status='open')
        Returns:
            List of similar scholarships with their similarity scores
        """
        # Get the vector for the target scholarship
        vector = self.index.fetch([scholarship_id])
        
        if not vector.vectors:
            raise ValueError(f"Scholarship with ID {scholarship_id} not found")
        
        # Query for similar scholarships
        query_response = self.index.query(
            vector=vector.vectors[scholarship_id].values,
            top_k=top_k + 1,  # Add 1 to account for the query scholarship itself
            include_metadata=True,
            filter=filter_dict
        )
        
        # Process and return results (excluding the query scholarship itself)
        similar_scholarships = []
        for match in query_response.matches:
            if match.id != scholarship_id:  # Exclude the query scholarship
                similar_scholarships.append({
                    'scholarship_id': match.id,
                    'similarity_score': match.score,
                    'metadata': match.metadata
                })
        
        return similar_scholarships

    def delete_index(self):
        """Delete the Pinecone index"""
        if self.index_name in self.pc.list_indexes().names():
            self.pc.delete_index(self.index_name)

def create_local_embeddings():
    """Function to create embeddings when running locally"""
    api_key = os.getenv('PINECONE_API_KEY')
    if not api_key:
        raise ValueError("PINECONE_API_KEY environment variable not set")
    
    # Initialize recommender
    recommender = ScholarshipRecommender(api_key)
    
    try:
        # Load scholarship data from local path
        df = pd.read_parquet('outputs/scholarship/scholarships_processed_new.parquet')
        
        # Convert fields_of_study_code to list if it's a numpy array
        if 'fields_of_study_code' in df.columns:
            df['fields_of_study_code'] = df['fields_of_study_code'].apply(
                lambda x: x.tolist() if isinstance(x, np.ndarray) else 
                         ([] if pd.isna(x) else x)
            )
        
        # Create embeddings
        print("Creating embeddings...")
        recommender.create_embeddings(df)
        print("Embeddings created successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        recommender.delete_index()

def main():
    # Check if running in Docker
    in_docker = os.path.exists('/.dockerenv')
    
    if in_docker:
        # Docker environment - just initialize the recommender
        api_key = os.getenv('PINECONE_API_KEY')
        if not api_key:
            raise ValueError("PINECONE_API_KEY environment variable not set")
        
        # Initialize recommender without creating embeddings
        ScholarshipRecommender(api_key)
        print("Recommender system initialized in Docker environment")
    else:
        # Local environment - create embeddings
        create_local_embeddings()

if __name__ == "__main__":
    main() 