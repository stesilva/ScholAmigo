import pandas as pd
from pinecone import Pinecone
from sentence_transformers import SentenceTransformer
import os
from typing import List, Dict, Any
import numpy as np
from airflow.models import Variable
import boto3
from retrieve_data_from_aws import retrieve_latest_file_from_s3

class ScholarshipRecommender:
    def __init__(self, api_key: str, environment: str = "gcp-starter"):
        """
        Initialize the recommendation system
        Args:
            api_key: Pinecone API key
            environment: Pinecone environment (default: gcp-starter)
        """
        # Get host from Airflow Variables
        host = Variable.get("PINECONE_HOST", default_var=None)
        if not host:
            raise ValueError("PINECONE_HOST variable not set in Airflow Variables")
        
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

def download_file_from_s3(bucket_name: str, file_key: str, local_path: str, profile_name: str = "bdm-2025"):
    """
    Downloads a file from S3 to a local path
    """
    try:
        session = boto3.Session(profile_name=profile_name)
        s3 = session.client("s3")
        
        # Check file metadata in S3
        try:
            response = s3.head_object(Bucket=bucket_name, Key=file_key)
            print(f"S3 file size: {response['ContentLength']} bytes")
            print(f"Last modified: {response['LastModified']}")
        except Exception as e:
            print(f"Error checking S3 file metadata: {str(e)}")
        
        s3.download_file(bucket_name, file_key, local_path)
        print(f"Successfully downloaded {file_key} to {local_path}")
        
        # Try reading the parquet file directly from S3 to verify
        try:
            s3_df = pd.read_parquet(f"s3://{bucket_name}/{file_key}")
            print(f"Direct S3 read - DataFrame shape: {s3_df.shape}")
            print("Direct S3 read - First few rows:")
            print(s3_df.head())
        except Exception as e:
            print(f"Error reading directly from S3: {str(e)}")
            
    except Exception as e:
        raise Exception(f"Error downloading file from S3: {str(e)}")

def create_local_embeddings():
    """Function to create embeddings when running in Airflow"""
    try:
        # Get API key from Airflow Variables
        api_key = Variable.get("PINECONE_API_KEY", default_var=None)
        if not api_key:
            raise ValueError("PINECONE_API_KEY variable not set in Airflow Variables")
        
        # Initialize recommender
        recommender = ScholarshipRecommender(api_key)
        
        try:
            # Retrieve latest scholarship data from S3
            print("Retrieving latest scholarship data from S3...")
            _, file_key = retrieve_latest_file_from_s3(
                bucket_name="scholarship-data-trusted",
                folder_name="german_scholarships_llm",
                profile_name="bdm-2025"
            )
            
            # Set up local path for downloaded file
            local_path = os.path.join('/tmp', 'latest_scholarships.parquet')
            
            # Get the directory path without _SUCCESS
            directory_path = file_key.replace('/_SUCCESS', '')
            
            # List objects in the directory to find the actual parquet file
            session = boto3.Session(profile_name="bdm-2025")
            s3 = session.client("s3")
            response = s3.list_objects_v2(
                Bucket="scholarship-data-trusted",
                Prefix=directory_path
            )
            
            if 'Contents' not in response:
                raise FileNotFoundError(f"No files found in directory: {directory_path}")
            
            # Find the actual parquet file
            parquet_files = [obj['Key'] for obj in response['Contents'] 
                           if obj['Key'].endswith('.parquet') and not obj['Key'].endswith('/_SUCCESS')]
            
            if not parquet_files:
                raise FileNotFoundError(f"No parquet files found in directory: {directory_path}")
            
            file_key = parquet_files[0]  # Use the first parquet file found

            # Download the file from S3
            print(f"Downloading file {file_key} from S3...")
            download_file_from_s3(
                bucket_name="scholarship-data-trusted",
                file_key=file_key,
                local_path=local_path,
                profile_name="bdm-2025"
            )
            
            # Verify file exists and size
            print(f"Checking file {local_path}...")
            if os.path.exists(local_path):
                file_size = os.path.getsize(local_path)
                print(f"File size: {file_size} bytes")
            else:
                raise FileNotFoundError(f"File not found at {local_path}")
            
            # Create embeddings
            print(f"Loading data from file: {local_path}")
            try:
                # Try reading with fastparquet engine
                df = pd.read_parquet(local_path, engine='fastparquet')
            except Exception as e:
                print(f"Error with fastparquet engine: {str(e)}")
                print("Trying pyarrow engine...")
                try:
                    df = pd.read_parquet(local_path, engine='pyarrow')
                except Exception as e:
                    print(f"Error with pyarrow engine: {str(e)}")
                    raise
            
            # Debug DataFrame
            print(f"DataFrame shape: {df.shape}")
            print("DataFrame columns:", df.columns.tolist())
            print("First few rows:")
            print(df.head())
            
            # Check for required columns
            required_columns = ['scholarship_id', 'scholarship_name', 'description', 'program_country', 
                              'program_level', 'fields_of_study_code', 'funding_category', 'status']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Handle fields_of_study_code - ensure it's a list
            def convert_to_list(x):
                if pd.isna(x).any():  # Check if any element is NaN
                    return []
                if isinstance(x, (list, np.ndarray)):
                    return x.tolist() if isinstance(x, np.ndarray) else x
                return []

            df['fields_of_study_code'] = df['fields_of_study_code'].apply(convert_to_list)
            
            if len(df) == 0:
                raise ValueError("DataFrame is empty after reading parquet file")
                
            # Create embeddings
            print(f"Creating embeddings for {len(df)} scholarships...")
            recommender.create_embeddings(df)
            print("Embeddings created successfully!")
            
            # Clean up the temporary file
            if os.path.exists(local_path):
                os.remove(local_path)
                print(f"Cleaned up temporary file: {local_path}")
            
        except Exception as e:
            print(f"Error processing scholarship data: {str(e)}")
            if 'recommender' in locals():
                recommender.delete_index()
            raise
            
    except Exception as e:
        print(f"Error: {str(e)}")
        if 'recommender' in locals():
            recommender.delete_index()
        raise  # Re-raise the exception for Airflow to catch

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