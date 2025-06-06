import time
from kafka import KafkaConsumer
import json
from psycopg2 import sql
import psycopg2 
import logging
from datetime import datetime, timedelta
from dateutil import parser
from recommendation_system import ScholarshipRecommender
import os
import redis
import json
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = ['broker:29092']
TOPICS = ['search_events', 'filter_events', 'details_events', 'faq_events']

PG_CONFIG = {
    'dbname': 'kafka',
    'user': 'kafka',
    'password': 'kafka_password',
    'host': 'postgres_kafka_consumer',
    'port': '5432'
}

REDIS_CONFIG = {
    'host': 'redis',  # Docker service name
    'port': 6379,
    'db': 0,
    'decode_responses': True
}

MAX_RETRIES = 5
RETRY_DELAY = 10  # seconds

class KafkaClickEventConsumer:
    def __init__(self, bootstrap_servers, topics, pg_config):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.pg_config = pg_config
        self.consumer = None
        self.pg_conn = None
        self.pg_cursor = None
        self.redis_client = None
        self.recommender = None
        
        # Load SQL queries
        self.sql_create_queries = self.load_sql_queries('/app/sql/create_queries.sql')
        self.sql_insert_queries = self.load_sql_queries('/app/sql/insert_queries.sql')

    def load_sql_queries(self, filename):
        with open(filename, 'r') as file:
            return file.read().split(';')

    def initialize_kafka(self) -> bool:
        """Initialize Kafka consumer with retries"""
        for attempt in range(MAX_RETRIES):
            try:
                self.consumer = KafkaConsumer(
                    *self.topics,
                    bootstrap_servers=self.bootstrap_servers,
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    key_deserializer=lambda k: k.decode('utf-8') if k is not None else None,
                    auto_offset_reset='earliest',
                    group_id='click_event_consumer_group',
                    consumer_timeout_ms=10000 
                )
                logger.info("Successfully connected to Kafka")
                return True
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed to connect to Kafka: {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    return False
        return False

    def initialize_postgres(self) -> bool:
        """Initialize PostgreSQL connection with retries"""
        for attempt in range(MAX_RETRIES):
            try:
                self.pg_conn = psycopg2.connect(**self.pg_config)
                self.pg_cursor = self.pg_conn.cursor()
                logger.info("Successfully connected to PostgreSQL")
                return True
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed to connect to PostgreSQL: {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    return False
        return False

    def initialize_redis(self) -> bool:
        """Initialize Redis connection with retries"""
        for attempt in range(MAX_RETRIES):
            try:
                self.redis_client = redis.Redis(**REDIS_CONFIG)
                self.redis_client.ping()  # Test connection
                logger.info("Successfully connected to Redis")
                return True
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed to connect to Redis: {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    return False
        return False

    def initialize_recommender(self) -> bool:
        """Initialize recommendation system with retries"""
        for attempt in range(MAX_RETRIES):
            try:
                api_key = os.getenv('PINECONE_API_KEY')
                if not api_key:
                    raise ValueError("PINECONE_API_KEY environment variable not set")
                self.recommender = ScholarshipRecommender(api_key)
                logger.info("Successfully initialized recommender system")
                return True
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed to initialize recommender: {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    return False
        return False

    def create_tables(self) -> bool:
        """Create tables with retries"""
        for attempt in range(MAX_RETRIES):
            try:
                for query in self.sql_create_queries:
                    if query.strip():
                        self.pg_cursor.execute(query)
                        self.pg_conn.commit()
                logger.info("Successfully created all tables")
                return True
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed to create tables: {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    return False
        return False

    def initialize_all_components(self) -> bool:
        """Initialize all components with proper sequence"""
        logger.info("Starting initialization sequence...")
        
        # Initialize PostgreSQL first (needed for tables)
        if not self.initialize_postgres():
            logger.error("Failed to initialize PostgreSQL")
            return False
            
        # Create tables
        if not self.create_tables():
            logger.error("Failed to create tables")
            return False
            
        # Initialize other components
        components = [
            (self.initialize_kafka, "Kafka"),
            (self.initialize_redis, "Redis"),
            (self.initialize_recommender, "Recommender")
        ]
        
        for init_func, name in components:
            if not init_func():
                logger.error(f"Failed to initialize {name}")
                return False
        
        logger.info("All components initialized successfully")
        return True

    def get_cache_key(self, user_id: str, scholarship_id: str) -> str:
        """Generate Redis key for recommendations"""
        return f"recommendations:{user_id}:{scholarship_id}"

    def get_cached_recommendations(self, user_id: str, scholarship_id: str) -> list:
        """
        Get recommendations from Redis cache
        Returns empty list if no cache found
        """
        cache_key = self.get_cache_key(user_id, scholarship_id)
        cached_data = self.redis_client.get(cache_key)
        
        if cached_data:
            return json.loads(cached_data)
        return []

    def store_recommendations_in_cache(
        self,
        user_id: str,
        scholarship_id: str,
        recommendations: list,
        expire_seconds: int = 3600  # 1 hour default
    ):
        """Store recommendations in Redis with expiration"""
        cache_key = self.get_cache_key(user_id, scholarship_id)
        self.redis_client.setex(
            cache_key,
            expire_seconds,
            json.dumps(recommendations)
        )

    def store_recommendations_in_db(self, user_id: str, source_scholarship_id: str, recommendations: list):
        """Store recommendations in PostgreSQL for permanent storage"""
        insert_query = """
        INSERT INTO recommendations_table 
        (user_id, source_scholarship_id, recommended_scholarship_id, similarity_score, timestamp)
        VALUES (%s, %s, %s, %s, %s);
        """
        timestamp = datetime.now()
        
        for rec in recommendations:
            self.pg_cursor.execute(insert_query, (
                user_id,
                source_scholarship_id,
                rec['scholarship_id'],
                rec['similarity_score'],
                timestamp
            ))
        
        self.pg_conn.commit()

    def get_scholarship_id_from_event(self, event):
        """Extract scholarship ID from event parameter"""
        param = event.get('Clicked_Parameter', '')
        logger.info(f"Extracting scholarship ID from parameter: {param}")
        if 'Scholarship ID:' in param:
            scholarship_id = param.split('Scholarship ID:')[1].strip()
            logger.info(f"Found scholarship ID: {scholarship_id}")
            return scholarship_id
        logger.warning(f"Could not find 'Scholarship ID:' in parameter: {param}")
        return None

    def get_recommendations(
        self,
        user_id: str,
        scholarship_id: str,
        use_cache: bool = True,
        cache_expire_seconds: int = 3600
    ) -> list:
        """
        Get recommendations using Redis cache with fallback to vector search
        """
        recommendations = []
        logger.info(f"Getting recommendations for user {user_id} and scholarship {scholarship_id}")
        
        # Try getting from Redis cache if enabled
        if use_cache:
            recommendations = self.get_cached_recommendations(user_id, scholarship_id)
            if recommendations:
                logger.info(f"Cache hit: Using Redis cached recommendations for user {user_id}")
                return recommendations
            else:
                logger.info("Cache miss: No recommendations found in Redis")
        
        # Cache miss or cache disabled - do real-time similarity search
        try:
            logger.info("Attempting to get recommendations from Pinecone")
            recommendations = self.recommender.get_similar_scholarships(
                scholarship_id=scholarship_id,
                top_k=5,
                filter_dict={'status': 'open'}
            )
            
            if recommendations:
                # Store in Redis cache
                self.store_recommendations_in_cache(
                    user_id,
                    scholarship_id,
                    recommendations,
                    cache_expire_seconds
                )
                logger.info(f"Cache miss: Stored new recommendations in Redis for user {user_id}")
                
                # Store in PostgreSQL for permanent storage
                self.store_recommendations_in_db(user_id, scholarship_id, recommendations)
                logger.info(f"Stored recommendations in PostgreSQL for user {user_id}")
            else:
                logger.warning("No recommendations returned from Pinecone")
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {str(e)}")
            logger.exception("Full traceback:")
        
        return recommendations

    def process_event(self, event, topic):
        # Store event in corresponding table
        table_name = topic.replace('_events', '_table')
        query_index = {'search_table': 0, 'filter_table': 1, 'details_table': 2, 'faq_table': 3}
        insert_query = self.sql_insert_queries[query_index[table_name]]
        
        self.pg_cursor.execute(sql.SQL(insert_query).format(sql.Identifier(table_name)), (
            event['User_ID'],
            parser.parse(event['Timestamp']),
            event['Page'],
            event['Clicked_Element'],
            event['Clicked_Parameter'],
            event['Duration'],
            event['Location']
        ))
        
        # Generate recommendations if user clicks Save button
        if (topic == 'details_events' and 
            event['Clicked_Element'] == 'Save button'):
            
            logger.info(f"Save button clicked - Processing event: {event}")
            scholarship_id = self.get_scholarship_id_from_event(event)
            logger.info(f"Extracted scholarship_id: {scholarship_id}")
            
            if scholarship_id:
                try:
                    recommendations = self.get_recommendations(
                        user_id=event['User_ID'],
                        scholarship_id=scholarship_id,
                        use_cache=True,
                        cache_expire_seconds=3600  # 1 hour cache
                    )
                    
                    logger.info(f"Got recommendations: {recommendations}")
                    
                    if recommendations:
                        logger.info(
                            f"Found {len(recommendations)} recommendations for user "
                            f"{event['User_ID']} based on scholarship {scholarship_id}"
                        )
                    else:
                        logger.warning(f"No recommendations found for scholarship_id: {scholarship_id}")
                except Exception as e:
                    logger.error(f"Error getting recommendations: {str(e)}")
            else:
                logger.warning(f"Could not extract scholarship ID from parameter: {event.get('Clicked_Parameter', '')}")
        
        self.pg_conn.commit()

    def mark_recommendation_clicked(self, user_id: str, recommended_scholarship_id: str):
        """
        Mark a recommendation as clicked in the database
        """
        query = """
        UPDATE recommendations_table
        SET clicked = TRUE
        WHERE user_id = %s AND recommended_scholarship_id = %s;
        """
        self.pg_cursor.execute(query, (user_id, recommended_scholarship_id))
        self.pg_conn.commit()

    def consume_events(self, duration_minutes):
        """Main event consumption loop with proper initialization"""
        if not self.initialize_all_components():
            logger.error("Failed to initialize components. Exiting...")
            return
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)

        logger.info("Starting to consume events...")
        try:
            for message in self.consumer:
                if datetime.now() >= end_time:
                    break
                topic = message.topic
                self.process_event(message.value, topic)
                logger.info(f"Processed event from {topic}: {message.value['User_ID']}")
        except Exception as e:
            logger.error(f"Error during event consumption: {str(e)}")
        finally:
            self.close()

    def close(self):
        """Cleanup resources"""
        try:
            if self.consumer:
                self.consumer.close()
            if self.pg_cursor:
                self.pg_cursor.close()
            if self.pg_conn:
                self.pg_conn.close()
            if self.redis_client:
                self.redis_client.close()
            logger.info("Successfully closed all connections")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

def main():
    consumer = KafkaClickEventConsumer(BOOTSTRAP_SERVERS, TOPICS, PG_CONFIG)
    consumer.consume_events(duration_minutes=10)

if __name__ == "__main__":
    main()
