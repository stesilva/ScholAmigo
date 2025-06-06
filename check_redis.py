import redis
import json

def check_redis():
    # Connect to Redis
    redis_client = redis.Redis(
        host='localhost',  # Use localhost since we're checking from outside Docker
        port=6379,
        db=0,
        decode_responses=True  # This ensures we get strings instead of bytes
    )
    
    try:
        # Test connection
        redis_client.ping()
        print("Successfully connected to Redis")
        
        # Get all keys matching the recommendations pattern
        keys = redis_client.keys('recommendations:*')
        print(f"\nFound {len(keys)} recommendation keys in Redis:")
        
        # Print details for each key
        for key in keys:
            # Get TTL (Time To Live) for the key
            ttl = redis_client.ttl(key)
            
            # Get the cached recommendations
            cached_data = redis_client.get(key)
            if cached_data:
                recommendations = json.loads(cached_data)
                
                # Extract user_id and scholarship_id from the key
                # Key format is 'recommendations:user_id:scholarship_id'
                _, user_id, scholarship_id = key.split(':')
                
                print(f"\nKey: {key}")
                print(f"User ID: {user_id}")
                print(f"Scholarship ID: {scholarship_id}")
                print(f"TTL: {ttl} seconds")
                print(f"Number of recommendations: {len(recommendations)}")
                print("Sample recommendation:", recommendations[0] if recommendations else "None")
            else:
                print(f"\nKey {key} exists but has no data")
        
    except redis.ConnectionError:
        print("Could not connect to Redis. Make sure Redis is running and accessible.")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        redis_client.close()

if __name__ == "__main__":
    check_redis() 