from fastapi import FastAPI, HTTPException
from typing import List, Optional
import redis
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel

app = FastAPI()

# Redis configuration (same as in kafka_consumer.py)
REDIS_CONFIG = {
    'host': 'redis',
    'port': 6379,
    'db': 0,
    'decode_responses': True
}

# PostgreSQL configuration
PG_CONFIG = {
    'dbname': 'kafka',
    'user': 'kafka',
    'password': 'kafka_password',
    'host': 'postgres_kafka_consumer',
    'port': '5432'
}

class Recommendation(BaseModel):
    scholarship_id: str
    similarity_score: float
    metadata: Optional[dict] = None

class RecommendationResponse(BaseModel):
    user_id: str
    source_scholarship_id: str
    recommendations: List[Recommendation]
    is_cached: bool

def get_redis_client():
    return redis.Redis(**REDIS_CONFIG)

def get_db_connection():
    return psycopg2.connect(**PG_CONFIG, cursor_factory=RealDictCursor)

@app.get("/recommendations/user/{user_id}", response_model=List[RecommendationResponse])
async def get_user_recommendations(user_id: str, limit: int = 5):
    """Get all recommendations for a user across different scholarships"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get recent recommendations from database
        cur.execute("""
            SELECT DISTINCT ON (source_scholarship_id)
                user_id, source_scholarship_id, recommended_scholarship_id, 
                similarity_score, timestamp
            FROM recommendations_table
            WHERE user_id = %s
            ORDER BY source_scholarship_id, timestamp DESC
            LIMIT %s
        """, (user_id, limit))
        
        recommendations = cur.fetchall()
        
        if not recommendations:
            raise HTTPException(status_code=404, detail="No recommendations found")
            
        return recommendations
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()

@app.get("/recommendations/scholarship/{scholarship_id}", response_model=RecommendationResponse)
async def get_scholarship_recommendations(
    scholarship_id: str,
    user_id: Optional[str] = None,
    use_cache: bool = True
):
    """Get recommendations for a specific scholarship with optional user context"""
    redis_client = get_redis_client()
    
    try:
        # Check cache first if enabled
        if use_cache:
            cache_key = f"recommendations:{user_id}:{scholarship_id}" if user_id else f"recommendations:general:{scholarship_id}"
            cached_data = redis_client.get(cache_key)
            
            if cached_data:
                recommendations = json.loads(cached_data)
                return RecommendationResponse(
                    user_id=user_id or "general",
                    source_scholarship_id=scholarship_id,
                    recommendations=recommendations,
                    is_cached=True
                )
        
        # If no cache or cache disabled, get from database
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = """
            SELECT r.*, s.metadata
            FROM recommendations_table r
            LEFT JOIN scholarships s ON r.recommended_scholarship_id = s.scholarship_id
            WHERE r.source_scholarship_id = %s
            ORDER BY r.timestamp DESC
            LIMIT 5
        """
        params = [scholarship_id]
        
        if user_id:
            query = query.replace("WHERE", "WHERE r.user_id = %s AND")
            params.insert(0, user_id)
            
        cur.execute(query, params)
        recommendations = cur.fetchall()
        
        if not recommendations:
            raise HTTPException(
                status_code=404,
                detail="No recommendations found for this scholarship"
            )
            
        # Format recommendations
        recommendation_list = [
            Recommendation(
                scholarship_id=rec['recommended_scholarship_id'],
                similarity_score=float(rec['similarity_score']),
                metadata=rec['metadata']
            )
            for rec in recommendations
        ]
        
        response = RecommendationResponse(
            user_id=user_id or "general",
            source_scholarship_id=scholarship_id,
            recommendations=recommendation_list,
            is_cached=False
        )
        
        # Cache the response if caching is enabled
        if use_cache:
            cache_key = f"recommendations:{user_id}:{scholarship_id}" if user_id else f"recommendations:general:{scholarship_id}"
            redis_client.setex(
                cache_key,
                3600,  # 1 hour cache
                json.dumps([rec.dict() for rec in recommendation_list])
            )
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        redis_client.close()
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@app.post("/recommendations/{user_id}/{scholarship_id}/clicked")
async def track_recommendation_click(user_id: str, scholarship_id: str):
    """Track when a user clicks on a recommended scholarship"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE recommendations_table
            SET clicked = TRUE
            WHERE user_id = %s AND recommended_scholarship_id = %s
            RETURNING id
        """, (user_id, scholarship_id))
        
        if cur.rowcount == 0:
            raise HTTPException(
                status_code=404,
                detail="Recommendation not found"
            )
            
        conn.commit()
        return {"message": "Click tracked successfully"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close() 