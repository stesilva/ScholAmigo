import logging
import os
from connector_neo4j import ConnectorNeo4j
from dotenv import load_dotenv
load_dotenv()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("user-analytics-pipeline")


#Load variables
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USERNAME")
password = os.getenv("NEO4J_PASSWORD")
db_name = os.getenv("NEO4J_DB")    
    
#ANALYTICS QUERIES
def get_popular_countries(session, top_n: int = 10):
    try:
        result = session.run("""
            MATCH (p:Person)-[:LIVES_IN]->(c:Country)
            RETURN c.country_name AS country, count(*) AS num_users
            ORDER BY num_users DESC
            LIMIT $top_n
        """, top_n=top_n)
        logger.info("Most popular countries:")
        for record in result:
            logger.info(f"{record['country']}: {record['num_users']}")
    except Exception as e:
        logger.error(f"Failed to get popular countries: {e}")

def get_common_skills(session, top_n: int = 10):
    try:
        result = session.run("""
            MATCH (p:Person)-[:HAS_SKILL]->(s:Skill)
            RETURN s.skill_name AS skill, count(*) AS num_people
            ORDER BY num_people DESC
            LIMIT $top_n
        """, top_n=top_n)
        logger.info("Most common skills:")
        for record in result:
            logger.info(f"{record['skill']}: {record['num_people']}")
    except Exception as e:
        logger.error(f"Failed to get common skills: {e}")
        
def get_top_related_skills(session, skill_name: str, top_n: int = 3):
    try:
        result = session.run("""
            MATCH (p:Person)-[:HAS_SKILL]->(s:Skill {skill_name: $skill_name})
            MATCH (p)-[:HAS_SKILL]->(other:Skill)
            WHERE other.skill_name <> $skill_name
            RETURN other.skill_name AS related_skill, count(*) AS co_occurrence
            ORDER BY co_occurrence DESC
            LIMIT $top_n
        """, skill_name=skill_name, top_n=top_n)
        logger.info(f"Top related skills for {skill_name}:")
        for record in result:
            logger.info(f"{record['related_skill']}: {record['co_occurrence']}")
    except Exception as e:
        logger.error(f"Failed to get related skills for {skill_name}: {e}")

def get_top_countries_for_skill(session, skill_name: str, top_n: int = 3):
    try:
        result = session.run("""
            MATCH (p:Person)-[:HAS_SKILL]->(s:Skill {skill_name: $skill_name})
            MATCH (p)-[:LIVES_IN]->(c:Country)
            RETURN c.country_name AS country, count(*) AS num_people
            ORDER BY num_people DESC
            LIMIT $top_n
        """, skill_name=skill_name, top_n=top_n)
        logger.info(f"Top countries for skill {skill_name}:")
        for record in result:
            logger.info(f"{record['country']}: {record['num_people']}")
    except Exception as e:
        logger.error(f"Failed to get top countries for skill {skill_name}: {e}")
        
        
def main():
    connector = ConnectorNeo4j(uri, user, password, db_name)
    connector.connect()
    session = connector.create_session()
    
    get_popular_countries(session,top_n=10)
    get_common_skills(session,top_n=10)
    
    #for a given skill, get top countries and related skills
    skill = "python"
    get_top_countries_for_skill(session, skill, top_n=3)
    get_top_related_skills(session, skill, top_n=3)
    
    connector.close()

if __name__ == "__main__":
    main()
