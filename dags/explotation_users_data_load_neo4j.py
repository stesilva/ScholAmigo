from connector_neo4j import ConnectorNeo4j
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

#Functions to create nodes and edges for each CSV file containing the generated and fetched data
#Created according to desing (assets/USERS_GRAPH_DESIGN.png)

def create_constrainsts(session):
    session.run("""CREATE CONSTRAINT unique_person_email IF NOT EXISTS
                    FOR (p:Person)
                    REQUIRE p.email IS UNIQUE""") #unique email within any user

    session.run("""CREATE CONSTRAINT unique_scholarship_name IF NOT EXISTS
        FOR (s:Scholarship)
        REQUIRE s.scholarship_name IS UNIQUE""") #unique scholarships
    
    session.run("""CREATE CONSTRAINT unique_country_name IF NOT EXISTS
        FOR (c:Country)
        REQUIRE c.country_name IS UNIQUE""") #unique countries
   
    session.run("""CREATE CONSTRAINT unique_language_name IF NOT EXISTS
        FOR (l:Language)
        REQUIRE l.language_name IS UNIQUE""") #unique language
    
    session.run("""CREATE CONSTRAINT unique_skill_name IF NOT EXISTS
        FOR (s:Skill)
        REQUIRE s.skill_name IS UNIQUE""") #unique skill name
    
    session.run("""CREATE CONSTRAINT unique_program_name IF NOT EXISTS
        FOR (dp:DegreeProgram)
        REQUIRE dp.program_name IS UNIQUE""") #unique program name (education)
    
    session.run("""CREATE CONSTRAINT unique_certification_name IF NOT EXISTS
        FOR (c:Certification)
        REQUIRE c.certification_name IS UNIQUE""") #unique certification name    
    
    session.run("""CREATE CONSTRAINT unique_honor_name IF NOT EXISTS
        FOR (h:Honor)
        REQUIRE h.honor_name IS UNIQUE""") #unique honor name     
    
    session.run("""CREATE CONSTRAINT unique_company_name IF NOT EXISTS
        FOR (c:Company)
        REQUIRE c.company_name IS UNIQUE""") #unique company name      
    
#basic user information
def load_user(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///user_basic_information.csv" AS row
        FIELDTERMINATOR ';'
        MERGE (person:Person {email: trim(row.Email)})
        SET person.name = trim(row.Name),
            person.age = toInteger(row.Age),
            person.gender = trim(row.Gender),
            person.plan = trim(row.Plan)"""
    )
    
        
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///user_basic_information.csv" AS row
        FIELDTERMINATOR ';'
        WITH row
        MERGE (person:Person {email: trim(row.Email)})
        MERGE (country:Country {country_name: trim(row.Country)})
        MERGE (person)-[rel:LIVES_IN]->(country)"""
    )

#basic alumni information
def load_alumni(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///alumni_basic_information.csv" AS row
        FIELDTERMINATOR ';'
        WITH row
        MERGE (person:Person {email: trim(row.Email)})
        SET person.name = trim(row.Name),
            person.age = toInteger(row.Age),
            person.gender = trim(row.Gender),
            person.status = trim(row.Status)
        MERGE (scholarship:Scholarship {scholarship_name: trim(row.Associeted_Scholarship)})
        MERGE (person)-[rel:HAS_SCHOLARSHIP]->(scholarship)
        SET rel.year_recipient = toInteger(row.Year_Scholarship)"""
    )
    
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///alumni_basic_information.csv" AS row
        FIELDTERMINATOR ';'
        WITH row
        MERGE (person:Person {email: trim(row.Email)})
        MERGE (country:Country {country_name: trim(row.Country)})
        MERGE (person)-[rel:LIVES_IN]->(country)"""
    )
    
#languages
def load_languages(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///languages.csv" AS row
        FIELDTERMINATOR ';'
        WITH row
        MERGE (person:Person {email: trim(row.email)})
        MERGE (language:Language {language_name: trim(row.language)})
        MERGE (person)-[rel:SPEAKS]->(language)
        SET rel.level = trim(row.level)"""
    )

#skills
def load_skills(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///skills.csv" AS row
        FIELDTERMINATOR ';'
        WITH row
        MERGE (person:Person {email: trim(row.email)})
        MERGE (skill:Skill {skill_name: trim(row.skill)})
        MERGE (person)-[rel:HAS_SKILL]->(skill)"""
    )
    
#education
def load_education(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///education.csv" AS row
        FIELDTERMINATOR ';'
        WITH row
        MERGE (person:Person {email: trim(row.email)})
        MERGE (education:DegreeProgram {program_name: trim(row.program_name)})
        MERGE (person)-[rel:HAS_DEGREE]->(education)
        SET rel.degree = trim(row.degree),
            rel.institution = trim(row.institution),
            rel.graduation_year = toInteger(row.graduation_year)"""
    )

#certificates
def load_certifications(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///certificates.csv" AS row
        FIELDTERMINATOR ';'
        WITH row
        MERGE (person:Person {email: trim(row.email)})
        MERGE (certification:Certification {certification_name: trim(row.certification_name)})
        MERGE (person)-[rel:HAS_CERTIFICATION]->(certification)
        SET rel.associated_with = trim(row.associated_with),
            rel.date = toInteger(row.date)"""
    )
    
#honors
def load_honors(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///honors.csv" AS row
        FIELDTERMINATOR ';'
        WITH row
        MERGE (person:Person {email: trim(row.email)})
        MERGE (honor:Honor {honor_name: trim(row.honor_name)})
        MERGE (person)-[rel:HAS_HONOR]->(honor)"""
    )


#experiences
def load_experiences(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///experiences.csv" AS row
        FIELDTERMINATOR ';'
        WITH row
        MERGE (person:Person {email: trim(row.email)})
        MERGE (company:Company {company_name: trim(row.company_name)})
        MERGE (person)-[rel:WORKED_AT]->(company)
        SET rel.role = trim(row.role),
            rel.duration = toInteger(row.duration)"""
    )

def connect_load_neo4j(uri,user,password, db_name):
    connector = ConnectorNeo4j(uri, user, password, db_name)
    connector.connect()
    session = connector.create_session()
    connector.clear_session(session)

    logger.info("Creating and loading nodes and edges ...")

    session.execute_write(create_constrainsts)
    session.execute_write(load_user)
    session.execute_write(load_alumni)
    session.execute_write(load_languages)
    session.execute_write(load_skills)
    session.execute_write(load_education)
    session.execute_write(load_certifications)
    session.execute_write(load_honors)
    session.execute_write(load_experiences)

    print('Creation and loading completed with successes.')

    connector.close()