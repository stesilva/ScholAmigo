import logging

logger = logging.getLogger(__name__)

def create_constrainsts(session):
    """Create Neo4j constraints"""
    constraints = [
        ("""CREATE CONSTRAINT unique_person_email IF NOT EXISTS
            FOR (p:Person) REQUIRE p.email IS UNIQUE"""),
        ("""CREATE CONSTRAINT unique_scholarship_name IF NOT EXISTS
            FOR (s:Scholarship) REQUIRE s.scholarship_name IS UNIQUE"""),
        ("""CREATE CONSTRAINT unique_country_name IF NOT EXISTS
            FOR (c:Country) REQUIRE c.country_name IS UNIQUE"""),
        ("""CREATE CONSTRAINT unique_language_name IF NOT EXISTS
            FOR (l:Language) REQUIRE l.language_name IS UNIQUE"""),
        ("""CREATE CONSTRAINT unique_skill_name IF NOT EXISTS
            FOR (s:Skill) REQUIRE s.skill_name IS UNIQUE"""),
        ("""CREATE CONSTRAINT unique_program_name IF NOT EXISTS
            FOR (dp:DegreeProgram) REQUIRE dp.program_name IS UNIQUE"""),
        ("""CREATE CONSTRAINT unique_certification_name IF NOT EXISTS
            FOR (c:Certification) REQUIRE c.certification_name IS UNIQUE"""),
        ("""CREATE CONSTRAINT unique_honor_name IF NOT EXISTS
            FOR (h:Honor) REQUIRE h.honor_name IS UNIQUE"""),
        ("""CREATE CONSTRAINT unique_company_name IF NOT EXISTS
            FOR (c:Company) REQUIRE c.company_name IS UNIQUE""")
    ]
    
    for constraint in constraints:
        try:
            session.run(constraint)
            logger.info(f"Created constraint: {constraint[:50]}...")
        except Exception as e:
            logger.warning(f"Error creating constraint: {str(e)}")

def load_user(session):
    """Load basic user information"""
    logger.info("Loading user nodes and relationships...")
    
    # Create Person nodes with basic information
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///user_basic_information.csv" AS row
        FIELDTERMINATOR ';'
        MERGE (person:Person {email: trim(row.Email)})
        SET person.name = trim(row.Name),
            person.age = toInteger(row.Age),
            person.gender = trim(row.Gender),
            person.plan = trim(row.Plan)"""
    )
    
    # Create Country relationships
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///user_basic_information.csv" AS row
        FIELDTERMINATOR ';'
        WITH row
        MERGE (person:Person {email: trim(row.Email)})
        MERGE (country:Country {country_name: trim(row.Country)})
        MERGE (person)-[rel:LIVES_IN]->(country)"""
    )

def load_alumni(session):
    """Load alumni information"""
    logger.info("Loading alumni nodes and relationships...")
    
    # Create Person and Scholarship relationships
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
    
    # Create Country relationships
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///alumni_basic_information.csv" AS row
        FIELDTERMINATOR ';'
        WITH row
        MERGE (person:Person {email: trim(row.Email)})
        MERGE (country:Country {country_name: trim(row.Country)})
        MERGE (person)-[rel:LIVES_IN]->(country)"""
    )

def load_languages(session):
    """Load language information"""
    logger.info("Loading language relationships...")
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///languages.csv" AS row
        FIELDTERMINATOR ';'
        WITH row
        MERGE (person:Person {email: trim(row.email)})
        MERGE (language:Language {language_name: trim(row.language)})
        MERGE (person)-[rel:SPEAKS]->(language)
        SET rel.level = trim(row.level)"""
    )

def load_skills(session):
    """Load skills information"""
    logger.info("Loading skill relationships...")
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///skills.csv" AS row
        FIELDTERMINATOR ';'
        WITH row
        MERGE (person:Person {email: trim(row.email)})
        MERGE (skill:Skill {skill_name: trim(row.skill)})
        MERGE (person)-[rel:HAS_SKILL]->(skill)"""
    )

def load_education(session):
    """Load education information"""
    logger.info("Loading education relationships...")
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

def load_certifications(session):
    """Load certification information"""
    logger.info("Loading certification relationships...")
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

def load_honors(session):
    """Load honors information"""
    logger.info("Loading honor relationships...")
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///honors.csv" AS row
        FIELDTERMINATOR ';'
        WITH row
        MERGE (person:Person {email: trim(row.email)})
        MERGE (honor:Honor {honor_name: trim(row.honor_name)})
        MERGE (person)-[rel:HAS_HONOR]->(honor)"""
    )

def load_experiences(session):
    """Load experience information"""
    logger.info("Loading experience relationships...")
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