from connector_neo4j import ConnectorNeo4j
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

#Functions to create nodes and edges for each CSV file containing the generated and fetched data
#Created according to desing version 1 (assets/graphDesignVersion1.png)

def load_node_paper(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///papers.csv" AS row
            CREATE (paper:Paper {
            paperDOI: trim(row.doi), 
            title: trim(row.title), 
            abstract: trim(row.abstract), 
            citationCount: toInteger(row.citationCount)})"""
    )     
    print('Created node for paper.')

def load_node_author(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///authors.csv" AS row
            CREATE (author:Author {
            authorID: trim(row.authorID), 
            name: trim(row.name)})"""
    )     
    print('Created node for author.')

def load_node_journal(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///journals.csv" AS row
            CREATE (journal:Journal {
            journalID: trim(row.journalID), 
            name: trim(row.name)})"""
    )      
    print('Created node for journal.')

def load_node_conference_workshop(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///conferences.csv" AS row
            CREATE (conferenceworkshop:ConferenceWorkshop {
            conferenceWorkshopID: trim(row.conferenceID), 
            name: trim(row.name),
            type: trim(row.type)})"""
    )      
    print('Created node for conference/workshop.')

def load_node_keyword(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///keywords.csv" AS row
            CREATE (keyword:Keyword {
            keyword: trim(row.keyword)})"""
    )      
    print('Created node for keyword.')


def load_edge_paper_authored_author(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///author_paper_relations.csv' AS row
            MATCH (paper:Paper {paperDOI: row.paperDOI})
            MATCH (author:Author {authorID: row.authorID})
            MERGE (paper)-[a:AUTHORED_BY {corresponding_author: row.correspondingAuthor}]->(author)"""
    )      
    print('Created edge for the relationship AUTHORED_BY')

def load_edge_paper_reviewed_author(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///reviewer_paper_relations.csv' AS row
            MATCH (paper:Paper {paperDOI: row.paperDOI})
            MATCH (author:Author {authorID: row.authorID})
            MERGE (paper)-[a:REVIEWED_BY]->(author)"""
    )      
    print('Created edge for the relationship REVIEWED_BY')

def load_edge_paper_has_keyword(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///keyword_paper_relations.csv' AS row
            MATCH (paper:Paper {paperDOI: row.paperDOI})
            MATCH (keyword:Keyword {keyword: row.keyword})
            MERGE (paper)-[hk:HAS_KEYWORD]->(keyword)"""
    )      
    print('Created edge for the relationship HAS_KEYWORD')

def load_edge_paper_cites_paper(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///citations.csv' AS row
            MATCH (paper:Paper {paperDOI: row.paperDOI})
            MATCH (citedPaper:Paper {paperDOI: row.citedPaperDOI})
            MERGE (paper)-[c:CITES]->(citedPaper)"""
    )      
    print('Created edge for the relationship CITES')

def load_edge_paper_presentedin_conference_workshop(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///conference_paper_relations.csv' AS row
            MATCH (paper:Paper {paperDOI: row.paperDOI})
            MATCH (conferenceWorkshop:ConferenceWorkshop {conferenceWorkshopID: row.conferenceID})
            MERGE (paper)-[a:PRESENTED_IN {edition: toInteger(row.edition), year: toInteger(row.year), city: row.city}]->(conferenceWorkshop)"""
    )      
    print('Created edge for the relationship PRESENTED_IN')

def load_edge_paper_publishedin_journal(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///journal_paper_relations.csv' AS row
            MATCH (paper:Paper {paperDOI: row.paperDOI})
            MATCH (journal:Journal {journalID: row.journalID})
            MATCH (journal:Journal {journalID: row.journalID})
            MERGE (paper)-[a:PUBLISHED_IN {year: toInteger(row.year), volume: toInteger(row.volume)}]->(journal)"""
    )      
    print('Created edge for the relationship PUBLISHED_IN')

def connect_load_neo4j(uri,user,password):
    connector = ConnectorNeo4j(uri, user, password)
    connector.connect()
    session = connector.create_session()
    connector.clear_session(session)

    logger.info("Creating and loading nodes and edges ...")

    session.execute_write(load_node_paper)
    session.execute_write(load_node_author)
    session.execute_write(load_node_journal)
    session.execute_write(load_node_conference_workshop)
    session.execute_write(load_node_keyword)

    session.execute_write(load_edge_paper_authored_author)
    session.execute_write(load_edge_paper_reviewed_author)
    session.execute_write(load_edge_paper_cites_paper)
    session.execute_write(load_edge_paper_has_keyword)
    session.execute_write(load_edge_paper_presentedin_conference_workshop)
    session.execute_write(load_edge_paper_publishedin_journal)

    print('Creation and loading completed with successes.')

    connector.close()