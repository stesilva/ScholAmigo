from neo4j import GraphDatabase

#Functions to connect to the Neo4j database and perform commands
class ConnectorNeo4j:
    def __init__(self, uri, user, password):
        self._uri = uri
        self._auth = (user,password)
        self._driver = None

    def connect(self):
        self._driver = GraphDatabase.driver(self._uri, auth=self._auth)

        try:
            self._driver.verify_connectivity()
            print("Connected to Neo4j successfully!")
        except Exception as e:
            print(f"Failed to connect to Neo4j: {e}")

    def close(self):
        if self._driver is not None:
            self._driver.close()
            print("Connection closed.")

    def create_session(self):
        session = self._driver.session()
        print("Neo4j session created.")
        return session

    def clear_session(self,session):
       session.run(
        "MATCH (n) DETACH DELETE n"
        )
       print("Cleared all nodes and relationships in the database.")

