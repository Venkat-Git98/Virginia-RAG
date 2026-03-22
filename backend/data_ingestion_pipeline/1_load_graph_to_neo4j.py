import os
import csv
import json
from neo4j import GraphDatabase
import logging
import re

# --- Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_credentials_from_env(env_path='.env'):
    """Manually parses the .env file to ensure the latest values are used."""
    if not os.path.exists(env_path):
        logging.error(f"FATAL: .env file not found at {env_path}")
        return None, None, None
    
    config = {}
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                # Simple parsing, handles potential quotes
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip().strip("'\"")
    
    logging.info(f"Manually loaded credentials from {env_path}")
    return config.get("NEO4J_URI"), config.get("NEO4J_USERNAME"), config.get("NEO4J_PASSWORD")

# Define the location of your FINAL, CHUNKED graph files
GRAPH_DATA_DIR = "data_ingestion/output/5_chunked_knowledge_graph"
NODES_FILE = os.path.join(GRAPH_DATA_DIR, "nodes_chunked.jsonl")
EDGES_FILE = os.path.join(GRAPH_DATA_DIR, "edges_chunked.csv")

# Define all possible node labels that your pipeline can create
ALL_NODE_LABELS = ["Chapter", "Section", "Subsection", "Table", "Diagram", "Math", "Standard", "Passage"]

def load_data_to_neo4j(driver):
    """
    Clears the database and loads nodes and edges from the pipeline output.
    """
    # 1. Clear the entire database (as requested in the reference script)
    with driver.session(database="neo4j") as session:
        logging.info("Clearing existing database...")
        session.run("MATCH (n) DETACH DELETE n")
        logging.info("Database cleared.")

    # 2. Create uniqueness constraints for all node labels for performance and data integrity
    with driver.session(database="neo4j") as session:
        logging.info("Creating uniqueness constraints...")
        for label in ALL_NODE_LABELS:
            session.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.uid IS UNIQUE;")
        logging.info("Constraints created successfully.")

    # 3. Load Nodes
    with driver.session(database="neo4j") as session:
        logging.info(f"Loading nodes from '{NODES_FILE}'...")
        with open(NODES_FILE, 'r', encoding='utf-8') as f:
            nodes_data = [json.loads(line) for line in f]
        
        # Pre-process nodes to serialize complex properties to JSON strings
        for node in nodes_data:
            for key, value in node['properties'].items():
                if isinstance(value, (dict, list)):
                    node['properties'][key] = json.dumps(value, ensure_ascii=False)

        # This query unpacks the JSON properties and dynamically sets the label
        cypher_query = """
        UNWIND $rows AS row
        MERGE (n {uid: row.uid})
        SET n += row.properties
        WITH n, row
        CALL apoc.create.setLabels(n, [row.label]) YIELD node
        RETURN count(node) AS created_nodes
        """
        result = session.run(cypher_query, rows=nodes_data)
        logging.info(f"Node loading complete. Processed {result.single()['created_nodes']} nodes.")

    # 4. Load Edges
    with driver.session(database="neo4j") as session:
        logging.info(f"Loading relationships from '{EDGES_FILE}'...")
        with open(EDGES_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            edges_data = list(reader)
        
        # This query parses the properties JSON string and uses it to create the relationship
        cypher_query = """
        UNWIND $rows AS row
        MATCH (start_node {uid: row.start_node_uid})
        MATCH (end_node {uid: row.end_node_uid})
        WITH start_node, end_node, row.relationship_type AS rel_type, apoc.convert.fromJsonMap(row.properties) AS props
        CALL apoc.merge.relationship(start_node, rel_type, {}, props, end_node) YIELD rel
        RETURN count(rel) AS created_rels
        """
        result = session.run(cypher_query, rows=edges_data)
        logging.info(f"Relationship loading complete. Processed {result.single()['created_rels']} edges.")

if __name__ == "__main__":
    uri, user, password = get_credentials_from_env()

    # Debugging: Log the loaded URI to confirm it's the correct one
    if uri:
        logging.info(f"Attempting to connect to Neo4j with URI: {uri[:25]}...")
    else:
        logging.error("NEO4J_URI not found in environment variables. Please check your .env file.")

    if not all([uri, user, password]):
        raise ValueError("Neo4j credentials not found in .env file. Please create or update it.")

    if not uri.startswith("neo4j+s://") and not uri.startswith("neo4j://"):
        logging.warning(
            "Your Neo4j URI does not start with 'neo4j+s://' or 'neo4j://'. "
            "If you are using a Neo4j Aura (cloud) instance, please ensure you use the correct URI from your dashboard."
        )

    try:
        # Added connection timeout and max lifetime for better stability with cloud instances
        driver = GraphDatabase.driver(
            uri, 
            auth=(user, password),
            max_connection_lifetime=30 * 60,  # 30 minutes
            connection_timeout=20  # 20 seconds
        )
        driver.verify_connectivity()
        logging.info("Successfully connected to Neo4j.")
        
        load_data_to_neo4j(driver)
        
        logging.info("\n✅ Graph structure loading finished successfully.")
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if 'driver' in locals() and driver:
            driver.close() 