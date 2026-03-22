import sys
import os
from pathlib import Path
import logging
import pandas as pd
import json
from neo4j import GraphDatabase
import time

# Ensure the root directory is in the system path to allow config import
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

import config

# Now, we can import from the unified_code directory
sys.path.append(str(Path(__file__).parent / "unified_code"))
from pipeline_5_populate_embeddings import create_vector_indexes, populate_embeddings

# --- Configuration ---
BASE_DIR = Path(__file__).parent
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)
CHUNKED_GRAPH_DIR = BASE_DIR / "output" / "5_chunked_knowledge_graph"
NODES_FILE = CHUNKED_GRAPH_DIR / "nodes_chunked.jsonl"
EDGES_FILE = CHUNKED_GRAPH_DIR / "edges_chunked.csv"

# --- Setup Logging ---
log_file_path = LOGS_DIR / "load_and_embed.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def delete_all_data(driver):
    """Deletes all nodes and relationships from the Neo4j database."""
    logger.info("--- Deleting all existing data from the database ---")
    with driver.session(database="neo4j") as session:
        session.run("MATCH (n) DETACH DELETE n;")
    logger.info("Database cleared successfully.")

def get_node_labels(nodes_path):
    """Scans the nodes file to get a set of unique labels."""
    labels = set()
    with open(nodes_path, 'r', encoding='utf-8') as f:
        for line in f:
            node = json.loads(line)
            labels.add(node.get('label'))
    return [label for label in labels if label]

def create_constraints(driver, node_labels):
    """Creates uniqueness constraints on node UIDs for each label."""
    logger.info("--- Creating database constraints ---")
    with driver.session(database="neo4j") as session:
        for label in node_labels:
            query = f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.uid IS UNIQUE;"
            session.run(query)
            logger.info(f"Constraint created for label: {label}")

def load_nodes_in_batches(driver, nodes_path):
    """Loads nodes from a JSONL file in batches, sanitizing complex properties."""
    logger.info(f"--- Loading nodes from {nodes_path} ---")
    
    nodes_by_label = {}
    with open(nodes_path, 'r', encoding='utf-8') as f:
        for line in f:
            node = json.loads(line)
            label = node.get('label')
            if not label:
                continue
            if label not in nodes_by_label:
                nodes_by_label[label] = []
            
            node_data = {'uid': node['uid']}
            properties = node.get('properties', {})

            # Sanitize properties: Neo4j cannot store nested dictionaries.
            # We convert any dict or list properties to a JSON string.
            for key, value in properties.items():
                if isinstance(value, (dict, list)):
                    properties[key] = json.dumps(value, ensure_ascii=False)
            
            node_data.update(properties)
            nodes_by_label[label].append(node_data)
    
    # Process each label group in a single transaction for efficiency
    with driver.session(database="neo4j") as session:
        for label, nodes in nodes_by_label.items():
            logger.info(f"Loading {len(nodes)} nodes with label '{label}'...")
            query = f"""
            UNWIND $nodes AS node_properties
            MERGE (n:{label} {{uid: node_properties.uid}})
            SET n += node_properties
            """
            session.run(query, nodes=nodes)
            
    logger.info("All nodes loaded successfully.")

def load_edges_in_batches(driver, edges_path):
    """
    Loads edges from a CSV file in batches.
    NOTE: This requires the APOC plugin for dynamic relationship types.
    """
    logger.info(f"--- Loading edges from {edges_path} ---")
    logger.warning("This step requires the APOC plugin to be installed in Neo4j.")
    
    edges_df = pd.read_csv(edges_path)
    edges = edges_df.to_dict('records')
    
    if not edges:
        logger.info("No edges to load.")
        return

    query = """
    UNWIND $edges AS edge
    MATCH (startNode {uid: edge.start_node_uid})
    MATCH (endNode {uid: edge.end_node_uid})
    CALL apoc.create.relationship(
        startNode, 
        edge.relationship_type, 
        {}, 
        endNode
    ) YIELD rel
    RETURN count(rel)
    """
    
    with driver.session(database="neo4j") as session:
        # Process in chunks to avoid memory issues with large CSVs
        batch_size = 10000
        for i in range(0, len(edges), batch_size):
            batch = edges[i:i + batch_size]
            session.run(query, edges=batch)
            logger.info(f"Loaded edge batch {i//batch_size + 1}...")
            
    logger.info("All edges loaded successfully.")

def main():
    """Main function to orchestrate loading and embedding."""
    logger.info("===== STARTING CHUNKED GRAPH LOAD AND EMBEDDING PIPELINE =====")
    
    if not (NODES_FILE.exists() and EDGES_FILE.exists()):
        logger.error(f"Chunked graph files not found in '{CHUNKED_GRAPH_DIR}'. Halting.")
        return

    driver = None
    try:
        # Establish Neo4j connection
        driver = GraphDatabase.driver(
            config.NEO4J_URI,
            auth=(config.NEO4J_USERNAME, config.NEO4J_PASSWORD)
        )
        driver.verify_connectivity()
        logger.info("Successfully connected to Neo4j.")

        # 1. Clear the database
        delete_all_data(driver)

        # 2. Create constraints
        node_labels = get_node_labels(NODES_FILE)
        create_constraints(driver, node_labels)
        
        # 3. Load nodes and edges
        load_nodes_in_batches(driver, NODES_FILE)
        load_edges_in_batches(driver, EDGES_FILE)

        # 4. Create vector indexes
        logger.info("--- Creating Vector Indexes ---")
        create_vector_indexes(driver)

        # 5. Populate embeddings for the newly loaded nodes
        logger.info("--- Populating Embeddings ---")
        populate_embeddings(driver)

        logger.info("===== SCRIPT COMPLETE: GRAPH LOADED AND EMBEDDED SUCCESSFULLY =====")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if driver:
            driver.close()
            logger.info("Neo4j connection closed.")

if __name__ == "__main__":
    main() 