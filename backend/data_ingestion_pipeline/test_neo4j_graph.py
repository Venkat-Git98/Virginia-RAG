import os
import json
from neo4j import GraphDatabase
import logging

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
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip().strip("'\"")
    
    logging.info(f"Manually loaded credentials from {env_path}")
    return config.get("NEO4J_URI"), config.get("NEO4J_USERNAME"), config.get("NEO4J_PASSWORD")

def fetch_section_graph(driver, section_prefix: str, output_file: str, max_depth: int = 2):
    """
    Fetches a subgraph centered around a specific section prefix and writes it to a JSON file.
    """
    cypher_query = """
    MATCH (n) WHERE n.uid STARTS WITH $section_prefix
    // Use APOC to get the subgraph around all matching starting nodes
    CALL apoc.path.subgraphAll(n, {maxLevel: $max_depth}) YIELD nodes, relationships
    // Collect all unique nodes and relationships from all the subgraphs found
    WITH COLLECT(nodes) AS node_collections, COLLECT(relationships) AS rel_collections
    UNWIND node_collections AS collection
    UNWIND collection AS node
    WITH COLLECT(DISTINCT node) AS unique_nodes, rel_collections
    UNWIND rel_collections AS collection
    UNWIND collection AS rel
    WITH unique_nodes, COLLECT(DISTINCT rel) AS unique_rels
    // Format the nodes and relationships for clean JSON output
    // Exclude the 'embedding' property from the nodes for readability
    WITH [n IN unique_nodes | {uid: n.uid, labels: labels(n), properties: apoc.map.removeKey(properties(n), 'embedding')}] AS nodes,
         [r IN unique_rels | {
             uid: elementId(r), 
             type: type(r), 
             properties: properties(r), 
             startNode: startNode(r).uid, 
             endNode: endNode(r).uid
         }] AS relationships
    RETURN nodes, relationships
    """
    
    with driver.session(database="neo4j") as session:
        logging.info(f"Executing query for section prefix: '{section_prefix}' with max depth: {max_depth}")
        result = session.run(cypher_query, section_prefix=section_prefix, max_depth=max_depth)
        data = result.single()
        
        if not data:
            logging.warning(f"No data found for section prefix '{section_prefix}'.")
            graph_data = {"nodes": [], "relationships": []}
        else:
            graph_data = data.data()
            logging.info(f"Found {len(graph_data['nodes'])} nodes and {len(graph_data['relationships'])} relationships.")
            
    logging.info(f"Writing graph data to '{output_file}'...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(graph_data, f, indent=2, ensure_ascii=False)
    logging.info("Write complete.")

if __name__ == "__main__":
    # --- Configuration ---
    SECTION_TO_QUERY = "1607"
    OUTPUT_JSON_FILE = "test.json"
    
    uri, user, password = get_credentials_from_env()

    if uri:
        logging.info(f"Attempting to connect to Neo4j with URI: {uri[:25]}...")
    else:
        logging.error("NEO4J_URI not found in environment variables. Please check your .env file.")

    if not all([uri, user, password]):
        raise ValueError("Neo4j credentials not found. Please check your .env file.")
    
    driver = None
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        logging.info("Successfully connected to Neo4j.")
        
        fetch_section_graph(driver, SECTION_TO_QUERY, OUTPUT_JSON_FILE)
        
        logging.info(f"\n✅ Graph test for section '{SECTION_TO_QUERY}' complete. Output saved to '{OUTPUT_JSON_FILE}'.")
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if driver:
            driver.close() 