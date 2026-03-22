import os
import logging
from neo4j import GraphDatabase
from typing import List

# --- Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_credentials_from_env(env_path='.env'):
    """Manually parses the .env file to get Neo4j credentials."""
    if not os.path.exists(env_path):
        logging.error(f"FATAL: .env file not found at {env_path}")
        return None, None, None
    
    config = {}
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                try:
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip().strip("'\"")
                except ValueError:
                    logging.warning(f"Could not parse line: {line}")
    
    return (
        config.get("NEO4J_URI"), 
        config.get("NEO4J_USERNAME"), 
        config.get("NEO4J_PASSWORD")
    )

def verify_embeddings(driver, labels: List[str]):
    """
    Checks the total count of nodes vs. the count of nodes with embeddings for given labels.
    """
    logging.info("--- Starting Embedding Verification ---")
    with driver.session(database="neo4j") as session:
        for label in labels:
            logging.info(f"\nVerifying label: :{label}")
            
            # Get total count of nodes with the label
            total_result = session.run(f"MATCH (n:{label}) RETURN count(n) AS total")
            total_count = total_result.single()['total']
            
            # Get count of nodes with the label AND a non-null embedding
            embedded_result = session.run(f"MATCH (n:{label}) WHERE n.embedding IS NOT NULL RETURN count(n) AS embedded_count")
            embedded_count = embedded_result.single()['embedded_count']
            
            logging.info(f"  - Total '{label}' nodes found: {total_count}")
            logging.info(f"  - Nodes with embeddings: {embedded_count}")
            
            if total_count == 0:
                logging.warning(f"  - No nodes found for label :{label}. Skipping sample check.")
                continue

            if embedded_count == 0:
                logging.error(f"  - FAILURE: 0 nodes have embeddings for label :{label}.")
                continue
            
            if embedded_count < total_count:
                logging.warning(f"  - WARNING: {total_count - embedded_count} node(s) of type :{label} are missing embeddings.")
            else:
                logging.info(f"  - SUCCESS: All {total_count} nodes of type :{label} have embeddings.")

            # Fetch a sample embedding to display
            sample_result = session.run(f"MATCH (n:{label}) WHERE n.embedding IS NOT NULL RETURN n.embedding AS embedding LIMIT 1")
            sample_embedding = sample_result.single()['embedding']
            if sample_embedding:
                logging.info(f"  - Sample embedding vector (first 5 values): {sample_embedding[:5]}...")
                logging.info(f"  - Vector dimension: {len(sample_embedding)}")

def main():
    uri, user, password = get_credentials_from_env()
    if not all([uri, user, password]):
        raise ValueError("Could not find all necessary Neo4j credentials in .env file.")

    driver = None
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        logging.info("Successfully connected to Neo4j.")
        
        labels_to_check = ["Passage", "Table", "Diagram"]
        verify_embeddings(driver, labels_to_check)
        
        logging.info("\n✅ Verification complete.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if driver:
            driver.close()
            logging.info("Neo4j connection closed.")

if __name__ == "__main__":
    main() 