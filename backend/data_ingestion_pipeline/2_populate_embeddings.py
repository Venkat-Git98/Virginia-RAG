import os
import google.generativeai as genai
from neo4j import GraphDatabase
import logging
from typing import List, Dict, Any
import json

# --- Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_credentials_from_env(env_path='.env'):
    """Manually parses the .env file to ensure the latest values are used."""
    if not os.path.exists(env_path):
        logging.error(f"FATAL: .env file not found at {env_path}")
        return None, None, None, None
    
    config = {}
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip().strip("'\"")
    
    logging.info(f"Manually loaded credentials from {env_path}")
    return (
        config.get("NEO4J_URI"), 
        config.get("NEO4J_USERNAME"), 
        config.get("NEO4J_PASSWORD"),
        config.get("GOOGLE_API_KEY")
    )

# --- Configuration ---
# Define which node labels should have embeddings. We focus on the most granular, text-rich nodes.
LABELS_TO_EMBED = ["Passage", "Table", "Diagram"]
# Define the embedding model to use
EMBEDDING_MODEL = 'models/embedding-001'
# Number of nodes to process in each batch
BATCH_SIZE = 50

def create_vector_indexes(driver):
    """Creates a vector index for each specified node label if it doesn't already exist."""
    with driver.session(database="neo4j") as session:
        logging.info("Creating vector indexes...")
        for label in LABELS_TO_EMBED:
            try:
                # Note: The index name must be unique
                index_name = f"{label.lower()}_embedding_index"
                session.run(f"""
                CREATE VECTOR INDEX `{index_name}` IF NOT EXISTS
                FOR (n:{label}) ON (n.embedding)
                OPTIONS {{ indexConfig: {{
                    `vector.dimensions`: 768,
                    `vector.similarity_function`: 'cosine'
                }}}}
                """)
                logging.info(f"   - Index for :{label} created or already exists.")
            except Exception as e:
                logging.error(f"   - Error creating index for :{label}: {e}")
        logging.info("Index creation process complete.")

def prepare_text_for_embedding(node: Dict[str, Any]) -> str:
    """Prepares the text content of a node for optimal embedding."""
    label = node.get('label')
    properties = node.get('properties', {})
    
    if label == "Table":
        title = properties.get('title', 'Untitled Table')
        
        # Deserialize headers and rows if they are JSON strings
        headers_data = properties.get('headers', '[]')
        rows_data = properties.get('rows', '[]')
        
        try:
            headers = json.loads(headers_data) if isinstance(headers_data, str) else headers_data
            rows = json.loads(rows_data) if isinstance(rows_data, str) else rows_data
        except json.JSONDecodeError:
            logging.warning(f"Could not decode JSON for table {properties.get('uid', 'N/A')}. Defaulting to empty.")
            headers, rows = [], []

        text_representation = f"Table: {title}\n"
        if headers:
            text_representation += " | ".join(map(str, headers)) + "\n"
        if rows:
            for row in rows:
                # Ensure row is a dictionary before processing
                if isinstance(row, dict):
                    row_values = [str(row.get(h, '')) for h in headers]
                    text_representation += " | ".join(row_values) + "\n"
        return text_representation
        
    elif label == "Diagram":
        # For diagrams, use the description
        return properties.get('description', '')
        
    else: # For Passage nodes
        return properties.get('text', '')

def populate_embeddings(driver):
    """Finds nodes missing embeddings, generates them, and writes them back to Neo4j."""
    while True:
        # 1. Fetch a batch of nodes that need embeddings
        with driver.session(database="neo4j") as session:
            result = session.run(f"""
                MATCH (n)
                WHERE (n:{' OR n:'.join(LABELS_TO_EMBED)})
                  AND n.embedding IS NULL AND (n.text IS NOT NULL OR n.description IS NOT NULL OR n.rows IS NOT NULL)
                RETURN n.uid AS uid, labels(n)[0] AS label, properties(n) as properties
                LIMIT $batch_size
            """, batch_size=BATCH_SIZE)
            nodes_to_process = [record.data() for record in result]

        if not nodes_to_process:
            logging.info("No more nodes to embed. Process is complete.")
            break
            
        logging.info(f"Found {len(nodes_to_process)} nodes to process in this batch...")
        
        # 2. Prepare the text for each node
        texts_to_embed = [prepare_text_for_embedding(node) for node in nodes_to_process]
        
        # Filter out any nodes that resulted in empty text
        valid_nodes_and_texts = [(node, text) for node, text in zip(nodes_to_process, texts_to_embed) if text and text.strip()]
        if not valid_nodes_and_texts:
            logging.warning("   - No valid text to embed in this batch. Skipping.")
            continue
        
        # 3. Generate embeddings
        logging.info(f"   ...generating embeddings for {len(valid_nodes_and_texts)} valid items...")
        texts_only = [text for node, text in valid_nodes_and_texts]
        try:
            response = genai.embed_content(model=EMBEDDING_MODEL, content=texts_only, task_type="RETRIEVAL_DOCUMENT")
            embeddings = response['embedding']
        except Exception as e:
            logging.error(f"   - ERROR: Failed to generate embeddings. Skipping batch. Details: {e}")
            continue

        # 4. Write embeddings back to Neo4j
        rows_to_update = [
            {"uid": node["uid"], "embedding": emb} 
            for (node, text), emb in zip(valid_nodes_and_texts, embeddings)
        ]

        logging.info("   ...writing embeddings back to Neo4j.")
        with driver.session(database="neo4j") as session:
            session.run("""
            UNWIND $rows AS row
            MATCH (n {uid: row.uid})
            SET n.embedding = row.embedding
            """, rows=rows_to_update)
        logging.info(f"   Batch of {len(rows_to_update)} embeddings written successfully.")

if __name__ == "__main__":
    uri, user, password, api_key = get_credentials_from_env()

    if not api_key: raise ValueError("GOOGLE_API_KEY not set in .env file.")
    genai.configure(api_key=api_key)
    logging.info("Gemini API configured successfully.")
    
    # Debugging: Log the loaded URI to confirm it's the correct one
    if uri:
        logging.info(f"Attempting to connect to Neo4j with URI: {uri[:25]}...")
    else:
        logging.error("NEO4J_URI not found in environment variables. Please check your .env file.")
        
    if not all([uri, user, password]): raise ValueError("Neo4j credentials not found in .env file.")
    
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        logging.info("Successfully connected to Neo4j.")
        
        # Step 1: Ensure vector indexes exist
        create_vector_indexes(driver)
        
        # Step 2: Find nodes without embeddings and populate them
        populate_embeddings(driver)

        logging.info("\n✅ Embedding population finished successfully.")
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if 'driver' in locals() and driver:
            driver.close() 