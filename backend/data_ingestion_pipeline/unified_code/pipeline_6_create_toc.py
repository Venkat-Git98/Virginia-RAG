import os
import json
from neo4j import GraphDatabase
import logging

# --- Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_credentials_from_env(env_path):
    """
    Manually parses the .env file to ensure the latest values are used.
    """
    if not os.path.exists(env_path):
        logging.error(f"FATAL: .env file not found at '{env_path}'. Please ensure it exists in the project root.")
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

def create_table_of_contents(driver):
    """
    Queries Neo4j to build a hierarchical table of contents and returns it as a list of dictionaries.
    """
    toc_data = []
    
    # This Cypher query fetches all Chapter nodes and their associated Section nodes.
    # It now matches any relationship type between Chapter and Section to be more robust.
    cypher_query = """
    MATCH (c:Chapter)
    OPTIONAL MATCH (c)-[]->(s:Section)
    WITH c, s ORDER BY s.title // Sort sections alphabetically by title
    RETURN c.uid AS chapter_uid, c.title AS chapter_title, 
           collect({uid: s.uid, title: s.title}) AS sections
    ORDER BY c.title // Sort chapters alphabetically by title
    """
    
    with driver.session(database="neo4j") as session:
        logging.info("Querying the graph to build the Table of Contents...")
        results = session.run(cypher_query)
        
        for record in results:
            chapter_info = {
                "id": record["chapter_uid"],
                "title": record["chapter_title"],
                "level": "chapter",
                "sections": []
            }
            
            # Filter out null entries that can result from OPTIONAL MATCH if a chapter has no sections.
            # The `collect` function will produce a list like `[{'uid': None, 'title': None}]` in such cases.
            sections = [
                {
                    "id": sec["uid"],
                    "title": sec["title"],
                    "level": "section"
                }
                for sec in record["sections"] if sec and sec["uid"] is not None
            ]

            chapter_info["sections"] = sections
            toc_data.append(chapter_info)
            
        logging.info(f"Successfully processed {len(toc_data)} chapters.")

    return toc_data

def save_toc_to_json(toc_data, output_path):
    """Saves the table of contents data to a formatted JSON file."""
    try:
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(toc_data, f, indent=2, ensure_ascii=False)
        logging.info(f"Table of Contents successfully saved to '{output_path}'")
    except Exception as e:
        logging.error(f"Failed to save TOC to JSON file: {e}")

def main():
    # Build a robust path to the .env file in the project root directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_file_path = os.path.join(script_dir, '..', '.env')
    
    # Build a robust path for the output file
    output_file_path = os.path.join(script_dir, 'output', 'table_of_contents.json')
    
    uri, user, password = get_credentials_from_env(env_path=env_file_path)
    
    if not all([uri, user, password]):
        raise ValueError("Neo4j credentials not found. Please check your .env file in the project root.")

    driver = None # Initialize driver to None
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        logging.info("Successfully connected to Neo4j.")
        
        table_of_contents = create_table_of_contents(driver)
        
        if table_of_contents:
            save_toc_to_json(table_of_contents, output_file_path)
        else:
            logging.warning("No data was retrieved to build the table of contents. The query may need adjustment.")
            
        logging.info("\n✅ Table of Contents generation finished.")
        
    except Exception as e:
        logging.error(f"An error occurred during the process: {e}")
    finally:
        if driver:
            driver.close()
            logging.info("Neo4j connection closed.")

if __name__ == "__main__":
    main() 