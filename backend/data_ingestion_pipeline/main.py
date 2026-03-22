import sys
from pathlib import Path
import logging
import pandas as pd
import json
import re
import os
from neo4j import GraphDatabase
import google.generativeai as genai
from dotenv import load_dotenv
import subprocess
import config

# Add the unified_code directory and root directory to the system path
sys.path.append(str(Path(__file__).parent / "unified_code"))

# Import pipeline functions
from pipeline_1_parse_document import process_document
from pipeline_2_enrich_images import get_gemini_model, enrich_file_with_image_analysis
from pipeline_3_create_graph import create_graph_files_for_chapter
from pipeline_4_refine_and_chunk import refine_and_chunk_graph
from pipeline_5_populate_embeddings import create_vector_indexes, populate_embeddings, get_credentials_from_env as get_embedding_credentials
from pipeline_6_create_toc import create_table_of_contents, save_toc_to_json

# --- Configuration ---
BASE_DIR = Path(__file__).parent
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)
log_file_path = LOGS_DIR / "data_ingestion.log"

# --- Setup Robust Logging ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Remove existing handlers
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
# Create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# Create and add console handler
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
# Create and add file handler
file_handler = logging.FileHandler(log_file_path)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# --- Path Configurations ---
INPUT_PDF_DIR = BASE_DIR / "input_pdfs"
INPUT_MD_DIR = BASE_DIR / "input_mdfiles"
OUTPUT_DIR = BASE_DIR / "output"
ENV_PATH = BASE_DIR / ".env"

STRUCTURED_JSON_DIR = OUTPUT_DIR / "1_structured_json"
ENRICHED_JSON_DIR = OUTPUT_DIR / "2_enriched_json"
GRAPH_PARTIALS_DIR = OUTPUT_DIR / "3_graph_partials"
FINAL_GRAPH_DIR = OUTPUT_DIR / "4_final_knowledge_graph"
CHUNKED_GRAPH_DIR = OUTPUT_DIR / "5_chunked_knowledge_graph"

# --- Gemini API Key (from config) ---
GEMINI_API_KEY = config.GOOGLE_API_KEY

def aggregate_graph_files():
    """Combines all partial graph files into a final, unified graph."""
    logger.info("--- Aggregating all partial graph files into a final knowledge graph ---")
    
    all_nodes, all_edges = [], []
    
    for node_file in GRAPH_PARTIALS_DIR.glob("*_nodes.jsonl"):
        with open(node_file, 'r', encoding='utf-8') as f:
            for line in f:
                all_nodes.append(json.loads(line))
    
    edge_files = list(GRAPH_PARTIALS_DIR.glob("*_edges.csv"))
    if edge_files:
        all_edges_df = pd.concat([pd.read_csv(f) for f in edge_files], ignore_index=True)
    else:
        all_edges_df = pd.DataFrame(columns=['start_node_uid', 'end_node_uid', 'relationship_type', 'properties'])

    if all_nodes:
        node_df = pd.DataFrame(all_nodes).drop_duplicates(subset=['uid'])
    else:
        node_df = pd.DataFrame(columns=['uid', 'label', 'properties'])
        
    all_edges_df.drop_duplicates(inplace=True)

    FINAL_GRAPH_DIR.mkdir(exist_ok=True)
    final_nodes_path = FINAL_GRAPH_DIR / "nodes.jsonl"
    final_edges_path = FINAL_GRAPH_DIR / "edges.csv"

    with open(final_nodes_path, 'w', encoding='utf-8') as f:
        for record in node_df.to_dict('records'):
            f.write(json.dumps(record) + '\n')
    
    all_edges_df.to_csv(final_edges_path, index=False)
    
    logger.info(f"Aggregation complete. Final graph saved in '{FINAL_GRAPH_DIR}'.")
    logger.info(f"Final nodes count: {len(node_df)} | Final edges count: {len(all_edges_df)}")

def run_neo4j_operations():
    """Initializes Neo4j connection and runs embedding and TOC generation."""
    logger.info("--- Starting Neo4j Post-Processing Steps ---")
    
    # Use the credential function from the embeddings script
    uri, user, password, api_key = get_embedding_credentials(ENV_PATH)

    if not all([uri, user, password, api_key]):
        logger.error("FATAL: Neo4j or Google API credentials not found in .env file. Halting.")
        return

    # Configure Google AI
    try:
        genai.configure(api_key=api_key)
        logger.info("Gemini API configured successfully.")
    except Exception as e:
        logger.error(f"Failed to configure Gemini API: {e}")
        return

    driver = None
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        logger.info("Successfully connected to Neo4j.")

        # Step 6: Create Indexes and Populate Embeddings
        logger.info("--- Step 6: Creating Vector Indexes and Populating Embeddings ---")
        create_vector_indexes(driver)
        populate_embeddings(driver)
        
        # Step 7: Create Table of Contents
        logger.info("--- Step 7: Creating Table of Contents ---")
        toc_data = create_table_of_contents(driver)
        if toc_data:
            output_dir = OUTPUT_DIR / "6_table_of_contents"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file_path = output_dir / 'table_of_contents.json'
            save_toc_to_json(toc_data, str(output_file_path))
        else:
            logger.warning("No data returned for Table of Contents.")

    except Exception as e:
        logger.error(f"An error occurred during Neo4j operations: {e}")
    finally:
        if driver:
            driver.close()
            logger.info("Neo4j connection closed.")

def main():
    """Main orchestrator for the entire data ingestion pipeline."""
    logger.info("===== STARTING FULL DATA INGESTION PIPELINE =====")

    # Create all necessary output directories
    for directory in [STRUCTURED_JSON_DIR, ENRICHED_JSON_DIR, GRAPH_PARTIALS_DIR, FINAL_GRAPH_DIR, CHUNKED_GRAPH_DIR]:
        directory.mkdir(parents=True, exist_ok=True)

    global_config = {
        "GEMINI_MODEL": "gemini-2.5-pro"
    }
    
    # Correctly load the .env file from the script's root directory
    dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    load_dotenv(dotenv_path=dotenv_path)

    # Initialize Gemini Model
    gemini_api_key = GEMINI_API_KEY
    logger.info(f"Loaded GEMINI_API_KEY: {repr(gemini_api_key)[:10]}... (type: {type(gemini_api_key)})")
    gemini_model = None
    if gemini_api_key:
        logger.info("--- Initializing Gemini Model for Image Analysis ---")
        try:
            genai.configure(api_key=gemini_api_key)
            gemini_model = genai.GenerativeModel(global_config["GEMINI_MODEL"])
            logger.info(f"Gemini {global_config['GEMINI_MODEL']} model initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Gemini model: {e}")
    else:
        logger.warning("GEMINI_API_KEY not found. Image analysis will be skipped.")

    md_files = sorted(list(INPUT_MD_DIR.glob("*.md")))
    if not md_files:
        logger.error(f"No markdown files found in '{INPUT_MD_DIR}'. Halting pre-processing.")
        return
        
    logger.info(f"Found {len(md_files)} chapters to process.")

    for md_file in md_files:
        chapter_name = md_file.stem
        logger.info(f"===== Processing Chapter: {chapter_name} =====")
        
        # Use a more robust regex to find the chapter number
        match = re.search(r'Chapter_(\d+)', chapter_name)
        if not match:
            logger.warning(f"Could not extract chapter number from '{md_file.name}'. Skipping chapter processing.")
            continue

        # Use group(1) to get the captured number
        chapter_num = match.group(1)
        pdf_file = INPUT_PDF_DIR / f"Chapter_{chapter_num}.pdf"
        
        if not pdf_file.exists():
            logger.warning(f"PDF '{pdf_file.name}' not found. Skipping image processing for this chapter.")
        
        # Step 1: Initial Parsing with Enhanced Regex
        logger.info("Step 1: Running Document Parsing Pipeline...")
        structured_json_path = process_document(md_file, pdf_file, STRUCTURED_JSON_DIR)
        
        # Step 1b: Targeted Gemini Table Refinement
        if gemini_api_key:
            logger.info("Step 1b: Attempting Gemini Table Refinement...")
            try:
                logger.info(f"Passing GEMINI_API_KEY to subprocess: {repr(gemini_api_key)[:10]}... (type: {type(gemini_api_key)})")
                refine_script_path = os.path.join(BASE_DIR, 'unified_code', 'pipeline_1b_refine_tables_with_gemini.py')
                subprocess.run(
                    [
                        sys.executable, 
                        refine_script_path, 
                        structured_json_path, 
                        structured_json_path, 
                        global_config["GEMINI_MODEL"],
                        gemini_api_key
                    ],
                    check=True, capture_output=True, text=True
                )
            except subprocess.CalledProcessError as e:
                logger.error(f"Error running Gemini table refinement script for {chapter_name}:")
                logger.error(f"STDOUT: {e.stdout}")
                logger.error(f"STDERR: {e.stderr}")
        else:
            logger.warning("GEMINI_API_KEY not found. Skipping Step 1b: Gemini Table Refinement.")

        # Step 2: Image Enrichment
        logger.info("Step 2: Running Image Enrichment Pipeline...")
        pdf_exists = pdf_file.exists()
        
        if gemini_model and pdf_exists:
            enriched_json_path = ENRICHED_JSON_DIR / f"{chapter_name}_enriched.json"
            enrich_file_with_image_analysis(structured_json_path, enriched_json_path, gemini_model)
        else:
            logger.warning("Skipping image enrichment for this chapter.")
            if not gemini_model:
                logger.warning(" -> Reason: Gemini model is not available.")
            if not pdf_exists:
                logger.warning(f" -> Reason: PDF file not found at '{pdf_file}'")
            enriched_json_path = structured_json_path # Use non-enriched file for subsequent steps
        
        # Step 3: Graph Creation
        logger.info("Step 3: Creating Graph Partials...")
        create_graph_files_for_chapter(enriched_json_path, GRAPH_PARTIALS_DIR)

    # Step 4: Aggregate Graph Files
    aggregate_graph_files()
    
    # Step 5: Refine and Chunk Graph
    logger.info("--- Step 5: Refining and Chunking Aggregated Graph ---")
    refine_and_chunk_graph(str(BASE_DIR))

    # Steps 6 & 7: Neo4j Operations
    run_neo4j_operations()

    logger.info("===== DATA INGESTION PIPELINE COMPLETE =====")

if __name__ == '__main__':
    main() 