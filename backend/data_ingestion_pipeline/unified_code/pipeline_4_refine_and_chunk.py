import os
import json
import pandas as pd
import logging
import re
from typing import List

# --- Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
INPUT_DIR = "data_ingestion/output/4_final_knowledge_graph"
OUTPUT_DIR = "data_ingestion/output/5_chunked_knowledge_graph"
NODES_FILE = os.path.join(INPUT_DIR, "nodes.jsonl")
EDGES_FILE = os.path.join(INPUT_DIR, "edges.csv")

# Chunking parameters
CHUNK_SIZE = 1500  # characters
CHUNK_OVERLAP = 200 # characters
LARGE_NODE_THRESHOLD = 1000 # Min characters to trigger splitting

def split_text(text: str) -> List[str]:
    """Splits text into overlapping chunks."""
    if not text:
        return []
    
    chunks = []
    start = 0
    while start < len(text):
        end = start + CHUNK_SIZE
        chunks.append(text[start:end])
        start += CHUNK_SIZE - CHUNK_OVERLAP
    return chunks

def refine_and_chunk_graph(base_dir: str):
    """
    Reads the generated graph, applies parent-child chunking to large text nodes,
    and saves a new, refined graph.
    """
    input_dir = os.path.join(base_dir, "output", "4_final_knowledge_graph")
    output_dir = os.path.join(base_dir, "output", "5_chunked_knowledge_graph")
    nodes_file = os.path.join(input_dir, "nodes.jsonl")
    edges_file = os.path.join(input_dir, "edges.csv")

    if not os.path.exists(nodes_file) or not os.path.exists(edges_file):
        logging.error(f"Input files not found in '{input_dir}'. Please run the main data ingestion pipeline first.")
        return

    os.makedirs(output_dir, exist_ok=True)
    
    # Load the initial graph
    with open(nodes_file, 'r', encoding='utf-8') as f:
        original_nodes = [json.loads(line) for line in f]
    original_edges_df = pd.read_csv(edges_file)
    
    new_nodes = []
    new_edges = original_edges_df.to_dict('records') # Start with all existing edges
    
    logging.info(f"Processing {len(original_nodes)} nodes for chunking...")

    for node in original_nodes:
        # We only chunk Section and Subsection nodes that have text
        if node['label'] in ['Section', 'Subsection'] and 'text' in node['properties'] and node['properties']['text'].strip():
            
            parent_node_uid = node['uid']
            original_text = node['properties'].pop('text') # Remove text from parent
            
            # Add the modified parent node (without text) to our new list
            new_nodes.append(node)

            # Decide whether to split the text or treat it as a single chunk
            if len(original_text) > LARGE_NODE_THRESHOLD:
                chunks = split_text(original_text)
            else:
                chunks = [original_text] # Treat the whole text as one chunk
            
            logging.info(f"  - Node {parent_node_uid}: Splitting into {len(chunks)} passage(s).")

            for i, chunk_text in enumerate(chunks):
                passage_uid = f"{parent_node_uid}-passage-{i}"
                
                # Create the new 'Passage' node
                passage_node = {
                    "uid": passage_uid,
                    "label": "Passage",
                    "properties": {
                        "text": chunk_text,
                        "parent_uid": parent_node_uid,
                        "chunk_index": i
                    }
                }
                new_nodes.append(passage_node)
                
                # Create the new 'HAS_CHUNK' edge
                has_chunk_edge = {
                    "start_node_uid": parent_node_uid,
                    "end_node_uid": passage_uid,
                    "relationship_type": "HAS_CHUNK",
                    "properties": "{}" # No properties for this edge type
                }
                new_edges.append(has_chunk_edge)

        else:
            # If the node is not a text-bearing section, add it to the list as-is
            new_nodes.append(node)

    # Save the new, refined graph files
    new_nodes_path = os.path.join(output_dir, "nodes_chunked.jsonl")
    new_edges_path = os.path.join(output_dir, "edges_chunked.csv")

    with open(new_nodes_path, 'w', encoding='utf-8') as f:
        for n in new_nodes:
            f.write(json.dumps(n) + '\n')
            
    pd.DataFrame(new_edges).to_csv(new_edges_path, index=False)
    
    logging.info(f"\nChunking complete. Refined graph created in '{output_dir}'.")
    logging.info(f"Original nodes: {len(original_nodes)} -> Refined nodes: {len(new_nodes)}")
    logging.info(f"Original edges: {len(original_edges_df)} -> Refined edges: {len(new_edges)}")

if __name__ == "__main__":
    # This assumes the script is run from the project root for standalone execution
    refine_and_chunk_graph(os.getcwd()) 