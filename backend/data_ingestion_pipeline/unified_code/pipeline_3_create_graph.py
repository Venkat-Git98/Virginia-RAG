import json
import csv
import re
from pathlib import Path
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass, asdict
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

@dataclass
class GraphNode:
    uid: str
    label: str
    properties: Dict[str, Any]

@dataclass
class GraphEdge:
    start_node_uid: str
    end_node_uid: str
    relationship_type: str
    properties: Dict[str, Any]

def _load_and_flatten_data(input_file: Path) -> Tuple[List[Dict], Dict[str, Dict]]:
    """Loads and flattens the hierarchical data from a JSON file."""
    with input_file.open('r', encoding='utf-8') as f:
        data = json.load(f)
    
    flat_blocks = []
    chapter_block = {
        'number': data.get('chapter', 'UnknownChapter'),
        'title': data.get('title', 'Unknown Title'),
        'level': 'Chapter',
        'content': [data.get('title', '')],
        'nodes': [],
        'referrals': []
    }
    flat_blocks.append(chapter_block)
    
    def recurse_sections(sections_list: list, parent_number: str):
        for section in sections_list:
            section['parent_number'] = parent_number
            section['level'] = 'Subsection' if '.' in section.get('number', '') else 'Section'
            subsections = section.pop('subsections', [])
            flat_blocks.append(section)
            if subsections:
                recurse_sections(subsections, section.get('number'))

    recurse_sections(data.get('sections', []), chapter_block['number'])
    block_lookup = {block.get('number'): block for block in flat_blocks if 'number' in block}
    log.info(f"Successfully flattened document into {len(flat_blocks)} hierarchical blocks.")
    return flat_blocks, block_lookup

def _create_graph_constructs(input_file: Path, blocks: List[Dict], block_lookup: Dict) -> Tuple[List[GraphNode], List[GraphEdge]]:
    """Creates all nodes and edges from the flattened blocks."""
    nodes, edges, all_node_uids = [], [], set()

    # Pass 1: Create all Section, Subsection, and promoted inline nodes
    for block in blocks:
        parent_uid = block.get('number')
        parent_label = block.get('level', 'Subsection')
        
        nodes.append(GraphNode(uid=parent_uid, label=parent_label, properties={
            'number': parent_uid, 'title': block.get('title', ''),
            'text': ' '.join(block.get('content', [])), 'source_chapter': input_file.stem
        }))
        all_node_uids.add(parent_uid)
        
        for i, embedded_node in enumerate(block.get('nodes', [])):
            node_type = embedded_node.get('type')
            if not node_type: continue
            node_label = node_type.capitalize()
            
            # This logic is crucial for creating unique IDs for tables and figures
            node_data = embedded_node.get('data', embedded_node)
            node_id_from_data = node_data.get('table_id') or node_data.get('figure_id') or node_data.get('id')
            
            if node_id_from_data and "Inline" not in str(node_id_from_data):
                 node_uid = f"{node_label}-{node_id_from_data}"
            else:
                 node_uid = f"{parent_uid}-{node_type}-{i+1}"

            if node_uid not in all_node_uids:
                node_props = {k: v for k, v in node_data.items() if k != 'type'}
                nodes.append(GraphNode(uid=node_uid, label=node_label, properties=node_props))
                all_node_uids.add(node_uid)
            edges.append(GraphEdge(parent_uid, node_uid, "CONTAINS", {'context': 'embedded content'}))

        # Pass 1.5: Create Image nodes from enriched data
        for i, image_data in enumerate(block.get('images', [])):
            image_uid = f"{parent_uid}-image-{i+1}"
            if image_uid not in all_node_uids:
                # The 'data' from enrichment is the properties for the node
                image_props = image_data.get('data', {})
                
                # Convert the absolute image path to a relative path
                if 'path' in image_props and isinstance(image_props['path'], str):
                    image_props['path'] = f"images/{Path(image_props['path']).name}"

                nodes.append(GraphNode(uid=image_uid, label="Image", properties=image_props))
                all_node_uids.add(image_uid)
                edges.append(GraphEdge(parent_uid, image_uid, "HAS_IMAGE", {'context': 'enriched image'}))

    # Pass 2: Create hierarchy, sequence, and reference edges
    for i, block in enumerate(blocks):
        current_uid = block.get('number')
        if block.get('parent_number'):
            edges.append(GraphEdge(block['parent_number'], current_uid, "CONTAINS", {'context': 'hierarchical'}))
        if i > 0 and 'number' in blocks[i-1]:
            edges.append(GraphEdge(blocks[i-1]['number'], current_uid, "FOLLOWS", {}))
        
        text_content = ' '.join(block.get('content', []))
        
        # Pattern for internal references (Tables, Figures)
        internal_patterns = { "Table": r'[Tt]able\s+([\d.-]+)', "Diagram": r'[Ff]igure\s+([\d.A-Z-]+)'}
        for label, pattern in internal_patterns.items():
            for found_id in re.findall(pattern, text_content):
                target_uid = f"{label}-{found_id}"
                if target_uid in all_node_uids:
                    edges.append(GraphEdge(current_uid, target_uid, "REFERENCES", {'text': f"{label} {found_id}"}))

        # Pattern for external standard references (ASCE 7, IBC, etc.)
        external_pattern = r'\b(ASCE\s\d+|IBC|IFC|IMC|IPC|IFGC|IECC|IEBC|IRC|VRC|NEC)\b'
        for standard in re.findall(external_pattern, text_content):
            standard_uid = f"Standard-{standard.replace(' ', '-')}"
            # Create a node for the standard if it doesn't exist yet
            if standard_uid not in all_node_uids:
                nodes.append(GraphNode(uid=standard_uid, label="Standard", properties={'name': standard}))
                all_node_uids.add(standard_uid)
                log.info(f"  Created new node for external standard: {standard}")
            # Create the edge from the section to the standard
            edges.append(GraphEdge(current_uid, standard_uid, "REFERENCES", {'text': standard}))

    log.info(f"Created {len(nodes)} nodes and {len(edges)} edges for {input_file.stem}.")
    return nodes, edges

def create_graph_files_for_chapter(input_file: Path, output_dir: Path):
    """Full pipeline to convert a single processed JSON file into graph files."""
    log.info(f"--- Starting Knowledge Graph creation for '{input_file.name}' ---")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    blocks, block_lookup = _load_and_flatten_data(input_file)
    nodes, edges = _create_graph_constructs(input_file, blocks, block_lookup)
    
    nodes_path = output_dir / f"{input_file.stem}_nodes.jsonl"
    edges_path = output_dir / f"{input_file.stem}_edges.csv"
    
    with nodes_path.open('w', encoding='utf-8') as f:
        for node in nodes: f.write(json.dumps(asdict(node)) + '\n')
    log.info(f"Saved {len(nodes)} nodes to '{nodes_path}'")
            
    with edges_path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['start_node_uid', 'end_node_uid', 'relationship_type', 'properties'])
        for edge in edges: writer.writerow([edge.start_node_uid, edge.end_node_uid, edge.relationship_type, json.dumps(edge.properties)])
    log.info(f"Saved {len(edges)} edges to '{edges_path}'")
    
    log.info(f"--- Graph creation for {input_file.name} complete ---") 