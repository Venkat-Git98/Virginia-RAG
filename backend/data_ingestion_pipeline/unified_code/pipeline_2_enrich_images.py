import os
import json
from pathlib import Path
from typing import Dict, Any, Optional
from PIL import Image
import google.generativeai as genai
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_gemini_model() -> Optional[genai.GenerativeModel]:
    """Loads the Gemini model using API key from .env file."""
    load_dotenv()
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        logging.error("GOOGLE_API_KEY not found in your .env file.")
        return None
    
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-pro-latest')
        logging.info("Gemini 1.5 Pro model initialized successfully.")
        return model
    except Exception as e:
        logging.error(f"Failed to initialize Gemini model: {e}")
        return None

def analyze_image_with_gemini(image_path: Path, model: genai.GenerativeModel) -> Optional[Dict]:
    """Uses a pre-loaded Gemini model to classify an image and extract content."""
    if not image_path.exists():
        logging.warning(f"   - Image file not found at {image_path}, skipping.")
        return None

    try:
        img = Image.open(image_path)
        prompt = """Analyze the attached image and respond with a single, minified JSON object with "image_type" and "content" keys.
        - For math/equations, "image_type" is "math", "content" is the raw LaTeX.
        - For diagrams/charts, "image_type" is "diagram", "content" is a detailed 150-word explanation.
        - For anything else, "image_type" is "other", "content" is an empty string.
        Provide only the raw, minified JSON object."""

        response = model.generate_content([prompt, img])
        json_response_str = response.text.strip().lstrip('```json').rstrip('```').strip()
        
        parsed_response = json.loads(json_response_str)
        image_type = parsed_response.get("image_type")
        content = parsed_response.get("content")

        if image_type == "math" and content:
            return {"type": "math", "latex": content.strip()}
        elif image_type == "diagram" and content:
            return {"type": "diagram", "description": content.strip(), "path": str(image_path.as_posix())}
        else:
            logging.info(f"   - Image '{image_path.name}' classified as '{image_type}' or has no content. Skipping.")
            return None
    except Exception as e:
        logging.error(f"   - Error processing {image_path.name}: {e}")
        return None

def enrich_structure_recursively(node: Dict[str, Any], model: genai.GenerativeModel):
    """Recursively traverses the JSON, finds image nodes, and updates them in-place."""
    if "nodes" in node and isinstance(node["nodes"], list):
        updated_nodes = []
        for item_node in node["nodes"]:
            if item_node.get("type") == "image" and "path" in item_node:
                image_path = Path(item_node["path"])
                logging.info(f"-> Analyzing image: {image_path.name}")
                
                analysis_result = analyze_image_with_gemini(image_path, model)
                
                if analysis_result:
                    if analysis_result["type"] == "math":
                        updated_nodes.append(analysis_result)
                        logging.info(f"   + Success: Replaced '{image_path.name}' with a math node.")
                    elif analysis_result["type"] == "diagram":
                        item_node.update(analysis_result)
                        if 'label' in item_node: del item_node['label']
                        updated_nodes.append(item_node)
                        logging.info(f"   + Success: Enriched '{image_path.name}' with a diagram description.")
                else:
                    updated_nodes.append(item_node)
                    logging.warning(f"   - Gemini processing failed for '{image_path.name}'. Keeping original node.")
            else:
                updated_nodes.append(item_node)
        node["nodes"] = updated_nodes

    for key in ["sections", "subsections"]:
        if key in node and isinstance(node.get(key), list):
            for child_node in node[key]:
                enrich_structure_recursively(child_node, model)

def enrich_file_with_image_analysis(structured_json_path: Path, output_json_path: Path, model: genai.GenerativeModel) -> Path:
    """Orchestrates the image enrichment for a single chapter's structured JSON file."""
    logging.info(f"--- Starting Image Enrichment for {structured_json_path.name} ---")
    
    if not structured_json_path.exists():
        logging.error(f"Input file not found: '{structured_json_path}'")
        raise FileNotFoundError(f"Input file not found: '{structured_json_path}'")

    with open(structured_json_path, 'r', encoding='utf-8') as f:
        main_data = json.load(f)

    enrich_structure_recursively(main_data, model)
    
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(main_data, f, indent=2, ensure_ascii=False)
        
    logging.info(f"--- Finished Image Enrichment. Saved to '{output_json_path.name}' ---")
    return output_json_path 