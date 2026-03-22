#!/usr/bin/env python3
"""
Pipeline Step 1b: Gemini Table Refinement

This script runs after the initial document parsing and refines any tables
that were not successfully identified by the regex parser. It uses the
Gemini 2.5 Pro model to analyze the context of each "Inline Table"
and assign a proper ID and title.
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional
from dotenv import load_dotenv
import logging
import google.generativeai as genai
import traceback
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Gemini API Key (HARDCODED) ---
GEMINI_API_KEY = config.GOOGLE_API_KEY

# --- Gemini Interaction Class ---
class GeminiTableRefiner:
    """A dedicated class for using Gemini to refine table information."""
    
    def __init__(self, api_key: str, model_name: str = "gemini-2.5-pro"):
        """Initializes the Gemini model."""
        try:
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel(model_name)
            logging.info(f"Gemini model '{model_name}' initialized successfully.")
        except Exception as e:
            logging.error(f"Failed to initialize Gemini model: {e}")
            raise

    def analyze_table_context(self, table_markdown: str, surrounding_text: str, section_info: Dict) -> Optional[Dict]:
        """
        Uses Gemini to analyze the table's context and determine its proper ID and title.
        """
        prompt = f"""
You are an expert structural engineer analyzing a chapter from a building code. Your task is to identify a table that was not automatically parsed.
Based on the table's content and the surrounding text from the document, determine the official table number and title.

**CONTEXT:**
- **Current Section:** {section_info.get('number', 'N/A')}
- **Section Title:** {section_info.get('title', 'N/A')}
- **Surrounding Text (leading up to the table):**
---
{surrounding_text[-2000:]}
---

**TABLE MARKDOWN:**
---
{table_markdown}
---

**INSTRUCTIONS:**
1.  Analyze all the provided information.
2.  Determine the correct `table_id` (e.g., "1604.3"). The ID is often related to the section number.
3.  Determine the `table_title` (e.g., "RISK CATEGORY OF BUILDINGS AND OTHER STRUCTURES").
4.  Provide a confidence score from 1 (low) to 10 (high).
5.  Explain your reasoning.

**Respond with a JSON object in this exact format. Do not include any other text or markdown formatting.**
{{
    "table_id": "string",
    "table_title": "string",
    "confidence": "number",
    "reasoning": "string"
}}
"""
        try:
            response = self.model.generate_content(prompt)
            # Clean up the response to get only the JSON part
            json_text = response.text.strip().lstrip('```json').rstrip('```')
            analysis_result = json.loads(json_text)
            return analysis_result
        except Exception as e:
            logging.error(f"Gemini analysis call failed for section {section_info.get('number')}. Error: {e}")
            logging.error(f"Failed response text: {response.text if 'response' in locals() else 'N/A'}")
            return None

# --- Main Refinement Logic ---

def refine_tables_with_gemini(data_hierarchy: Dict, refiner: GeminiTableRefiner) -> bool:
    """
    Recursively finds tables marked as "Inline Table" and refines them using Gemini.
    Returns True if any tables were modified.
    """
    modified = False

    def traverse_and_refine(node: Dict, path_context: List[str]):
        nonlocal modified
        
        # Build current context
        current_text = f"Section {node.get('number', '')}: {node.get('title', '')}\n{' '.join(node.get('content', []))}"
        new_path_context = path_context + [current_text]
        
        # Process nodes (images, tables, etc.)
        if "nodes" in node:
            for item in node.get("nodes", []):
                table_data = item.get("data", {})
                if item.get("type") == "table" and table_data.get("table_id") == "Inline Table":
                    logging.info(f"Found 'Inline Table' in section {node.get('number')}. Refining with Gemini...")
                    
                    full_context = "\n".join(new_path_context)
                    section_info = {"number": node.get("number"), "title": node.get("title")}
                    table_md = table_data.get("source_markdown", "")

                    if not table_md:
                        logging.warning(f"Skipping table in section {node.get('number')} due to missing 'source_markdown'.")
                        continue

                    analysis = refiner.analyze_table_context(table_md, full_context, section_info)
                    
                    if analysis and analysis.get("confidence", 0) >= 7:
                        new_id = analysis['table_id']
                        logging.info(f"Gemini identified table as '{new_id}' with confidence {analysis['confidence']}")
                        table_data["table_id"] = new_id
                        table_data["title"] = analysis["table_title"]
                        table_data["gemini_analysis"] = analysis
                        modified = True
                    else:
                        logging.warning(f"Gemini analysis had low confidence or failed for table in section {node.get('number')}.")

        # Recurse into subsections
        for sub_node in node.get("sections", []) + node.get("subsections", []):
            traverse_and_refine(sub_node, new_path_context)

    traverse_and_refine(data_hierarchy, [])
    return modified

def process_structured_json(input_path: Path, output_path: Path, api_key: str, model_name: str) -> None:
    """
    Loads a structured JSON file, refines its tables using Gemini,
    and saves the result to the output path.
    """
    if not input_path.exists():
        logging.error(f"Input JSON file not found: {input_path}")
        return

    logging.info(f"Starting Gemini refinement for {input_path.name}...")
    
    try:
        refiner = GeminiTableRefiner(api_key, model_name)
    except Exception:
        logging.error("Halting refinement due to Gemini initialization failure.")
        return

    with open(input_path, 'r', encoding='utf-8') as f:
        data_hierarchy = json.load(f)
    
    was_modified = refine_tables_with_gemini(data_hierarchy, refiner)

    if was_modified:
        # Ensure the output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data_hierarchy, f, indent=2, ensure_ascii=False)
        logging.info(f"Successfully refined tables and saved changes to {output_path.name}.")
    else:
        logging.info(f"No tables required Gemini refinement in {input_path.name}.")
        # If no changes, still copy the file to the destination to ensure the pipeline continues
        import shutil
        if input_path.resolve() != output_path.resolve():
            shutil.copy(input_path, output_path)
        else:
            logging.info(f"Input and output paths are the same ({input_path}). Skipping copy.")


if __name__ == "__main__":
    logging.info(f'sys.argv: {sys.argv}')
    if len(sys.argv) != 5:
        print("Usage: python pipeline_1b_refine_tables_with_gemini.py <input_json> <output_json> <model_name> <api_key>")
        sys.exit(1)

    input_file = Path(sys.argv[1])
    output_file = Path(sys.argv[2])
    model = sys.argv[3]
    gemini_api_key = GEMINI_API_KEY

    logging.info(f"Using config.GOOGLE_API_KEY: {repr(gemini_api_key)[:10]}... (type: {type(gemini_api_key)})")

    try:
        genai.configure(api_key=gemini_api_key)
        logging.info("Gemini API configured successfully.")
        logging.info(f"Starting Gemini refinement for {input_file.name}...")
        process_structured_json(input_file, output_file, gemini_api_key, model)
    except Exception as e:
        logging.error(f"Gemini configuration or refinement failed: {e}")
        logging.error(traceback.format_exc())
        sys.exit(1) 