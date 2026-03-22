import re
import json
import fitz  # PyMuPDF
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==============================================================================
# TABLE PARSING LOGIC
# ==============================================================================
def parse_complex_table(md_input: str, context: Optional[Dict] = None) -> Optional[Dict]:
    """
    Parses a markdown table string into a structured dictionary,
    using context to infer table IDs when they are not explicit.
    """
    try:
        lines = [ln.strip() for ln in md_input.strip().splitlines() if ln.strip()]
        if not lines:
            return None

        table_id = "Inline Table"
        title = ""
        table_body_start_index = 0
        
        # Enhanced regex patterns for table ID and title that handle markdown bolding
        id_pattern = re.compile(r'^\s*\**TABLE\s+([\d\w.]+)\**')
        title_pattern = re.compile(r'^\s*\**([A-Z\s,]+)\**$')

        # Search for ID and title in the first few lines
        header_lines = lines[:3]
        for i, line in enumerate(header_lines):
            id_match = id_pattern.match(line)
            if id_match:
                table_id = id_match.group(1)
                # Check subsequent lines for a title
                if len(header_lines) > i + 1 and not header_lines[i+1].strip().startswith('|'):
                    title_match = title_pattern.match(header_lines[i+1])
                    if title_match:
                        title = title_match.group(1).strip()
                break # Found the ID, stop searching
        
        # Context-based inference if still an Inline Table
        if table_id == "Inline Table" and context and 'number' in context:
            logging.info(f"Inferring table ID from section context: {context['number']}")
            table_id = context['number']

        # Find where the actual table data starts (the first line with pipes)
        for i, line in enumerate(lines):
            if line.strip().startswith('|'):
                table_body_start_index = i
                break
                
        tbl = [ln for ln in lines[table_body_start_index:] if ln.strip().startswith("|")]
        data = [ln for ln in tbl if not re.match(r"\|\s*---\s*\|", ln)]
        if not data: return None

        raw_hdrs = data[0].strip("|").split("|")
        headers = [h.replace("<br>", " ").strip() for h in raw_hdrs]
        rows = []
        for ln in data[1:]:
            cells = [c.strip() for c in ln.strip("|").split("|")]
            if len(cells) != len(headers): continue
            row = {hdr: [p.strip() for p in cell.split("<br>") if p.strip()] for hdr, cell in zip(headers, cells)}
            rows.append(row)

        return {"table_id": table_id, "title": title, "headers": headers, "rows": rows, "source_markdown": md_input}
    except Exception as e:
        logging.warning(f"Could not parse a table block: {e}")
        return None

# ==============================================================================
# TEXT HIERARCHY & STRUCTURE PARSING
# ==============================================================================
def preprocess_markdown(raw_text: str) -> List[str]:
    """Removes boilerplate and prepares markdown for parsing."""
    footer_pattern = re.compile(r"Copyright ©.*?PDF from:.*?\n", re.DOTALL)
    cleaned_text = re.sub(footer_pattern, "", raw_text)
    cleaned_text = re.sub(r'\n?\*\*2021 Virginia Construction Code\*\*\n', '', cleaned_text)
    cleaned_text = re.sub(r'(\n\*\*TABLE\s+[\d\w.]+\*\*)', r'\n\n\1', cleaned_text)
    return cleaned_text.split('\n')

def clean_and_extract_from_content(raw_content_line: str) -> (str, List[str]):
    """Processes a single line to extract referrals and clean markdown."""
    line = raw_content_line.strip()
    if line.startswith("CHAPTER") or not line or "design#VACC2021P1" in line: return "", []
    link_pattern = re.compile(r'\[(.*?)\]\(.*?\)')
    referrals = link_pattern.findall(line)
    normalized_line = re.sub(link_pattern, r'\1', line)
    normalized_line = re.sub(r'(\*\*|\*)', '', normalized_line)
    return normalized_line.strip(), referrals

def parse_document_structure(lines: List[str]) -> Dict[str, Any]:
    CHAPTER_RE = re.compile(r'^CHAPTER\s+(\d+)\s+(.+)$')
    SECTION_RE = re.compile(r'^\*\*SECTION\s+(\d+)\*\*')
    SUBSECTION_ONE_BLOCK_RE = re.compile(r'^\*\*(\d{4}(?:\.\d+)*)\s+(.+?)\*\*')
    SUBSECTION_TWO_BLOCK_RE = re.compile(r'^\*\*(\d{4}(?:\.\d+)*)\*\*\s+\*\*(.+?)\*\*')
    BOLD_TITLE_RE = re.compile(r'^\*\*(.+)\*\*')
    FORMAL_TABLE_START_RE = re.compile(r'^\*\*TABLE\s+[\d\w.]+\*\*')
    PIPE_TABLE_START_RE = re.compile(r'^\s*\|')

    data = {'chapter': None, 'title': None, 'sections': []}
    path_stack, content_buffer, table_buffer = [data], [], []
    expecting_section_title, is_parsing_table = False, False

    def flush_content_buffer():
        nonlocal content_buffer
        if content_buffer:
            parent = path_stack[-1]
            if 'content' not in parent: parent['content'] = []
            if 'nodes' not in parent: parent['nodes'] = []
            for line in content_buffer:
                cleaned_text, refs = clean_and_extract_from_content(line)
                if cleaned_text: parent['content'].append(cleaned_text)
                if refs:
                    if 'referrals' not in parent: parent['referrals'] = []
                    parent['referrals'].extend(refs)
            content_buffer = []

    def flush_table_buffer():
        nonlocal table_buffer, is_parsing_table
        if table_buffer:
            # Pre-validation: Check if buffer contains at least one valid table row
            if not any('|' in line and '---' not in line for line in table_buffer):
                logging.warning("A block identified as a table had no valid data rows. Reverting to content.")
                content_buffer.extend(table_buffer)
                table_buffer, is_parsing_table = [], False
                flush_content_buffer()
                return

            parent = path_stack[-1]
            if 'nodes' not in parent: parent['nodes'] = []
            table_md_string = "\n".join(table_buffer)
            # Pass the parent context to the parser for better table ID inference
            parsed_table_data = parse_complex_table(table_md_string, context=parent)
            if parsed_table_data:
                parent['nodes'].append({"type": "table", "data": parsed_table_data})
                logging.info(f"  Successfully parsed table '{parsed_table_data.get('table_id', 'N/A')}'")
            else:
                logging.warning("  Could not parse a block as a table. Reverting to content.")
            content_buffer.extend(table_buffer)
            flush_content_buffer()
        table_buffer, is_parsing_table = [], False

    def process_subsection_match(match):
        nonlocal path_stack, expecting_section_title
        flush_content_buffer(); flush_table_buffer()
        number, title_raw = match.groups()
        depth = number.count('.') + 1
        while len(path_stack) > depth: path_stack.pop()
        cleaned_title, refs = clean_and_extract_from_content(title_raw)
        new_sub = {'number': number, 'title': cleaned_title.strip(), 'content': [], 'referrals': refs, 'nodes': [], 'subsections': []}
        parent = path_stack[-1]
        container_key = 'subsections' if 'subsections' in parent else 'sections'
        parent.setdefault(container_key, []).append(new_sub)
        path_stack.append(new_sub)
        expecting_section_title = False

    for raw_line in lines:
        line = raw_line.strip()
        is_formal_table, is_pipe_table = FORMAL_TABLE_START_RE.match(line), PIPE_TABLE_START_RE.match(line)

        if not is_parsing_table and (is_formal_table or is_pipe_table):
            flush_content_buffer()
            is_parsing_table = True

        if is_parsing_table:
            if not line or not (PIPE_TABLE_START_RE.match(line) or BOLD_TITLE_RE.match(line)):
                flush_table_buffer()
                if not line: continue
            else:
                table_buffer.append(raw_line)
                continue
        
        if not line:
            flush_content_buffer(); continue
        if any(re.match(p, line) for p in [CHAPTER_RE, SECTION_RE, SUBSECTION_ONE_BLOCK_RE, SUBSECTION_TWO_BLOCK_RE]):
            flush_content_buffer()

        if m_chap := CHAPTER_RE.match(line):
            if data['chapter'] is None:
                data['chapter'], data['title'] = m_chap.groups()
                path_stack = [data]
        elif m_sec := SECTION_RE.match(line):
            path_stack = [data]
            new_section = {'number': m_sec.group(1), 'title': '', 'content': [], 'referrals': [], 'nodes': [], 'subsections': []}
            data['sections'].append(new_section)
            path_stack.append(new_section)
            expecting_section_title = True
        elif m_sub_two := SUBSECTION_TWO_BLOCK_RE.match(line):
            process_subsection_match(m_sub_two)
        elif m_sub_one := SUBSECTION_ONE_BLOCK_RE.match(line):
            process_subsection_match(m_sub_one)
        elif expecting_section_title and (m_title := BOLD_TITLE_RE.match(line)):
            path_stack[-1]['title'] = m_title.group(1).strip()
            content_buffer.clear()
            expecting_section_title = False
        else:
            content_buffer.append(raw_line)
            expecting_section_title = False
    
    flush_content_buffer(); flush_table_buffer()
    return data

# ==============================================================================
# ASSET EXTRACTION & ORCHESTRATION
# ==============================================================================
def find_section_in_json(hierarchy: Dict, section_number: str) -> Optional[Dict]:
    """Recursively searches the JSON hierarchy for a given section number."""
    if hierarchy.get("number") == section_number: return hierarchy
    for key in ["sections", "subsections"]:
        if key in hierarchy:
            for item in hierarchy[key]:
                if found := find_section_in_json(item, section_number):
                    return found
    return None

def extract_and_connect_assets(pdf_path: Path, data_hierarchy: Dict, image_dir: Path):
    """Processes the PDF to find IMAGES and injects them into the data_hierarchy."""
    logging.info("Starting Pass 2: Image Asset Extraction and Connection")
    doc = fitz.open(pdf_path)
    image_dir.mkdir(exist_ok=True, parents=True)
    UNWANTED_IMAGE_HASH = "f29c29f8ce7aa7e2b0c03272bd8f870207cf17bca2409ba5aa70781b744a9a59"

    page_to_section_map, last_section_found = {}, None
    section_header_re = re.compile(r'^\s*(\d{4}(?:\.\d+)*)\s')
    for page_num in range(len(doc)):
        text = doc.load_page(page_num).get_text()
        for line in text.split('\n'):
            if match := section_header_re.match(line):
                last_section_found = match.group(1).strip()
        page_to_section_map[page_num + 1] = last_section_found
    
    for page_num in range(len(doc)):
        if parent_section_number := page_to_section_map.get(page_num + 1):
            if parent_node := find_section_in_json(data_hierarchy, parent_section_number):
                if "nodes" not in parent_node: parent_node["nodes"] = []
                for img_index, img in enumerate(doc.load_page(page_num).get_images(full=True), start=1):
                    xref = img[0]
                    base_image = doc.extract_image(xref)
                    if hashlib.sha256(base_image["image"]).hexdigest() == UNWANTED_IMAGE_HASH:
                        continue
                    image_name = f"{pdf_path.stem}_p{page_num + 1}_img{img_index}.{base_image['ext']}"
                    image_path = image_dir / image_name
                    image_path.write_bytes(base_image["image"])
                    parent_node["nodes"].append({"type": "image", "path": str(image_path.as_posix()), "label": f"Image from Page {page_num + 1}"})
                    logging.info(f"  Connected image '{image_name}' to section '{parent_section_number}'")
    doc.close()
    return data_hierarchy

def process_document(md_file: Path, pdf_file: Path, output_dir: Path) -> Path:
    """Main function to run the complete, multi-pass workflow for one document."""
    logging.info(f"Starting document processing pipeline for {md_file.name}...")
    final_json_file = output_dir / f"{md_file.stem}_structured.json"
    image_dir = output_dir / "images"

    logging.info("--- Pass 1: Parsing Text Hierarchy and Tables from Markdown ---")
    raw_md_text = md_file.read_text(encoding='utf-8')
    clean_lines = preprocess_markdown(raw_md_text)
    data_hierarchy = parse_document_structure(clean_lines)

    final_data = extract_and_connect_assets(pdf_file, data_hierarchy, image_dir)

    logging.info(f"--- Writing Final Structured JSON to '{final_json_file}' ---")
    with open(final_json_file, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, indent=2, ensure_ascii=False)
    
    logging.info(f"Processing for {md_file.name} complete.")
    return final_json_file 