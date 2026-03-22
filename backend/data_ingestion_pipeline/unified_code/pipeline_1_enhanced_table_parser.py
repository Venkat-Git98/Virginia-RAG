import re
import json
from typing import Dict, List, Optional, Tuple
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EnhancedTableParser:
    """Enhanced table parser with multiple table name detection strategies."""
    
    def __init__(self):
        # Table detection patterns
        self.formal_table_re = re.compile(r'^\*\*TABLE\s+([\d.\w]+)\*\*', re.IGNORECASE)
        self.table_title_re = re.compile(r'^\*\*([^*]+)\*\*$')
        self.pipe_table_re = re.compile(r'^\s*\|')
        self.section_re = re.compile(r'^\*\*(\d{4}(?:\.\d+)*)\s+(.+?)\*\*')
        
        # Context-based table reference patterns
        self.table_ref_patterns = [
            re.compile(r'Table\s+([\d.]+)', re.IGNORECASE),
            re.compile(r'in\s+accordance\s+with\s+Table\s+([\d.]+)', re.IGNORECASE),
            re.compile(r'as\s+(?:given|shown|specified)\s+in\s+Table\s+([\d.]+)', re.IGNORECASE),
            re.compile(r'see\s+Table\s+([\d.]+)', re.IGNORECASE)
        ]
    
    def extract_contextual_table_name(self, preceding_content: List[str], current_section: str) -> Optional[str]:
        """Extract table name from context using preceding content and section information."""
        
        # Strategy 1: Look for explicit table references in recent content
        recent_lines = preceding_content[-10:] if len(preceding_content) > 10 else preceding_content
        
        for line in reversed(recent_lines):
            for pattern in self.table_ref_patterns:
                if match := pattern.search(line):
                    table_id = match.group(1)
                    logging.info(f"Found contextual table reference: Table {table_id}")
                    return table_id
        
        # Strategy 2: Infer from section number
        if current_section:
            # If we're in section 1604.3, and we find a table, it's likely Table 1604.3
            section_parts = current_section.split('.')
            if len(section_parts) >= 2:
                potential_table_id = f"{section_parts[0]}.{section_parts[1]}"
                logging.info(f"Inferred table ID from section context: {potential_table_id}")
                return potential_table_id
        
        return None
    
    def parse_enhanced_table(self, table_lines: List[str], context: Dict) -> Optional[Dict]:
        """Parse table with enhanced name detection."""
        
        if not table_lines:
            return None
        
        table_id = None
        title = ""
        table_start_idx = 0
        
        # Strategy 1: Look for formal TABLE declaration
        if len(table_lines) > 0:
            if formal_match := self.formal_table_re.match(table_lines[0]):
                table_id = formal_match.group(1)
                # Next line might be the title
                if len(table_lines) > 1 and table_lines[1].startswith('**'):
                    title = re.sub(r'\*+', '', table_lines[1]).strip()
                    table_start_idx = 2
                else:
                    table_start_idx = 1
                logging.info(f"Found formal table: TABLE {table_id}")
        
        # Strategy 2: Use contextual information if no formal declaration
        if not table_id:
            table_id = self.extract_contextual_table_name(
                context.get('preceding_content', []),
                context.get('current_section', '')
            )
        
        # Strategy 3: Generate smart default based on section
        if not table_id:
            section = context.get('current_section', '')
            if section:
                table_id = f"{section}-table"
                logging.info(f"Generated contextual table ID: {table_id}")
            else:
                table_id = "Inline Table"
        
        # Find actual table content (pipe-delimited rows)
        table_rows = []
        for i, line in enumerate(table_lines[table_start_idx:], table_start_idx):
            if self.pipe_table_re.match(line):
                table_rows.append(line)
        
        if not table_rows:
            return None
        
        # Parse headers and data
        try:
            # Remove separator rows
            data_rows = [row for row in table_rows if not re.match(r'\|\s*[-:]+\s*\|', row)]
            
            if not data_rows:
                return None
            
            # Extract headers
            header_row = data_rows[0]
            raw_headers = [h.strip() for h in header_row.strip('|').split('|')]
            headers = [h.replace('<br>', ' ').strip() for h in raw_headers]
            
            # Extract data rows
            rows = []
            for row_line in data_rows[1:]:
                cells = [c.strip() for c in row_line.strip('|').split('|')]
                if len(cells) == len(headers):
                    row_dict = {}
                    for header, cell in zip(headers, cells):
                        # Handle multi-line cells
                        cell_values = [v.strip() for v in cell.split('<br>') if v.strip()]
                        row_dict[header] = cell_values if cell_values else [cell]
                    rows.append(row_dict)
            
            return {
                "table_id": table_id,
                "title": title,
                "headers": headers,
                "rows": rows,
                "context": {
                    "section": context.get('current_section', ''),
                    "detection_method": "enhanced_parser"
                }
            }
            
        except Exception as e:
            logging.warning(f"Error parsing table: {e}")
            return None

# Integration function for the existing pipeline
def integrate_enhanced_parser():
    """Function to integrate enhanced parser into existing pipeline."""
    parser = EnhancedTableParser()
    
    def enhanced_parse_complex_table(md_input: str, context: Dict = None) -> Optional[Dict]:
        """Enhanced version of the original parse_complex_table function."""
        if context is None:
            context = {}
        
        lines = [ln.strip() for ln in md_input.strip().splitlines() if ln.strip()]
        return parser.parse_enhanced_table(lines, context)
    
    return enhanced_parse_complex_table 