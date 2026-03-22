import google.generativeai as genai
import json
import re
from typing import Dict, List, Optional, Any
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class GeminiTableIntelligence:
    """Advanced table analysis using Gemini 2.5 Pro for perfect table identification."""
    
    def __init__(self, api_key: str):
        """Initialize Gemini model for table analysis."""
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.5-pro-latest')
        
    def analyze_table_context(self, table_content: str, surrounding_context: str, section_info: Dict) -> Dict:
        """Use Gemini to intelligently analyze table context and extract proper naming."""
        
        prompt = f"""
You are an expert in analyzing technical documents, specifically construction codes and standards. 
Your task is to analyze the provided table and its context to determine the most accurate table identification.

CONTEXT INFORMATION:
- Current Section: {section_info.get('number', 'Unknown')}
- Section Title: {section_info.get('title', 'Unknown')}
- Document Type: Construction Code (Chapter 16 - Structural Design)

SURROUNDING CONTEXT (3-5 paragraphs before the table):
{surrounding_context}

TABLE CONTENT:
{table_content}

ANALYSIS REQUIREMENTS:
1. Identify the most appropriate table number/ID (e.g., "1604.3", "1607.1")
2. Extract the full table title if present
3. Determine table purpose/description
4. Identify if this table has official designation in construction codes
5. Rate confidence level (1-10) in the identification

Respond with a JSON object in this exact format:
{{
    "table_id": "string - official table number (e.g., '1604.3') or contextual ID",
    "table_title": "string - official table title if found",
    "description": "string - brief description of table purpose",
    "confidence_level": "number - 1 to 10",
    "detection_method": "string - how the ID was determined",
    "official_designation": "boolean - whether this is an officially numbered table",
    "reasoning": "string - explanation of the identification logic"
}}

IMPORTANT: 
- Look for explicit "TABLE X.X" patterns in the context
- Consider section numbering patterns for inference
- Construction codes often have tables numbered matching their sections
- Be precise about confidence levels
- Return valid JSON only
"""

        try:
            response = self.model.generate_content(prompt)
            result_text = response.text.strip()
            
            # Extract JSON from response
            json_match = re.search(r'\{.*\}', result_text, re.DOTALL)
            if json_match:
                analysis_result = json.loads(json_match.group())
                logging.info(f"Gemini analysis complete: {analysis_result.get('table_id')} (confidence: {analysis_result.get('confidence_level')})")
                return analysis_result
            else:
                logging.warning("Could not extract JSON from Gemini response")
                return self._fallback_analysis(section_info)
                
        except Exception as e:
            logging.error(f"Gemini analysis failed: {e}")
            return self._fallback_analysis(section_info)
    
    def _fallback_analysis(self, section_info: Dict) -> Dict:
        """Fallback analysis when Gemini fails."""
        section_num = section_info.get('number', 'Unknown')
        return {
            "table_id": f"{section_num}-table" if section_num != 'Unknown' else "Inline Table",
            "table_title": "",
            "description": "Table analysis failed - using fallback",
            "confidence_level": 3,
            "detection_method": "fallback",
            "official_designation": False,
            "reasoning": "Gemini analysis unavailable, using section-based fallback"
        }
    
    def enhanced_table_extraction(self, markdown_text: str, section_context: Dict) -> List[Dict]:
        """Extract and analyze all tables from markdown with full context."""
        
        tables = []
        lines = markdown_text.split('\n')
        
        # Find table boundaries and context
        table_start_indices = []
        for i, line in enumerate(lines):
            if re.match(r'^\s*\|', line.strip()):
                # Check if this is the start of a new table
                if not table_start_indices or i > table_start_indices[-1] + 50:
                    table_start_indices.append(i)
        
        for start_idx in table_start_indices:
            # Extract table content
            table_lines = []
            for i in range(start_idx, len(lines)):
                line = lines[i].strip()
                if re.match(r'^\s*\|', line):
                    table_lines.append(line)
                elif table_lines and line == '':
                    continue  # Skip empty lines within table
                elif table_lines:
                    break  # End of table
            
            if len(table_lines) < 2:  # Need at least header and one data row
                continue
            
            # Extract surrounding context (50 lines before table)
            context_start = max(0, start_idx - 50)
            context_lines = lines[context_start:start_idx]
            surrounding_context = '\n'.join(context_lines)
            
            # Combine table content
            table_content = '\n'.join(table_lines)
            
            # Analyze with Gemini
            analysis = self.analyze_table_context(
                table_content=table_content,
                surrounding_context=surrounding_context,
                section_info=section_context
            )
            
            # Parse table structure
            table_data = self._parse_table_structure(table_lines)
            if table_data:
                # Merge Gemini analysis with parsed data
                enhanced_table = {
                    **table_data,
                    "table_id": analysis["table_id"],
                    "title": analysis["table_title"],
                    "analysis": analysis
                }
                tables.append(enhanced_table)
        
        return tables
    
    def _parse_table_structure(self, table_lines: List[str]) -> Optional[Dict]:
        """Parse the basic table structure from markdown lines."""
        try:
            # Remove separator lines
            data_lines = [line for line in table_lines if not re.match(r'\|\s*[-:]+', line)]
            
            if len(data_lines) < 2:
                return None
            
            # Parse headers
            header_line = data_lines[0]
            headers = [h.strip() for h in header_line.strip('|').split('|')]
            
            # Parse rows
            rows = []
            for line in data_lines[1:]:
                cells = [c.strip() for c in line.strip('|').split('|')]
                if len(cells) == len(headers):
                    row = {}
                    for header, cell in zip(headers, cells):
                        # Handle multi-line cells
                        cell_values = [v.strip() for v in cell.split('<br>') if v.strip()]
                        row[header] = cell_values if cell_values else [cell]
                    rows.append(row)
            
            return {
                "headers": headers,
                "rows": rows,
                "structure_type": "markdown_table"
            }
            
        except Exception as e:
            logging.warning(f"Error parsing table structure: {e}")
            return None

class GeminiTablePipeline:
    """Integration pipeline for Gemini-based table analysis."""
    
    def __init__(self, api_key: str):
        self.analyzer = GeminiTableIntelligence(api_key)
    
    def process_document_tables(self, markdown_file: Path, output_file: Path) -> Dict:
        """Process all tables in a document with Gemini intelligence."""
        
        logging.info(f"Processing tables in {markdown_file.name} with Gemini intelligence")
        
        # Read markdown content
        markdown_content = markdown_file.read_text(encoding='utf-8')
        
        # Extract chapter/section info
        chapter_match = re.search(r'CHAPTER\s+(\d+)\s+(.+)', markdown_content)
        chapter_info = {
            "number": chapter_match.group(1) if chapter_match else "Unknown",
            "title": chapter_match.group(2) if chapter_match else "Unknown"
        }
        
        # Find all sections
        sections = re.findall(r'\*\*(\d{4}(?:\.\d+)*)\s+(.+?)\*\*', markdown_content)
        
        all_tables = []
        processed_sections = {}
        
        # Process each section context
        for section_num, section_title in sections:
            section_context = {
                "number": section_num,
                "title": section_title.strip(),
                "chapter": chapter_info
            }
            
            # Extract section content
            section_pattern = rf'\*\*{re.escape(section_num)}\s+.+?\*\*'
            section_matches = list(re.finditer(section_pattern, markdown_content, re.DOTALL))
            
            if section_matches:
                section_start = section_matches[0].end()
                # Find next section or end of document
                next_section_start = len(markdown_content)
                for next_section_num, _ in sections:
                    if next_section_num > section_num:
                        next_pattern = rf'\*\*{re.escape(next_section_num)}\s+'
                        next_match = re.search(next_pattern, markdown_content[section_start:])
                        if next_match:
                            next_section_start = section_start + next_match.start()
                            break
                
                section_content = markdown_content[section_start:next_section_start]
                
                # Extract tables from this section
                section_tables = self.analyzer.enhanced_table_extraction(
                    section_content, section_context
                )
                
                for table in section_tables:
                    table["section_context"] = section_context
                    all_tables.append(table)
                
                processed_sections[section_num] = {
                    "context": section_context,
                    "tables_found": len(section_tables)
                }
        
        # Save results
        results = {
            "document_info": {
                "source_file": str(markdown_file),
                "chapter_info": chapter_info,
                "total_sections_processed": len(processed_sections),
                "total_tables_found": len(all_tables)
            },
            "tables": all_tables,
            "processing_summary": processed_sections
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Processed {len(all_tables)} tables and saved to {output_file}")
        return results

def create_gemini_enhanced_parser(api_key: str):
    """Factory function to create Gemini-enhanced parser for integration."""
    
    pipeline = GeminiTablePipeline(api_key)
    
    def gemini_parse_table(table_content: str, context: Dict) -> Optional[Dict]:
        """Parse single table with Gemini intelligence."""
        analyzer = GeminiTableIntelligence(api_key)
        
        # Extract surrounding context from the context dict
        surrounding_context = context.get('surrounding_text', '')
        section_info = context.get('section_info', {})
        
        analysis = analyzer.analyze_table_context(
            table_content=table_content,
            surrounding_context=surrounding_context,
            section_info=section_info
        )
        
        # Parse basic structure
        lines = table_content.strip().split('\n')
        table_data = analyzer._parse_table_structure(lines)
        
        if table_data:
            return {
                **table_data,
                "table_id": analysis["table_id"],
                "title": analysis["table_title"],
                "gemini_analysis": analysis
            }
        
        return None
    
    return gemini_parse_table, pipeline 