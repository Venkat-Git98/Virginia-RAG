import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import re
from datetime import datetime

# Import our custom solutions
from pipeline_1_enhanced_table_parser import EnhancedTableParser
from pipeline_gemini_table_analyzer import GeminiTableIntelligence
from pipeline_ocr_table_extractor import EnhancedPDFTableExtractor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class HybridTableExtractor:
    """Hybrid table extraction system combining multiple approaches for maximum accuracy."""
    
    def __init__(self, config: Dict = None):
        """Initialize hybrid extractor with configuration."""
        self.config = config or {}
        
        # Initialize extractors
        self.enhanced_parser = EnhancedTableParser()
        
        # Initialize Gemini if API key provided
        self.gemini_analyzer = None
        if self.config.get('gemini_api_key'):
            self.gemini_analyzer = GeminiTableIntelligence(self.config['gemini_api_key'])
        
        # Initialize OCR extractor if tesseract path provided
        self.ocr_extractor = None
        if self.config.get('tesseract_path'):
            self.ocr_extractor = EnhancedPDFTableExtractor(self.config['tesseract_path'])
        
        # Scoring weights for different methods
        self.method_weights = {
            'enhanced_regex': 0.7,
            'gemini_analysis': 0.9,
            'ocr_correlation': 0.8,
            'contextual_inference': 0.6
        }
    
    def extract_tables_comprehensive(self, pdf_path: Path, markdown_path: Path, 
                                   output_dir: Path) -> Dict:
        """Comprehensive table extraction using all available methods."""
        
        logging.info(f"Starting comprehensive table extraction for {pdf_path.name}")
        
        # Read markdown content
        markdown_content = markdown_path.read_text(encoding='utf-8')
        
        # Method 1: Enhanced regex-based parsing
        regex_results = self._extract_with_enhanced_regex(markdown_content)
        
        # Method 2: Contextual inference
        contextual_results = self._extract_with_contextual_inference(markdown_content)
        
        # Consolidate results
        consolidated_tables = self._consolidate_results(regex_results, contextual_results)
        
        # Generate final results
        final_results = {
            'extraction_summary': {
                'source_pdf': str(pdf_path),
                'source_markdown': str(markdown_path),
                'extraction_timestamp': datetime.now().isoformat(),
                'total_tables_found': len(consolidated_tables),
                'method_statistics': {
                    'enhanced_regex': len(regex_results),
                    'contextual_inference': len(contextual_results)
                }
            },
            'consolidated_tables': consolidated_tables,
            'method_details': {
                'regex_results': regex_results,
                'contextual_results': contextual_results
            }
        }
        
        # Save results
        output_file = output_dir / f"{pdf_path.stem}_hybrid_tables.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_results, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Comprehensive extraction complete. {len(consolidated_tables)} tables found.")
        return final_results
    
    def _extract_with_enhanced_regex(self, markdown_content: str) -> List[Dict]:
        """Extract tables using enhanced regex patterns."""
        
        logging.info("Extracting tables with enhanced regex method")
        
        tables = []
        sections = self._parse_sections(markdown_content)
        
        for section in sections:
            section_tables = self._find_tables_in_section(section)
            
            for table_content in section_tables:
                parsed_table = self._parse_table_enhanced(table_content, section)
                
                if parsed_table:
                    parsed_table['extraction_method'] = 'enhanced_regex'
                    parsed_table['confidence_score'] = self._calculate_regex_confidence(parsed_table)
                    tables.append(parsed_table)
        
        return tables
    
    def _extract_with_contextual_inference(self, markdown_content: str) -> List[Dict]:
        """Extract tables using contextual inference."""
        
        logging.info("Extracting tables with contextual inference method")
        
        tables = []
        lines = markdown_content.split('\n')
        
        # Find table patterns with advanced context analysis
        table_contexts = []
        for i, line in enumerate(lines):
            if re.match(r'^\s*\|', line.strip()):
                # Analyze context around table
                context_start = max(0, i - 30)
                context_lines = lines[context_start:i]
                
                # Look for table references, section numbers, etc.
                table_references = []
                section_context = None
                
                for j, context_line in enumerate(reversed(context_lines)):
                    # Look for table references
                    table_ref_match = re.search(r'Table\s+([\d.]+)', context_line, re.IGNORECASE)
                    if table_ref_match:
                        table_references.append(table_ref_match.group(1))
                    
                    # Look for section context
                    section_match = re.search(r'\*\*(\d{4}(?:\.\d+)*)\s+(.+?)\*\*', context_line)
                    if section_match and not section_context:
                        section_context = {
                            'number': section_match.group(1),
                            'title': section_match.group(2)
                        }
                
                # Infer table ID
                table_id = self._infer_table_id(table_references, section_context)
                
                table_contexts.append({
                    'line_number': i,
                    'inferred_table_id': table_id,
                    'references': table_references,
                    'section_context': section_context,
                    'context_lines': context_lines
                })
        
        # Extract actual table content for each context
        for context in table_contexts:
            table_content = self._extract_table_at_line(lines, context['line_number'])
            if table_content:
                table_dict = {
                    'table_id': context['inferred_table_id'],
                    'title': self._infer_table_title(context['context_lines']),
                    'extraction_method': 'contextual_inference',
                    'confidence_score': self._calculate_contextual_confidence(context),
                    'table_data': table_content,
                    'inference_details': context
                }
                tables.append(table_dict)
        
        return tables
    
    def _consolidate_results(self, regex_results: List[Dict], contextual_results: List[Dict]) -> List[Dict]:
        """Consolidate results from all methods using intelligent merging."""
        
        logging.info("Consolidating results from all extraction methods")
        
        all_results = []
        all_results.extend([(r, 'enhanced_regex') for r in regex_results])
        all_results.extend([(r, 'contextual_inference') for r in contextual_results])
        
        # Group similar tables
        table_groups = self._group_similar_tables(all_results)
        
        # Merge groups into final tables
        consolidated_tables = []
        for group in table_groups:
            merged_table = self._merge_table_group(group)
            consolidated_tables.append(merged_table)
        
        # Sort by confidence and table ID
        consolidated_tables.sort(key=lambda x: (x.get('confidence_score', 0), x.get('table_id', 'zzz')), reverse=True)
        
        return consolidated_tables
    
    def _parse_sections(self, markdown_content: str) -> List[Dict]:
        """Parse markdown content into sections."""
        
        sections = []
        lines = markdown_content.split('\n')
        current_section = None
        
        for i, line in enumerate(lines):
            section_match = re.search(r'\*\*(\d{4}(?:\.\d+)*)\s+(.+?)\*\*', line)
            if section_match:
                if current_section:
                    current_section['raw_content'] = '\n'.join(lines[current_section['start_line']:i])
                    sections.append(current_section)
                
                current_section = {
                    'number': section_match.group(1),
                    'title': section_match.group(2),
                    'start_line': i,
                    'content': []
                }
        
        # Add last section
        if current_section:
            current_section['raw_content'] = '\n'.join(lines[current_section['start_line']:])
            sections.append(current_section)
        
        return sections
    
    def _find_tables_in_section(self, section: Dict) -> List[List[str]]:
        """Find all tables in a section."""
        
        tables = []
        if 'raw_content' not in section:
            return tables
        
        lines = section['raw_content'].split('\n')
        current_table = []
        
        for line in lines:
            if re.match(r'^\s*\|', line.strip()):
                current_table.append(line)
            elif current_table:
                if line.strip() == '':
                    continue  # Skip empty lines in table
                else:
                    # End of table
                    tables.append(current_table)
                    current_table = []
        
        # Add last table if exists
        if current_table:
            tables.append(current_table)
        
        return tables
    
    def _parse_table_enhanced(self, table_lines: List[str], section: Dict) -> Optional[Dict]:
        """Parse table with enhanced logic."""
        
        if not table_lines:
            return None
        
        # Look for formal table declaration
        table_id = None
        title = ""
        
        # Check if first line is a table header
        if table_lines[0].strip().startswith('**TABLE'):
            table_match = re.search(r'\*\*TABLE\s+([\d.]+)\*\*', table_lines[0])
            if table_match:
                table_id = table_match.group(1)
                # Next line might be title
                if len(table_lines) > 1 and table_lines[1].startswith('**'):
                    title = re.sub(r'\*+', '', table_lines[1]).strip()
        
        # If no formal table ID, infer from section
        if not table_id:
            section_num = section.get('number', '')
            if section_num:
                parts = section_num.split('.')
                if len(parts) >= 2:
                    table_id = f"{parts[0]}.{parts[1]}"
                else:
                    table_id = f"{section_num}-table"
            else:
                table_id = "Inline Table"
        
        # Parse table structure
        pipe_lines = [line for line in table_lines if re.match(r'^\s*\|', line)]
        if len(pipe_lines) < 2:
            return None
        
        try:
            # Remove separator lines
            data_lines = [line for line in pipe_lines if not re.match(r'\|\s*[-:]+', line)]
            
            if len(data_lines) < 2:
                return None
            
            # Headers
            headers = [h.strip() for h in data_lines[0].strip('|').split('|')]
            
            # Rows
            rows = []
            for line in data_lines[1:]:
                cells = [c.strip() for c in line.strip('|').split('|')]
                if len(cells) == len(headers):
                    row = {}
                    for header, cell in zip(headers, cells):
                        row[header] = cell
                    rows.append(row)
            
            return {
                'table_id': table_id,
                'title': title,
                'headers': headers,
                'rows': rows,
                'section_context': section
            }
            
        except Exception as e:
            logging.warning(f"Error parsing table: {e}")
            return None
    
    def _infer_table_id(self, references: List[str], section_context: Dict) -> str:
        """Infer table ID from references and context."""
        
        if references:
            return references[0]  # Use first reference found
        
        if section_context:
            section_num = section_context['number']
            # Infer table ID from section number
            parts = section_num.split('.')
            if len(parts) >= 2:
                return f"{parts[0]}.{parts[1]}"
        
        return "Inline Table"
    
    def _infer_table_title(self, context_lines: List[str]) -> str:
        """Infer table title from context lines."""
        
        for line in reversed(context_lines):
            # Look for bold text that might be a title
            title_match = re.search(r'\*\*([^*]+)\*\*', line)
            if title_match:
                title = title_match.group(1).strip()
                if len(title) > 5 and not title.startswith('TABLE'):
                    return title
        
        return ""
    
    def _extract_table_at_line(self, lines: List[str], start_line: int) -> Optional[Dict]:
        """Extract table content starting at specific line."""
        
        table_lines = []
        for i in range(start_line, len(lines)):
            line = lines[i].strip()
            if re.match(r'^\s*\|', line):
                table_lines.append(line)
            elif table_lines and line == '':
                continue
            elif table_lines:
                break
        
        if len(table_lines) < 2:
            return None
        
        # Parse table structure
        try:
            data_lines = [line for line in table_lines if not re.match(r'\|\s*[-:]+', line)]
            
            if len(data_lines) < 2:
                return None
            
            headers = [h.strip() for h in data_lines[0].strip('|').split('|')]
            rows = []
            
            for line in data_lines[1:]:
                cells = [c.strip() for c in line.strip('|').split('|')]
                if len(cells) == len(headers):
                    row = {}
                    for header, cell in zip(headers, cells):
                        row[header] = cell
                    rows.append(row)
            
            return {
                'headers': headers,
                'rows': rows
            }
            
        except Exception:
            return None
    
    def _group_similar_tables(self, all_results: List[Tuple[Dict, str]]) -> List[List[Tuple[Dict, str]]]:
        """Group similar tables from different extraction methods."""
        
        groups = []
        used_indices = set()
        
        for i, (table1, method1) in enumerate(all_results):
            if i in used_indices:
                continue
            
            current_group = [(table1, method1)]
            used_indices.add(i)
            
            for j, (table2, method2) in enumerate(all_results[i+1:], i+1):
                if j in used_indices:
                    continue
                
                if self._are_tables_similar(table1, table2):
                    current_group.append((table2, method2))
                    used_indices.add(j)
            
            groups.append(current_group)
        
        return groups
    
    def _are_tables_similar(self, table1: Dict, table2: Dict) -> bool:
        """Check if two tables are similar enough to be merged."""
        
        # Compare table IDs
        id1 = table1.get('table_id', '').lower()
        id2 = table2.get('table_id', '').lower()
        
        if id1 != 'inline table' and id2 != 'inline table':
            if id1 == id2:
                return True
            
            # Check if IDs are similar
            if id1 in id2 or id2 in id1:
                return True
        
        # Compare headers
        headers1 = table1.get('headers', [])
        headers2 = table2.get('headers', [])
        
        if headers1 and headers2:
            # Calculate header similarity
            set1 = set(h.lower() for h in headers1)
            set2 = set(h.lower() for h in headers2)
            
            if set1 & set2:  # Any overlap
                similarity = len(set1 & set2) / len(set1 | set2)
                return similarity > 0.5
        
        return False
    
    def _merge_table_group(self, group: List[Tuple[Dict, str]]) -> Dict:
        """Merge a group of similar tables into a single best table."""
        
        if len(group) == 1:
            return group[0][0]
        
        # Calculate weighted scores for each table
        scored_tables = []
        for table, method in group:
            base_confidence = table.get('confidence_score', 0.5)
            method_weight = self.method_weights.get(method, 0.5)
            
            weighted_score = base_confidence * method_weight
            scored_tables.append((table, method, weighted_score))
        
        # Sort by weighted score
        scored_tables.sort(key=lambda x: x[2], reverse=True)
        
        # Use best table as base
        best_table = scored_tables[0][0].copy()
        
        # Merge information from other tables
        for table, method, score in scored_tables[1:]:
            # Merge better table_id if current one is generic
            if best_table.get('table_id') == 'Inline Table' and table.get('table_id') != 'Inline Table':
                best_table['table_id'] = table['table_id']
            
            # Merge better title if current one is empty
            if not best_table.get('title') and table.get('title'):
                best_table['title'] = table['title']
            
            # Add extraction methods used
            if 'extraction_methods' not in best_table:
                best_table['extraction_methods'] = [scored_tables[0][1]]
            best_table['extraction_methods'].append(method)
        
        # Update confidence based on consensus
        if len(group) > 1:
            confidence_scores = [table.get('confidence_score', 0.5) for table, _ in group]
            best_table['confidence_score'] = sum(confidence_scores) / len(confidence_scores)
            best_table['consensus_level'] = len(group)
        
        return best_table
    
    def _calculate_regex_confidence(self, table: Dict) -> float:
        """Calculate confidence score for regex-based extraction."""
        
        base_confidence = 0.7
        
        # Bonus for formal table ID
        if table.get('table_id') != 'Inline Table':
            base_confidence += 0.2
        
        # Bonus for having title
        if table.get('title'):
            base_confidence += 0.1
        
        return min(base_confidence, 1.0)
    
    def _calculate_contextual_confidence(self, context: Dict) -> float:
        """Calculate confidence score for contextual inference."""
        
        base_confidence = 0.6
        
        # Bonus for explicit references
        if context.get('references'):
            base_confidence += 0.2
        
        # Bonus for section context
        if context.get('section_context'):
            base_confidence += 0.1
        
        return min(base_confidence, 1.0)

# Factory function for easy integration
def create_hybrid_extractor(config: Dict = None) -> HybridTableExtractor:
    """Create hybrid table extractor with configuration."""
    return HybridTableExtractor(config)

# Example usage configuration
EXAMPLE_CONFIG = {
    'gemini_api_key': 'your_gemini_api_key_here',
    'tesseract_path': '/usr/bin/tesseract',  # or path to tesseract.exe on Windows
    'method_weights': {
        'enhanced_regex': 0.7,
        'gemini_analysis': 0.9,
        'ocr_correlation': 0.8,
        'contextual_inference': 0.6
    }
} 