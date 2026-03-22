import fitz  # PyMuPDF
import pytesseract
from PIL import Image
import cv2
import numpy as np
import re
import json
from typing import Dict, List, Optional, Tuple
import logging
from pathlib import Path
import hashlib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class OCRTableExtractor:
    """OCR-enhanced table extraction from PDF documents."""
    
    def __init__(self, tesseract_path: str = None):
        """Initialize OCR table extractor."""
        if tesseract_path:
            pytesseract.pytesseract.tesseract_cmd = tesseract_path
        
        self.table_patterns = [
            re.compile(r'TABLE\s+(\d+\.?\d*)', re.IGNORECASE),
            re.compile(r'Table\s+(\d+\.?\d*)', re.IGNORECASE),
            re.compile(r'^\s*(\d{4}\.?\d*)\s*$', re.MULTILINE)  # Section numbers
        ]
    
    def extract_page_text_with_regions(self, page) -> Dict:
        """Extract text from PDF page with positional information."""
        
        # Get text blocks with position info
        blocks = page.get_text("dict")
        
        # Extract text with coordinates
        text_regions = []
        for block in blocks["blocks"]:
            if "lines" in block:
                for line in block["lines"]:
                    for span in line["spans"]:
                        text_regions.append({
                            "text": span["text"],
                            "bbox": span["bbox"],
                            "font": span["font"],
                            "size": span["size"],
                            "flags": span["flags"]
                        })
        
        return {
            "text_regions": text_regions,
            "full_text": page.get_text()
        }
    
    def detect_table_headers_ocr(self, page, crop_region: Tuple = None) -> List[Dict]:
        """Use OCR to detect table headers that might be missed in text extraction."""
        
        # Convert PDF page to image
        mat = fitz.Matrix(2.0, 2.0)  # Increase resolution
        pix = page.get_pixmap(matrix=mat)
        img_data = pix.tobytes("png")
        
        # Convert to OpenCV format
        nparr = np.frombuffer(img_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        # Crop if region specified
        if crop_region:
            x1, y1, x2, y2 = crop_region
            img = img[int(y1*2):int(y2*2), int(x1*2):int(x2*2)]
        
        # Preprocess image for better OCR
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        
        # Apply image processing for better table detection
        processed_imgs = [
            gray,  # Original grayscale
            cv2.threshold(gray, 127, 255, cv2.THRESH_BINARY)[1],  # Binary threshold
            cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                                cv2.THRESH_BINARY, 11, 2)  # Adaptive threshold
        ]
        
        ocr_results = []
        
        for i, processed_img in enumerate(processed_imgs):
            try:
                # Extract text with position information
                ocr_data = pytesseract.image_to_data(processed_img, output_type=pytesseract.Output.DICT)
                
                # Filter for table-related text
                table_candidates = []
                for j, text in enumerate(ocr_data['text']):
                    if text.strip() and ocr_data['conf'][j] > 30:  # Confidence threshold
                        # Check if this looks like a table header
                        for pattern in self.table_patterns:
                            if pattern.search(text):
                                table_candidates.append({
                                    'text': text.strip(),
                                    'bbox': (ocr_data['left'][j], ocr_data['top'][j], 
                                           ocr_data['left'][j] + ocr_data['width'][j],
                                           ocr_data['top'][j] + ocr_data['height'][j]),
                                    'confidence': ocr_data['conf'][j],
                                    'preprocessing': f'method_{i}'
                                })
                
                if table_candidates:
                    ocr_results.extend(table_candidates)
                    
            except Exception as e:
                logging.warning(f"OCR processing failed for method {i}: {e}")
                continue
        
        return ocr_results
    
    def correlate_tables_with_content(self, pdf_path: Path, markdown_content: str) -> Dict:
        """Correlate PDF table detection with markdown content."""
        
        logging.info(f"Correlating tables between PDF and markdown for {pdf_path.name}")
        
        doc = fitz.open(pdf_path)
        correlated_tables = []
        
        # Extract markdown table positions (approximate)
        markdown_lines = markdown_content.split('\n')
        markdown_table_positions = []
        
        for i, line in enumerate(markdown_lines):
            if re.match(r'^\s*\|', line.strip()):
                # Found a table line, look for context
                context_start = max(0, i - 20)
                context_lines = markdown_lines[context_start:i]
                context_text = '\n'.join(context_lines)
                
                markdown_table_positions.append({
                    'line_number': i,
                    'context': context_text,
                    'table_line': line
                })
        
        # Process each page
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            page_text_info = self.extract_page_text_with_regions(page)
            
            # Detect table headers using OCR
            ocr_table_headers = self.detect_table_headers_ocr(page)
            
            # Correlate with markdown tables
            for ocr_header in ocr_table_headers:
                best_match = self._find_best_markdown_match(
                    ocr_header, markdown_table_positions, page_text_info
                )
                
                if best_match:
                    correlated_table = {
                        'page_number': page_num + 1,
                        'pdf_table_header': ocr_header,
                        'markdown_match': best_match,
                        'extracted_table_id': self._extract_table_id(ocr_header['text']),
                        'confidence_score': self._calculate_confidence(ocr_header, best_match)
                    }
                    correlated_tables.append(correlated_table)
        
        doc.close()
        
        return {
            'source_pdf': str(pdf_path),
            'total_pages': len(doc),
            'correlated_tables': correlated_tables,
            'markdown_tables_found': len(markdown_table_positions)
        }
    
    def _find_best_markdown_match(self, ocr_header: Dict, markdown_positions: List[Dict], 
                                 page_text: Dict) -> Optional[Dict]:
        """Find the best matching markdown table for an OCR-detected header."""
        
        ocr_text = ocr_header['text'].lower()
        best_match = None
        best_score = 0
        
        for md_pos in markdown_positions:
            context = md_pos['context'].lower()
            
            # Calculate similarity based on:
            # 1. Text overlap
            # 2. Pattern matching
            # 3. Position correlation
            
            score = 0
            
            # Text similarity
            ocr_words = set(ocr_text.split())
            context_words = set(context.split())
            if ocr_words & context_words:
                score += len(ocr_words & context_words) / len(ocr_words | context_words)
            
            # Pattern matching for table numbers
            ocr_numbers = re.findall(r'\d+\.?\d*', ocr_text)
            context_numbers = re.findall(r'\d+\.?\d*', context)
            if ocr_numbers and context_numbers:
                if any(num in context_numbers for num in ocr_numbers):
                    score += 0.5
            
            # Position correlation (approximate)
            # This is more complex and would need more sophisticated analysis
            
            if score > best_score:
                best_score = score
                best_match = md_pos
        
        return best_match if best_score > 0.3 else None
    
    def _extract_table_id(self, text: str) -> str:
        """Extract table ID from OCR text."""
        
        # Try different patterns
        patterns = [
            r'TABLE\s+(\d+\.?\d*)',
            r'Table\s+(\d+\.?\d*)',
            r'(\d{4}\.?\d*)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return "Unknown"
    
    def _calculate_confidence(self, ocr_header: Dict, markdown_match: Dict) -> float:
        """Calculate confidence score for table correlation."""
        
        base_confidence = ocr_header['confidence'] / 100.0
        
        # Adjust based on match quality
        if markdown_match:
            # Text similarity bonus
            ocr_text = ocr_header['text'].lower()
            context_text = markdown_match['context'].lower()
            
            ocr_words = set(ocr_text.split())
            context_words = set(context_text.split())
            
            if ocr_words & context_words:
                similarity = len(ocr_words & context_words) / len(ocr_words | context_words)
                base_confidence *= (1 + similarity)
        
        return min(base_confidence, 1.0)

class EnhancedPDFTableExtractor:
    """Enhanced PDF table extractor combining multiple techniques."""
    
    def __init__(self, tesseract_path: str = None):
        self.ocr_extractor = OCRTableExtractor(tesseract_path)
    
    def extract_tables_with_context(self, pdf_path: Path, markdown_path: Path, 
                                  output_dir: Path) -> Dict:
        """Extract tables with full context using multiple methods."""
        
        logging.info(f"Starting enhanced table extraction for {pdf_path.name}")
        
        # Read markdown content
        markdown_content = markdown_path.read_text(encoding='utf-8')
        
        # Correlate PDF and markdown tables
        correlation_results = self.ocr_extractor.correlate_tables_with_content(
            pdf_path, markdown_content
        )
        
        # Extract enhanced table information
        enhanced_tables = []
        
        for correlated_table in correlation_results['correlated_tables']:
            table_id = correlated_table['extracted_table_id']
            
            # Try to find full table content in markdown
            markdown_table = self._extract_markdown_table_content(
                markdown_content, correlated_table['markdown_match']
            )
            
            if markdown_table:
                enhanced_table = {
                    'table_id': table_id,
                    'title': self._extract_table_title(correlated_table),
                    'source_page': correlated_table['page_number'],
                    'confidence': correlated_table['confidence_score'],
                    'extraction_method': 'ocr_correlation',
                    'table_data': markdown_table,
                    'ocr_info': correlated_table['pdf_table_header']
                }
                enhanced_tables.append(enhanced_table)
        
        # Save results
        results = {
            'extraction_summary': {
                'source_pdf': str(pdf_path),
                'source_markdown': str(markdown_path),
                'total_tables_found': len(enhanced_tables),
                'correlation_results': correlation_results
            },
            'enhanced_tables': enhanced_tables
        }
        
        output_file = output_dir / f"{pdf_path.stem}_enhanced_tables.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Enhanced table extraction complete. Results saved to {output_file}")
        return results
    
    def _extract_markdown_table_content(self, markdown_content: str, markdown_match: Dict) -> Optional[Dict]:
        """Extract the actual table content from markdown."""
        
        if not markdown_match:
            return None
        
        lines = markdown_content.split('\n')
        start_line = markdown_match['line_number']
        
        # Find the full table
        table_lines = []
        for i in range(start_line, len(lines)):
            line = lines[i].strip()
            if re.match(r'^\s*\|', line):
                table_lines.append(line)
            elif table_lines and line == '':
                continue  # Skip empty lines
            elif table_lines:
                break  # End of table
        
        if len(table_lines) < 2:
            return None
        
        # Parse table structure
        try:
            # Remove separator lines
            data_lines = [line for line in table_lines if not re.match(r'\|\s*[-:]+', line)]
            
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
                'headers': headers,
                'rows': rows
            }
            
        except Exception as e:
            logging.warning(f"Error parsing markdown table: {e}")
            return None
    
    def _extract_table_title(self, correlated_table: Dict) -> str:
        """Extract table title from correlated information."""
        
        # Look for title in the markdown match context
        if correlated_table['markdown_match']:
            context = correlated_table['markdown_match']['context']
            
            # Look for patterns like **TABLE X.X** followed by **TITLE**
            title_patterns = [
                r'\*\*TABLE\s+[\d.]+\*\*\s*\*\*([^*]+)\*\*',
                r'\*\*([^*]+)\*\*\s*$'
            ]
            
            for pattern in title_patterns:
                match = re.search(pattern, context, re.MULTILINE)
                if match:
                    return match.group(1).strip()
        
        return ""

# Factory function for integration
def create_ocr_enhanced_parser(tesseract_path: str = None):
    """Create OCR-enhanced parser for integration with existing pipeline."""
    
    extractor = EnhancedPDFTableExtractor(tesseract_path)
    
    def ocr_enhanced_parse(pdf_path: Path, markdown_path: Path, 
                          output_dir: Path) -> Dict:
        """Parse tables with OCR enhancement."""
        return extractor.extract_tables_with_context(pdf_path, markdown_path, output_dir)
    
    return ocr_enhanced_parse 