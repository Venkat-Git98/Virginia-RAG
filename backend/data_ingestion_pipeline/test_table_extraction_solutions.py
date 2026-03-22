#!/usr/bin/env python3
"""
Demonstration script for testing all table extraction solutions.
This script tests each method on your actual Chapter 16 data and compares results.
"""

import json
import sys
from pathlib import Path
from typing import Dict, List
import time
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('table_extraction_test.log')
    ]
)

def test_enhanced_regex_solution():
    """Test Solution 1: Enhanced Regex Parser"""
    
    print("\n" + "="*60)
    print("TESTING SOLUTION 1: Enhanced Regex Parser")
    print("="*60)
    
    try:
        from unified_code.pipeline_1_enhanced_table_parser import EnhancedTableParser
        
        # Read the markdown content
        markdown_file = Path("input_mdfiles/chapter16.md")
        if not markdown_file.exists():
            print(f"❌ Markdown file not found: {markdown_file}")
            return None
        
        markdown_content = markdown_file.read_text(encoding='utf-8')
        parser = EnhancedTableParser()
        
        # Parse sections and extract tables
        sections = parse_sections_simple(markdown_content)
        extracted_tables = []
        
        start_time = time.time()
        
        for section in sections:
            tables_in_section = find_tables_in_section(section)
            
            for table_lines in tables_in_section:
                context = {
                    'preceding_content': section.get('content', []),
                    'current_section': section.get('number', ''),
                    'section_title': section.get('title', '')
                }
                
                parsed_table = parser.parse_enhanced_table(table_lines, context)
                if parsed_table:
                    extracted_tables.append(parsed_table)
        
        processing_time = time.time() - start_time
        
        # Save results
        results = {
            'method': 'enhanced_regex',
            'processing_time_seconds': processing_time,
            'total_tables_found': len(extracted_tables),
            'tables': extracted_tables
        }
        
        output_file = Path("output/test_enhanced_regex_results.json")
        output_file.parent.mkdir(exist_ok=True, parents=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        # Print summary
        print(f"✅ Enhanced Regex Processing Complete")
        print(f"   Tables found: {len(extracted_tables)}")
        print(f"   Processing time: {processing_time:.2f} seconds")
        print(f"   Results saved to: {output_file}")
        
        # Show sample results
        print("\n📊 Sample Table Results:")
        for i, table in enumerate(extracted_tables[:3]):  # Show first 3 tables
            print(f"   {i+1}. Table ID: {table.get('table_id', 'Unknown')}")
            print(f"      Title: {table.get('title', 'No title')}")
            print(f"      Headers: {len(table.get('headers', []))} columns")
            print(f"      Rows: {len(table.get('rows', []))} rows")
        
        return results
        
    except Exception as e:
        print(f"❌ Enhanced Regex test failed: {e}")
        logging.error(f"Enhanced Regex test error: {e}")
        return None

def test_contextual_inference():
    """Test contextual inference method"""
    
    print("\n" + "="*60)
    print("TESTING: Contextual Inference Method")
    print("="*60)
    
    try:
        markdown_file = Path("input_mdfiles/chapter16.md")
        markdown_content = markdown_file.read_text(encoding='utf-8')
        
        start_time = time.time()
        extracted_tables = extract_with_contextual_inference(markdown_content)
        processing_time = time.time() - start_time
        
        results = {
            'method': 'contextual_inference',
            'processing_time_seconds': processing_time,
            'total_tables_found': len(extracted_tables),
            'tables': extracted_tables
        }
        
        output_file = Path("output/test_contextual_inference_results.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Contextual Inference Complete")
        print(f"   Tables found: {len(extracted_tables)}")
        print(f"   Processing time: {processing_time:.2f} seconds")
        print(f"   Results saved to: {output_file}")
        
        return results
        
    except Exception as e:
        print(f"❌ Contextual inference test failed: {e}")
        logging.error(f"Contextual inference test error: {e}")
        return None

def test_hybrid_solution():
    """Test Solution 4: Hybrid Approach"""
    
    print("\n" + "="*60)
    print("TESTING SOLUTION 4: Hybrid Multi-Method")
    print("="*60)
    
    try:
        from unified_code.pipeline_hybrid_table_extractor import create_hybrid_extractor
        
        # Basic configuration (no external APIs for demo)
        config = {
            'method_weights': {
                'enhanced_regex': 0.7,
                'contextual_inference': 0.6
            }
        }
        
        extractor = create_hybrid_extractor(config)
        
        # Test on actual files
        pdf_path = Path("input_pdfs/Chapter_16.pdf")
        markdown_path = Path("input_mdfiles/chapter16.md")
        output_dir = Path("output/hybrid_test/")
        
        if not markdown_path.exists():
            print(f"❌ Markdown file not found: {markdown_path}")
            return None
        
        start_time = time.time()
        results = extractor.extract_tables_comprehensive(pdf_path, markdown_path, output_dir)
        processing_time = time.time() - start_time
        
        print(f"✅ Hybrid Processing Complete")
        print(f"   Tables found: {results['extraction_summary']['total_tables_found']}")
        print(f"   Processing time: {processing_time:.2f} seconds")
        print(f"   Regex tables: {results['extraction_summary']['method_statistics']['enhanced_regex']}")
        print(f"   Contextual tables: {results['extraction_summary']['method_statistics']['contextual_inference']}")
        
        return results
        
    except Exception as e:
        print(f"❌ Hybrid test failed: {e}")
        logging.error(f"Hybrid test error: {e}")
        return None

def analyze_current_state():
    """Analyze the current state of table extraction"""
    
    print("\n" + "="*60)
    print("ANALYZING CURRENT STATE")
    print("="*60)
    
    try:
        # Load current enriched JSON
        enriched_file = Path("output/2_enriched_json/chapter16_enriched.json")
        if not enriched_file.exists():
            print(f"❌ Current enriched file not found: {enriched_file}")
            return None
        
        with open(enriched_file, 'r', encoding='utf-8') as f:
            current_data = json.load(f)
        
        # Analyze tables in current state
        total_tables = 0
        inline_tables = 0
        named_tables = 0
        table_ids = []
        
        def count_tables_recursive(node):
            nonlocal total_tables, inline_tables, named_tables, table_ids
            
            if 'nodes' in node:
                for item in node['nodes']:
                    if item.get('type') == 'table':
                        total_tables += 1
                        table_id = item['data'].get('table_id', '')
                        table_ids.append(table_id)
                        
                        if table_id == 'Inline Table':
                            inline_tables += 1
                        else:
                            named_tables += 1
            
            # Recursively check subsections
            for key in ['sections', 'subsections']:
                if key in node:
                    for child in node[key]:
                        count_tables_recursive(child)
        
        count_tables_recursive(current_data)
        
        print(f"📊 Current State Analysis:")
        print(f"   Total tables: {total_tables}")
        print(f"   Named tables: {named_tables}")
        print(f"   Inline tables: {inline_tables}")
        print(f"   Identification rate: {named_tables/total_tables*100:.1f}%" if total_tables > 0 else "   No tables found")
        
        print(f"\n🏷️  Current Table IDs:")
        unique_ids = list(set(table_ids))
        for table_id in sorted(unique_ids):
            count = table_ids.count(table_id)
            print(f"   '{table_id}': {count} tables")
        
        return {
            'total_tables': total_tables,
            'named_tables': named_tables,
            'inline_tables': inline_tables,
            'identification_rate': named_tables/total_tables*100 if total_tables > 0 else 0,
            'table_ids': table_ids
        }
        
    except Exception as e:
        print(f"❌ Current state analysis failed: {e}")
        return None

def compare_results(results_list: List[Dict]):
    """Compare results from different methods"""
    
    print("\n" + "="*60)
    print("COMPARISON RESULTS")
    print("="*60)
    
    if not results_list:
        print("❌ No results to compare")
        return
    
    print(f"{'Method':<20} {'Tables Found':<15} {'Processing Time':<15} {'Success Rate'}")
    print("-" * 70)
    
    for result in results_list:
        if result:
            method = result.get('method', 'Unknown')
            tables = result.get('total_tables_found', 0)
            time_taken = result.get('processing_time_seconds', 0)
            
            # For hybrid results, check different structure
            if 'extraction_summary' in result:
                tables = result['extraction_summary']['total_tables_found']
                method = 'hybrid'
            
            print(f"{method:<20} {tables:<15} {time_taken:<15.2f} {'✅' if tables > 0 else '❌'}")

def extract_with_contextual_inference(markdown_content: str) -> List[Dict]:
    """Simple contextual inference implementation for testing"""
    
    import re
    
    tables = []
    lines = markdown_content.split('\n')
    
    for i, line in enumerate(lines):
        if re.match(r'^\s*\|', line.strip()):
            # Found a table, analyze context
            context_start = max(0, i - 20)
            context_lines = lines[context_start:i]
            
            # Look for table references
            table_id = "Inline Table"
            section_context = None
            
            for context_line in reversed(context_lines):
                # Look for explicit table references
                table_ref = re.search(r'Table\s+([\d.]+)', context_line, re.IGNORECASE)
                if table_ref:
                    table_id = table_ref.group(1)
                    break
                
                # Look for section numbers
                section_match = re.search(r'\*\*(\d{4}(?:\.\d+)*)', context_line)
                if section_match and not section_context:
                    section_num = section_match.group(1)
                    parts = section_num.split('.')
                    if len(parts) >= 2:
                        table_id = f"{parts[0]}.{parts[1]}"
                        section_context = section_num
            
            # Extract table content
            table_content = extract_table_at_line(lines, i)
            if table_content:
                tables.append({
                    'table_id': table_id,
                    'title': '',
                    'extraction_method': 'contextual_inference',
                    'confidence_score': 0.7 if table_id != 'Inline Table' else 0.4,
                    **table_content
                })
    
    return tables

def extract_table_at_line(lines: List[str], start_line: int) -> Dict:
    """Extract table content starting at specific line"""
    
    import re
    
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
        return {}
    
    try:
        # Remove separator lines
        data_lines = [line for line in table_lines if not re.match(r'\|\s*[-:]+', line)]
        
        if len(data_lines) < 2:
            return {}
        
        # Parse headers and rows
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
        return {}

def parse_sections_simple(markdown_content: str) -> List[Dict]:
    """Simple section parser for testing"""
    
    import re
    
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
    
    if current_section:
        current_section['raw_content'] = '\n'.join(lines[current_section['start_line']:])
        sections.append(current_section)
    
    return sections

def find_tables_in_section(section: Dict) -> List[List[str]]:
    """Find tables in a section"""
    
    import re
    
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
                continue
            else:
                tables.append(current_table)
                current_table = []
    
    if current_table:
        tables.append(current_table)
    
    return tables

def main():
    """Main test function"""
    
    print("🔍 COMPREHENSIVE TABLE EXTRACTION TESTING")
    print("=" * 60)
    print(f"Test started at: {datetime.now()}")
    print(f"Working directory: {Path.cwd()}")
    
    # Check if required files exist
    required_files = [
        "input_mdfiles/chapter16.md",
        "output/2_enriched_json/chapter16_enriched.json"
    ]
    
    for file_path in required_files:
        if not Path(file_path).exists():
            print(f"❌ Required file missing: {file_path}")
            return
    
    # Create output directory
    Path("output").mkdir(exist_ok=True)
    
    results = []
    
    # Test 1: Analyze current state
    print("\n🔍 Step 1: Analyzing current pipeline state...")
    current_state = analyze_current_state()
    
    # Test 2: Enhanced regex solution
    print("\n🚀 Step 2: Testing enhanced regex solution...")
    regex_results = test_enhanced_regex_solution()
    if regex_results:
        results.append(regex_results)
    
    # Test 3: Contextual inference
    print("\n🧠 Step 3: Testing contextual inference...")
    contextual_results = test_contextual_inference()
    if contextual_results:
        results.append(contextual_results)
    
    # Test 4: Hybrid solution
    print("\n🔄 Step 4: Testing hybrid solution...")
    hybrid_results = test_hybrid_solution()
    if hybrid_results:
        results.append(hybrid_results)
    
    # Compare results
    print("\n📊 Step 5: Comparing results...")
    compare_results(results)
    
    # Generate final report
    print("\n📋 FINAL SUMMARY")
    print("="*60)
    
    if current_state:
        print(f"Current state: {current_state['inline_tables']}/{current_state['total_tables']} tables are unnamed")
        print(f"Current identification rate: {current_state['identification_rate']:.1f}%")
    
    if results:
        best_result = max(results, key=lambda x: x.get('total_tables_found', 0))
        print(f"\nBest performing method: {best_result.get('method', 'Unknown')}")
        print(f"Tables identified: {best_result.get('total_tables_found', 0)}")
        
        improvement = 0
        if current_state and current_state['total_tables'] > 0:
            current_rate = current_state['identification_rate']
            new_rate = (best_result.get('total_tables_found', 0) / current_state['total_tables']) * 100
            improvement = new_rate - current_rate
            print(f"Improvement: +{improvement:.1f} percentage points")
    
    print(f"\n✅ Testing completed at: {datetime.now()}")
    print("Check the output/ directory for detailed results.")

if __name__ == "__main__":
    main() 