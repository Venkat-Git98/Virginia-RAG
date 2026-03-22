#!/usr/bin/env python3
"""
PDF to Markdown Conversion Script using PyMuPDF4LLM.

This script provides a standalone utility to convert all PDF files in a specified
input directory into Markdown files in an output directory. It is intended to be
run as a preliminary step before the main data ingestion pipeline.
"""
import pymupdf4llm
from pathlib import Path
import logging
import argparse

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def convert_all_pdfs_to_md(input_dir: Path, output_dir: Path):
    """
    Converts all PDF files in the input directory to Markdown format
    and saves them to the output directory.

    Args:
        input_dir (Path): The directory containing the PDF files to convert.
        output_dir (Path): The directory where Markdown files will be saved.
    """
    if not input_dir.is_dir():
        log.error(f"Input directory not found: {input_dir}")
        return

    # Ensure the output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Output directory set to: {output_dir}")

    pdf_files = list(input_dir.glob("*.pdf"))
    if not pdf_files:
        log.warning(f"No PDF files were found in '{input_dir}'.")
        return

    log.info(f"Found {len(pdf_files)} PDF(s) to convert.")

    for pdf_path in pdf_files:
        log.info(f"--- Starting conversion for: {pdf_path.name} ---")
        try:
            # Generate the markdown using pymupdf4llm
            md_text = pymupdf4llm.to_markdown(str(pdf_path))
            
            # Define the output path
            output_md_path = output_dir / f"{pdf_path.stem}.md"
            
            # Save the markdown content to the file
            output_md_path.write_text(md_text, encoding="utf-8")
            log.info(f"Successfully converted and saved to: {output_md_path.name}")

        except Exception as e:
            log.error(f"Failed to convert {pdf_path.name}. Error: {e}", exc_info=True)
    
    log.info("--- All PDF conversions complete. ---")

def main():
    """Main function to run the script from the command line."""
    # Setup argument parser
    parser = argparse.ArgumentParser(description="Convert PDF files to Markdown using PyMuPDF4LLM.")
    
    # Get the project's base directory
    base_dir = Path(__file__).parent
    
    parser.add_argument(
        "--input_dir",
        type=str,
        default=str(base_dir / "input_pdfs"),
        help="Path to the directory containing input PDF files."
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=str(base_dir / "input_mdfiles"),
        help="Path to the directory where output Markdown files will be saved."
    )
    
    args = parser.parse_args()

    # Convert paths to Path objects
    input_path = Path(args.input_dir)
    output_path = Path(args.output_dir)

    convert_all_pdfs_to_md(input_path, output_path)

if __name__ == "__main__":
    main() 