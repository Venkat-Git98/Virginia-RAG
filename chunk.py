import json
import spacy
import re
from collections import defaultdict
import string 
import random
from tqdm import tqdm
import spacy
from spacy.cli import download

try:
    # Attempt to load the spaCy model
    nlp = spacy.load("en_core_web_sm")
except OSError:
    # If the model isn't found, download it
    download("en_core_web_sm")
    # Retry loading the model after installation
    nlp = spacy.load("en_core_web_sm")
# Load spaCy model
# nlp = spacy.load("en_core_web_sm")

def semantic_refinement(chunks):
    """Refines each chunk to ensure semantic coherence."""
    refined_chunks = []
    for chunk in chunks:
        doc = nlp(chunk['content'])
        sentences = [sent.text.strip() for sent in doc.sents]
        refined_chunk = {
            'content': ' '.join(sentences),
            'references': chunk.get('references', ''),
            'file_name': chunk['file_name']
        }
        refined_chunks.append(refined_chunk)
    return refined_chunks

def sliding_window_chunking(content, metadata, window_size=1500, stride=250):
    """Applies sliding window technique with metadata preservation."""
    words = content.split()
    chunks = []
    start = 0
    chunk_num = 1
    
    while start + window_size <= len(words):
        chunk = {
            'content': " ".join(words[start:start + window_size]),
            'references': metadata.get('references', ''),
            'file_name': metadata['file_name'],
            'chunk_number': chunk_num
        }
        chunks.append(chunk)
        start += stride
        chunk_num += 1
    
    if start < len(words):
        chunk = {
            'content': " ".join(words[start:]),
            'references': metadata.get('references', ''),
            'file_name': metadata['file_name'],
            'chunk_number': chunk_num
        }
        chunks.append(chunk)
    print("sliding_Window_Chunking_completed")
    return chunks

def extract_metadata_from_filename(file_name):
    pattern = r"Chapter_(Appendix_[A-Za-z0-9]+|\d+)_Subsection_([A-Za-z0-9]+)_Section\s*([A-Za-z0-9 ]+)"
    match = re.search(pattern, file_name)
    if match:
        chapter = match.group(1)
        chapter_type = "appendix" if chapter.startswith("Appendix_") else "chapter"
        chapter_number = chapter.split("_")[1] if chapter_type == "appendix" else int(chapter)
        
        return {
            "chapter_type": chapter_type,
            "chapter": chapter_number,
            "subsection": match.group(2),
            "section": match.group(3).strip()
        }
    print("metadata extraction completed")
    return generate_random_metadata({
        "chapter_type": None,
        "chapter": None,
        "subsection": None,
        "section": None
    })
    
def generate_random_metadata(metadata):
    if metadata['chapter_type'] is None:
        metadata['chapter_type'] = random.choice(['chapter', 'appendix'])
    
    if metadata['chapter'] is None:
        metadata['chapter'] = (random.choice(string.ascii_uppercase) 
                             if metadata['chapter_type'] == 'appendix' 
                             else random.randint(1, 100))
    
    if metadata['subsection'] is None:
        metadata['subsection'] = f"{random.randint(1, 999):03d}"
    
    if metadata['section'] is None:
        metadata['section'] = f"{random.randint(1, 999):03d}"
    print("generate random completed")
    return metadata
def create_unique_chunk_ids(chunks):
    for chunk in chunks:
        chapter_type = chunk.get('chapter_type', 'unknown')
        chapter = chunk.get('chapter', 'unknown')
        section = chunk.get('section', 'unknown')
        subsection = chunk.get('subsection', 'unknown')
        subsection_chunk_number = chunk.get('subsection_chunk_number', 1)
        chunk['chunk_id'] = f"{chapter_type}_{chapter}_S{section}_SS{subsection}_C{subsection_chunk_number}"
    return chunks


def sort_chunks(chunks):
    def get_sort_key(chunk):
        # Retrieve the values and provide defaults to ensure all keys are comparable
        chapter = chunk.get('chapter', '')
        subsection = chunk.get('subsection', '')
        section = chunk.get('section', '')

        # Normalize all sorting keys to strings to avoid comparison issues between str and int
        chapter_key = str(chapter) if chapter is not None else '99999'  # Use a high number as str for undefined chapters
        subsection_key = str(subsection) if subsection is not None else '99999'
        
        # Assuming sections could be numeric or strings, standardize to string
        section_key = str(section).split()[0] if section else '99999'

        return (chapter_key, subsection_key, section_key)

    return sorted(chunks, key=get_sort_key)

def process_chunks(chunks):
    processed_chunks = []
    for chunk in tqdm(chunks, desc="Processing chunks"):
        # Extract and generate metadata
        metadata = extract_metadata_from_filename(chunk['file_name'])
        chunk.update(metadata)
        
        # Apply semantic refinement
        refined_chunks = semantic_refinement([chunk])
        
        # Apply sliding window chunking
        final_chunks = []
        for refined_chunk in refined_chunks:
            windowed_chunks = sliding_window_chunking(
                refined_chunk['content'],
                metadata={'file_name': chunk['file_name'], 'references': chunk.get('references', '')}
            )
            final_chunks.extend(windowed_chunks)
        
        # Generate chunk IDs and metadata
        for idx, final_chunk in enumerate(final_chunks, 1):
            final_chunk['chunk_id'] = f"{metadata['chapter_type']}_{metadata['chapter']}_S{metadata['section']}_SS{metadata['subsection']}_C{idx}"
            final_chunk['metadata'] = {
                'chapter_type': metadata['chapter_type'],
                'chapter': metadata['chapter'],
                'section': metadata['section'],
                'subsection': metadata['subsection'],
                'subsection_chunk_number': idx,
                'references': chunk.get('references', ''),
                'file_name': chunk['file_name']
            }
        
        processed_chunks.extend(final_chunks)
    print("chunk.py completed")
    return processed_chunks
