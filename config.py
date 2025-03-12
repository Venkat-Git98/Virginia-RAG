# config.py
import os
from pathlib import Path
import streamlit as st
# Base directory configuration
BASE_DIR = Path(__file__).parent
SRC_DIR = BASE_DIR / "src"
APP_DIR = BASE_DIR / "app"
print(BASE_DIR)
# Add src directory to Python path
import sys
sys.path.append(str(SRC_DIR))
OPENAI_API_KEY = st.secrets["API_KEYS"]["OPENAI_API_KEY"]
PINECONE_API_KEY = st.secrets["API_KEYS"]["PINECONE_API_KEY"]

DATA_DIR = BASE_DIR / "data"
UPLOADED_CODES_DIR = DATA_DIR / "uploaded_codes"
EXISTING_CODES_DIR = DATA_DIR / "existing_codes"
PROCESSED_CHUNKS_PATH = UPLOADED_CODES_DIR / "processed_chunks.json"
EXISTING_CHUNKS_PATH = EXISTING_CODES_DIR / "existing_chunks.json"
# Directory paths
# DATA_DIR = BASE_DIR / "data"
#PROCESSED_DIR = DATA_DIR / "processed"

# EXISTING_CHUNKS_PATH = DATA_DIR / "existing_codes" / "existing_chunks.json" #"processed_chunks.json"
# File paths
# PROCESSED_CHUNKS_PATH =  PROCESSED_DIR/ "processed_chunks.json"

EMBEDDINGS_DIR = DATA_DIR / "embeddings"
EMBEDDINGS_PATH = EMBEDDINGS_DIR / "embeddings.json"

# Pinecone settings
PINECONE_INDEX_NAME = "virginia"
PINECONE_INDEX_upload_NAME = "ragbuildingcodesopenaiupload"
# OpenAI settings
EMBEDDING_MODEL = "text-embedding-3-large"
COMPLETION_MODEL = "gpt-4o"

# Create directories if they don't exist
for directory in [DATA_DIR, UPLOADED_CODES_DIR, EMBEDDINGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Function to get absolute path
def get_abs_path(relative_path):
    return BASE_DIR / relative_path