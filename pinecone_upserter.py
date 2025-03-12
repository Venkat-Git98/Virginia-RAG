#from pinecone import Pinecone
import pinecone
import json
from generator import generate_embeddings
from config import (
    PINECONE_API_KEY,
    PINECONE_INDEX_NAME,
    PROCESSED_CHUNKS_PATH,
    EMBEDDINGS_PATH
)

class PineconeUpserter:
    def __init__(self):
        self.pc = Pinecone(api_key=PINECONE_API_KEY)
        self.index = self.pc.Index(PINECONE_INDEX_NAME)

    def load_chunks(self):
        """Load chunks from processed JSON file"""'''PROCESSED_CHUNKS_PATH'''
        with open(EMBEDDINGS_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)

    def upsert_chunks(self):
        """Generate embeddings and upsert to Pinecone"""
        try:
            # Load processed chunks
            chunks = self.load_chunks()
            
            # Generate embeddings
            embeddings_with_ids = generate_embeddings(chunks)
            
            # Prepare vectors for upserting
            vectors_to_upsert = [
                (item['chunk_id'], 
                 item['embedding'], 
                 item.get('metadata', {}))
                for item in embeddings_with_ids
            ]
            
            # Upsert in batches
            batch_size = 100
            for i in range(0, len(vectors_to_upsert), batch_size):
                batch = vectors_to_upsert[i:i + batch_size]
                self.index.upsert(vectors=batch)
            
            return True, f"Successfully upserted {len(vectors_to_upsert)} vectors to Pinecone"
        
        except Exception as e:
            return False, f"Error upserting to Pinecone: {str(e)}"
