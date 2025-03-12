import streamlit as st
import openai
import pinecone
#from pinecone import Pinecone
import json
from io import BytesIO
import fitz  # PyMuPDF
from pathlib import Path
from config import (
    OPENAI_API_KEY,
    PINECONE_API_KEY,
    PINECONE_INDEX_NAME,
    EMBEDDING_MODEL,
    PROCESSED_CHUNKS_PATH,
    PINECONE_INDEX_upload_NAME,
    EXISTING_CHUNKS_PATH,
    EMBEDDINGS_PATH,
    APP_DIR
)
from generator import generate_query_embedding
from pinecone_upserter import PineconeUpserter
from pinecone_ops import PineconeManager
from document_processor import DocumentProcessor
import atexit
import os
import signal
import shutil  # To handle directory deletion
import stat

# Must be the first Streamlit command
st.set_page_config(
    page_title="Virginia Building Code Assistant",
    page_icon="🏗️",
    layout="wide"
)
pinecone.init(api_key=PINECONE_API_KEY)

openai.api_key = OPENAI_API_KEY
# pc = Pinecone(api_key=PINECONE_API_KEY)
index = pinecone.Index(PINECONE_INDEX_NAME)
upload_index = pinecone.Index(PINECONE_INDEX_upload_NAME))  # For uploaded 


st.markdown("""
    <style>
    .main {
        background-color: #0e1117;
        color: white;
    }
    .stButton>button {
        background-color: #4CAF50;
        color: white;
    }
    </style>
    """, unsafe_allow_html=True)

# Update the process_uploaded_pdf function
def process_uploaded_pdf(file_path):
    processor = DocumentProcessor()
    success, message = processor.process_uploaded_document(file_path)
    return success, message


def get_chunk_content(chunk_id, source="uploaded"):
    """Retrieve chunk content from appropriate JSON file based on source"""
    #print(source)
    try:
        # Select appropriate JSON file based on source '''PROCESSED_CHUNKS_PATH'''s
        json_path =  PROCESSED_CHUNKS_PATH if source == "uploaded" else EXISTING_CHUNKS_PATH
        #print(json_path)
        with open(json_path, 'r') as file:
            data = json.load(file)
        for item in data:
            if item.get('chunk_id') == chunk_id:
                return item.get('content', "Content not found")
        return "Chunk ID not found"
    except Exception as e:
        return f"Error retrieving content: {str(e)}"
    
def prepare_input(retrieved_chunks_with_content, query):
    """Prepare prompt for GPT model"""
    combined_content = "\n\n".join([
        f"ID: {chunk['chunk_id']}\nContent: {chunk['content']}" 
        for chunk in retrieved_chunks_with_content
    ])

    prompt = f"""You are an expert in United States building codes. Armed with sections from these codes, your task is to provide comprehensive and informative answers to user queries. Ensure your responses are well-supported by citing the specific sources of the information. 
            * **NOTE: If the provided context or sections are not directly relevant, rely on your general knowledge of building codes to formulate the answer. Avoid mentioning the absence of specific sections or content directly related to the query. Instead, provide the most relevant information available based on your expertise.**
            


    User Question: {query}

    Context: 
    {combined_content}

    Response:"""

    return prompt 
    

def generate_response(prompt):
    """Generate response using GPT-4"""
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1000,
            temperature=0.7,
        )
        return response['choices'][0]['message']['content']
    except Exception as e:
        return f"Error generating response: {str(e)}"
    
def augment_query(original_query):
    """Enhance user query using GPT-4"""
    custom_prompt = '''
        You are a building code expert assistant tasked with refining queries related to U.S. building codes.
    Focus on enhancing the query by:
    - Identifying and emphasizing main concepts related to building codes.
    - Using specific building code terminology.
    - Including relevant code references where appropriate.
    - Clarifying legal requirements or standards.
    - Keeping geographic specificity accurate.
    - Specifying whether the query seeks definitions, requirements, or procedures.
   '''
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": custom_prompt},
                {"role": "user", "content": f"Original query: {original_query}\nPlease enhance this query to be more specific and relevant to building codes."}
            ],
            max_tokens=150,
            temperature=0.7,
        )
        return response['choices'][0]['message']['content']
    except Exception as e:
        print(f"Error augmenting query: {str(e)}")
        return original_query

    
def fix_permissions(path):
    """Fix permissions on path and contents"""
    try:
        for root, dirs, files in os.walk(path):
            for dir_path in dirs:
                os.chmod(os.path.join(root, dir_path), stat.S_IWUSR)
            for file_path in files:
                os.chmod(os.path.join(root, file_path), stat.S_IWUSR)
    except Exception:
        pass    
def cleanup_files_and_vectors():
    """Delete temporary files, directories, and vector database contents"""
    try:
        # Delete files
        files_to_delete = [EMBEDDINGS_PATH, PROCESSED_CHUNKS_PATH]
        for file_path in files_to_delete:
            if Path(file_path).exists():
                Path(file_path).unlink()
        
        # Delete directories with permission fixing
        directories_to_delete = [Path('split_sections')]
        for dir_path in directories_to_delete:
            if dir_path.exists():
                fix_permissions(dir_path)
                shutil.rmtree(dir_path, ignore_errors=True)
        
        # Delete vectors only from upload index with namespace handling
        try:
            upload_index.delete(delete_all=True, namespace='')
        except Exception as e:
            print(f"Upload index cleanup: {str(e)}")
                
        return True, "Cleanup completed successfully"
    except Exception as e:
        return False, f"Cleanup failed: {str(e)}"



with st.sidebar:
    st.header("Document Upload")
    search_source = st.radio(
        "Search Source",
        ["Virginia Building Codes", "Upload Documents"],
        index=0  # Default to existing codes
    )
        # Add cleanup button
    if st.button("🧹 Cleanup"):
        with st.spinner("Cleaning up files and vector database..."):
            success, message = cleanup_files_and_vectors()
            if success:
                st.success(message)
            else:
                st.error(message)

    if search_source == "Upload Documents":
        uploaded_file = st.file_uploader(
            "Upload Building Code PDF",
            type=['pdf'],
            help="Upload the Virginia Building Code document"
        )
    
        if uploaded_file is not None:
            try:
                bytes_data = uploaded_file.read()
                file_name = uploaded_file.name
                temp_path = f"temp_{file_name}"
                
                with open(temp_path, "wb") as f:
                    f.write(bytes_data)
                    
                st.success(f"✅ {file_name} uploaded")
                
                if st.button("Process Document", type="primary"):
                    with st.spinner("Processing document..."):
                        success, message = process_uploaded_pdf(temp_path)
                        upserter = PineconeUpserter()
                        success, message = upserter.upsert_chunks()
                        if success:
                            st.success(message)
                        else:
                            st.error(message)
                    # num_chunks = process_uploaded_pdf(temp_path)
                    #st.success(f"✅ Document processed into {num_chunks} chunks")
                        
            except Exception as e:
                st.error(f"Error: {str(e)}")
                

# Main content
st.title("🏗️ Virginia Building Code Assistant")
st.divider()

# Query input
user_query = st.text_area("Enter your query about Virginia Building Codes:", height=100)

# Modify the query processing section
if st.button("Submit Query"):
    if user_query:
        try:
            source = "existing" if search_source == "Virginia Building Codes" else "uploaded"
            # Augment and embed query
            augmented_query = augment_query(user_query)
            query_embedding = generate_query_embedding(augmented_query)
            
            with st.expander("Enhanced Query"):
                st.write(augmented_query)

            # Select appropriate index based on search source
            active_index = upload_index if search_source == "Upload Documents" else index

            # Query selected Pinecone index
            results = active_index.query(
                vector=query_embedding,
                top_k=3,
                include_metadata=True
            )
        
            # Update the chunk content retrieval
            retrieved_chunks_with_content = []
            for match in results['matches']:
                chunk_id = match['id']
                content = get_chunk_content(chunk_id, source=source)
                
                chunk_info = {
                    'chunk_id': chunk_id,
                    'score': match['score'],
                    'metadata': match['metadata'],
                    'content': content
                }
                retrieved_chunks_with_content.append(chunk_info)
 
            # Display retrieved chunks
            st.success("Retrieved relevant building codes")
            for chunk in retrieved_chunks_with_content:
                with st.expander(f"Chapter {chunk['metadata'].get('chapter', 'Unknown')}, Section {chunk['metadata'].get('section', 'Unknown')}"):
                    st.write(f"Score: {chunk['score']:.4f}")
                    st.write(f"Subsection: {chunk['metadata'].get('subsection', 'Unknown')}")
                    st.write(f"Content: {chunk['content']}")
            
            # Generate and display AI response
            prompt = prepare_input(retrieved_chunks_with_content, user_query)
            ai_response = generate_response(prompt)
            
            st.header("AI Assistant Response")
            st.write(ai_response)
            
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
    else:
        st.warning("Please enter a query")
