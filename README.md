

# **🏗 Virginia Building Code Assistant**  

A **Retrieval-Augmented Generation (RAG)** system designed for efficient querying and understanding of **Virginia building codes** using **Natural Language Processing (NLP)** and **Semantic Search**.

## **🌐 Live Deployment**  
The **Virginia Building Code Assistant** is deployed on **Streamlit Community Cloud**, providing users with seamless access without any setup.  

🔗 **[Access the Live App](https://virginia-building-codes.streamlit.app/)**  

---

## **🚀 Features**  
This application provides advanced **building code search capabilities** through **AI-powered Natural Language Processing**:

### **🔍 1. Natural Language Query Processing**  
- Users can enter **plain English queries**, and the system intelligently retrieves relevant sections of **Virginia Building Codes**.

### **📂 2. Dual Search Functionality**  
- Query **official Virginia Building Codes** or **custom uploaded PDFs** for personalized document search.

### **🧠 3. Semantic Search with Vector Databases**  
- Uses **Pinecone’s vector database** to search for **contextually relevant results** based on embeddings.

### **🤖 4. Query Enhancement with GPT-4**  
- **Before querying the database**, the system **enhances** the input query using **GPT-4**, improving search accuracy.

### **🎛 5. User-Friendly Streamlit Interface**  
- **Intuitive UI** built using **Streamlit**, allowing users to **easily upload PDFs, enter queries, and retrieve results**.

### **📄 6. Advanced PDF Processing**  
- Supports **chunking and processing** of **custom building code documents** to make them **searchable**.

---

## **🛠 Prerequisites**
Before setting up the project, make sure you have:

- ✅ **Python 3.8+**  
- ✅ **Git Installed**  
- ✅ **OpenAI API Key**  
- ✅ **Pinecone API Key**  

---

## **⚙️ Installation Guide**

Follow the steps below to set up and run the project **locally**.

### **1️⃣ Clone the Repository**
```bash
git clone https://github.com/Venkat-Git98/RAG_Building_Codes_Virginia.git
cd virginia-building-code-assistant
```

### **2️⃣ Create a Virtual Environment**  
It’s always recommended to work inside a virtual environment to avoid dependency conflicts.
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

### **3️⃣ Install Dependencies**
```bash
pip install -r requirements.txt
```

### **4️⃣ Install Additional NLP Requirements**  
This project requires **spaCy** and **NLTK** for text processing. Install them using:
```bash
python -m spacy download en_core_web_sm
python -m nltk.downloader stopwords punkt
```

---

## **🔑 Configuration Guide**  

### **1️⃣ Create a `.env` File**  
The project requires API keys for **OpenAI** and **Pinecone**.  

Create a **`.env` file** in the **project root directory** and add the following:
```env
OPENAI_API_KEY=your_openai_key
PINECONE_API_KEY=your_pinecone_key
PINECONE_INDEX_NAME=your_index_name
PINECONE_INDEX_upload_NAME=your_upload_index_name
```
🔹 Replace placeholders with your actual API keys and **index names**.  

---

## **📁 Project Structure**
Understanding the folder structure helps in navigating the project efficiently.  

```
virginia-building-code-assistant/
├── app/
│   ├── main.py              # Streamlit interface (UI)
│   ├── document_processor.py # Handles PDF text extraction and chunking
│   ├── clean.py             # Utilities for text cleaning
│   ├── chunk.py             # Splits large documents into smaller sections
│   ├── generator.py         # Generates embeddings for text using OpenAI
│   ├── pinecone_ops.py      # Manages vector database operations
│   └── config.py            # Configuration settings (API keys, directories)
├── data/
│   ├── existing_codes/      # Stores processed Virginia building codes
│   ├── uploaded_codes/      # Stores user-uploaded documents
│   └── embeddings/          # Stores generated text embeddings
├── requirements.txt         # Lists all dependencies for the project
└── .env                     # Stores API keys (not included in Git)
```

---

## **🚀 Usage Guide**

### **1️⃣ Start the Application**
Run the Streamlit app:
```bash
streamlit run app/main.py
```

### **2️⃣ Using the Interface**
- **📂 Select Search Source**: Choose between **Virginia Building Codes** or **Custom Document Uploads**.  
- **📄 Upload PDFs**:
  - Go to the **sidebar**, click **"Upload Document"**, and select a **PDF file**.  
  - Click **"Process Document"** and wait for processing.  
- **🔍 Enter Your Query**:  
  - Use the input box to enter a **building code-related question** in **plain English**.  
- **📜 View Results**:
  - **Enhanced Query** (modified for better accuracy).  
  - **Relevant Virginia Building Codes**.  
  - **AI-generated response** based on retrieved text.  

---

## **⚠️ Error Handling & Debugging**

### **🔄 Cleanup Function**
- The **"Cleanup"** button in the sidebar helps manage storage by:
  - **🗑 Removing temporary files**  
  - **🔄 Clearing vector database entries**  
  - **🔧 Fixing permission issues**  

### **🛠 Common Issues & Fixes**
#### **1️⃣ OpenAI API Errors**
- **Issue**: Invalid API Key  
- **Fix**: Ensure your **OpenAI API key** is correct in the `.env` file.  

#### **2️⃣ Pinecone Errors**
- **Issue**: Index not found  
- **Fix**: Ensure correct **Pinecone API key and index name** are used.  

---

## **📌 Dependencies**
```text
streamlit==1.28.0
openai==1.3.0
pinecone-client==2.2.4
PyMuPDF==1.23.5
pymupdf4llm==0.1.1
pandas==2.1.1
nltk==3.8.1
spacy==3.7.2
python-dotenv==1.0.0
tqdm==4.66.1
```

---



## **📧 Contact & Support**
For any issues, feel free to raise a GitHub issue or reach out via email.

📧 **Email**: svenkatesh.js@gmail.com  
