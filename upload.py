import pandas as pd
import json
from langchain_pinecone import PineconeEmbeddings, PineconeVectorStore
from langchain_core.documents import Document

# Load the CSV file
file_path = 'fda_food_nutrient_data_sample.csv'
df = pd.read_csv(file_path, low_memory=False)

# Initialize dictionaries for texts and metadatas
texts = {}
metadatas = {}

# Iterate through each row and build the dictionaries
for idx, row in df.iterrows():
    doc_key = f"doc{idx + 1}"  # Create keys like 'doc1', 'doc2', etc.
    
    # Add food_name to texts dictionary
    texts[doc_key] = row['food_name']
    
    # Filter out null values from metadata
    metadata = {col: row[col] for col in df.columns if pd.notnull(row[col]) and col != 'food_name'}
    metadatas[doc_key] = metadata

# Combine the dictionaries into a single output structure
output_data = {
    "texts": texts,
    "metadatas": metadatas
}

# Export the output dictionary as a JSON file for inspection (optional)
output_file = 'output_data.json'
with open(output_file, 'w') as f:
    json.dump(output_data, f, indent=4)

print(f"Data has been exported to {output_file}")

# Initialize Pinecone Embeddings
embedding_model = PineconeEmbeddings(model="multilingual-e5-large")

# Connect to an existing Pinecone index
index_name = "food-data"
vector_store = PineconeVectorStore.from_existing_index(index_name=index_name, embedding=embedding_model)

# Create Document objects from the processed data
documents = [
    Document(page_content=texts[doc_id], metadata=metadatas[doc_id])
    for doc_id in texts.keys()
]

# Add documents (texts and metadata) to Pinecone
vector_store.add_documents(documents)

print(f"Uploaded {len(documents)} documents to Pinecone index '{index_name}'")