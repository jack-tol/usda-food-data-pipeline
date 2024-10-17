import pandas as pd
from langchain_pinecone import PineconeEmbeddings, PineconeVectorStore
from langchain_core.documents import Document

df = pd.read_csv('fda_food_nutrient_data_small_sample.csv', low_memory=False)

texts = []
metadatas = []

for _, row in df.iterrows():
    food_name = row['FOOD_NAME']
    texts.append(food_name)
    
    metadata = {}
    for column in df.columns:
        if pd.notna(row[column]) and column != 'FOOD_NAME':
            metadata[column] = row[column]
    
    metadatas.append(metadata)

embedding_model = PineconeEmbeddings(model="multilingual-e5-large")

index_name = "food-data"
vector_store = PineconeVectorStore.from_existing_index(index_name=index_name, embedding=embedding_model)

documents = [
    Document(page_content=texts[doc_id], metadata=metadatas[doc_id])
    for doc_id in range(len(texts))
]

vector_store.add_documents(documents)

print(f"Uploaded {len(documents)} documents to Pinecone index '{index_name}'")