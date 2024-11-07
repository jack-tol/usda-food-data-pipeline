import pandas as pd
import time
from pinecone.grpc import PineconeGRPC as Pinecone
from pinecone import ServerlessSpec

pc = Pinecone()

df = pd.read_csv('usda_branded_food_data.csv', low_memory=False, dtype={'FOOD_RECORD_ID': str})

texts = []
metadatas = []

for _, row in df.iterrows():
    food_name = row['FOOD_NAME']
    texts.append(food_name)
    metadata = {column: row[column] for column in df.columns if pd.notna(row[column]) and column != 'FOOD_NAME'}
    metadata["FOOD_NAME"] = food_name
    metadatas.append(metadata)

batch_size = 90
index_name = "branded-food-data"

if not pc.has_index(index_name):
    pc.create_index(
        name=index_name,
        dimension=1024,
        metric="cosine",
        spec=ServerlessSpec(
            cloud='aws', 
            region='us-east-1'
        ) 
    ) 

while not pc.describe_index(index_name).status['ready']:
    time.sleep(1)

index = pc.Index(index_name)

def generate_embeddings_with_retry(pc, text_batch, max_retries=5):
    for attempt in range(max_retries):
        try:
            embeddings = pc.inference.embed(
                model="multilingual-e5-large",
                inputs=text_batch,
                parameters={"input_type": "passage", "truncate": "END"}
            )
            return embeddings
        except Exception as e:
            print(f"Embedding generation failed on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                print("Max retries for embedding generation reached. Skipping this batch.")
                return None

def upload_documents_with_retry(index, texts, metadatas, batch_size, max_retries=15):
    for i in range(0, len(texts), batch_size):
        text_batch = texts[i:i + batch_size]
        metadata_batch = metadatas[i:i + batch_size]

        embeddings = generate_embeddings_with_retry(pc, text_batch)
        if embeddings is None:
            continue

        records = []
        for j, embedding in enumerate(embeddings):
            records.append({
                "id": str(i + j),
                "values": embedding['values'],
                "metadata": metadata_batch[j]
            })

        for attempt in range(max_retries):
            try:
                index.upsert(vectors=records)
                print(f"Uploaded {len(records)} records to Pinecone index '{index_name}' on attempt {attempt + 1}")
                break
            except Exception as e:
                print(f"Attempt {attempt + 1} failed with error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print("Max retries reached. Some records may not have been uploaded.")

upload_documents_with_retry(index, texts, metadatas, batch_size)