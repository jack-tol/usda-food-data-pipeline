import pandas as pd
import re
import csv  # Importing csv directly from the standard library

# Define chunk size
chunk_size = 10000  # Adjust the chunk size as needed based on your memory capacity

# Create an empty list to collect the processed chunks
processed_chunks = []

# Function to normalize text
def normalize_text(text):
    # Remove special/hidden characters and normalize to UTF-8
    normalized = text.encode('utf-8', errors='ignore').decode('utf-8')
    # Replace multiple spaces with a single space
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    # Remove spaces before or after punctuation (e.g., quotes, commas)
    normalized = re.sub(r'\s*([.,!?;:()"])', r'\1', normalized)  # Space before punctuation
    normalized = re.sub(r'([.,!?;:()"])\s*', r'\1', normalized)  # Space after punctuation
    return normalized

# Iterate over the CSV file in chunks
for chunk in pd.read_csv("fda_food_nutrient_data.csv", chunksize=chunk_size, low_memory=False):
    # Convert the 'FOOD_ID' column to int
    chunk['FOOD_ID'] = chunk['FOOD_ID'].astype(int)
    
    # Convert all other columns to strings and apply normalization
    for col in chunk.columns:
        if col != 'FOOD_ID':
            chunk[col] = chunk[col].astype(str).apply(
                lambda x: "" if pd.isna(x) or x.lower() == 'nan' else normalize_text(x)
            )
    
    # Normalize column names without excessive quoting
    chunk.columns = [normalize_text(col) for col in chunk.columns]
    
    # Append the processed chunk to the list
    processed_chunks.append(chunk)

# Concatenate all chunks into a single DataFrame
df_final = pd.concat(processed_chunks)

# Export the adjusted DataFrame to a new CSV file using standard csv library quoting
df_final.to_csv(
    "fda_food_nutrient_data_adjusted_dtype.csv",
    index=False,
    quoting=csv.QUOTE_NONNUMERIC,  # This will quote all string fields without adding extra quotes
    na_rep=''  # Keeps NaN values as empty cells
)
