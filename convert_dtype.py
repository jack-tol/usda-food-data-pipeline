import pandas as pd

# Define chunk size
chunk_size = 10000  # Adjust the chunk size as needed based on your memory capacity

# Create an empty list to collect the processed chunks
processed_chunks = []

# Iterate over the CSV file in chunks
for chunk in pd.read_csv("food_data_final.csv", chunksize=chunk_size, low_memory=False):
    # Convert the 'FOOD_ID' column to int
    chunk['FOOD_ID'] = chunk['FOOD_ID'].astype(int)
    
    # Convert all other columns to strings
    for col in chunk.columns:
        if col != 'FOOD_ID':
            chunk[col] = chunk[col].astype(str)
    
    # Append the processed chunk to the list
    processed_chunks.append(chunk)

# Concatenate all chunks into a single DataFrame
df_final = pd.concat(processed_chunks)

# Export the adjusted DataFrame to a new CSV file
df_final.to_csv("food_data_final_adjusted_dtype.csv", index=False)
