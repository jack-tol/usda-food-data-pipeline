import pandas as pd
import re
import logging
import csv  # Importing csv for quoting constants used directly in pandas
import os  # Importing os for file operations

# Set up logging
logging.basicConfig(level=logging.INFO)

# Function to normalize text
def normalize_text(text):
    # Remove special/hidden characters and normalize to UTF-8
    normalized = text.encode('utf-8', errors='ignore').decode('utf-8')
    # Replace multiple spaces with a single space
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    # Remove spaces before or after punctuation (e.g., quotes, commas), but keep commas in ingredients field
    normalized = re.sub(r'\s*([.!?;:()"])', r'\1', normalized)  # Space before punctuation except commas
    normalized = re.sub(r'([.!?;:()"])\s*', r'\1', normalized)  # Space after punctuation except commas
    return normalized

# Function to normalize and convert dtype in chunks
def normalize_and_convert_dtype_in_chunks(df, chunk_size=100000):
    chunks = []
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start + chunk_size].copy()
        
        # Convert all columns except 'FOOD_ID' and 'FOOD_INGREDIENTS' to strings and normalize
        for col in chunk.columns:
            if col not in ['FOOD_ID', 'FOOD_INGREDIENTS']:
                chunk[col] = chunk[col].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
                chunk[col] = chunk[col].apply(lambda x: normalize_text(x) if x.lower() != 'nan' else "")
            elif col == 'FOOD_INGREDIENTS':
                # Ensure ingredients field remains unchanged except removing excess whitespace
                chunk[col] = chunk[col].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()

        # Normalize column names
        chunk.columns = [normalize_text(col) for col in chunk.columns]
        
        # Drop rows where 'FOOD_ID' or 'FOOD_NAME' is missing
        chunk = chunk.dropna(subset=['FOOD_ID', 'FOOD_NAME'])
        
        chunks.append(chunk)

    # Concatenate all chunks back into a single DataFrame
    normalized_and_converted_df = pd.concat(chunks, ignore_index=True)
    return normalized_and_converted_df

def cleanup_files(file_list):
    """Remove files from the file system."""
    for file in file_list:
        try:
            os.remove(file)
            logging.info(f"Successfully removed {file}")
        except OSError as e:
            logging.error(f"Error removing {file}: {e}")

def read_and_clean_branded_food(file_path):
    branded_food_df = pd.read_csv(file_path, low_memory=False)
    columns_to_drop = [
        "subbrand_name", "brand_owner", "brand_name", "gtin_upc",
        "not_a_significant_source_of", "household_serving_fulltext",
        "branded_food_category", "data_source", "package_weight",
        "modified_date", "available_date", "market_country",
        "discontinued_date", "preparation_state_code", "trade_channel",
        "short_description"
    ]
    branded_food_df.drop(columns=columns_to_drop, inplace=True)
    branded_food_df['food_serving_size'] = branded_food_df['serving_size'].astype(str) + ' ' + branded_food_df['serving_size_unit']
    branded_food_df.drop(columns=['serving_size', 'serving_size_unit'], inplace=True)
    branded_food_df.rename(columns={'fdc_id': 'food_id'}, inplace=True)
    return branded_food_df

def merge_branded_food_with_food_data(branded_food_df, food_file_path):
    food_df = pd.read_csv(food_file_path)
    merged_branded_food_df = pd.merge(branded_food_df, food_df[['fdc_id', 'description']], left_on='food_id', right_on='fdc_id', how='left')
    merged_branded_food_df.rename(columns={'description': 'food_name'}, inplace=True)
    merged_branded_food_df.drop(columns=['fdc_id'], inplace=True)
    merged_branded_food_df.rename(columns={'ingredients': 'food_ingredients'}, inplace=True)
    merged_branded_food_df = merged_branded_food_df[['food_id', 'food_name', 'food_ingredients', 'food_serving_size']]
    return merged_branded_food_df

def clean_nutrient(file_path):
    nutrient_df = pd.read_csv(file_path, low_memory=False)
    cleaned_nutrient_df = nutrient_df[['id', 'name', 'unit_name']].rename(
        columns={'id': 'nutrient_id', 'name': 'nutrient_name', 'unit_name': 'nutrient_unit'}
    )
    cleaned_nutrient_df['nutrient_id'] = pd.to_numeric(cleaned_nutrient_df['nutrient_id'], errors='coerce')
    cleaned_nutrient_df = cleaned_nutrient_df.sort_values(by='nutrient_id', ascending=True)
    return cleaned_nutrient_df

def clean_food_nutrient(file_path):
    food_nutrient_df = pd.read_csv(file_path, low_memory=False)
    cleaned_food_nutrient_df = food_nutrient_df[['fdc_id', 'nutrient_id', 'amount']].rename(columns={'fdc_id': 'food_id'})
    cleaned_food_nutrient_df['food_id'] = pd.to_numeric(cleaned_food_nutrient_df['food_id'], errors='coerce')
    cleaned_food_nutrient_df = cleaned_food_nutrient_df.sort_values(by='food_id', ascending=True)
    return cleaned_food_nutrient_df

def merge_food_nutrient_with_nutrient_information(cleaned_food_nutrient_df, cleaned_nutrient_df):
    merged_food_nutrient_df = pd.merge(cleaned_food_nutrient_df, cleaned_nutrient_df, on='nutrient_id')
    merged_food_nutrient_df['nutrient_amount'] = merged_food_nutrient_df['amount'].astype(str) + " " + merged_food_nutrient_df['nutrient_unit']
    merged_food_nutrient_df = merged_food_nutrient_df[['food_id', 'nutrient_name', 'nutrient_amount']]
    return merged_food_nutrient_df

def pivot_food_nutrient_with_nutrient_information(merged_food_nutrient_df):
    pivoted_nutrient_df = merged_food_nutrient_df.pivot_table(
        index='food_id', columns='nutrient_name', values='nutrient_amount', aggfunc='first'
    )
    pivoted_nutrient_df.reset_index(inplace=True)
    return pivoted_nutrient_df

def merge_final_data(merged_branded_food_df, pivoted_nutrient_df):
    final_merged_df = pd.merge(merged_branded_food_df, pivoted_nutrient_df, on='food_id', how='left')
    
    # Convert all text columns to uppercase
    final_merged_df = final_merged_df.apply(lambda x: x.str.upper() if x.dtype == "object" else x)
    
    # Convert column names to uppercase
    final_merged_df.columns = [col.upper() for col in final_merged_df.columns]
    
    # Sort the final DataFrame by 'food_id' in ascending order
    final_merged_df = final_merged_df.sort_values(by='FOOD_ID', ascending=True)
    
    return final_merged_df

def main():
    branded_food_df = read_and_clean_branded_food("branded_food.csv")
    merged_branded_food_df = merge_branded_food_with_food_data(branded_food_df, "food.csv")
    cleaned_nutrient_df = clean_nutrient("nutrient.csv")
    cleaned_food_nutrient_df = clean_food_nutrient("food_nutrient.csv")
    merged_food_nutrient_df = merge_food_nutrient_with_nutrient_information(cleaned_food_nutrient_df, cleaned_nutrient_df)
    pivoted_nutrient_df = pivot_food_nutrient_with_nutrient_information(merged_food_nutrient_df)
    final_df = merge_final_data(merged_branded_food_df, pivoted_nutrient_df)
    
    # Normalize and convert data types directly in the DataFrame in chunks
    normalized_and_converted_df = normalize_and_convert_dtype_in_chunks(final_df)
    
    # Export the final normalized DataFrame to a CSV file
    normalized_and_converted_df.to_csv("fda_food_nutrient_data.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
    
    # Cleanup initial CSV files
    files_to_cleanup = ["branded_food.csv", "food.csv", "nutrient.csv", "food_nutrient.csv"]
    cleanup_files(files_to_cleanup)

if __name__ == "__main__":
    main()