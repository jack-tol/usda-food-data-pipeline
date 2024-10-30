import pandas as pd
import numpy as np
import csv
import os
import requests
from bs4 import BeautifulSoup
import zipfile
import shutil

thresholds = {
    'VITAMIN A, IU (IU)': 333333,
    'VITAMIN D (D2 + D3), INTERNATIONAL UNITS (IU)': 4000000,
    'VITAMIN E (LABEL ENTRY PRIMARILY) (IU)': 1493,
    'G': 100,
    'MG': 100000,
    'UG': 100000000,
    'ENERGY (KCAL)': 900,
    'ENERGY (KJ)': 3766,
}

def download_and_extract_data():
    url = "https://fdc.nal.usda.gov/download-datasets.html"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    download_link = next(
        (
            row.find_all("td")[2].find("a")["href"]
            for table in soup.find_all("table", class_="downloads_table")
            for row in table.find_all("tr")
            if row.find("td") and "Full Download of All Data Types" in row.find("td").get_text(strip=True)
        ),
        None
    )

    if download_link:
        full_url = f"https://fdc.nal.usda.gov{download_link}"
        file_name = full_url.split("/")[-1]
        
        with requests.get(full_url, stream=True) as response:
            response.raise_for_status()
            with open(file_name, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
        
        extracted_folder_name = file_name.replace('.zip', '')
        with zipfile.ZipFile(file_name, 'r') as zip_ref:
            zip_ref.extractall(extracted_folder_name)
        
        nested_folder = next(
            (name for name in os.listdir(extracted_folder_name) if os.path.isdir(os.path.join(extracted_folder_name, name))),
            None
        )

        if nested_folder:
            nested_folder_path = os.path.join(extracted_folder_name, nested_folder)
            files_to_extract = ["nutrient.csv", "food.csv", "branded_food.csv", "food_nutrient.csv"]
            for file in files_to_extract:
                src = os.path.join(nested_folder_path, file)
                dest = os.path.join(".", file)
                if os.path.exists(src):
                    shutil.move(src, dest)

            shutil.rmtree(extracted_folder_name)
            os.remove(file_name)

def clean_branded_food(branded_food_df):
    df_sorted = branded_food_df.sort_values(by=['gtin_upc', 'fdc_id'], ascending=[True, False])
    df_latest = df_sorted.drop_duplicates(subset='gtin_upc', keep='first')
    df_filtered = df_latest[['fdc_id', 'gtin_upc', 'ingredients', 'serving_size', 'serving_size_unit']].rename(
        columns={'fdc_id': 'FOOD_RECORD_ID', 'gtin_upc': 'FOOD_ID', 'ingredients': 'FOOD_INGREDIENTS'}
    )
    for col in df_filtered.select_dtypes(include='object').columns:
        df_filtered[col] = df_filtered[col].str.strip().str.upper()

    df_filtered['FOOD_SERVING_SIZE'] = (
        df_filtered['serving_size'].astype(str).str.strip() + ' ' + 
        df_filtered['serving_size_unit'].str.strip().str.upper()
    )
    df_filtered = df_filtered.drop(columns=['serving_size', 'serving_size_unit']).sort_values(by='FOOD_RECORD_ID')
    return df_filtered

def clean_food(food_df, cleaned_branded_food_df):
    food_df = food_df.rename(columns={'fdc_id': 'FOOD_RECORD_ID', 'description': 'FOOD_NAME'})
    food_df['FOOD_NAME'] = food_df['FOOD_NAME'].str.strip().str.upper()
    return food_df[food_df['FOOD_RECORD_ID'].isin(cleaned_branded_food_df['FOOD_RECORD_ID'])][['FOOD_RECORD_ID', 'FOOD_NAME']]

def clean_nutrient(nutrient_df):
    nutrient_df = nutrient_df.rename(columns={'id': 'NUTRIENT_ID', 'name': 'NUTRIENT_NAME', 'unit_name': 'NUTRIENT_UNIT'})
    nutrient_df['FOOD_NUTRIENT_NAME'] = (
        nutrient_df['NUTRIENT_NAME'].str.upper().str.strip() +
        ' (' + nutrient_df['NUTRIENT_UNIT'].str.upper().str.strip() + ')'
    )
    return nutrient_df[['NUTRIENT_ID', 'FOOD_NUTRIENT_NAME']]

def clean_food_nutrient(cleaned_branded_food_df, food_nutrient_df):
    food_nutrient_df = food_nutrient_df.rename(columns={
        "fdc_id": "FOOD_RECORD_ID",
        "nutrient_id": "NUTRIENT_ID",
        "amount": "NUTRIENT_QUANTITY"
    })
    filtered_food_nutrient_df = food_nutrient_df[food_nutrient_df['FOOD_RECORD_ID'].isin(cleaned_branded_food_df['FOOD_RECORD_ID'])]
    aggregated_df = filtered_food_nutrient_df.groupby(["FOOD_RECORD_ID", "NUTRIENT_ID"], as_index=False).mean()
    return aggregated_df.pivot(index="FOOD_RECORD_ID", columns="NUTRIENT_ID", values="NUTRIENT_QUANTITY").reset_index()

def map_nutrient_names_to_nutrient_ids(cleaned_nutrient_df, cleaned_food_nutrient_df):
    nutrient_map = dict(zip(cleaned_nutrient_df['NUTRIENT_ID'], cleaned_nutrient_df['FOOD_NUTRIENT_NAME']))
    return cleaned_food_nutrient_df.rename(columns=nutrient_map)

def merge_cleaned_data_into_final_df(cleaned_branded_food_df, cleaned_food_df, mapped_nutrient_names_to_nutrient_ids_df):
    merged_data = pd.merge(cleaned_branded_food_df, cleaned_food_df, on='FOOD_RECORD_ID', how='inner')
    final_data = pd.merge(merged_data, mapped_nutrient_names_to_nutrient_ids_df, on='FOOD_RECORD_ID', how='inner')
    ordered_columns = ['FOOD_RECORD_ID', 'FOOD_ID', 'FOOD_NAME', 'FOOD_SERVING_SIZE', 'FOOD_INGREDIENTS'] + sorted(
        [col for col in final_data.columns if col not in ['FOOD_RECORD_ID', 'FOOD_ID', 'FOOD_NAME', 'FOOD_SERVING_SIZE', 'FOOD_INGREDIENTS']]
    )
    return final_data[ordered_columns]

def apply_nutrient_thresholds(final_data):
    nutrient_columns = final_data.columns[final_data.columns.get_loc('FOOD_SERVING_SIZE') + 1:]
    for column in nutrient_columns:
        unit = column.split('(')[-1].replace(')', '').strip()
        threshold = thresholds.get(column, thresholds.get(unit))
        if threshold is not None:
            final_data[column] = final_data[column].where(final_data[column] <= threshold, np.nan)
    return final_data

def remove_invalid_serving_sizes(final_data):
    final_data = final_data[~final_data['FOOD_SERVING_SIZE'].str.contains("IU", na=False)]
    return final_data

def execute_pipeline():
    download_and_extract_data()

    branded_food_df = pd.read_csv("branded_food.csv", low_memory=False)
    food_df = pd.read_csv("food.csv", low_memory=False)
    nutrient_df = pd.read_csv("nutrient.csv", low_memory=False)
    food_nutrient_df = pd.read_csv("food_nutrient.csv", low_memory=False)

    cleaned_branded_food_df = clean_branded_food(branded_food_df)
    cleaned_food_df = clean_food(food_df, cleaned_branded_food_df)
    cleaned_nutrient_df = clean_nutrient(nutrient_df)
    cleaned_food_nutrient_df = clean_food_nutrient(cleaned_branded_food_df, food_nutrient_df)
    mapped_nutrient_names_to_nutrient_ids_df = map_nutrient_names_to_nutrient_ids(cleaned_nutrient_df, cleaned_food_nutrient_df)
    
    final_data = merge_cleaned_data_into_final_df(cleaned_branded_food_df, cleaned_food_df, mapped_nutrient_names_to_nutrient_ids_df)
    
    final_data = apply_nutrient_thresholds(final_data)

    final_data = remove_invalid_serving_sizes(final_data)

    final_data.to_csv("usda_branded_food_data.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)

    for file in ["branded_food.csv", "food.csv", "nutrient.csv", "food_nutrient.csv"]:
        os.remove(file)

execute_pipeline()