import os
import re
import csv
import shutil
import zipfile
import requests
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

thresholds: dict[str, int] = {
    'VITAMIN A, IU (IU)': 333333,
    'VITAMIN D (D2 + D3), INTERNATIONAL UNITS (IU)': 4000000,
    'VITAMIN E (LABEL ENTRY PRIMARILY) (IU)': 1493,
    'G': 100,
    'MG': 100000,
    'UG': 100000000,
    'ENERGY (KCAL)': 900,
    'ENERGY (KJ)': 3766,
}

def download_usda_food_data() -> str | None:
    options = Options()
    options.add_argument("--log-level=3")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get('https://fdc.nal.usda.gov/download-datasets.html')

    try:
        match = re.search(r'Full Download of All Data Types.*?href="(/fdc-datasets/FoodData_Central_csv_.*?\.zip)"',
                          driver.page_source, re.DOTALL)
        download_link = f"https://fdc.nal.usda.gov{match.group(1)}" if match else None

        if download_link:
            response = requests.get(download_link, stream=True)
            filepath = os.path.join(os.getcwd(), download_link.split('/')[-1])

            with open(filepath, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            return filepath
        else:
            print("CSV link not found.")
            return None
    finally:
        driver.quit()

def extract_zip(zip_path: str, extract_to: str) -> None:
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

def move_target_files(source_folder: str, target_files: list[str]) -> None:
    for file_name in target_files:
        source_path = os.path.join(source_folder, file_name)
        if os.path.exists(source_path):
            shutil.move(source_path, os.getcwd())

def cleanup(files_to_delete: list[str]) -> None:
    for path in files_to_delete:
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)

def clean_branded_food(branded_food_df: pd.DataFrame) -> pd.DataFrame:
    df_sorted = branded_food_df.sort_values(by=['gtin_upc', 'fdc_id'], ascending=[True, False])
    df_latest = df_sorted.drop_duplicates(subset='gtin_upc', keep='first')
    df_filtered = df_latest[['fdc_id', 'gtin_upc', 'ingredients', 'serving_size', 'serving_size_unit']].rename(
        columns={'fdc_id': 'FOOD_RECORD_ID', 'gtin_upc': 'FOOD_ID', 'ingredients': 'FOOD_INGREDIENTS'}
    )
    for col in df_filtered.select_dtypes(include='object').columns:
        df_filtered[col] = df_filtered[col].str.strip().str.upper()

    df_filtered['serving_size'] = pd.to_numeric(df_filtered['serving_size'], errors='coerce').round(2)
    df_filtered['FOOD_SERVING_SIZE'] = (
        df_filtered['serving_size'].astype(str).str.strip() + ' ' + df_filtered['serving_size_unit'].str.strip().str.upper()
    )
    return df_filtered.drop(columns=['serving_size', 'serving_size_unit']).sort_values(by='FOOD_RECORD_ID')

def clean_food(food_df: pd.DataFrame, cleaned_branded_food_df: pd.DataFrame) -> pd.DataFrame:
    food_df = food_df.rename(columns={'fdc_id': 'FOOD_RECORD_ID', 'description': 'FOOD_NAME'})
    food_df['FOOD_NAME'] = food_df['FOOD_NAME'].str.strip().str.upper()
    return food_df[food_df['FOOD_RECORD_ID'].isin(cleaned_branded_food_df['FOOD_RECORD_ID'])][['FOOD_RECORD_ID', 'FOOD_NAME']]

def clean_nutrient(nutrient_df: pd.DataFrame) -> pd.DataFrame:
    nutrient_df = nutrient_df.rename(columns={'id': 'NUTRIENT_ID', 'name': 'NUTRIENT_NAME', 'unit_name': 'NUTRIENT_UNIT'})
    nutrient_df['FOOD_NUTRIENT_NAME'] = (
        nutrient_df['NUTRIENT_NAME'].str.upper().str.strip() + ' (' + nutrient_df['NUTRIENT_UNIT'].str.upper().str.strip() + ')'
    )
    return nutrient_df[['NUTRIENT_ID', 'FOOD_NUTRIENT_NAME']]

def clean_food_nutrient(cleaned_branded_food_df: pd.DataFrame, food_nutrient_df: pd.DataFrame) -> pd.DataFrame:
    food_nutrient_df = food_nutrient_df.rename(columns={
        "fdc_id": "FOOD_RECORD_ID",
        "nutrient_id": "NUTRIENT_ID",
        "amount": "NUTRIENT_QUANTITY"
    })
    filtered_food_nutrient_df = food_nutrient_df[food_nutrient_df['FOOD_RECORD_ID'].isin(cleaned_branded_food_df['FOOD_RECORD_ID'])]
    aggregated_df = filtered_food_nutrient_df.groupby(["FOOD_RECORD_ID", "NUTRIENT_ID"], as_index=False).mean()
    return aggregated_df.pivot(index="FOOD_RECORD_ID", columns="NUTRIENT_ID", values="NUTRIENT_QUANTITY").reset_index()

def map_nutrient_names_to_nutrient_ids(cleaned_nutrient_df: pd.DataFrame, cleaned_food_nutrient_df: pd.DataFrame) -> pd.DataFrame:
    nutrient_map = dict(zip(cleaned_nutrient_df['NUTRIENT_ID'], cleaned_nutrient_df['FOOD_NUTRIENT_NAME']))
    return cleaned_food_nutrient_df.rename(columns=nutrient_map)

def merge_cleaned_data_into_final_df(cleaned_branded_food_df: pd.DataFrame, cleaned_food_df: pd.DataFrame, mapped_nutrient_names_to_nutrient_ids_df: pd.DataFrame) -> pd.DataFrame:
    merged_data = pd.merge(cleaned_branded_food_df, cleaned_food_df, on='FOOD_RECORD_ID', how='inner')
    final_data = pd.merge(merged_data, mapped_nutrient_names_to_nutrient_ids_df, on='FOOD_RECORD_ID', how='inner')
    ordered_columns = ['FOOD_RECORD_ID', 'FOOD_ID', 'FOOD_NAME', 'FOOD_SERVING_SIZE', 'FOOD_INGREDIENTS'] + sorted(
        [col for col in final_data.columns if col not in ['FOOD_RECORD_ID', 'FOOD_ID', 'FOOD_NAME', 'FOOD_SERVING_SIZE', 'FOOD_INGREDIENTS']]
    )
    return final_data[ordered_columns]

def apply_nutrient_thresholds(final_data: pd.DataFrame) -> pd.DataFrame:
    nutrient_columns = final_data.columns[final_data.columns.get_loc('FOOD_SERVING_SIZE') + 1:]
    for column in nutrient_columns:
        unit = column.split('(')[-1].replace(')', '').strip()
        threshold = thresholds.get(column, thresholds.get(unit))
        if threshold is not None:
            final_data[column] = final_data[column].where(final_data[column] <= threshold, np.nan)
    final_data[nutrient_columns] = final_data[nutrient_columns].round(2)
    return final_data

def remove_invalid_serving_sizes(final_data: pd.DataFrame) -> pd.DataFrame:
    final_data = final_data[~final_data['FOOD_SERVING_SIZE'].str.contains("IU", na=False)]
    final_data = final_data.dropna(subset=['FOOD_SERVING_SIZE'])
    return final_data

def execute_pipeline() -> None:
    zip_file_path = download_usda_food_data()
    if zip_file_path:
        extract_zip(zip_file_path, os.getcwd())
        
        target_files = ["branded_food.csv", "food.csv", "nutrient.csv", "food_nutrient.csv"]
        extracted_folder_path = os.path.splitext(zip_file_path)[0]
        move_target_files(extracted_folder_path, target_files)
        cleanup([zip_file_path, extracted_folder_path])

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

        final_data = final_data.dropna(subset=['FOOD_INGREDIENTS'])        
        final_data = apply_nutrient_thresholds(final_data)
        final_data = remove_invalid_serving_sizes(final_data)

        final_data['FOOD_RECORD_ID'] = final_data['FOOD_RECORD_ID'].astype(str)
        
        final_data.to_csv("usda_branded_food_data.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)

        for file in target_files:
            os.remove(file)

execute_pipeline()