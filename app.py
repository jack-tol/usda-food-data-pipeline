import pandas as pd
import os

def clean_food_nutrient():
    df = pd.read_csv('food_nutrient.csv', low_memory=False)
    food_nutrient_df_cleaned = df[['fdc_id', 'nutrient_id', 'amount']].rename(columns={'fdc_id': 'food_id'})
    food_nutrient_df_cleaned['food_id'] = pd.to_numeric(food_nutrient_df_cleaned['food_id'], errors='coerce')
    food_nutrient_df_cleaned = food_nutrient_df_cleaned.sort_values(by='food_id', ascending=True)
    return food_nutrient_df_cleaned

def clean_food():
    df = pd.read_csv('food.csv', low_memory=False)
    food_df_cleaned = df[['fdc_id', 'description']].rename(columns={'fdc_id': 'food_id', 'description': 'food_name'})
    food_df_cleaned = food_df_cleaned.sort_values(by='food_id', ascending=True)
    return food_df_cleaned

def clean_nutrient():
    df = pd.read_csv('nutrient.csv', low_memory=False)
    nutrient_df_cleaned = df[['id', 'name', 'unit_name']].rename(columns={'id': 'nutrient_id', 'name': 'nutrient_name', 'unit_name': 'nutrient_unit'})
    nutrient_df_cleaned['nutrient_id'] = pd.to_numeric(nutrient_df_cleaned['nutrient_id'], errors='coerce')
    nutrient_df_cleaned = nutrient_df_cleaned.sort_values(by='nutrient_id', ascending=True)
    return nutrient_df_cleaned

def merge_food_nutrient_with_nutrient_information(food_nutrient_df_cleaned, nutrient_df_cleaned):
    merged_df = pd.merge(food_nutrient_df_cleaned, nutrient_df_cleaned, on='nutrient_id')
    merged_df['nutrient_amount'] = merged_df['amount'].astype(str) + " " + merged_df['nutrient_unit']
    merged_food_nutrient_df = merged_df[['food_id', 'nutrient_name', 'nutrient_amount']]
    return merged_food_nutrient_df

def pivot_food_nutrient_with_nutrient_information(merged_food_nutrient_df):
    pivoted_food_nutrient_df = merged_food_nutrient_df.pivot_table(index='food_id', columns='nutrient_name', values='nutrient_amount', aggfunc='first')
    pivoted_food_nutrient_df.reset_index(inplace=True)
    return pivoted_food_nutrient_df

def merge_with_food_names(pivoted_food_nutrient_df, food_df_cleaned):
    merged_data_with_food_names = pd.merge(pivoted_food_nutrient_df, food_df_cleaned[['food_id', 'food_name']], on='food_id', how='inner')
    cols = ['food_id', 'food_name'] + [col for col in merged_data_with_food_names.columns if col not in ['food_id', 'food_name']]
    final_food_nutrient_data = merged_data_with_food_names[cols]
    return final_food_nutrient_data

def clean_up(files):
    for file in files:
        if os.path.exists(file):
            os.remove(file)

def main():
    food_nutrient_df_cleaned = clean_food_nutrient()
    food_df_cleaned = clean_food()
    nutrient_df_cleaned = clean_nutrient()
    
    merged_food_nutrient_df = merge_food_nutrient_with_nutrient_information(food_nutrient_df_cleaned, nutrient_df_cleaned)
    pivoted_food_nutrient_df = pivot_food_nutrient_with_nutrient_information(merged_food_nutrient_df)
    final_food_nutrient_data = merge_with_food_names(pivoted_food_nutrient_df, food_df_cleaned)
    
    final_food_nutrient_data_cleaned = final_food_nutrient_data.dropna(subset=['food_id', 'food_name'])

    final_food_nutrient_data_cleaned.to_csv('fda_food_nutrient_data.csv', index=False)
    
    clean_up(['food_nutrient.csv', 'food.csv', 'nutrient.csv'])

if __name__ == "__main__":
    main()