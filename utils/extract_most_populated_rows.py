import pandas as pd

df = pd.read_csv('usda_branded_food_data.csv', low_memory=False, dtype={'FOOD_RECORD_ID': str})
df['nan_count'] = df.isna().sum(axis=1)
top_10_least_nan = df.nsmallest(10, 'nan_count').drop(columns=['nan_count'])
top_10_least_nan.to_csv('top_10_least_nan_records.csv', index=False)