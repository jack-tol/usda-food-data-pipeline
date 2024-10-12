import pandas as pd

df = pd.read_csv('fda_food_nutrient_data.csv', low_memory=False)

df['non_nan_count'] = df.iloc[:, 2:].notna().sum(axis=1)

num_of_samples = 100000

df_top_n = df.sort_values(by='non_nan_count', ascending=False).head(num_of_samples)

df_top_n = df_top_n.drop(columns=['non_nan_count'])

df_top_n.to_csv('fda_food_nutrient_data_sample.csv', index=False)

print(f"Top {num_of_samples} rows with the most nutrient data exported to 'fda_food_nutrient_data_sample.csv'")