import pandas as pd
from datasets import Dataset

df = pd.read_csv('usda_branded_food_data.csv', low_memory=False, dtype={'FOOD_RECORD_ID': str})

dataset = Dataset.from_pandas(df)

dataset.push_to_hub("usda_branded_food_data")