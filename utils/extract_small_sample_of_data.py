# scipt which reads the full processed dataset, and extracts a small 100,000 row sample and exports it to a new file

import pandas as pd

df = pd.read_csv("usda_branded_food_data.csv", low_memory=False)

sample_df = df.sample(n=100000, random_state=42)

sample_df.to_csv("usda_branded_food_data_small_sample.csv", index=False)