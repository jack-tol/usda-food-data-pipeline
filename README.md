# USDA Food Data Pipeline & USDA Food Assistant

This repository contains code for processing and refining the USDA FoodData Central (FDC) dataset, a publicly accessible and comprehensive resource providing information on foods available on U.S. consumer shelves. Additionally, it includes the code for the USDA Food Assistant, an interactive tool designed to allow users to explore food data in a conversational format.

## Overview

The **USDA Food Data Pipeline** consolidates and cleans data from the USDA FoodData Central, spanning 34 CSV files, to create a single, structured dataset. This pipeline automates downloading, cleaning, merging, and normalizing the data, making it ready for machine learning and analysis.

The **USDA Food Assistant** application enables users to interact with this dataset in a conversational format, retrieving detailed food information and answering nutrition-related questions. The assistant combines semantic search with language generation, providing users with contextually relevant answers about ingredients, nutrients, and serving sizes.

## Features

- **Data Pipeline**: Automates the process of data retrieval, cleaning, and transformation for the USDA FoodData Central dataset.
- **Interactive Assistant**: Allows users to query the dataset and receive detailed responses on food items.
- **Semantic Search**: Enables similarity-based retrieval of food data using a Pinecone index and a `multilingual-e5-large` embedding model.
- **Machine-Learning Ready Dataset**: Outputs a structured dataset available for various applications.

For more detailed information on this pipeline and assistant, please refer to the blog post linked [here](https://jacktol.net/posts/building_a_data_pipeline_for_usda_fooddata_central/).

## Dataset Access

The cleaned USDA Branded Food Dataset, created by this pipeline, is available on Hugging Face Datasets [here](https://huggingface.co/datasets/jacktol/usda_branded_food_data). 

## Demo

The USDA Food Assistant, hosted on Hugging Face Spaces, can be accessed [here](https://huggingface.co/spaces/jacktol/usda-food-assistant).

### License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
