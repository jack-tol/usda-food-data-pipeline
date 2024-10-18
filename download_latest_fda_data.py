import requests
from bs4 import BeautifulSoup
import zipfile
import os
import shutil

# URL of the page containing the download link
url = "https://fdc.nal.usda.gov/download-datasets.html"
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# Find the download link based on table and row structure
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
    # Construct the full URL of the download link
    full_url = f"https://fdc.nal.usda.gov{download_link}"
    print(f"Latest download link found: {full_url}")
    
    # File name based on the URL
    file_name = full_url.split("/")[-1]
    
    # Download the file using streaming to handle large files efficiently
    with requests.get(full_url, stream=True) as response:
        response.raise_for_status()  # Ensure the request was successful
        with open(file_name, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    
    print(f"Downloaded file: {file_name}")
    
    # Unzip the file
    extracted_folder_name = file_name.replace('.zip', '')  # Assuming the extracted folder name is the same as the zip file without the extension
    with zipfile.ZipFile(file_name, 'r') as zip_ref:
        zip_ref.extractall(extracted_folder_name)
    print(f"Extracted contents of {file_name} to folder: {extracted_folder_name}")
    
    # Get the name of the nested folder within the extracted folder
    nested_folder = next(
        (name for name in os.listdir(extracted_folder_name) if os.path.isdir(os.path.join(extracted_folder_name, name))),
        None
    )
    
    if nested_folder:
        nested_folder_path = os.path.join(extracted_folder_name, nested_folder)
        
        # Define the files to extract and move
        files_to_extract = ["nutrient.csv", "food.csv", "branded_food.csv", "food_nutrient.csv"]
        files_found = []

        # Check if the specified files exist and move them to the parent directory if they do
        for file in files_to_extract:
            src = os.path.join(nested_folder_path, file)
            dest = os.path.join(".", file)
            if os.path.exists(src):
                shutil.move(src, dest)
                print(f"Moved {file} to the parent directory.")
                files_found.append(file)
            else:
                print(f"{file} not found in the nested folder.")

        # Print a message if not all files are found
        if len(files_found) != len(files_to_extract):
            print("Not all specified files were found. Keeping the extracted folder and zip file for debugging purposes.")
        else:
            # Only clean up if all files are successfully moved
            shutil.rmtree(extracted_folder_name)
            print(f"Removed the extracted folder: {extracted_folder_name}")
            
            os.remove(file_name)
            print(f"Removed the zip file: {file_name}")
    else:
        print("No nested folder found within the extracted folder. Keeping files for inspection.")
    
else:
    print("Download link not found.")
