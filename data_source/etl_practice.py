import os
import glob
import pandas as pd
import json
import xml.etree.ElementTree as ET
from datetime import datetime

# Define paths
log_file = "log_file.txt"
target_file = "transformed_data.csv"
data_folder = "./data"

# Define functions
def extract_csv(file_path):
    return pd.read_csv(file_path)

def extract_json(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return pd.DataFrame(data)

def extract_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    data = []
    for record in root.findall("record"):
        row = {child.tag: child.text for child in record}
        data.append(row)
    return pd.DataFrame(data)

def extract():
    csv_data = extract_csv(os.path.join(data_folder, "source1.csv"))
    json_data = extract_json(os.path.join(data_folder, "source2.json"))
    xml_data = extract_xml(os.path.join(data_folder, "source3.xml"))
    return pd.concat([csv_data, json_data, xml_data], ignore_index=True)

def transform(data):
    if 'price' in data.columns:
        data['price'] = pd.to_numeric(data['price'], errors='coerce').round(2)
    else:
        log("Column 'price' not found in the data.")
    return data

def load(data, target_file):
    data.to_csv(target_file, index=False)

def log(message):
    with open(log_file, 'a') as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"{timestamp} - {message}\n")

# ETL Process
if __name__ == "__main__":
    log("ETL Process Started")

    try:
        log("Extracting data...")
        extracted_data = extract()
        log("Extraction completed.")

        log("Transforming data...")
        transformed_data = transform(extracted_data)
        log("Transformation completed.")

        log("Loading data...")
        load(transformed_data, target_file)
        log("Loading completed.")
    except Exception as e:
        log(f"ETL process failed: {e}")

    log("ETL Process Finished")
