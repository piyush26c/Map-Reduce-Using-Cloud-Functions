import functions_framework
import os 
import re
import json
import hashlib
import csv
import json
import threading
import requests
import pandas as pd
import threading
from io import StringIO
from google.cloud import storage



@functions_framework.http
def create_json_file(request):
    request_json = request.get_json(silent=True)
    mapperid = request_json["mapperid"]

    client = storage.Client.from_service_account_json('piyush-chaudhari-fall2023-9ae1ed20a7f3.json')
    mapper_bucket_name = "mapper_bucket"
    mapper_bucket = client.get_bucket(mapper_bucket_name)
    
    aggregated_dict = dict()
    folder_name = f"mapper{mapperid}" # mapper folder name
    file_name = f"mapper{mapperid}.csv" # individual mapper csv file
    file_path = f"{folder_name}/{file_name}"
    # print('file_path:', file_path)
    blob = mapper_bucket.blob(file_path)


    # Download the content of the file as text
    content_text = blob.download_as_text()

    # Use pandas to read the CSV from the string
    csv_data = StringIO(content_text)
    df = pd.read_csv(csv_data)

    # Iterate through the DataFrame and create a list of tuples
    tuples_list = [tuple(row) for _, row in df.iterrows()]
    
    for word, filename, count in tuples_list:
        key = word
        if key in aggregated_dict:
            # check if file name exists
            if filename in aggregated_dict[key]:
                aggregated_dict[key][filename].append(count)
            else:
                aggregated_dict[key][filename] = [count]
        else:
            aggregated_dict[key] = {filename : [count]}

    
    json_file_path = f'mapper{mapperid}.json'

    # Create a blob (file) in the specified folder
    blob = mapper_bucket.blob(f"{folder_name}/{json_file_path}")
    # Convert the JSON data to a string
    json_string = json.dumps(aggregated_dict, indent=4)
    # Upload the JSON data to the specified file in Google Cloud Storage
    blob.upload_from_string(json_string, content_type="application/json")


    print(f"create_json_file OK - {mapperid}")
    return f"create_json_file OK - {mapperid}"




   
