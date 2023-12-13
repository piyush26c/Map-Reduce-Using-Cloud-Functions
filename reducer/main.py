import functions_framework
import os 
import re
import json
import hashlib
import csv
import pandas as pd
from google.cloud import storage
from io import StringIO


@functions_framework.http
def reducer(request):
    request_json = request.get_json(silent=True)

    client = storage.Client.from_service_account_json('piyush-chaudhari-fall2023-9ae1ed20a7f3.json')
    reducer_bucket_name = "reducer_bucket"
    groupby_bucket_name = "groupby_bucket"
    reducer_bucket = client.get_bucket(reducer_bucket_name)
    groupby_bucket = client.get_bucket(groupby_bucket_name)
    reducer_name = request_json["reducer_name"] # parameter
    folder_name = f"groupby" # groupby folder name
    file_name = f"keys_to_reducer.json" 
    file_path = f"{folder_name}/{file_name}"
    # print('file_path:', file_path)
    blob = groupby_bucket.blob(file_path)
    # Download the content of the file as text
    content_text = blob.download_as_text()
    keys_to_reducer = json.loads(content_text)

    # reading groupby json file
    groupbyblob = groupby_bucket.blob(f"{folder_name}/groupby.json")
    # Download the content of the file as text
    groupbycontent_text = groupbyblob.download_as_text()
    groupby_dict = json.loads(groupbycontent_text)

    output_dict = {}
    for word in keys_to_reducer[reducer_name]:
        output_dict[word] = {}
        for filename in groupby_dict[word].keys():
            output_dict[word][filename] = sum(groupby_dict[word][filename])

    # save the reducer<>.json to storage
    # Create a blob (file) in the specified folder
    reducerblob = reducer_bucket.blob(f"{reducer_name}/{reducer_name}.json")
    # Convert the JSON data to a string
    json_string = json.dumps(output_dict, indent=4)
    # Upload the JSON data to the specified file in Google Cloud Storage
    reducerblob.upload_from_string(json_string, content_type="application/json")
    print(f"{reducer_name} OK")
    return f"{reducer_name} OK"

   
