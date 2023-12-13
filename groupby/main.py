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


def hash_word(word, num_files):
    # Hash the word and get an integer value
    hash_value = int(hashlib.sha256(word.encode('utf-8')).hexdigest(), 16)
    # Use modulo to map the hash value to a file index
    file_index = hash_value % num_files
    return file_index

def create_json_files(mapperid):
    url = f"https://us-central1-piyush-chaudhari-fall2023.cloudfunctions.net/create_json_file{mapperid}"
    parameters = {"mapperid" : mapperid}
    r = requests.post(url, json=parameters)
    r.content.decode()


@functions_framework.http
def groupby(request):
    request_json = request.get_json(silent=True)
    client = storage.Client.from_service_account_json('piyush-chaudhari-fall2023-9ae1ed20a7f3.json')
    # gcs_bucket_name = 'eccmrbucket'
    # bucket = client.get_bucket(gcs_bucket_name)

    mapper_bucket_name = "mapper_bucket"
    groupby_bucket_name = "groupby_bucket"
    mapper_bucket = client.get_bucket(mapper_bucket_name)
    groupby_bucket = client.get_bucket(groupby_bucket_name)

    number_of_mappers = request_json['number_of_mappers'] # indexing start from 0 {eg. mapper0, mapper1 ...}

    print("*************GROUPBY: First Step Started*************")
    # first create individual json files

    threads = []
    for mapperid in range(0, number_of_mappers):
        aggregated_dict = dict()
        folder_name = f"mapper{mapperid}" # mapper folder name
        file_name = f"mapper{mapperid}.csv" # individual mapper csv file
        file_path = f"{folder_name}/{file_name}"
        # print('file_path:', file_path)
        blob = mapper_bucket.blob(file_path)
        if not blob.exists():
            continue
        thread = threading.Thread(target=create_json_files, args=(mapperid,))
        threads.append(thread)
        
    # Start all threads
    for thread in threads:
        thread.start()

    # Join all threads
    for thread in threads:
        thread.join()
    
    print("*************GROUPBY: First Step Done*************")
    # second merge those created individual json files to a single json file
    groupby = {}
    for mapperid in range(0, number_of_mappers):
        folder_name = f"mapper{mapperid}" # mapper folder name
        file_name = f"mapper{mapperid}.json" # individual mapper json file
        file_path = f"{folder_name}/{file_name}"
        # print('file_path:', file_path)
        blob = mapper_bucket.blob(file_path)
        if not blob.exists():
            continue
        # Download the content of the file as text
        content_text = blob.download_as_text()
        json_object = json.loads(content_text)
        # print(json_object["electronic"])

        if mapperid == 0:
            groupby = json_object
            continue

        # otherwise merging process should start (from mapperid1)
        for ipkey in json_object.keys():
            if ipkey in groupby.keys():
                # word found now check for filenames
                for ipfilename in json_object[ipkey].keys():
                    if ipfilename in groupby[ipkey].keys():
                        groupby[ipkey][ipfilename] += json_object[ipkey][ipfilename]
                    else:
                        groupby[ipkey][ipfilename] = json_object[ipkey][ipfilename]
            else:
                groupby[ipkey] = json_object[ipkey]

    # Create a blob (file) in the specified folder
    groupbyblob = groupby_bucket.blob(f"groupby/groupby.json")
    # Convert the JSON data to a string
    json_string = json.dumps(groupby, indent=4)
    # Upload the JSON data to the specified file in Google Cloud Storage
    groupbyblob.upload_from_string(json_string, content_type="application/json")

    print("*************GROUPBY: Second Step Done*************")
    # third allocate keys to reducersid by creating dictionary <key: reducerid, value: list of keys []>
    keys_to_reducer = {}
    number_of_reducers = request_json['number_of_reducers']

    # creating empty structure
    for reducerid in range(0, number_of_reducers):
        keys_to_reducer[f"reducer{reducerid}"] = []

    for key in groupby.keys():
        generatedid = hash_word(key, number_of_reducers)
        reducerid = f"reducer{generatedid}"
        keys_to_reducer[reducerid].append(key)

    # Create a blob (file) in the specified folder
    keys_to_reducerblob = groupby_bucket.blob(f"groupby/keys_to_reducer.json")
    # Convert the JSON data to a string
    json_string = json.dumps(keys_to_reducer, indent=4)
    # Upload the JSON data to the specified file in Google Cloud Storage
    keys_to_reducerblob.upload_from_string(json_string, content_type="application/json")
    print(f"groupby OK")
    
    return f"groupby OK"




   
