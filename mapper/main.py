import functions_framework
import os 
import re
import json
import hashlib
import csv
from google.cloud import storage

# mapper code
def create_folder_and_upload_file(bucket, folder_name, file_name, local_file_path):
    # Check if the folder exists
    folder_path = f"{folder_name}/"
    folder_blob = bucket.blob(folder_path)
    
    if not folder_blob.exists():
        # If the folder doesn't exist, create it
        bucket.blob(folder_path).upload_from_string('')  # Creating an empty file as a marker for the folder

    # Upload the file to the specified folder
    file_path = f"{folder_name}/{file_name}"
    blob = bucket.blob(file_path)
    blob.upload_from_filename(local_file_path)

    print(f"File '{file_name}' uploaded to folder '{folder_name}' in bucket.")



@functions_framework.http
def mapper(request):
    request_json = request.get_json(silent=True)

    # mapper code
    client = storage.Client.from_service_account_json('piyush-chaudhari-fall2023-9ae1ed20a7f3.json')
    # gcs_bucket_name = 'eccrm_dataset_bucket'
    # bucket = client.get_bucket(gcs_bucket_name)
    # folder_name = 'dataset'
    
    tuples_list = []
    mapper_name = request_json['mapper_name']
    file_list = request_json['file_list']
    
    # CSV file path
    csv_file_path = f'{mapper_name}.csv'

    for filename in file_list:
        bucket_name = filename[0]
        folder_name = filename[1]
        filename = filename[2]
        file_path = f"{folder_name}/{filename}"
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_path)
        content = blob.download_as_text()

        # Remove extra spaces and new lines, convert to lowercase
        # processed_content = re.sub(r'\s+', ' ', content).lower()
        processed_content = re.sub(r'\s+', ' ', content).lower()
        processed_content = re.sub(r'[^a-zA-Z0-9\s]', '', processed_content)
        
        # Extract words
        words = processed_content.split()

        # Create list of tuples
        temp_tuples_list = [(word, os.path.basename(file_path), 1) for word in words]
        tuples_list = tuples_list + temp_tuples_list
        
    # Writing the list of tuples to a CSV file
    with open(csv_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['Word', 'Document', 'Count'])  # Writing header row
        csv_writer.writerows(tuples_list)  # Writing data rows

    mapper_bucket_name = "mapper_bucket"
    mapper_bucket = client.get_bucket(mapper_bucket_name)
    create_folder_and_upload_file(mapper_bucket, mapper_name, csv_file_path, csv_file_path)
    print(f"{mapper_name} OK")
    return f"{mapper_name} OK"
