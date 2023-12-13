import functions_framework
import os 
import re
import json
import hashlib
import csv
import json
import threading
import requests
from google.cloud import storage

def read_file_from_gcs(bucket, folder_name, file_name):
    blob = bucket.blob(f"{folder_name}/{file_name}")
    content = blob.download_as_text()
    return content

def write_file_to_gcs(bucket, folder_name, file_name, content):
    blob = bucket.blob(f"{folder_name}/{file_name}")
    blob.upload_from_string(content)

def divide_and_save_to_gcs(bucket, content, number_of_mappers, base_file_name):
    lines = content.splitlines()
    total_lines = len(lines)
    lines_per_part = total_lines // number_of_mappers

    for i in range(number_of_mappers):
        start_index = i * lines_per_part
        end_index = (i + 1) * lines_per_part if i < (number_of_mappers-1) else total_lines
        part_content = '\n'.join(lines[start_index:end_index])
    
        write_file_to_gcs(bucket, f"temp{i}", base_file_name, part_content)

def distribute_files_to_buckets(file_tuples, k, bucket_name, folder_name):
    # Sort files in descending order of size
    sorted_files = sorted(file_tuples, key=lambda x: x[1], reverse=True)

    # Initialize buckets
    buckets = {i: [] for i in range(k)}
    bucket_sizes = [0] * k

    # Greedy allocation of files to buckets
    for file_name, size in sorted_files:
        min_bucket_index = min(range(k), key=lambda i: bucket_sizes[i])
        buckets[min_bucket_index].append((bucket_name, folder_name, file_name))
        bucket_sizes[min_bucket_index] += size

    return buckets

def executeGroupBy(number_of_mappers, number_of_reducers):
    url = "https://us-central1-piyush-chaudhari-fall2023.cloudfunctions.net/groupby"
    parameters = {"number_of_mappers":number_of_mappers, "number_of_reducers":number_of_reducers}
    r = requests.post(url, json=parameters)
    return r.content.decode() == "groupby OK"

# Function to be executed by each mapper thread
def thread_function_mapper(thread_id, file_list):
    url = f"https://us-central1-piyush-chaudhari-fall2023.cloudfunctions.net/mapper{thread_id}"
    parameters = {"mapper_name" : f"mapper{thread_id}", "file_list" : file_list}
    r = requests.post(url, json=parameters)
    result = r.content.decode()

    # Store the result in the thread object
    if result == f"mapper{thread_id} OK":
        threading.current_thread().return_value = 1
    else:
        threading.current_thread().return_value = 0
    
# Function to be executed by each reducer thread
def thread_function_reducer(thread_id):
    url = f"https://us-central1-piyush-chaudhari-fall2023.cloudfunctions.net/reducer{thread_id}"
    parameters = {"reducer_name" : f"reducer{thread_id}"}
    r = requests.post(url, json=parameters)
    result = r.content.decode()

    # Store the result in the thread object
    if result == f"reducer{thread_id} OK":
        threading.current_thread().return_value = 1
    else:
        threading.current_thread().return_value = 0


def merge_reducer_outputs(eccmr_final_result_bucket, number_of_reducers, reducer_bucket):
    final_dict = {}
    for reducerid in range(number_of_reducers):
        file_path = f"reducer{reducerid}/reducer{reducerid}.json"
        reducerblob = reducer_bucket.blob(file_path)
        content_text = reducerblob.download_as_text()
        json_object = json.loads(content_text)

        if reducerid == 0:
            final_dict = json_object
            continue

        # otherwise merging would start from reducerid 1
        for ipkey in json_object.keys():
            if ipkey in final_dict.keys():
                # word found now check for filenames
                for ipfilename in json_object[ipkey].keys():
                    if ipfilename in final_dict[ipkey].keys():
                        final_dict[ipkey][ipfilename] += json_object[ipkey][ipfilename]
                    else:
                        final_dict[ipkey][ipfilename] = json_object[ipkey][ipfilename]
            else:
                final_dict[ipkey] = json_object[ipkey]
    # print("this is final_dict(0):", final_dict)
    # what if reducer bucket alread has file? we have to merge it too
    final_file_blob = eccmr_final_result_bucket.blob(f"final_results.json")
    if (final_file_blob.exists()):
        # merge
        content_text = final_file_blob.download_as_text()
        json_object = json.loads(content_text)
        for ipkey in json_object.keys():
            if ipkey in final_dict.keys():
                # word found now check for filenames
                for ipfilename in json_object[ipkey].keys():
                    if ipfilename in final_dict[ipkey].keys():
                        final_dict[ipkey][ipfilename] += json_object[ipkey][ipfilename]
                    else:
                        final_dict[ipkey][ipfilename] = json_object[ipkey][ipfilename]
            else:
                final_dict[ipkey] = json_object[ipkey]
    # print("this is final_dict(1):", final_dict)
    # Convert the JSON data to a string
    json_string = json.dumps(final_dict, indent=4)
    # Upload the JSON data to the specified file in Google Cloud Storage
    final_file_blob.upload_from_string(json_string, content_type="application/json")

def delete_intermediate_files(eccrm_dataset_temp_bucket, mapper_bucket, reducer_bucket, groupby_bucket):
    bucket_list = [eccrm_dataset_temp_bucket, mapper_bucket, reducer_bucket, groupby_bucket]
    for bucket in bucket_list:
        blobs = bucket.list_blobs()
        for blob in blobs:
            blob.delete()

@functions_framework.http
def master(request):
    request_json = request.get_json(silent=True)

    #input parameters
    filenames = request_json["filenames"]
    number_of_mappers = request_json["number_of_mappers"]
    number_of_reducers = request_json["number_of_reducers"]

    client = storage.Client.from_service_account_json('piyush-chaudhari-fall2023-9ae1ed20a7f3.json')
    eccrm_dataset_bucket_name = 'eccrm_dataset_bucket'
    eccrm_dataset_bucket = client.get_bucket(eccrm_dataset_bucket_name)
    eccrm_dataset_temp_bucket_name = 'eccrm_dataset_temp_bucket'
    eccrm_dataset_temp_bucket = client.get_bucket(eccrm_dataset_temp_bucket_name)
    eccmr_final_result_bucket_name = 'eccmr_final_result_bucket'
    eccmr_final_result_bucket = client.get_bucket(eccmr_final_result_bucket_name)
    mapper_bucket_name = 'mapper_bucket'
    mapper_bucket = client.get_bucket(mapper_bucket_name)
    reducer_bucket_name = 'reducer_bucket'
    reducer_bucket = client.get_bucket(reducer_bucket_name)
    groupby_bucket_name = 'groupby_bucket'
    groupby_bucket = client.get_bucket(groupby_bucket_name)

    allocation = {}

    if len(filenames) == 1:
        # cloud storage trigger event 
        # divide the file into #number_of_mappers chunks and dump it on google cloud storage
        folder_name = "dataset"
        file_content = read_file_from_gcs(eccrm_dataset_bucket, folder_name, filenames[0])
        divide_and_save_to_gcs(eccrm_dataset_temp_bucket, file_content, number_of_mappers, filenames[0])
        basefilename = filenames[0]
        filenames = []
        for indx in range(number_of_mappers):
            allocation[indx] = [(eccrm_dataset_temp_bucket_name, f"temp{indx}", basefilename)]  # [(bucketname, foldername=[temp<id>], filename)]
            # filenames.append((f"temp{indx}", basefilename)) 
        print(allocation)
    else:
        # distribute files of dataset to mapper in a way such that every mapper gets equall/similar dataload
        # implemented greedy algorithm of partition.
        file_tuples = []
        foldername = "dataset"
        for filename in filenames:
            blob = eccrm_dataset_bucket.blob(f"{foldername}/{filename}")
            blob.reload()
            file_tuples.append((filename, round(blob.size/1000)))
        
        allocation = distribute_files_to_buckets(file_tuples, number_of_mappers, eccrm_dataset_bucket_name, foldername)           
        print(allocation)

    # first spawn mappers
    # Create threads
    threads = []
    threadid = 0
    for key in allocation.keys():
        if len(allocation[key]) == 0:  # meaning no files present for that id/bucket/ skip it then
            continue
        thread = threading.Thread(target=thread_function_mapper, args=(threadid, allocation[key]))
        threads.append(thread)
        threadid += 1

    # Start all threads
    for thread in threads:
        thread.start()

    # Join all threads
    for thread in threads:
        thread.join()

    # Main thread checks results
    results_mappers = [thread.return_value for thread in threads]

    if not (sum(results_mappers) == len(threads)):
        print("ERROR OCCURED IN MAPPERS EXECUTION.")
        return "master NOT OK - ERROR OCCURED IN MAPPERS EXECUTION."

    print("***********MAPPER EXECUTED SUCCESSFULLY.***********")

    print("***********GROUPBY EXECUTION STARTED.***********")
    # groupby
    if not (executeGroupBy(number_of_mappers, number_of_reducers)):
        print("ERROR OCCURED IN GROUPBY FaaS EXECUTION")
        return "master NOT OK - ERROR OCCURED IN GROUPBY FaaS EXECUTION."

    print("***********GROUPBY EXECUTED SUCCESSFULLY.***********")

    print("***********REDUCERS EXECUTION STARTED.***********")
    # spawn reducers
    # Create threads
    threads = []
    for threadid in range(number_of_reducers):
        thread = threading.Thread(target=thread_function_reducer, args=(threadid,))
        threads.append(thread)

    # Start all threads
    for thread in threads:
        thread.start()

    # Join all threads
    for thread in threads:
        thread.join()

    # Main thread checks results
    results_reducers = [thread.return_value for thread in threads]

    if not (sum(results_reducers) == number_of_reducers):
        print("ERROR OCCURED IN REDUCERS EXECUTION.")
        return "master NOT OK - ERROR OCCURED IN REDUCERS EXECUTION."

    print("***********REDUCERS EXECUTED SUCCESSFULLY.***********")

    # merge reducer output to single file
    merge_reducer_outputs(eccmr_final_result_bucket, number_of_reducers, reducer_bucket)
    print("***********REDUCERS MERGED SUCCESSFULLY.***********")

    print("***********DELETION OF INTERMEDIATE FILES IN PROGRESS***********")
    delete_intermediate_files(eccrm_dataset_temp_bucket, mapper_bucket, reducer_bucket, groupby_bucket)
    print("***********DELETION OF INTERMEDIATE FILES DONE SUCCESSFULLY***********")

    print("***********MASTER OK.***********")
    return "master OK"
