import functions_framework
import requests

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def fileaddtrigger(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    url = "https://us-central1-piyush-chaudhari-fall2023.cloudfunctions.net/master"
    name = name.split("/")[1]
    filenames = [name]
    parameters = {"filenames" : filenames, "number_of_mappers" : 8, "number_of_reducers" : 4}
    r = requests.post(url, json=parameters)

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")
