import streamlit as st
import re
import json
import json
import pandas as pd
from google.cloud import storage
import pandas as pd
import time
from datetime import datetime, timezone


# Import CSS
st.markdown('<link rel="stylesheet" href="style.css">', unsafe_allow_html=True)

def is_file_updated_recently(threshold_seconds):
    client = storage.Client.from_service_account_json('piyush-chaudhari-fall2023-9ae1ed20a7f3.json')
    eccmr_final_result_bucket_name = 'eccmr_final_result_bucket'
    eccmr_final_result_bucket = client.get_bucket(eccmr_final_result_bucket_name)
    file_name = "final_results.json"
    file_path = f"{file_name}"
    blob = eccmr_final_result_bucket.blob(file_path) 

    # Check if the file exists
    if not blob.exists():
        print(f"File '{file_path}' does not exist.")
        return False
       
    # Retrieve the metadata
    blob.reload()
     # Check if 'updated' is not None
    if blob.updated is None:
        print(f"File '{file_path}' has no 'updated' timestamp.")
        return False
    
    # Make both datetimes timezone-aware
    updated_time = blob.updated.replace(tzinfo=timezone.utc)
    current_time = datetime.utcnow().replace(tzinfo=timezone.utc)
    time_difference = current_time - updated_time

    return time_difference.total_seconds() <= threshold_seconds


def is_valid_input(text):
    return bool(re.match(r'^[^\s\n]+$', text))

def find_results(input_text):
    if not is_valid_input(input_text):
        st.warning("Invalid input! Please avoid spaces and newline characters.")
        return None

    client = storage.Client.from_service_account_json('piyush-chaudhari-fall2023-9ae1ed20a7f3.json')
    eccmr_final_result_bucket_name = 'eccmr_final_result_bucket'
    eccmr_final_result_bucket = client.get_bucket(eccmr_final_result_bucket_name)
    file_name = "final_results.json"
    file_path = f"{file_name}"
    # print('file_path:', file_path)
    blob = eccmr_final_result_bucket.blob(file_path)
    if not blob.exists():
        print("result file does not exists.")
        return None

    # Download the content of the file as text
    content_text = blob.download_as_text()
    json_object = json.loads(content_text)

    # preprocess the input word
    input_text = input_text.strip().lower()
    input_text = re.sub(r'[^a-zA-Z0-9\s]', '', input_text)
    print("processed input text:", input_text)
    df = None

    if input_text in json_object.keys():
        df = pd.DataFrame(list(json_object[input_text].items()), columns=['Document', 'Count'])
        # Sorting the DataFrame by the second column in descending order
        df = df.sort_values(by='Count', ascending=False)
        # Reindexing the sorted DataFrame
        df = df.reset_index(drop=True)

    return df

def save_uploaded_file(uploaded_file):
    if uploaded_file is not None:
        file_path = uploaded_file.name
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getvalue())
        
        # uploading file in eccrm_dataset_bucket
        client = storage.Client.from_service_account_json('piyush-chaudhari-fall2023-9ae1ed20a7f3.json')
        eccrm_dataset_bucket_name = 'eccrm_dataset_bucket'
        eccrm_dataset_bucket = client.get_bucket(eccrm_dataset_bucket_name)
        
        # Define the file name and destination folder in the bucket
        file_name = uploaded_file.name.split("/")[-1]
        folder_name = "dataset"
        destination_blob_name = f"{folder_name}/{file_name}"
        
        # Upload the file to GCS
        blob = eccrm_dataset_bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)

        return f"File uploaded successfully."
    return None

def main():
    st.title("Map Reduce Assignment (Engineering Cloud Computing)")
    tabs = ["Find Occurances", "Upload File"]
    choice = st.sidebar.selectbox("Select Tab:", tabs)
    
    if choice == tabs[0]:
        st.header(tabs[0])

        # Tab 1 Content
        input_text = st.text_area("Enter Text:", placeholder="Enter the word ...", help="Enter word that you want to search in corpus and its occurances in each document")

        if st.button("Find"):
            if is_valid_input(input_text):
                with st.spinner("Finding results..."):
                    result_df = find_results(input_text)
                if result_df is not None:
                    st.dataframe(result_df, use_container_width=True)
                else:
                    st.info("Word not present in corpus.")
            else:
                st.warning("Invalid input! Please avoid spaces and newline characters.")

    elif choice == tabs[1]:
        st.header(tabs[1])

        # Tab 2 Content
        uploaded_file_key = "uploaded_file_key"
        uploaded_file = st.file_uploader("Choose a .txt file:", type=["txt"])
        if uploaded_file is not None:
            if st.button("Upload"):
                with st.spinner("Uploading file..."):
                    upload_result = save_uploaded_file(uploaded_file)
                st.success(upload_result)

                with st.spinner("Updating the inverted index..."):
                    loop_interval = 1
                    threshold_seconds = 20
                    while True:
                        if is_file_updated_recently(threshold_seconds):
                            break
                        time.sleep(loop_interval)
                st.success("Indexing complete. Thank you for waiting.")
                # Display countdown message and refresh the page
                refresh_in = 6
                countdown_placeholder = st.empty()
                for i in range(refresh_in, 0, -1):
                    countdown_placeholder.text(f"Refreshing the page in {i} seconds.")
                    time.sleep(1)
                st.experimental_rerun()

    # Adding the "Made with love (emoji) Piyush Chaudhari" message at the bottom
    st.markdown(
        '<div style="position: fixed; bottom: 10px; left: 50%; transform: translateX(-50%);">'
        'Made with ❤️ Piyush Rajendra Chaudhari</div>',
        unsafe_allow_html=True
    )
if __name__ == "__main__":
    main()
