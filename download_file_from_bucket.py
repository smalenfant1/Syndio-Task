from google.cloud import storage
import pandas as pd
import os
from google.cloud import bigquery

# set GOOGLE_APPLICATION_CREDENTIALS environment variable in Python code to the path key.json file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "syndio-406616-85dd9ef5db2b.json"
def download_from_gcs(bucket_name, source_blob_name, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)
    df=pd.read_csv(destination_file_name)
    # Fill missing values in the 'compensation' column with median
    median_compensation = df['compensation'].median()
    df['compensation'].fillna(median_compensation, inplace=True)

    # Write the DataFrame to a temporary CSV file
    temp_csv_path = 'temp_csv_file.csv'
    df.to_csv(temp_csv_path, index=False)

    print(f"{source_blob_name} downloaded from {bucket_name} and processed. The results are stored in {destination_file_name}")

# bucket_name = 'comp_data_bucket'
# gcs_blob_name = 'ML_Engineer_-_demo_data_for_assessment.csv'
# local_file_path = 'temp_csv_file.csv'

# # Download file from Cloud Storage
# download_from_gcs(bucket_name, gcs_blob_name, local_file_path)



