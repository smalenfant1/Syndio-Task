from google.cloud import storage
import os
import pandas as pd
from google.cloud import bigquery
import create_bucket as cb
import download_file_from_bucket as db
import create_dataset as cd

# set GOOGLE_APPLICATION_CREDENTIALS environment variable in Python code to the path key.json file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "syndio-406616-85dd9ef5db2b.json"

# Create a bucket name, specify a file name, blob name, temporary file name

your_bucket_name = 'comp_data_bucket'
local_file_path = 'ML_Engineer_-_demo_data_for_assessment.csv'
destination_blob_name = 'ML_Engineer_-_demo_data_for_assessment.csv'
local_temp_file = 'temp_csv_file.csv'
your_table_id = 'comp_data'
your_project_id = 'syndio-406616'
your_dataset_id = 'compensation_dataset'


# Call the function to create a bucket
cb.create_bucket(your_bucket_name)

# Call the function to upload a file to created bucket
cb.upload_to_bucket(your_bucket_name, local_file_path, destination_blob_name)

# Download file from Cloud Storage
db.download_from_gcs(your_bucket_name, destination_blob_name , local_file_path)

# Create dataset in BQ

cd.create_dataset(your_project_id, your_dataset_id)

# Call the function to upload data to BQ
cd.upload_to_bigquery(your_dataset_id, your_table_id, local_temp_file)

os.remove(local_temp_file)