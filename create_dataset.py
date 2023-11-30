from google.cloud import storage
import pandas as pd
import os
from google.cloud import bigquery

# set GOOGLE_APPLICATION_CREDENTIALS environment variable in Python code to the path key.json file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "syndio-406616-85dd9ef5db2b.json"

def create_dataset(project_id, dataset_id):
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Construct a full Dataset object to send to the API
    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")

    # Set dataset location (change to your desired location)
    dataset.location = "US"

    # Send the dataset to the API for creation
    try:
        client.create_dataset(dataset)  # API request
        print(f"Dataset {dataset_id} created successfully in project {project_id}.")
    except Exception as e:
        print(f"Failed to create dataset {dataset_id}. Error: {str(e)}")

# your_project_id = 'syndio-406616'
# your_dataset_id = 'compensation_dataset'
# create_dataset(your_project_id, your_dataset_id)

def upload_to_bigquery(dataset_id, table_id, file_path):
    # Initialize BigQuery client
    client = bigquery.Client()

    # Create a reference to the table
    table_ref = client.dataset(dataset_id).table(table_id)

    # Define job configuration
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Our has headers, set this to 1
        autodetect=True,  # Detect schema automatically
    )

    # Load data from the local CSV file into the specified BigQuery table
    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for the job to complete

    print(f"Loaded {job.output_rows} rows into {dataset_id}.{table_id}")

# your_dataset_id = 'compensation_dataset'
# your_table_id = 'comp_data'
# local_csv_path = 'temp_csv_file.csv'

# # Call the function
# upload_to_bigquery(your_dataset_id, your_table_id, local_csv_path)

# os.remove('temp_csv_file.csv')