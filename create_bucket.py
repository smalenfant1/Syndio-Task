from google.cloud import storage
import os

# set GOOGLE_APPLICATION_CREDENTIALS environment variable in Python code to the path key.json file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "syndio-406616-85dd9ef5db2b.json"

def create_bucket(bucket_name):
    # Initialize the storage client
    storage_client = storage.Client()

    # Create the new bucket
    try:
        bucket = storage_client.create_bucket(bucket_name)
        print(f"Bucket {bucket.name} created.")
    except Exception as e:
        print(f"Failed to create bucket {bucket_name}. Error: {str(e)}")

# Create a bucket name
# your_bucket_name = 'comp_data_bucket'

# # Call the function
# create_bucket(your_bucket_name)


def upload_to_bucket(bucket_name, source_file_path, destination_blob_name):
    # Initialize the storage client
    storage_client = storage.Client()

    # Get the bucket where the file will be uploaded
    bucket = storage_client.bucket(bucket_name)

    # Create a blob object representing the file to upload
    blob = bucket.blob(destination_blob_name)

    # Upload the file to the bucket
    try:
        blob.upload_from_filename(source_file_path)
        print(f"File {source_file_path} uploaded to {bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f"Failed to upload file to bucket. Error: {str(e)}")

# your_bucket_name = 'comp_data_bucket'
# local_file_path = 'ML_Engineer_-_demo_data_for_assessment.csv'
# destination_blob_name = 'ML_Engineer_-_demo_data_for_assessment.csv'

# # Call the function
# upload_to_bucket(your_bucket_name, local_file_path, destination_blob_name)
