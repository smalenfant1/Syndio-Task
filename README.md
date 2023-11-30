This pipeline solves for populating missing data in the csv file and then uploads the file into BigQuery.

### Commands:

1. Make Sure API for BigQuery, CLoud Storage and Cloud Dataflow Services are enabled.
2. Activate Cloud Shell
3. Install gcloud using 'pip install gcloud' in your terminal
4. Activate account with this command: 'gcloud auth list'
5. List the project ID with this command: 'gcloud config list project'
6. Now set a variable equal to your project id: PROJECTID='syndio-406616'


### Create Cloud Storage Bucket (3 ways of creating a Bucket)

1. Create a Standard Storage Class bucket by running this in CLI:
    'gsutil mb -c standard -l central1 gs://comp_data_bucket'
2. Or you can create a bucket and copy the file to Your Bucket at the same time by running this command:
    'gsutil cp /Users/svitlanamalenfant/Desktop/GCP_Pipeline/ML_Engineer_-_demo_data_for_assessment.csv gs://comp_data_bucket/'

3. Another option to create Cloud Storage Bucket and Upload a file is to run 'create_bucket.py' script

### Next download CSV file from the created bucket to clean and populate missing compensation values using median.
Run 'download_file_from_bucket.py'. It will connect to the bucket and download the csv file, populate missing values and store the clean temporary file in your folder.

### Create the BigQuery Dataset

1. Create a Dataset name called 'compensation_dataset' in Big Query: 'bq mk compensation_dataset'

Another option is to use a script and run 'create_dataset.py'

### Final step is to populate the dataset with values from our clean CSV file
Run 'WriteToBigQuery.py" script where you need to specify your input
'python WriteToBigQuery.py --input temp_csv_file.csv'
This will upload the data in JSON format.

If you run 'upload_to_dataset.py' then it will be uploaded as CSV format.

### To Run a whole process, you need to execute 'main.py' in order to upload the data in CSV format. 

### To Run a whole process, you need to execute 'python main_json.py --input temp_csv_file.csv' in order to upload the data in JSON format. I wanted to give an option to a user in this case, to point at the file that they want to process.
