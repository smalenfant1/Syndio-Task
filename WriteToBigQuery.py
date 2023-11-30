import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
from google.cloud import bigquery

import os

# Set GOOGLE_APPLICATION_CREDENTIALS environment variable to the path of your service account key file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "syndio-406616-85dd9ef5db2b.json"

gcs_temp_location = 'gs://comp_data_bucket/temp'

parser = argparse.ArgumentParser()

parser.add_argument('--input',
                      dest='input',
                      required=True,
                      help='Input file to process.')
parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output table to write results to.')

path_args, pipeline_args = parser.parse_known_args()

inputs_pattern = path_args.input
outputs_prefix = path_args.output

options = PipelineOptions(pipeline_args)
p = beam.Pipeline(options=options)


cleaned_data = (
	p
	| beam.io.ReadFromText(inputs_pattern, skip_header_lines=1)
	| beam.Map(lambda row: row.lower())
	| beam.Map(lambda row: row+',1')		
)

#BigQuery

client = bigquery.Client()

dataset_id = "syndio-406616.compensation_dataset"

dataset = bigquery.Dataset(dataset_id)

dataset.location = "US"
dataset.description = "dataset for compensation"

dataset_ref = client.create_dataset(dataset, timeout = 30)

def to_json(csv_str):
    fields = csv_str.split(',')
    json_str = {
        "employee_id": fields[0],
        "ssg": fields[1],
        "compensation": fields[2],
        "gender": fields[3],
        "city": fields[4],
        "years_of_experience": fields[5]
    }
    return json_str

table_schema = 'employee_id:INTEGER,ssg:STRING,compensation:FLOAT,gender:STRING,city:STRING,years_of_experience:FLOAT'

(cleaned_data
| 'cleaned_data to json' >> beam.Map(to_json)
| 'write to bigquery' >> beam.io.WriteToBigQuery(
table='syndio-406616:compensation_dataset.comp_data',  # Replace with your BigQuery table reference
schema=table_schema,
create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
# additional_bq_parameters={'timePartitioning' : {'type':'DAY'}}
custom_gcs_temp_location=gcs_temp_location

)

)
from apache_beam.runners.runner import PipelineState
ret = p.run()
if ret.state == PipelineState.DONE:
    print('Success!!!')
else:
    print('Error Running beam pipeline')
    
os.remove('temp_csv_file.csv')