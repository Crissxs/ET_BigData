import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.http import HttpSource
from google.cloud import storage
import zipfile
import tempfile
import os

class DownloadAndExtractFiles(beam.DoFn):
    def process(self, element):
        url = element['url']
        response = requests.get(url)
        filename = url.split('/')[-1]
        folder_name = filename.split('.')[0]

        # Save to a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(response.content)
            temp_filename = temp_file.name

        # Extract the zip file
        with zipfile.ZipFile(temp_filename, 'r') as zip_ref:
            zip_ref.extractall('/tmp/' + folder_name)

        # Upload extracted files to GCS
        storage_client = storage.Client()
        bucket_name = 'duoc-red-bucket'
        bucket = storage_client.bucket(bucket_name)
        
        for root, _, files in os.walk('/tmp/' + folder_name):
            for file in files:
                local_file_path = os.path.join(root, file)
                gcs_path = os.path.join('Datos_Historicos', folder_name, file)
                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(local_file_path)
                yield f'Uploaded {gcs_path}'

        # Clean up temporary files
        os.remove(temp_filename)
        for root, _, files in os.walk('/tmp/' + folder_name):
            for file in files:
                os.remove(os.path.join(root, file))

def run(argv=None):
    pipeline_options = PipelineOptions(argv)
    setup_options = pipeline_options.view_as(SetupOptions)
    setup_options.extra_packages = ['requests']
    p = beam.Pipeline(options=pipeline_options)

    input_json = 'gs://duoc-red-bucket/datos_transporte_et.json'
    
    # Read the JSON file from GCS
    data = p | 'Read JSON' >> beam.io.ReadFromText(input_json)
    resources = data | 'Extract Resources' >> beam.Map(lambda line: json.loads(line)['result']['resources'])

    # Use HttpSource to read data from URLs
    urls = resources | 'Get URLs' >> beam.Map(lambda resource: resource['url'])
    downloaded_files = urls | 'Download Files' >> beam.io.Read(HttpSource())

    # Process downloaded files and upload to GCS
    downloaded_files | 'Process Files' >> beam.ParDo(DownloadAndExtractFiles())

    p.run().wait_until_finish()

if __name__ == '__main__':
    run()
