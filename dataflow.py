import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import requests
from apache_beam.io.gcp.gcsio import GcsIO
import logging

class ExtractUrls(beam.DoFn):
    def process(self, element):
        response = requests.get("https://us-central1-duoc-bigdata-sc-2023-01-01.cloudfunctions.net/datos_transporte_et")
        response.raise_for_status()
        data = response.json()
        resources = data['result']['resources']
        urls = [resource['url'] for resource in resources if resource['url'].endswith('.zip')]
        for url in urls:
            yield url

class DownloadAndSaveZip(beam.DoFn):
    def __init__(self, output_bucket):
        self.output_bucket = output_bucket

    def process(self, url):
        response = requests.get(url)
        response.raise_for_status()
        zip_filename = url.split('/')[-1]
        gcs = GcsIO()
        output_path = f"{self.output_bucket}/{zip_filename}"
        if not gcs.exists(output_path):
            with gcs.open(output_path, 'wb') as f:
                f.write(response.content)
            logging.info(f"Archivo {zip_filename} guardado en {output_path}")
            yield output_path
        else:
            logging.info(f"Archivo {zip_filename} ya existe en {output_path}, saltando la descarga")

def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'duocuc-red'
    google_cloud_options.region = 'us-central1'
    google_cloud_options.staging_location = 'gs://duoc-red-bucket/staging'
    google_cloud_options.temp_location = 'gs://duoc-red-bucket/temp'

    output_bucket = 'gs://duoc-red-bucket/Datos_Historicos'

    with beam.Pipeline(options=pipeline_options) as p:
        urls = p | 'Extract URLs' >> beam.ParDo(ExtractUrls())
        _ = urls | 'Download and Save ZIP' >> beam.ParDo(DownloadAndSaveZip(output_bucket))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
