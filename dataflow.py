import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions, GoogleCloudOptions
import logging
import requests
from apache_beam.io.gcp.gcsio import GcsIO

class ExtractUrls(beam.DoFn):
    def process(self, element):
        response = requests.get("https://us-central1-duoc-bigdata-sc-2023-01-01.cloudfunctions.net/datos_transporte_et")
        response.raise_for_status()
        data = response.json()
        resources = data['result']['resources']
        urls = [resource['url'] for resource in resources if resource['url'].endswith('.zip')]
        for url in urls:
            yield url

class DownloadZip(beam.DoFn):
    def process(self, element):
        url = element
        response = requests.get(url)
        response.raise_for_status()
        yield response.content

class SaveZipToGCS(beam.DoFn):
    def __init__(self, output_prefix):
        self.output_prefix = output_prefix

    def process(self, element):
        content = element
        file_path = f'{self.output_prefix}.zip'
        gcs = GcsIO()
        if not gcs.exists(file_path):  # Check if file already exists
            with gcs.open(file_path, 'wb') as f:
                f.write(content)
            yield file_path
        else:
            logging.info(f"File {file_path} already exists, skipping download.")
            yield file_path

def run(argv=None):
    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Set Google Cloud options
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'duocuc-red'
    google_cloud_options.staging_location = 'gs://duoc-red-bucket/staging'
    google_cloud_options.temp_location = 'gs://duoc-red-bucket/temp'

    with beam.Pipeline(options=pipeline_options) as p:
        # Obtener las URLs directamente del endpoint
        urls = p | 'ExtractUrls' >> beam.ParDo(ExtractUrls())

        # Descargar archivos ZIP
        downloaded_files = (
            urls | 'DownloadZip' >> beam.ParDo(DownloadZip())
            | 'SaveZipToGCS' >> beam.ParDo(SaveZipToGCS('gs://duoc-red-bucket/Datos_Historicos'))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
