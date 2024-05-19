import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
import logging
import requests
import zipfile
import io
import os
import json
from apache_beam.io.gcp.gcsio import GcsIO

class ParseJsonFn(beam.DoFn):
    def process(self, element):
        data = json.loads(element)
        for entry in data:
            yield (entry['url'], entry['file_name_prefix'])

class DownloadZip(beam.DoFn):
    def process(self, element):
        url, file_name_prefix = element
        response = requests.get(url)
        response.raise_for_status()
        yield (file_name_prefix, response.content)

class SaveZipToGCS(beam.DoFn):
    def __init__(self, output_prefix):
        self.output_prefix = output_prefix

    def process(self, element):
        file_name_prefix, content = element
        file_path = f'{self.output_prefix}/{file_name_prefix}.zip'
        gcs = GcsIO()
        if not gcs.exists(file_path):  # Check if file already exists
            with gcs.open(file_path, 'wb') as f:
                f.write(content)
            yield file_path
        else:
            logging.info(f"File {file_path} already exists, skipping download.")
            yield file_path

class ExtractZip(beam.DoFn):
    def process(self, element):
        gcs = GcsIO()
        file_path = element
        with gcs.open(file_path, 'rb') as f:
            content = f.read()
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                for zip_info in z.infolist():
                    if zip_info.filename.endswith('.txt'):
                        with z.open(zip_info) as file:
                            yield (f"{os.path.basename(file_path)}_{zip_info.filename}", file.read())

class SaveExtractedFileToGCS(beam.DoFn):
    def __init__(self, output_prefix):
        self.output_prefix = output_prefix

    def process(self, element):
        file_name, content = element
        file_path = f'{self.output_prefix}/{file_name}'
        gcs = GcsIO()
        with gcs.open(file_path, 'wb') as f:
            f.write(content)
        yield file_path

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', dest='project', required=True, help='GCP project ID.')
    parser.add_argument('--runner', dest='runner', required=True, help='DataflowRunner.')
    parser.add_argument('--staging_location', dest='staging_location', required=True, help='Staging location.')
    parser.add_argument('--temp_location', dest='temp_location', required=True, help='Temp location.')
    parser.add_argument('--output_prefix', dest='output_prefix', required=True, help='Output directory prefix to save files.')
    parser.add_argument('--input_file', dest='input_file', required=True, help='Input JSON file containing URLs and file name prefixes.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).runner = known_args.runner
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        # Leer el archivo JSON de entrada con URLs y prefijos de nombres de archivos
        file_data = (
            p
            | 'ReadInputJson' >> beam.io.ReadFromText(known_args.input_file)
            | 'ParseJson' >> beam.ParDo(ParseJsonFn())
        )
        
        # Descargar archivos ZIP
        downloaded_files = (
            file_data
            | 'DownloadZip' >> beam.ParDo(DownloadZip())
            | 'SaveZipToGCS' >> beam.ParDo(SaveZipToGCS(known_args.output_prefix))
        )
        
        # Extraer archivos TXT de los ZIP descargados
        extracted_files = (
            downloaded_files
            | 'ExtractZip' >> beam.ParDo(ExtractZip())
            | 'SaveExtractedFileToGCS' >> beam.ParDo(SaveExtractedFileToGCS(known_args.output_prefix))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
