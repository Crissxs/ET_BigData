import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
import logging
import argparse

def run(argv=None):
    """Main entry point; defines and runs the pipeline."""
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', dest='output', required=True, help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Definir las opciones de la pipeline
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Crear el pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        
        # Crear una PCollection con 10 elementos, todos "hola"
        output = (
            p
            | 'Create' >> beam.Create(['hola'] * 10)
        )
        
        # Escribir la salida a un archivo de texto
        output | 'Write' >> beam.io.WriteToText(known_args.output)
    
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
