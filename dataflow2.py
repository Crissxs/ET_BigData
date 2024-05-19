import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging

def run(argv=None):
    """Main entry point; defines and runs the pipeline."""
    
    # Definir las opciones de la pipeline
    pipeline_options = PipelineOptions(argv)
    
    # Crear el pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        
        # Crear una PCollection con 10 elementos, todos "hola"
        output = (
            p
            | 'Create' >> beam.Create(['hola'] * 10)
        )
        
        # Escribir la salida a un archivo de texto
        output | 'Write' >> beam.io.WriteToText('output/hola.txt')
    
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
