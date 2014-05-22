import luigi, luigi.hadoop
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

import sys
import json

class AvroJobRunner(luigi.hadoop.HadoopJobRunner):
    """A job runner that allows reading avro files.

    Return an instance of this class from the job_runner method
    in your task. Need to add avro-mapred-jar to the config file too.
    """
    def __init__(self):
        config = luigi.configuration.get_config()
        streaming_jar = config.get('hadoop', 'streaming-jar')
        input_format = 'org.apache.avro.mapred.AvroAsTextInputFormat'
        super(AvroJobRunner, self).__init__(streaming_jar=streaming_jar,
                                            input_format=input_format,
                                            libjars=[config.get('avro', 'avro-mapred-jar')])

class AvroTask(luigi.hadoop.JobTask):
    """This is a task that allows you to write
    avro files from your tasks.
    """
    def avro_schema(self):
        """Specify your avro schema as a dict.
        """
        raise NotImplementedError("You must specify an avro schema to write an avro file.")

    def writer(self, outputs, stdout, stderr=sys.stderr):
        """Overrides base method for hadoop.JobTask
        """
        schema = avro.schema.parse(json.dumps(self.avro_schema()))

        writer = DataFileWriter(stdout, DatumWriter(), schema)
        
        for output in outputs:
            writer.append(output[1])
        #Needn't call close, cause the luigi job
        #will do that.
        writer.flush()
