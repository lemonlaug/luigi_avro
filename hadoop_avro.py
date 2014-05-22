import luigi, luigi.hadoop
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

import sys
import json

class AvroTask(luigi.hadoop.JobTask):
    """This is the function that defines some
    common functionalit for research input datasets.
    """
    def get_avro_schema(self):
        """Should we specify as dict and do json.dumps?
        Or specify as a separate file?
        """
        raise NotImplementedError("You must specify an avro schema to write an avro file.")

    def writer(self, outputs, stdout, stderr=sys.stderr):
        """Overrides base method for hadoop.JobTask
        """
        schema = avro.schema.parse(json.dumps(self.get_avro_schema()))

        writer = DataFileWriter(stdout, DatumWriter(), schema)
        
        for output in outputs:
            writer.append(output[1])
        #Do you close stdout? At min should call writer.flush()
        writer.flush()
