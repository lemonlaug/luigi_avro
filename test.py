import luigi
from luigi.mock import MockFile
from luigi import scheduler, worker
import hadoop_avro
import os
import json
from unittest import TestCase

from avro.datafile import DataFileReader
from avro.io import DatumReader

class LuigiTestCase(TestCase):
    def schedule_run(self, task):
        sch = scheduler.CentralPlannerScheduler()
        w = worker.Worker(scheduler=sch)
        w.add(task)
        w.run()

class DummyInput(luigi.Task):
    def output(self):
        return MockFile('mockfile.txt')
    
    def run(self):
        f = self.output().open('w')
        f.write('{"integer": 1, "string": "fake"}\n')
        f.write('{"integer":2, "string": "real"}\n')
        f.close()

class TestAvroWrite(LuigiTestCase):
    def setUp(self):
        try:
            os.remove('avro_out.avro')
        except OSError:
            pass

    def test_writing(self):

        class AvroWrite(hadoop_avro.AvroTask):
            def requires(self):
                return DummyInput()

            def mapper(self, line):
                line = json.loads(line)
                yield None, line

            def get_avro_schema(self):
                schema = {'namespace': 'test',
                          'type': 'record',
                          'name': 'test_data',
                          'fields': [{'name': 'integer', 'type': ['int', 'null']},
                                     {'name': 'string', 'type': ['string', 'null']}]
                          }
                return schema

            def output(self):
                return luigi.LocalTarget('avro_out.avro')
                
        aw = AvroWrite()
        self.schedule_run(aw)

        #Now file should exist and be readable.
        reader = DataFileReader(open("avro_out.avro", "r"), DatumReader())
        data = [x for x in reader]
        self.assertEqual(len(data), 2)
        self.assertEqual(data[1]['integer'], 1)
        self.assertEqual(data[0]['string'], 'real')
