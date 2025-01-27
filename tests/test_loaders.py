import csv
import os.path
from unittest import TestCase
from simple_etl.loaders import CSVLoader

class TestCSVLoader(TestCase):

    def setUp(self):
        self.test_filename = "loader_test.csv"
        # self.test_fileobj = open(self.test_filename, "w")

    def test_sucessfull_loading(self):

        columns = {'Index': str, 'Message': str}

        records = [
            {'Index': '1', 'Message': 'Hello'},
            {'Index': '2', 'Message': 'World'}
        ]

        with open(self.test_filename, "w") as f:

            loader = CSVLoader(f)

            loader.start(columns)
            for r in records:
                loader.load_record(r)

        self.assertTrue(os.path.exists(self.test_filename))

        with open(self.test_filename, "r") as f:
            reader = csv.reader(f)

            for i, row in enumerate(reader):
                if i == 0:
                    self.assertEqual(list(columns.keys()), row)
                else:
                    ref_record = records[i-1].values()
                    self.assertEqual(len(ref_record), len(row))


    def tearDown(self):
        if os.path.exists(self.test_filename):
            os.remove(self.test_filename)


