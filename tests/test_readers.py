from unittest import TestCase
from simple_etl.readers import DictReader

class ReadersTestCase(TestCase):

    def test_dict_reader(self):

        test_data = [
            {'name': 'John', 'age': '23'},
            {'name': 'Linda', 'age': '43'},
        ]

        reader = DictReader(test_data)

        i = 0
        for keys, values in reader.read_row():            
            # self.assertEqual(keys, list(test_data[i].keys()))
            # self.assertEqual(values, list(test_data[i].values()))
            i -= 1
