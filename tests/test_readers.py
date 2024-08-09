from unittest import TestCase
from simple_etl.readers import CSVReader, DictReader

# class ReadersTestCase(TestCase):

#     def test_dict_reader(self):

#         test_data = [
#             {'name': 'John', 'age': '23'},
#             {'name': 'Linda', 'age': '43'},
#         ]

#         reader = DictReader(test_data)

#         i = 0
#         for keys, values in reader.read_row():            
#             self.assertEqual(keys, list(test_data[i].keys()))
#             self.assertEqual(values, list(test_data[i].values()))
#             i -= 1


class TestCSVReader(TestCase):

    def setUp(self) -> None:
        self.test_file = "tests/test_data/simple.csv"
    

    def test_correct_reading(self):

        reader = CSVReader(self.test_file, delimiter=',')
        
        data = []

        for row in reader.read_row():
            self.assertIsInstance(row, tuple)
            self.assertEqual(2, len(row), "A tuple with two elements is expected")

            header, values = row            

            data.append(values)

        self.assertEqual(3, len(header))
        self.assertEqual(4, len(data))
        
        self.assertEqual(data[1][1], '15')
        self.assertEqual(data[3][2], '13')
        self.assertEqual(data[2][0], '57')
        self.assertEqual(data[0][2], '10')








