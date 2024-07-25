import pdb
from unittest import TestCase

from simple_etl.loaders import RichConsole, SimpleConsole
from simple_etl.locators import DictKey
from simple_etl.readers import DictReader
from simple_etl.tasks import ETLTask
from .data import track_data
from .etl_tasks.driver_times import task


class TestMapping:
    column_1 = DictKey("c1")
    column_2 = DictKey("c2")
    column_3 = DictKey("c3")


class ETLTaskTestCase(TestCase):

    def test_add_output_column(self):

        task = ETLTask(mapping=TestMapping)

        func_1 = lambda x: x
        func_2 = lambda x, y: x + y
        func_3 = lambda x, y, z: x + y + z

        test_input = [
            {
                "column_name": "output_1",
                "func": func_1,
                "injects": [TestMapping.column_1],
            },
            {
                "column_name": "output_2",
                "func": func_2,
                "injects": [TestMapping.column_1, TestMapping.column_2],
            },
            {
                "column_name": "output_3",
                "func": func_3,
                "injects": [
                    TestMapping.column_1,
                    TestMapping.column_2,
                    TestMapping.column_3,
                ],
            },
        ]

        for oc in test_input:
            task.add_output_column(**oc)

        output_columns = task.output_columns

        for input in test_input:
            column_name = input["column_name"]

            self.assertIn(column_name, output_columns)
            column_specs = output_columns[column_name]

            self.assertEqual(input["func"], column_specs[0])
            self.assertEqual(len(input["injects"]), len(column_specs[1]))

            for mapping_name, allocator in column_specs[1]:
                self.assertIn(allocator, input["injects"])

                if allocator == TestMapping.column_1:
                    self.assertEqual(mapping_name, "column_1")
                elif allocator == TestMapping.column_2:
                    self.assertEqual(mapping_name, "column_2")
                elif allocator == TestMapping.column_3:
                    self.assertEqual(mapping_name, "column_3")

    def test_dependencies_to_mapping_columns(self):
        task = ETLTask(mapping=TestMapping)

        result = task._ETLTask__dependencies_to_mapping_columns(
            [TestMapping.column_1, TestMapping.column_3]
        )

        self.assertEqual(2, len(result))
        result_dict = dict(result)
        
        self.assertIn('column_1', result_dict)
        self.assertEqual(result_dict['column_1'], TestMapping.column_1)

        self.assertIn('column_3', result_dict)
        self.assertEqual(result_dict['column_3'], TestMapping.column_3)


    def test_dependencies_to_mapping_columns_with_wrong_injects(self):
        pass # TODO pending

    
    def test_get_columns_for_mapping(self):

        task = ETLTask(mapping=TestMapping)

        result = task._ETLTask__get_columns_for_mapping()

        self.assertEqual(3, len(result))
        result_dict = dict(result)

        self.assertIn('column_1', result_dict)
        self.assertEqual(result_dict['column_1'], TestMapping.column_1)

        self.assertIn('column_2', result_dict)
        self.assertEqual(result_dict['column_2'], TestMapping.column_2)

        self.assertIn('column_3', result_dict)
        self.assertEqual(result_dict['column_3'], TestMapping.column_3)





    

    # def test_task_processing(self):

    #     reader = DictReader(track_data)
    #     task.reader = reader

    #     task.load(RichConsole())
