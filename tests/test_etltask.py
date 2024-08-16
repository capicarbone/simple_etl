import pdb
from unittest import TestCase

from simple_etl.loaders import DummyLoader, RichConsole, SimpleConsole
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

            for locator in column_specs[1]:
                self.assertIn(locator, input["injects"])

                # if locator == TestMapping.column_1:
                #     self.assertEqual(mapping_name, "column_1")
                # elif locator == TestMapping.column_2:
                #     self.assertEqual(mapping_name, "column_2")
                # elif locator == TestMapping.column_3:
                #     self.assertEqual(mapping_name, "column_3")

    # def test_dependencies_to_mapping_columns(self):
    #     task = ETLTask(mapping=TestMapping)

    #     result = task._ETLTask__dependencies_to_mapping_columns(
    #         [TestMapping.column_1, TestMapping.column_3]
    #     )

    #     self.assertEqual(2, len(result))
    #     result_dict = dict(result)

    #     self.assertIn("column_1", result_dict)
    #     self.assertEqual(result_dict["column_1"], TestMapping.column_1)

    #     self.assertIn("column_3", result_dict)
    #     self.assertEqual(result_dict["column_3"], TestMapping.column_3)

    def test_dependencies_to_mapping_columns_with_wrong_injects(self):
        pass  # TODO pending

    # def test_get_columns_for_mapping(self):

    #     task = ETLTask()

    #     result = task._ETLTask__get_columns_for_mapping(TestMapping)

    #     self.assertEqual(3, len(result))


    #     self.assertIn(TestMapping.column_1, result)
    #     # self.assertEqual(result_dict["column_1"], TestMapping.column_1)

    #     self.assertIn(TestMapping.column_2, result)
    #     # self.assertEqual(result_dict["column_2"], TestMapping.column_2)

    #     self.assertIn(TestMapping.column_3, result)
    #     # self.assertEqual(result_dict["column_3"], TestMapping.column_3)

    def test_process(self):
        task = ETLTask(mapping=TestMapping)

        task.add_output_column(
            "column_1",
            lambda x, y: 11111,
            injects=[TestMapping.column_1, TestMapping.column_2],
        )

        task.add_output_column(
            "column_2",
            lambda x, y: 222,
            injects=[TestMapping.column_2, TestMapping.column_3],
        )

        task.reader = DictReader([{"c1": 23, "c2": 33}, {"c1": 12, "c2": 22}])

        loader = DummyLoader()
        task.load(loader)

        output = loader.output

        self.assertEqual(2, len(output))

        for r in output:
            self.assertIn("column_1", r)
            self.assertIn("column_2", r)
            self.assertEqual(r["column_1"], 11111)
            self.assertEqual(r["column_2"], 222)
            self.assertEqual(2, len(r))

    # def test_task_processing(self):

    #     reader = DictReader(track_data)
    #     task.reader = reader

    #     task.load(RichConsole())
