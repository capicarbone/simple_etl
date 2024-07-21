from unittest import TestCase

from simple_etl.loaders import RichConsole, SimpleConsole
from simple_etl.readers import DictReader
from .data import track_data
from .etl_tasks.driver_times import task


class ETLTaskTestCase(TestCase):

    def test_task_processing(self):

        reader = DictReader(track_data)
        task.reader = reader

        task.load(RichConsole())
