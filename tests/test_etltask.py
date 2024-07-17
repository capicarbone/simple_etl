from unittest import TestCase

from simple_etl.tasks import ETLTask
from simple_etl.readers import DictReader
from tests.mappings import TrackDataMapping
from .data import track_data

def car_number_and_driver_name(car_number, driver_name):
    return " ".join([car_number, driver_name])

class ETLTaskTestCase(TestCase):

    def test_task_processing(self):

        reader = DictReader(track_data)
        task = ETLTask(reader, TrackDataMapping)
        task.add_output_column('car and driver', car_number_and_driver_name, TrackDataMapping.car_number, TrackDataMapping.driver_name)
        task.load()