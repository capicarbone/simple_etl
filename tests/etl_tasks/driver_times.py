from typing import Annotated
from simple_etl.tasks import ETLTask
from simple_etl.locators import DictKey


class TrackDataMapping:
    driver_name = DictKey("driver name")
    car_number = DictKey("car number")


task = ETLTask(TrackDataMapping)


@task.output_column("DriverID")
def driver_id(
    car_number: Annotated[str, TrackDataMapping.car_number],
    driver_name: Annotated[str, TrackDataMapping.driver_name],
):
    return " ".join([car_number, driver_name.upper()]).strip()


@task.output_column("DriverLastName")
def driver_last_name(driver_name: Annotated[str, TrackDataMapping.driver_name]):
    names = driver_name.split(" ")
    if len(names) < 2:
        return ""
    else:
        return names[-1]
