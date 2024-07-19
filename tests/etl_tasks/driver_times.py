from simple_etl.tasks import ETLTask
from simple_etl.locators import DictKey


class TrackDataMapping:
    driver_name = DictKey("driver name")
    car_number = DictKey("car number")


task = ETLTask(TrackDataMapping)


@task.output_column(
    "DriverID", TrackDataMapping.car_number, TrackDataMapping.driver_name
)
def driver_id(car_number: str, driver_name: str):
    return " ".join([car_number, driver_name.upper()]).strip()


@task.output_column("DriverLastName", TrackDataMapping.driver_name)
def driver_last_name(driver_name: str):
    names = driver_name.split(" ")
    if len(names) < 2:
        return ""
    else:
        return names[-1]