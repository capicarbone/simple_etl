import sys


sys.path.insert(0, "../../../simple_etl")

from typing import Annotated
from simple_etl import ETLTask
from simple_etl.readers import CSVReader
from simple_etl.loaders import RichConsole
from simple_etl.locators import Column


class TrackDataMapping:
    lap = Column("Lap")
    min_speed = Column("Min speed")
    max_speed = Column("Max speed")
    avg_speed = Column("Avg speed")
    s1 = Column("S1")
    s2 = Column("S2")
    s3 = Column("S3")


task = ETLTask(TrackDataMapping, CSVReader("track-data.csv"))


def kmp_to_mph(value) -> float:
    return round(value / 1.609344, 1)


# Transforms


@task.output_column("Lap")
def lap(lap: Annotated[str, TrackDataMapping.lap]):
    return lap


@task.output_column("Min speed (mph)")
def min_speed_mph(kmh: Annotated[str, TrackDataMapping.min_speed]):
    return str(kmp_to_mph(float(kmh)))


@task.output_column("Max speed (mph)")
def max_speed_mph(kmh: Annotated[str, TrackDataMapping.max_speed]):
    return str(kmp_to_mph(float(kmh)))


@task.output_column("Avg speed (mph)")
def avg_speed_mph(kmh: Annotated[str, TrackDataMapping.avg_speed]):
    return str(kmp_to_mph(float(kmh)))


@task.output_column("S1")
def s1(sector: Annotated[str, TrackDataMapping.s1]):
    return sector


@task.output_column("S2")
def s2(sector: Annotated[str, TrackDataMapping.s2]):
    return sector


@task.output_column("S3")
def s3(sector: Annotated[str, TrackDataMapping.s3]):
    return sector


@task.output_column("Lap time")
def lap_time(
    s1: Annotated[str, TrackDataMapping.s1],
    s2: Annotated[str, TrackDataMapping.s2],
    s3: Annotated[str, TrackDataMapping.s3],
):
    pass


if __name__ == "__main__":
    task.load(RichConsole())
