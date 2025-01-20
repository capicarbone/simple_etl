import sys


sys.path.insert(0, "../../../simple_etl")

from typing import Annotated
from simple_etl import ETLTask
from datetime import timedelta
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


task = ETLTask(CSVReader("track-data.csv"))


def kmp_to_mph(value) -> float:
    return round(value / 1.609344, 1)


# Transforms

task.passtrhough(TrackDataMapping.lap)

@task.output_column("Min speed (mph)")
def min_speed_mph(kmh: Annotated[str, TrackDataMapping.min_speed]):
    return str(kmp_to_mph(float(kmh)))


@task.output_column("Max speed (mph)")
def max_speed_mph(kmh: Annotated[str, TrackDataMapping.max_speed]):
    return str(kmp_to_mph(float(kmh)))


@task.output_column("Avg speed (mph)")
def avg_speed_mph(kmh: Annotated[str, TrackDataMapping.avg_speed]):
    return str(kmp_to_mph(float(kmh)))


task.passtrhough(TrackDataMapping.s1)
task.passtrhough(TrackDataMapping.s2)
task.passtrhough(TrackDataMapping.s3)

@task.output_column("Lap time")
def lap_time(
    s1: Annotated[str, TrackDataMapping.s1],
    s2: Annotated[str, TrackDataMapping.s2],
    s3: Annotated[str, TrackDataMapping.s3],
):

    laptime = timedelta()
    for s in [s1, s2, s3]:
        hours, minutes, seconds = s.split(":")
        seconds, milliseconds = seconds.split(".")

        laptime += timedelta(
            hours=int(hours),
            minutes=int(minutes),
            seconds=int(seconds),
            milliseconds=int(milliseconds),
        )

    laptime = str(laptime)

    if len(laptime.split(":")[0]) == 1:
        laptime = "0" + laptime

    return laptime[0:12]


if __name__ == "__main__":
    task.load(RichConsole())
