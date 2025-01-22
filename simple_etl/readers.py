import io
from typing import Any
import csv


class SourceReader:

    def read_row(self) -> Any:
        raise NotImplemented("Missing read_row")


class DictReader(SourceReader):

    def __init__(self, data: dict) -> None:
        super().__init__()
        self.data = data

    def read_row(self) -> Any:

        for d in self.data:
            yield d


class CSVReader(SourceReader):

    def __init__(self, fileobj: io.StringIO, header_index=0, **kwargs) -> None:
        super().__init__()
        self.fileobj = fileobj
        self.reader_params = kwargs
        self.header_index = header_index
        self.header: list[str] = None

    def read_row(self) -> Any:

        reader = csv.reader(self.fileobj, **self.reader_params)
        for i, row in enumerate(reader):

            if not self.header:
                if i == self.header_index:
                    self.header = row
                continue

            yield (self.header, row)

