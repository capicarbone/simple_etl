
from typing import Any


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

    