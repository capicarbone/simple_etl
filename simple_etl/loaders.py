from rich.console import Console
from rich.table import Table


# TODO as context manager
class ResultLoader:

    def setup_target(self, columns):
        # TODO it must receive the columns with their types
        raise NotImplemented()

    def load_record(self, record: dict):
        raise NotImplemented()

    def commit(self):
        raise NotImplemented()


class SimpleConsole(ResultLoader):

    def setup_target(self, columns):
        pass

    def commit(self):
        pass

    def load_record(self, record: dict):
        print(record)


class RichConsole(ResultLoader):

    def __init__(self, title="Result") -> None:
        self.title = title

    def setup_target(self, columns):
        self.table = Table(title=self.title)
        for c in columns:
            self.table.add_column(c)

    def load_record(self, record: dict):

        self.table.add_row(*record.values())

    def commit(self):
        console = Console()
        console.print(self.table)
