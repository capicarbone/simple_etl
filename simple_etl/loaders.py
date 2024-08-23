from rich.console import Console
from rich.table import Table


class ResultLoader:

    def start(self, columns):
        # TODO it must receive the columns with their types
        raise NotImplemented()
    

    def __enter__(self):
        self.start(self.columns)
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def load_record(self, record: dict):
        raise NotImplemented()

    def commit(self):
        raise NotImplemented()


class SimpleConsole(ResultLoader):

    def start(self, columns):
        pass

    def commit(self):
        pass

    def load_record(self, record: dict):
        print(record)


class RichConsole(ResultLoader):

    def __init__(self, title="Result") -> None:
        self.title = title

    def start(self, columns):
        self.table = Table(title=self.title)
        for c in columns:
            self.table.add_column(c)

    def load_record(self, record: dict):

        self.table.add_row(*[str(v) for v in record.values()])

    def commit(self):
        console = Console()
        console.print(self.table)


class DummyLoader(ResultLoader):

    def __init__(self) -> None:
        super().__init__()
        self.output = []

    def start(self, columns):
        pass

    def load_record(self, record: dict):
        
        self.output.append(record)

    def commit(self):
        pass