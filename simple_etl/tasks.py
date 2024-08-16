import inspect
from typing import Callable, Annotated

from simple_etl.loaders import ResultLoader
from simple_etl.locators import ValueLocator
from simple_etl.readers import SourceReader


class ETLTask:

    def __init__(self, mapping: Callable = None, reader: SourceReader = None) -> None:
        self.reader = reader
        # self.mapping_columns = self.__get_columns_for_mapping(mapping)
        self.output_columns: dict[str, tuple[Callable, list[ValueLocator]]] = {}

    def output_column(self, column_name: str):
        def decorator(func: Callable):
            annotations: list[Annotated] = func.__annotations__.values()
            injects = [a.__metadata__[0] for a in annotations]
            self.add_output_column(column_name, func, injects)
            return func

        return decorator

    def add_output_column(
        self, column_name: str, func: Callable, injects: list[ValueLocator]
    ):

        # TODO convert dependencies to mapping_columns
        self.output_columns[column_name] = (
            func,
            injects,
        )

    def __get_columns_for_mapping(self, mapping_class) -> list[ValueLocator]:

        attrs = inspect.getmembers(
            mapping_class,
            lambda x: not inspect.isroutine(x) and isinstance(x, ValueLocator),
        )

        return [at[1] for at in attrs]

    def __dependencies_to_mapping_columns(
        self, injects: list[ValueLocator]
    ) -> list[tuple[str, ValueLocator]]:

        result = []

        for dependency in injects:

            mc = None

            for mapping_column in self.mapping_columns:
                if mapping_column[1] is dependency:
                    mc = mapping_column
                    break

            if not mc:
                raise Exception("Dependency missing in mapping")

            result.append(mc)

        return result

    def process(self):

        # TODO validate the existance a raader

        for record in self.reader.read_row():

            values_map = {}

            output_record = {}

            for output_column_name, process_data in self.output_columns.items():
                func, injects = process_data

                parameters = []

                for locator in injects:

                    if locator not in values_map:

                        values_map[locator] = locator.get_value(record)

                    parameters.append(values_map[locator])

                # print(f"Calling {func.__name__} with values {parameters}")
                output_record[output_column_name] = func(*parameters)

            yield output_record

    def load(self, loader: ResultLoader):

        loader.columns = self.output_columns.keys()

        with loader as l:

            for output_record in self.process():
                l.load_record(output_record)

            l.commit()
