import inspect
from typing import Any, Callable, Annotated

from simple_etl.loaders import ResultLoader
from simple_etl.locators import ValueLocator
from simple_etl.readers import SourceReader


class ETLTask:

    def __init__(self) -> None:

        self.output_columns: dict[str, tuple[Callable, list[ValueLocator]]] = {}

    def _extract_locators_from_function_parameters(self, func: Callable) -> list[ValueLocator]:
        annotations: list[Annotated] = func.__annotations__.values()
        return [a.__metadata__[0] for a in annotations]


    def computed(self, column_name: str):
        def decorator(func: Callable):
            injects = self._extract_locators_from_function_parameters(func)
            self.add_computed(column_name, func, injects)
            return func

        return decorator

    def add_computed(
        self, column_name: str, func: Callable, injects: list[ValueLocator]
    ):

        # TODO convert dependencies to mapping_columns
        self.output_columns[column_name] = (
            func,
            injects,
        )

    def passthrough(self, locator: ValueLocator, output_name=None):
        self.add_computed(
            column_name=output_name or locator.identifier,
            func=lambda x: x,
            injects=[locator]
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

    def process(self, reader: SourceReader):

        # TODO validate the existance a raader

        for record in reader.read_row():

            values_map = {}

            output_record = {}

            for output_column_name, process_data in self.output_columns.items():
                func, injects = process_data

                parameters = self._get_output_parameters(record, injects, values_map)

                # print(f"Calling {func.__name__} with values {parameters}")
                output_record[output_column_name] = func(*parameters)

            yield output_record


    def _get_output_parameters(self, record: Any, locators: list[ValueLocator], values_cache: dict[ValueLocator, Any]):

        parameters = []
        
        for locator in locators:

            if locator not in values_cache:

                values_cache[locator] = locator.get_value(record)

            parameters.append(values_cache[locator])

        return parameters
        

    def load(self, reader: SourceReader, loader: ResultLoader):

        loader.columns = self.output_columns.keys()

        with loader as l:

            for output_record in self.process(reader):
                l.load_record(output_record)

            l.commit()


class GroupedETLTask(ETLTask):

    # def __init__(self, mapping: Callable[..., Any] = None, reader: SourceReader = None) -> None:
    #     super().__init__(mapping, reader)

    def group(self):
        def decorator(func: Callable):
            injects = self._extract_locators_from_function_parameters(func)
            self.register_group(func, injects)
            
            return func
        return decorator


    def register_group(self, func: Callable, injects: list[ValueLocator]):
        self.group_rule = (func, injects)
        

    def process(self, reader: SourceReader):

        groups = {}
        for record in reader.read_row():

            values_map = {}

            output_record = {}
            
            group_func, group_locators = self.group_rule
            
            group_parameters = self._get_output_parameters(record, group_locators, values_map)
            
            group = group_func(*group_parameters)
            
            agg_values = groups.get(group, {})

            for output_column_name, process_data in self.output_columns.items():
                func, injects = process_data

                parameters = self._get_output_parameters(record, injects, values_map)

                # print(f"Calling {func.__name__} with values {parameters}")
                
                if output_column_name in agg_values:
                    parameters.append(agg_values[output_column_name])
                output_record[output_column_name] = func(*parameters)

            groups[group] = output_record

        for record in groups.values():
            yield record
        
    

    
