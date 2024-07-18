import inspect
from typing import Callable
from simple_etl.locators import ValueLocator
from simple_etl.readers import SourceReader


class ETLTask:

    def __init__(self, mapping: Callable, reader: SourceReader = None) -> None:
        self.reader = reader
        self.mapping_class = mapping
        self.mapping_columns = self.__get_columns_for_mapping()
        self.output_columns : dict[str, tuple[Callable, tuple[tuple[str, ValueLocator]]]] = {}



    def output_column(self, column_name: str, *args):
        def decorator(func):
            self.add_output_column(column_name, func, *args)
            return func

        return decorator


    def add_output_column(self, column_name: str, func,  *args):

        # TODO convert dependencies to mapping_columns
        self.output_columns[column_name] = (func, self.__dependencies_to_mapping_columns(args))


    def __get_columns_for_mapping(self) -> tuple[str, ValueLocator]:

        attrs = inspect.getmembers(self.mapping_class, lambda x: not inspect.isroutine(x) and isinstance(x, ValueLocator))

        return attrs
    

    def __dependencies_to_mapping_columns(self, dependencies):

        result = []

        for dependency in dependencies:

            mc = None

            for mapping_column in self.mapping_columns:
                if mapping_column[1] is dependency:
                    mc = mapping_column
                    break

            if mc:
                result.append(mc)
            else:
                raise Exception("Dependency missing in mapping")
            
        return result


    def process(self):

        self.output_data = []


        # TODO validate the existance a raader
        

        for record in self.reader.read_row():

            values_map = {}

            output_record = {}
            
            for output_column_name, process_data in self.output_columns.items():
                func, dependencies = process_data

                parameters = []

                for dependecy in dependencies:
                    dependency_key, locator = dependecy

                    if dependency_key not in values_map:                    
                        
                        values_map[dependency_key] = locator.get_value(record)

                    parameters.append(values_map[dependency_key])

                
                # print(f"Calling {func.__name__} with values {parameters}")
                output_record[output_column_name] = func(*parameters)            
            
            self.output_data.append(output_record)

        print(self.output_data)

            
    def load(self):
        self.process()