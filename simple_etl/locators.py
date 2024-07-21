from typing import Any


class ValueLocator:

    def locate(self, record_data: Any):        
        raise NotImplemented()

    def validate_type(self, record_data: Any) -> bool:
        raise NotImplemented()
    
    def get_value(self, record_data: Any):
        if self.validate_type(record_data):
            return self.locate(record_data)
        else:
            # TODO Add locator name
            raise Exception("Invalid data type for locator")



class DictKey(ValueLocator):

    def __init__(self, key:str) -> None:
        super().__init__()
        self.key = key

    def locate(self, record_data: dict):
        return record_data.get(self.key, None)
    

    def validate_type(self, record_data: Any) -> bool:
        # TODO Implement accordingly
        return True
    
