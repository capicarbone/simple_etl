from typing import Any
from attr import dataclass
from traitlets import Bool


class ValueLocator:

    def locate(self, record_data: Any):        
        raise NotImplemented()

    def validate_type(self, record_data: Any) -> Bool:
        raise NotImplemented()
    
    def get_value(self, record_data: Any):
        if self.validate_type(record_data):
            return self.locate(record_data)
        else:
            # TODO Add locator name
            raise Exception("Invalid data type for locator")



@dataclass
class DictKey(ValueLocator):
    key: str

    def locate(self, record_data: dict):
        return record_data.get(self.key, None)
    

    def validate_type(self, record_data: Any) -> Bool:
        # TODO Implement accordingly
        return True
    
