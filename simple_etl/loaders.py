
class ResultLoader:
    
    def save_record(record: dict):
        raise NotImplemented()


class SimpleConsole(ResultLoader):

    def save_record(self, record: dict):
        print(record)
    