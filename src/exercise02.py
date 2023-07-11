file_path = '../resources/fixed-length/users.txt'

from dataclasses import dataclass

@dataclass
class DataUser:
    id: int
    name: str
    city: str
    school: str

def parse_data_user(line:str) -> DataUser:
    id: int(line[0:4])
    name: line[0:4]
    city: line[0:4]
    school: line[0:4]