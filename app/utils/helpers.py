from typing import List, Any, Iterator
from datetime import date

def number_to_column(number: int) -> str:
    column = ""
    while number > 0:
        remainder = (number - 1) % 26
        column = chr(65 + remainder) + column
        number = (number - 1) // 26
    return column

def column_to_number(column: str) -> int:
    column = column.upper()
    number = 0
    for char in column:
        number = number * 26 + (ord(char) - ord('A')) + 1
    return number

def convert_dates_to_string(data: List[tuple]) -> List[List[Any]]:
    processed_data = []
    for record in data:
        row = list(record)
        for i, value in enumerate(row):
            if isinstance(value, date):
                row[i] = value.strftime("%Y-%m-%d")
        processed_data.append(row)
    return processed_data

def chunk_data(data: List[Any], chunk_size: int) -> Iterator[List[Any]]:
    if chunk_size <= 0:
        yield data
        return

    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]