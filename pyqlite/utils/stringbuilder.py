import os


class StringBuilder:
    def __init__(self, string: str = '') -> None:
        self.__string: str = string

    def len(self) -> int:
        return len(self.__string)

    def to_str(self) -> str:
        return self.__string

    def append(self, string: str) -> None:
        self.__string += string

    def append_line(self, string: str) -> None:
        self.__string += f"{string}{os.linesep}"

    def insert(self, pos: int, string: str) -> None:
        if pos < 0:
            raise ValueError('Invalid pos: ' + str(pos))
        self.__string = self.__string[:pos] + string + self.__string[pos:]

    def remove(self, start_index: int, len: int) -> None:
        if start_index < 0:
            raise ValueError('Invalid start_index: ' + str(start_index))
        if len < 1:
            raise ValueError('Invalid len: ' + str(len))
        self.__string = self.__string[:start_index] + \
            self.__string[start_index + len:]

    def replace(self, old_string: str, new_string: str) -> None:
        self.__string = self.__string.replace(old_string, new_string)

    def clear(self) -> None:
        self.__string = ''
