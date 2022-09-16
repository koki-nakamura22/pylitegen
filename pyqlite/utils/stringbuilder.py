import os


class StringBuilder:
    """Represents a mutable string of characters.
    """

    def __init__(self, string: str = '') -> None:
        """Constructor

        Parameters
        ----------
        string : str, optional
            Initial string, by default ''
        """
        self.__string: str = string

    def len(self) -> int:
        """Get string length.

        Returns
        -------
        int
            String length
        """
        return len(self.__string)

    def to_str(self) -> str:
        """Get StringBuilder value as str.

        Returns
        -------
        str
            StringBuilder value as str
        """
        return self.__string

    def append(self, string: str) -> None:
        """Append string.

        Parameters
        ----------
        string : str
            String to append
        """
        self.__string += string

    def append_line(self, string: str) -> None:
        """Append string and a new line character.

        Parameters
        ----------
        string : str
            String to append
        """
        self.__string += f"{string}{os.linesep}"

    def insert(self, pos: int, string: str) -> None:
        """Insert string to specific position.

        Parameters
        ----------
        pos : int
            Specific position
        string : str
            String to insert

        Raises
        ------
        ValueError
            Raises ValueError if pos contains less than 0
        """
        if pos < 0:
            raise ValueError('Invalid pos: ' + str(pos))
        self.__string = self.__string[:pos] + string + self.__string[pos:]

    def remove(self, start_index: int, len: int) -> None:
        """Remove specific range string.

        Parameters
        ----------
        start_index : int
            Remove start position
        len : int
            Remove string length

        Raises
        ------
        ValueError
            Raises ValueError if start_inidex contains less than 0
        ValueError
            Raises ValueError if len contains less than 1
        """
        if start_index < 0:
            raise ValueError('Invalid start_index: ' + str(start_index))
        if len < 1:
            raise ValueError('Invalid len: ' + str(len))
        self.__string = self.__string[:start_index] + \
            self.__string[start_index + len:]

    def replace(self, old_string: str, new_string: str) -> None:
        """Replace string.

        Parameters
        ----------
        old_string : str
            Replace target string
        new_string : str
            Replace string
        """
        self.__string = self.__string.replace(old_string, new_string)

    def clear(self) -> None:
        """Clear string.
        """
        self.__string = ''
