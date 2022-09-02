import re
from typing import Iterator


def __parse_words(string: str) -> Iterator[str]:
    for block in re.split(r"[ _-]+", string):
        yield block


def to_pascal_case(string: str) -> str:
    words_iter = __parse_words(string)
    return "".join(word.capitalize() for word in words_iter)
