from typing import List

from pyqlite.utils.file import save_as_text
from pyqlite.utils.string import to_pascal_case
from pyqlite.utils.stringbuilder import StringBuilder
from pyqlite.generator.column import Column


def __gen_base_model():
    code = """from abc import ABC
import dataclasses
from typing import List

class BaseModel(ABC):
    def values_as_list(self) -> List:
        val_list = list()
        members = vars(self)
        for k in members:
            val_list.append(members[k])
        return val_list

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)
"""
    save_as_text(f"model.py", code)


def generate(table_name: str, columns: List[Column]):
    exists_any_type = False
    is_use_optional = False
    pks = list()
    members_code = StringBuilder()
    for c in columns:
        if c.data_type_to_py_type_str() == 'Any':
            exists_any_type = True

        if c.is_not_null():
            data_type = c.data_type_to_py_type_str()
        else:
            is_use_optional = True
            data_type = f"Optional[{c.data_type_to_py_type_str()}]"

        if c.is_pk():
            pks.append(c.name)

        members_code.append_line(f"    {c.name}: {data_type}")
    members_code.append_line(f"    table_name: Final[str] = '{table_name}'")
    members_code.append(f"    pk_names: Final[List[str]] = {str(pks)}")

    code_str = StringBuilder()
    code_str.append_line(f"from dataclasses import dataclass")

    import_typing_str = StringBuilder()
    import_typing_str.append('from typing import Final, List')
    if exists_any_type:
        import_typing_str.append(', Any')
    if is_use_optional:
        import_typing_str.append(', Optional')

    code_str.append_line(import_typing_str.to_str())
    code_str.append_line('')
    code_str.append_line('from model import BaseModel')
    code_str.append_line('')
    code_str.append_line('')
    code_str.append_line("@dataclass(init=True, eq=True, frozen=True)")
    code_str.append_line(f"class {to_pascal_case(table_name)}(BaseModel):")
    code_str.append_line(members_code.to_str())

    __gen_base_model()
    save_as_text(f"{table_name}.py", code_str.to_str())
