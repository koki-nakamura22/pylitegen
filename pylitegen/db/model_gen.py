from typing import List

from utils.file import save_as_text
from utils.string import to_pascal_case
from utils.stringbuilder import StringBuilder
from .column import Column


def gen(table_name: str, columns: List[Column]):
    exists_any_type = False
    is_use_optional = False
    members_code = StringBuilder()
    for c in columns:
        if c.data_type_to_py_type_str() == 'Any':
            exists_any_type = True

        if c.is_not_null():
            data_type = c.data_type_to_py_type_str()
        else:
            is_use_optional = True
            data_type = f"Optional[{c.data_type_to_py_type_str()}]"

        members_code.append_line(f"    {c.name}: {data_type}")
    members_code.append(f"    table_name: Final[str] = '{table_name}'")

    code_str = StringBuilder()
    code_str.append_line(f"from dataclasses import dataclass")

    import_typing_str = StringBuilder()
    import_typing_str.append('from typing import Final')
    if exists_any_type:
        import_typing_str.append(', Any')
    if is_use_optional:
        import_typing_str.append(', Optional')

    code_str.append_line(import_typing_str.to_str())
    code_str.append_line('')
    code_str.append_line('')
    code_str.append_line("@dataclass(init=True, eq=True, frozen=True)")
    code_str.append_line(f"class {to_pascal_case(table_name)}:")
    code_str.append_line(members_code.to_str())

    save_as_text(f"{table_name}.py", code_str.to_str())
