import os
from typing import List

from pylitegen.utils.file import save_as_text
from pylitegen.utils.string import to_pascal_case
from .column import Column


def gen(table_name: str, columns: List[Column]):
    exists_any_type = False
    is_use_optional = False
    members_code = ""
    for c in columns:
        if c.data_type_to_py_type_str() == 'Any':
            exists_any_type = True

        if c.is_not_null():
            data_type = c.data_type_to_py_type_str()
        else:
            is_use_optional = True
            data_type = f"Optional[{c.data_type_to_py_type_str()}]"

        members_code += f"    {c.name}: {data_type}{os.linesep}"
    members_code += f"    table_name: str = '{table_name}'{os.linesep}"

    code_str = f"from dataclasses import dataclass{os.linesep}"

    import_typing_str = 'from typing import '
    if exists_any_type:
        import_typing_str += 'Any, '
    if is_use_optional:
        import_typing_str += 'Optional'
    import_typing_str = import_typing_str.rstrip().rstrip(',')

    if 19 < len(import_typing_str):
        code_str += f"{import_typing_str}{os.linesep}"
    code_str += os.linesep
    code_str += os.linesep
    code_str += f"@dataclass(init=True, eq=True, frozen=True){os.linesep}"
    code_str += f"class {to_pascal_case(table_name)}:{os.linesep}"
    code_str += str(members_code)

    save_as_text(f"{table_name}.py", code_str)
