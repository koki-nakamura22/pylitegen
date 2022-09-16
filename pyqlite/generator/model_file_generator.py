import os
from typing import List

import inflection

from pyqlite.utils.file import save_as_text
from pyqlite.utils.string import to_pascal_case
from pyqlite.utils.stringbuilder import StringBuilder
from pyqlite.generator.column import Column


class ModelFileGenerator:
    """ModelFileGenerator
    """

    @classmethod
    def generate(
            cls,
            table_name: str,
            columns: List[Column],
            output_path: str):
        """Generate model files by database metadata.

        Parameters
        ----------
        table_name : str
            The target table name
        columns : List[Column]
            Columns data from the target table
        output_path : str
            Model files output path
        """
        exists_any_type = False
        is_use_pk = False
        is_use_optional = False
        members_code = StringBuilder()
        singularized_table_name = inflection.singularize(table_name)

        # Build members code
        for c in columns:
            if c.data_type_to_py_type_str() == 'Any':
                exists_any_type = True

            if c.is_pk():
                is_use_pk = True
                data_type = f"Final[{c.data_type_to_py_type_str()}]"
            elif c.is_not_null():
                data_type = c.data_type_to_py_type_str()
            else:
                is_use_optional = True
                data_type = f"Optional[{c.data_type_to_py_type_str()}] = None"

            members_code.append_line(f"    {c.name}: {data_type}")
        members_code.append_line(
            f"    __table_name: ClassVar[str] = '{table_name}'")

        # Build whole code
        code_str = StringBuilder()
        code_str.append_line(f"from dataclasses import dataclass")

        import_typing_str = StringBuilder()
        import_typing_str.append('from typing import ClassVar')
        if exists_any_type:
            import_typing_str.append(', Any')
        if is_use_pk:
            import_typing_str.append(', Final')
        if is_use_optional:
            import_typing_str.append(', Optional')

        code_str.append_line(import_typing_str.to_str())
        code_str.append_line('')
        code_str.append_line('from pyqlite.model import BaseModel')
        code_str.append_line('')
        code_str.append_line('')
        code_str.append_line("@dataclass(init=True, eq=True)")
        code_str.append_line(
            f"class {to_pascal_case(singularized_table_name)}(BaseModel):")
        code_str.append(members_code.to_str())

        save_path = os.path.join(output_path, f"{singularized_table_name}.py")
        save_as_text(save_path, code_str.to_str())
