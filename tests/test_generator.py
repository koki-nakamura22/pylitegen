import difflib
import os
import shutil
import sys
import pytest
from pytest import main
from typing import Final

import tests.import_path_resolver
from pyqlite.generator import Generator
from tests.create_test_db import DBForTestCreator

currnet_dir: Final[str] = os.path.dirname(__file__)
db_filepath: Final[str] = os.path.join(currnet_dir, 'test.db')
output_path: Final[str] = os.path.join(currnet_dir, 'models')
expected_files_path: Final[str] = os.path.join(currnet_dir, 'expected_files')


class TestColumn:
    @classmethod
    def setup_class(cls):
        if os.path.exists(db_filepath):
            os.remove(db_filepath)
        db_creator = DBForTestCreator(currnet_dir)
        db_creator.create()

        Generator.generate(db_filepath, output_path)

        if not os.path.exists(expected_files_path):
            os.makedirs(expected_files_path)

    @classmethod
    def teardown_class(cls):
        if os.path.exists(db_filepath):
            os.remove(db_filepath)

        if os.path.exists(output_path):
            shutil.rmtree(output_path)

        if os.path.exists(expected_files_path):
            shutil.rmtree(expected_files_path)

    @pytest.mark.generate
    def test_generate_model_files(self):
        model_file_name = 'all_optional_column.py'
        with open(os.path.join(output_path, model_file_name), 'r', encoding='UTF-8') as f:
            content = f.read()

            expected_file_content = '''from dataclasses import dataclass
from typing import ClassVar, Optional

from pyqlite.model import BaseModel


@dataclass(init=True, eq=True)
class AllOptionalColumn(BaseModel):
    col1: Optional[str] = None
    col2: Optional[int] = None
    col3: Optional[float] = None
    __table_name: ClassVar[str] = 'all_optional_columns'
'''

            assert content == expected_file_content

    @pytest.mark.generate
    def test_generate_model_files_with_pk_column(self):
        model_file_name = 'backup_user.py'
        with open(os.path.join(output_path, model_file_name), 'r', encoding='UTF-8') as f:
            content = f.read()

            expected_file_content = '''from dataclasses import dataclass
from typing import ClassVar, Final, Optional

from pyqlite.model import BaseModel


@dataclass(init=True, eq=True)
class BackupUser(BaseModel):
    id: Final[int]
    name: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    __table_name: ClassVar[str] = 'backup_users'
'''
            assert content == expected_file_content

    @pytest.mark.generate
    def test_generate_model_files_with_not_null_column(self):
        model_file_name = 'user_edited_history.py'
        with open(os.path.join(output_path, model_file_name), 'r', encoding='UTF-8') as f:
            content = f.read()

            expected_file_content = '''from dataclasses import dataclass
from typing import ClassVar, Optional

from pyqlite.model import BaseModel


@dataclass(init=True, eq=True)
class UserEditedHistory(BaseModel):
    datetime: str
    note: Optional[str] = None
    __table_name: ClassVar[str] = 'user_edited_histories'
'''
            assert content == expected_file_content

    @pytest.mark.generate
    def test_generate_model_files_with_pk_and_not_null_column(self):
        model_file_name = 'user.py'
        with open(os.path.join(output_path, model_file_name), 'r', encoding='UTF-8') as f:
            content = f.read()

            expected_file_content = '''from dataclasses import dataclass
from typing import ClassVar, Final, Optional

from pyqlite.model import BaseModel


@dataclass(init=True, eq=True)
class User(BaseModel):
    id: Final[int]
    name: str
    phone: str
    address: Optional[str] = None
    __table_name: ClassVar[str] = 'users'
'''
            assert content == expected_file_content


if __name__ == '__main__':
    sys.exit(main())
