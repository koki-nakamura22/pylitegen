import os
import sys
import pytest
from pytest import main
from typing import Final

import tests.import_path_resolver
from pyqlite.generator.column import Column
from tests.create_test_db import DBForTestCreator

currnet_dir: Final[str] = os.path.dirname(__file__)
db_filepath: Final[str] = os.path.join(currnet_dir, 'test.db')


class TestColumn:
    @classmethod
    def setup_class(cls):
        if os.path.exists(db_filepath):
            os.remove(db_filepath)
        db_creator = DBForTestCreator(currnet_dir)
        db_creator.create()

    @classmethod
    def teardown_class(cls):
        if os.path.exists(db_filepath):
            os.remove(db_filepath)

    @pytest.mark.column
    @pytest.mark.skip(reason="This case is not needed to run because done in other test cases")
    def test_normal_column(self):
        pass

    @pytest.mark.column
    def test_is_null(self):
        c = Column(1, 'TestColumn', 'TEXT', False, None, 1)
        assert c.is_not_null() == False

    @pytest.mark.column
    def test_is_not_null(self):
        c = Column(1, 'TestColumn', 'TEXT', True, None, 0)
        assert c.is_not_null()

    @pytest.mark.column
    def test_is_pk(self):
        c = Column(1, 'TestColumn', 'TEXT', True, None, 1)
        assert c.is_pk()

    @pytest.mark.column
    def test_is_not_pk(self):
        c = Column(1, 'TestColumn', 'TEXT', True, None, 0)
        assert c.is_pk() == False

    @pytest.mark.column
    def test_convert_data_type_text_to_str(self):
        c = Column(1, 'TestColumn', 'TEXT', True, None, 1)
        assert c.data_type_to_py_type_str() == 'str'

    @pytest.mark.column
    def test_convert_data_type_numeric_to_any(self):
        c = Column(1, 'TestColumn', 'NUMERIC', True, None, 1)
        assert c.data_type_to_py_type_str() == 'Any'

    @pytest.mark.column
    def test_convert_data_type_integer_to_int(self):
        c = Column(1, 'TestColumn', 'INTEGER', True, None, 1)
        assert c.data_type_to_py_type_str() == 'int'

    @pytest.mark.column
    def test_convert_data_type_real_to_float(self):
        c = Column(1, 'TestColumn', 'REAL', True, None, 1)
        assert c.data_type_to_py_type_str() == 'float'

    @pytest.mark.column
    def test_convert_data_type_null_to_any(self):
        c = Column(1, 'TestColumn', 'NULL', True, None, 1)
        assert c.data_type_to_py_type_str() == 'Any'


if __name__ == '__main__':
    sys.exit(main())
