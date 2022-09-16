from abc import ABC
import copy
from dataclasses import dataclass, field
from typing import ClassVar, Dict, Final, List, Type


@dataclass()
class BaseModel(ABC):
    """Base for model classes used by the user
    """

    def __post_init__(self):
        """Init data class funciton
        """
        self.__set_cache()

    #############
    # Cache for update
    #############
    __cache: dict = field(
        default_factory=lambda: dict(),
        init=False,
        compare=False)

    def __set_cache(self) -> None:
        """Set cache for update
        """
        members = self.members
        for k in members:
            self._BaseModel__cache[k] = members[k]  # type: ignore

    def __get_data_to_be_updated(self) -> dict:
        """Get data to be updated for update.

        Returns
        -------
        dict
            Data to be updated
        """
        data_to_be_updated = dict()
        members = self.members
        for k in members:
            if members[k] != self._BaseModel__cache[k]:  # type: ignore
                data_to_be_updated[k] = members[k]
        return data_to_be_updated
    #############

    #############
    # Class type
    #############
    @classmethod
    def get_class_type(cls) -> Type:
        """Get model class type.

        Returns
        -------
        Type
            Model class type
        """
        return cls

    @property
    def class_type(self) -> Type:
        """Model class type
        """
        return self.__class__
    #############

    #############
    # Table name
    #############
    __table_name: ClassVar[str]

    @classmethod
    def get_table_name(cls) -> str:
        """Get table name.

        Returns
        -------
        str
            Table name
        """
        return getattr(cls, f"_{cls.__name__}__table_name")

    @property
    def table_name(self) -> str:
        """Table name
        """
        return self.__class__.get_table_name()
    #############

    #############
    # Primary Keys
    #############
    @classmethod
    def get_pks(cls) -> List[str]:
        """Get primary key names.

        Returns
        -------
        List[str]
            Primary key names
        """
        if not hasattr(cls, '__pks'):
            pks = list()
            for k, v in cls.__annotations__.items():
                if hasattr(v, '__origin__') and v.__origin__ is Final:
                    pks.append(k)
            setattr(cls, '__pks', pks)
            return pks
        else:
            return getattr(cls, '__pks')

    @property
    def pks(self) -> List[str]:
        """Primary key names
        """
        return self.__class__.get_pks()
    #############

    #############
    # Members
    #############
    @property
    def members(self) -> Dict:
        """Model members dict("name": value)
        """
        excludes = ['_BaseModel__cache']
        members = copy.deepcopy(vars(self))
        for keyword in excludes:
            del members[keyword]
        return members

    @classmethod
    def get_member_names(cls) -> List[str]:
        """Get model members names.

        Returns
        -------
        List[str]
            Model members names
        """
        member_names = list()
        for k, v in cls.__annotations__.items():
            if not hasattr(v, '__origin__'):
                member_names.append(k)
            elif v.__origin__ is not ClassVar:
                member_names.append(k)
        return member_names

    @property
    def member_names(self) -> List[str]:
        """Model members names
        """
        return list(self.members.keys())

    @property
    def values(self) -> List:
        """Model members values
        """
        return list(self.members.values())

    def to_dict(self) -> dict:
        """Convert model data to dict.

        Returns
        -------
        dict
            Converted model data as dict
        """
        return self.members
    #############
