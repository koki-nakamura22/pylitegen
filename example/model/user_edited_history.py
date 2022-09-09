from dataclasses import dataclass
from typing import ClassVar, List, Optional

from example.model import BaseModel


@dataclass(init=True, eq=True)
class UserEditedHistory(BaseModel):
    datetime: str
    note: Optional[str] = None
    table_name: ClassVar[str] = 'user_edited_histories'
