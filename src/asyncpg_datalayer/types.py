import typing

import pydantic
import sqlalchemy.orm

Col = sqlalchemy.orm.attributes.InstrumentedAttribute
GetCol = typing.Callable[[str], Col]
Obj = typing.Union[dict, pydantic.BaseModel]
Filters = typing.Union[dict, list[tuple], pydantic.BaseModel]
