import typing

import pydantic
import sqlalchemy.orm
import sqlalchemy.orm.attributes

Record = typing.TypeVar("Record", bound=sqlalchemy.orm.DeclarativeBase)
Model = typing.TypeVar("Model", bound=pydantic.BaseModel)


def to_dict(
    model_obj: Record,
) -> dict[str, typing.Any]:
    column_names = [c.name for c in model_obj.__table__.columns.values()]
    return {c: getattr(model_obj, c) for c in column_names}
