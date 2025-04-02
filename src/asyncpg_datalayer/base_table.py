from sqlalchemy.orm import DeclarativeBase


# Add sqlalchemy.ext.asyncio.AsyncAttrs mixin to DeclarativeBase if necessary
# See https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#sqlalchemy.ext.asyncio.AsyncAttrs
class Base(DeclarativeBase):
    pass
