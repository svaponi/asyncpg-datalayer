import base64
import datetime
import json
import uuid

import sqlalchemy.orm.attributes


def build_cursor(
    last_record: object,
    sort_col: sqlalchemy.orm.attributes.InstrumentedAttribute,
    order_by_cols: list[sqlalchemy.orm.attributes.InstrumentedAttribute] = None,
) -> str | None:
    cursor_parts = {sort_col.key: getattr(last_record, sort_col.key)}
    for col in order_by_cols:
        cursor_parts[col.key] = getattr(last_record, col.key)
    return _encode_cursor(cursor_parts)


def with_scrolling(
    query: sqlalchemy.sql.Select,
    size: int,
    cursor: str | None,
    sort_asc: bool,
    sort_col: sqlalchemy.orm.attributes.InstrumentedAttribute,
    order_by_cols: list[sqlalchemy.orm.attributes.InstrumentedAttribute] = None,
) -> sqlalchemy.sql.Select:

    order_by_col_1 = None
    order_by_col_2 = None
    order_by_col_3 = None
    if order_by_cols:
        if len(order_by_cols) > 3:
            raise ValueError("only up to 3 order_by_cols are supported")
        if len(order_by_cols) > 0:
            order_by_col_1 = order_by_cols[0]
        if len(order_by_cols) > 1:
            order_by_col_2 = order_by_cols[1]
        if len(order_by_cols) > 2:
            order_by_col_3 = order_by_cols[2]

    if size < 1:
        raise ValueError("size must be greater than 0")

    query = query.limit(size)

    if sort_asc:
        query = query.order_by(sort_col.asc())
    else:
        query = query.order_by(sort_col.desc())

    if order_by_col_1:
        query = query.order_by(order_by_col_1.asc())

    if order_by_col_2:
        query = query.order_by(order_by_col_2.asc())

    if order_by_col_3:
        query = query.order_by(order_by_col_3.asc())

    if cursor:
        cursor_parts = _decode_cursor(cursor)

        def get_value(_col: sqlalchemy.orm.attributes.InstrumentedAttribute):
            _value = cursor_parts.get(_col.key)
            if not _value:
                raise ValueError(f"cursor does not contain {_col.key}")
            return _value

        _field_value = get_value(sort_col)
        if sort_asc:
            cond = sort_col > _field_value
        else:
            cond = sort_col < _field_value

        if order_by_col_1:
            _conditions = sqlalchemy.and_(
                sort_col == _field_value,
                order_by_col_1 > get_value(order_by_col_1),
            )
            cond = sqlalchemy.or_(cond, _conditions)

        if order_by_col_2:
            _conditions = sqlalchemy.and_(
                sort_col == _field_value,
                order_by_col_1 == get_value(order_by_col_1),
                order_by_col_2 > get_value(order_by_col_2),
            )
            cond = sqlalchemy.or_(cond, _conditions)

        if order_by_col_3:
            _conditions = sqlalchemy.and_(
                sort_col == _field_value,
                order_by_col_1 == get_value(order_by_col_1),
                order_by_col_2 == get_value(order_by_col_2),
                order_by_col_3 > get_value(order_by_col_3),
            )
            cond = sqlalchemy.or_(cond, _conditions)

        query = query.where(cond)

    return query


_VALUE = "v"
_TYPE = "$t"

class _CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return {_TYPE: "uuid", _VALUE: obj.hex}
        if isinstance(obj, datetime.datetime):
            return {_TYPE: "datetime", _VALUE: obj.timestamp()}
        return super().default(obj)

def _custom_decode(obj):
    v = obj.get(_VALUE)
    t = obj.get(_TYPE)
    if t == "uuid":
        return uuid.UUID(hex=v)
    if t == "datetime":
        return datetime.datetime.fromtimestamp(v)
    return obj

def _encode_cursor(cursor_parts: dict) -> str:
    raw = json.dumps(cursor_parts, cls=_CustomEncoder).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("utf-8")


def _decode_cursor(cursor: str) -> dict:
    padding = "=" * (-len(cursor) % 4)
    raw = base64.urlsafe_b64decode(cursor + padding)
    return json.loads(raw, object_hook=_custom_decode)
