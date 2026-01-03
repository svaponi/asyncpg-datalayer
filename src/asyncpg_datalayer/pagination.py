import sqlalchemy.orm.attributes


def with_pagination(
    query: sqlalchemy.sql.Select,
    size: int,
    page: int,
    sort_asc: bool,
    sort_col: sqlalchemy.orm.attributes.InstrumentedAttribute,
    order_by_cols: list[sqlalchemy.orm.attributes.InstrumentedAttribute] | None = None,
) -> sqlalchemy.sql.Select:
    if size < 1:
        raise ValueError("size must be greater than 0")
    if page < 1:
        raise ValueError("page must be greater than 0")

    order_by = sort_col.asc() if sort_asc else sort_col.desc()
    order_bys = [order_by]
    if order_by_cols:
        order_bys.extend([o.asc() for o in order_by_cols])

    offset = (page - 1) * size
    query = query.limit(size).offset(offset)
    assert order_bys, "At least one order_by is required"

    for order_by in order_bys:
        query = query.order_by(order_by)
    return query
