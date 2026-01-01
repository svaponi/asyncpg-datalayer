import sqlalchemy.orm.attributes

from asyncpg_datalayer.types import GetCol


def with_pagination_and_sorting(
    query: sqlalchemy.sql.Select,
    page: int | None,
    size: int | None,
    sort_by: str | None,
    get_col_func: GetCol,
    default_order_bys: list[sqlalchemy.orm.attributes.InstrumentedAttribute],
) -> sqlalchemy.sql.Select:
    if sort_by:
        sort_field, sort_asc = parse_sort_by(sort_by)
        try:
            order_by = get_col_func(sort_field)
        except Exception:
            raise ValueError(f"invalid sort field: {sort_field}")
        if not sort_asc:
            order_by = order_by.desc()
        default_order_bys = [order_by, *default_order_bys]

    if page is not None or size is not None:
        size = size or 10
        if size < 1:
            raise ValueError("size must be greater than 0")
        page = page or 1
        if page < 1:
            raise ValueError("page must be greater than 0")
        offset = (page - 1) * size
        query = query.limit(size).offset(offset)
        # with pagination, order_by is required
        assert default_order_bys, "At least one order_by is required"

    for order_by in default_order_bys:
        query = query.order_by(order_by)
    return query


def parse_sort_by(sort_by: str) -> tuple[str, bool]:
    if ":" not in sort_by:
        return sort_by, True
    sort_field, sort_order = sort_by.split(":", maxsplit=1)
    if sort_order not in ["asc", "desc", None]:
        raise ValueError(f"invalid sort order: {sort_order}")
    order_asc = sort_order != "desc"
    return sort_field, order_asc
