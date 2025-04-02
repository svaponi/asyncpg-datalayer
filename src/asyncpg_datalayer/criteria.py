import dataclasses
import enum
import typing

import sqlalchemy.orm.attributes

from asyncpg_datalayer.types import Col, GetCol


class _Operator(enum.StrEnum):
    AND = "AND"
    OR = "OR"
    IS_NULL = "IS_NULL"
    IS_NOT_NULL = "IS_NOT_NULL"
    GTE = "GTE"
    GT = "GT"
    LTE = "LTE"
    LT = "LT"
    LIKE = "LIKE"
    ILIKE = "ILIKE"


@dataclasses.dataclass
class _Criterion:
    op: _Operator
    value: typing.Any


class Criteria:
    def __init__(self, get_col: GetCol):
        self.get_col = get_col

    def _build_where_from_criterion(
        self,
        col: Col,
        criterion: _Criterion,
    ) -> sqlalchemy.BinaryExpression[bool] | None:
        # criteria without value
        if criterion.op == _Operator.IS_NULL:
            return col.is_(None)
        if criterion.op == _Operator.IS_NOT_NULL:
            return col.is_not(None)

        # from here on, we need a value
        value = criterion.value
        if value is None:
            return None
        if criterion.op == _Operator.GTE:
            return col >= value
        if criterion.op == _Operator.GT:
            return col > value
        if criterion.op == _Operator.LTE:
            return col <= value
        if criterion.op == _Operator.LT:
            return col < value
        if criterion.op == _Operator.LIKE:
            return col.like(value)
        if criterion.op == _Operator.ILIKE:
            return col.ilike(value)
        if criterion.op in (_Operator.AND, _Operator.OR):
            expressions = [
                self._build_where_from_col_value(col, value) for value in value
            ]
            expressions = [expr for expr in expressions if expr is not None]
            if not expressions:
                return None
            if criterion.op == _Operator.AND:
                return sqlalchemy.and_(*expressions)
            if criterion.op == _Operator.OR:
                return sqlalchemy.or_(*expressions)
        raise ValueError(f"Invalid operator {criterion.op}")

    def _build_where_from_col_value(
        self, col: Col, value: typing.Any
    ) -> sqlalchemy.BinaryExpression[bool] | None:
        if value is None:
            return None
        if isinstance(value, (list, set)):
            return col.in_(value)
        if isinstance(value, _Criterion):
            return self._build_where_from_criterion(col, value)
        return col == value

    def _build_where_from_key_value(
        self, key: str, value: typing.Any
    ) -> sqlalchemy.BinaryExpression[bool] | None:
        if value is None:
            return None
        col = self.get_col(key)
        if not isinstance(col, Col):
            raise ValueError(
                f"get_col('{key}') returned {type(col).__name__}, expected {Col}"
            )
        return self._build_where_from_col_value(col, value)

    def build_where_expr(
        self, criteria: list[tuple[str, typing.Any]]
    ) -> sqlalchemy.BinaryExpression[bool]:
        expressions = [
            self._build_where_from_key_value(key, value)
            for key, value in criteria
            if value is not None
        ]
        expressions = [expr for expr in expressions if expr is not None]
        if not expressions:
            return sqlalchemy.and_(sqlalchemy.true())
        return sqlalchemy.and_(*expressions)


def and_(*args) -> _Criterion:
    return _Criterion(_Operator.AND, args)


def or_(*args) -> _Criterion:
    return _Criterion(_Operator.OR, args)


def gte(value: typing.Any) -> _Criterion:
    return _Criterion(_Operator.GTE, value)


def gt(value: typing.Any) -> _Criterion:
    return _Criterion(_Operator.GT, value)


def lte(value: typing.Any) -> _Criterion:
    return _Criterion(_Operator.LTE, value)


def lt(value: typing.Any) -> _Criterion:
    return _Criterion(_Operator.LT, value)


def like(value: typing.Any) -> _Criterion:
    return _Criterion(_Operator.LIKE, value)


def ilike(value: typing.Any) -> _Criterion:
    return _Criterion(_Operator.ILIKE, value)


def icontains(value: typing.Any) -> _Criterion:
    return ilike(f"%{value}%" if value else None)


def is_null() -> _Criterion:
    return _Criterion(_Operator.IS_NULL, None)


def is_not_null() -> _Criterion:
    return _Criterion(_Operator.IS_NOT_NULL, None)
