import asyncio
import datetime
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import (
    text,
    Uuid,
    String,
    DateTime,
    ForeignKeyConstraint, Boolean,
)
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, relationship

from asyncpg_datalayer.base_repository import BaseRepository
from asyncpg_datalayer.criteria import gte, lte, is_null, is_not_null, or_
from asyncpg_datalayer.db import DB
from asyncpg_datalayer.errors import ConstraintViolationException


class _Base(AsyncAttrs, DeclarativeBase):
    pass


class Org(_Base):
    __tablename__ = "org"
    org_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, server_default=text("uuid_generate_v4()")
    )
    code: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False)
    last_updated_at: Mapped[datetime.datetime | None] = mapped_column(DateTime)


class User(_Base):
    __tablename__ = "user_account"
    __table_args__ = (
        ForeignKeyConstraint(
            ["org_id"], ["org.org_id"], name="user_account__org_id_fkey"
        ),
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, server_default=text("uuid_generate_v4()")
    )
    # Note that null is allowed, so the FK is not enforced
    org_id: Mapped[uuid.UUID | None] = mapped_column(Uuid)
    org: Mapped[Org] = relationship("Org")
    email: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    full_name: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False)
    last_updated_at: Mapped[datetime.datetime | None] = mapped_column(DateTime)


class Tax(_Base):
    __tablename__ = "tax"
    tax_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False)
    last_modified_at: Mapped[datetime.datetime | None] = mapped_column(DateTime)


class Location(_Base):
    __tablename__ = "location"
    location_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False)
    last_modified_at: Mapped[datetime.datetime | None] = mapped_column(DateTime)


class LocationTax(_Base):
    __tablename__ = "location_tax"
    __table_args__ = (
        ForeignKeyConstraint(
            ["location_id"],
            ["location.location_id"],
            name="location_tax__location_id_fkey",
        ),
        ForeignKeyConstraint(
            ["tax_id"], ["tax.tax_id"], name="location_tax__tax_id_fkey"
        ),
    )
    location_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    tax_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    is_active: Mapped[bool | None] = mapped_column(Boolean, nullable=True)


class OrgRepository(BaseRepository[Org]):
    def __init__(self, db: DB) -> None:
        super().__init__(db, Org)


class UserRepository(BaseRepository[User]):
    def __init__(self, db: DB) -> None:
        super().__init__(db, User)


class TaxRepository(BaseRepository[Tax]):
    def __init__(self, db: DB) -> None:
        super().__init__(db, Tax)


class LocationRepository(BaseRepository[Location]):
    def __init__(self, db: DB) -> None:
        super().__init__(db, Location)


class LocationTaxRepository(BaseRepository[LocationTax]):
    def __init__(self, db: DB) -> None:
        super().__init__(db, LocationTax)


@pytest_asyncio.fixture()
async def initialized_db(db):
    await db.exec_ddl('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
    async with db.connection() as conn:
        await conn.run_sync(_Base.metadata.create_all)
    yield db


@pytest.fixture()
def user_repository(initialized_db):
    yield UserRepository(initialized_db)


@pytest.fixture()
def org_repository(initialized_db):
    yield OrgRepository(initialized_db)


@pytest.fixture
def location_repository(initialized_db):
    yield LocationRepository(initialized_db)


@pytest.fixture
def tax_repository(initialized_db):
    yield TaxRepository(initialized_db)


@pytest.fixture
def location_tax_repository(initialized_db):
    yield LocationTaxRepository(initialized_db)


@pytest.mark.asyncio
async def test_crud(user_repository):
    with pytest.raises(Exception) as e:
        await user_repository.insert(dict(email="foo@example.com", name="John Doe"))

    with pytest.raises(Exception) as e:
        await user_repository.insert(dict(email="foo@example.com", full_name=123))

    id1 = await user_repository.insert(dict(email="foo@example.com"))
    id2 = await user_repository.insert(
        dict(email="jdoe@example.com", full_name="John Doe")
    )

    with pytest.raises(Exception):
        await user_repository.insert(dict(email="foo@example.com"))

    all_ = await user_repository.get_all()
    assert all_
    assert len(all_) == 2

    filtered_ = await user_repository.get_all(dict(email="jdoe@example.com"))
    assert len(filtered_) == 1

    item1 = await user_repository.get_or_none_by_id(id1)
    assert item1.email == "foo@example.com"
    assert item1.full_name is None

    item2 = await user_repository.get_or_none_by_id(id2)
    assert item2.email == "jdoe@example.com"
    assert item2.full_name == "John Doe"

    await user_repository.update_by_id(id1, dict(email="bar@example.com"))
    item1 = await user_repository.get_or_none_by_id(id1)
    assert item1.email == "bar@example.com"
    assert item1.full_name is None
    assert item1.created_at < item1.last_updated_at


@pytest.mark.asyncio
async def test_concurrency(user_repository):
    tasks = [
        user_repository.insert(dict(email=f"foo{i}@example.com")) for i in range(100)
    ]
    results = await asyncio.gather(*tasks)
    assert all(foo for foo in results)


@pytest.mark.asyncio
async def test_awaitable_attrs(user_repository, org_repository):
    org_id = await org_repository.insert(dict(code="foo", name="Foo"))
    org = await org_repository.get_or_none_by_id(org_id)

    user_id = await user_repository.insert(
        dict(email="foo@example.com", full_name="Foo Bar", org_id=org_id)
    )
    foo = await user_repository.get_or_none_by_id(user_id)
    assert foo.org_id == org_id

    async with user_repository.db.get_session() as session:
        foo = await user_repository.get_or_none_by_id(user_id, reuse_session=session)
        assert foo.org_id == org_id
        awaited_org: Org = await foo.awaitable_attrs.org
        assert awaited_org.org_id == org.org_id
        assert awaited_org.name == org.name
        assert awaited_org.code == org.code


@pytest.mark.asyncio
async def test_pagination(user_repository):
    tasks = [
        user_repository.insert(dict(email=f"foo{i}@example.com")) for i in range(100)
    ]
    inserted_ids = await asyncio.gather(*tasks)

    user_ids = []
    page = 1
    while True:
        results, _ = await user_repository.get_page(
            page=page,
            size=17,
        )
        if not results:
            break
        user_ids += [user.user_id for user in results]
        page += 1

    assert set(inserted_ids) == set(user_ids)


@pytest.mark.asyncio
async def test_pagination_order(user_repository):
    inserted_ids = [
        await user_repository.insert(dict(email=f"foo{i}@example.com"))
        for i in range(10)
    ]

    page_1, count_1 = await user_repository.get_page(
        page=1,
        size=8,
        sort_by="created_at",
    )
    assert len(page_1) == 8
    assert count_1 == 10
    assert page_1[0].user_id == inserted_ids[0]
    assert page_1[7].user_id == inserted_ids[7]
    page_2, count_2 = await user_repository.get_page(
        page=2,
        size=8,
        sort_by="created_at",
        skip_count=True,
    )
    assert len(page_2) == 2
    assert count_2 == -1
    assert page_2[0].user_id == inserted_ids[8]
    assert page_2[1].user_id == inserted_ids[9]

    page_1, count_1 = await user_repository.get_page(
        page=1,
        size=8,
        sort_by="created_at:desc",
    )
    assert len(page_1) == 8
    assert count_1 == 10
    assert page_1[0].user_id == inserted_ids[9]
    assert page_1[7].user_id == inserted_ids[2]
    page_2, count_2 = await user_repository.get_page(
        page=2,
        size=8,
        sort_by="created_at:desc",
        skip_count=True,
    )
    assert len(page_2) == 2
    assert count_2 == -1
    assert page_2[0].user_id == inserted_ids[1]
    assert page_2[1].user_id == inserted_ids[0]


@pytest.mark.asyncio
async def test_transaction_rollback_on_error(user_repository):
    with pytest.raises(RuntimeError):
        async with user_repository.db.get_session() as session:
            await user_repository.insert(
                dict(email=f"foo@example.com"), reuse_session=session
            )
            await user_repository.insert(
                dict(email=f"bar@example.com"), reuse_session=session
            )
            results = await user_repository.get_all(reuse_session=session)
            assert len(results) == 2
            assert set(r.email for r in results) == {
                "foo@example.com",
                "bar@example.com",
            }
            raise RuntimeError("Kaboom!")

    results = await user_repository.get_all()
    assert len(results) == 0


@pytest.mark.asyncio
async def test_transaction_rollback_on_integrity_violation(user_repository):
    with pytest.raises(ConstraintViolationException):
        async with user_repository.db.get_session() as session:
            await user_repository.insert(
                dict(email=f"foo@example.com"), reuse_session=session
            )
            await user_repository.insert(
                dict(email=f"bar@example.com"), reuse_session=session
            )
            results = await user_repository.get_all(reuse_session=session)
            assert len(results) == 2
            assert set(r.email for r in results) == {
                "foo@example.com",
                "bar@example.com",
            }
            await user_repository.insert(
                dict(email=f"foo@example.com"), reuse_session=session
            )

    results = await user_repository.get_all()
    assert len(results) == 0


@pytest.mark.asyncio
async def test_insert_many_update_many_delete_by_ids(user_repository):
    alice_id, bob_id, sam_id = await user_repository.insert_many(
        [
            dict(email=f"alice@example.com"),
            dict(email=f"bob@example.com"),
            dict(email=f"sam@example.com"),
        ]
    )

    results = await user_repository.get_all()
    assert len(results) == 3
    result = await user_repository.get_or_none_by_id(alice_id)
    assert result.email == "alice@example.com"
    result = await user_repository.get_or_none_by_id(bob_id)
    assert result.email == "bob@example.com"
    result = await user_repository.get_or_none_by_id(sam_id)
    assert result.email == "sam@example.com"

    none_id = uuid.uuid4()
    result = await user_repository.get_or_none_by_id(none_id)
    assert result is None

    none_id = uuid.uuid4()
    rowcount = await user_repository.update_many(
        update_obj=dict(full_name="Bob Alice"),
        filters=dict(user_id={alice_id, bob_id, none_id}),
    )
    assert rowcount == 2

    result = await user_repository.get_or_none_by_id(alice_id)
    assert result.full_name == "Bob Alice"
    result = await user_repository.get_or_none_by_id(bob_id)
    assert result.full_name == "Bob Alice"
    result = await user_repository.get_or_none_by_id(sam_id)
    assert result.full_name is None

    rowcount = await user_repository.delete_by_ids({sam_id, bob_id, none_id})
    assert rowcount == 2

    results = await user_repository.get_all()
    assert len(results) == 1
    result = await user_repository.get_or_none_by_id(alice_id)
    assert result.email == "alice@example.com"
    result = await user_repository.get_or_none_by_id(bob_id)
    assert result is None
    result = await user_repository.get_or_none_by_id(sam_id)
    assert result is None


@pytest.mark.asyncio
async def test_criteria_gte_and_lte(user_repository):
    await user_repository.insert(dict(email=f"alice@example.com", full_name="Alice"))
    mark0 = datetime.datetime.now()
    await user_repository.insert(dict(email=f"bob@example.com", full_name="Bob"))
    mark1 = datetime.datetime.now()
    await user_repository.insert(dict(email=f"sam@example.com", full_name="Sam"))

    results = await user_repository.get_all(filters=dict(created_at=gte(mark0)))
    assert len(results) == 2
    assert {r.email for r in results} == {"bob@example.com", "sam@example.com"}

    results = await user_repository.get_all(filters=dict(created_at=lte(mark1)))
    assert len(results) == 2
    assert {r.email for r in results} == {"alice@example.com", "bob@example.com"}


@pytest.mark.asyncio
async def test_criteria_is_null_and_is_not_null(user_repository):
    await user_repository.insert_many(
        [
            dict(email=f"alice@example.com", full_name=None),
            dict(email=f"bob@example.com", full_name="Bob"),
            dict(email=f"sam@example.com", full_name=None),
        ]
    )

    results = await user_repository.get_all(filters=dict(full_name=is_null()))
    assert {r.email for r in results} == {"alice@example.com", "sam@example.com"}

    results = await user_repository.get_all(filters=dict(full_name=is_not_null()))
    assert {r.email for r in results} == {"bob@example.com"}

    # ATTENTION: if you pass None to the filter, it will be ignored, that's on purpose!
    results = await user_repository.get_all(filters=dict(full_name=None))
    assert {r.email for r in results} == {
        "alice@example.com",
        "bob@example.com",
        "sam@example.com",
    }


@pytest.mark.asyncio
async def test_criteria_and_or(user_repository):
    await user_repository.insert_many(
        [
            dict(email=f"alice@example.com", full_name="Alice"),
            dict(email=f"bob@example.com", full_name="Bob"),
            dict(email=f"sam@example.com", full_name=None),
        ]
    )

    results = await user_repository.get_all(
        filters=[("full_name", or_("Alice", is_null()))]
    )
    assert {r.email for r in results} == {"alice@example.com", "sam@example.com"}

    results = await user_repository.get_all(
        filters=dict(full_name=or_("Alice", is_null()))
    )
    assert {r.email for r in results} == {"alice@example.com", "sam@example.com"}


@pytest.mark.asyncio
async def test_insert_multikey(location_repository, tax_repository, location_tax_repository):
    ca_location_id = await location_repository.insert(
        dict(location_id=uuid.uuid4(), name="canada")
    )
    us_location_id = await location_repository.insert(
        dict(location_id=uuid.uuid4(), name="usa")
    )

    church_tax_id = await tax_repository.insert(
        dict(tax_id=uuid.uuid4(), name="church tax")
    )
    carbon_tax_id = await tax_repository.insert(
        dict(tax_id=uuid.uuid4(), name="carbon tax")
    )

    ca_church = await location_tax_repository.insert_multikey(
        dict(location_id=ca_location_id, tax_id=church_tax_id)
    )
    assert ca_church == (ca_location_id, church_tax_id)

    ca_carbon = await location_tax_repository.insert_multikey(
        dict(location_id=ca_location_id, tax_id=carbon_tax_id)
    )
    assert ca_carbon == (ca_location_id, carbon_tax_id)

    us_carbon = await location_tax_repository.insert_multikey(
        dict(location_id=us_location_id, tax_id=carbon_tax_id)
    )
    assert us_carbon == (us_location_id, carbon_tax_id)

    location_taxes = await location_tax_repository.get_all()
    assert location_taxes
    assert len(location_taxes) == 3

    location_taxes = await location_tax_repository.get_all(
        dict(location_id=ca_location_id)
    )
    assert location_taxes
    assert len(location_taxes) == 2

    location_taxes = await location_tax_repository.get_all(dict(tax_id=carbon_tax_id))
    assert location_taxes
    assert len(location_taxes) == 2


@pytest.mark.asyncio
async def test_insert_many_multikey(location_repository, tax_repository, location_tax_repository):
    ca_location_id = await location_repository.insert(
        dict(location_id=uuid.uuid4(), name="canada")
    )
    us_location_id = await location_repository.insert(
        dict(location_id=uuid.uuid4(), name="usa")
    )

    church_tax_id = await tax_repository.insert(
        dict(tax_id=uuid.uuid4(), name="church tax")
    )
    carbon_tax_id = await tax_repository.insert(
        dict(tax_id=uuid.uuid4(), name="carbon tax")
    )

    ca_church, ca_carbon, us_carbon = (
        await location_tax_repository.insert_many_multikey(
            [
                dict(location_id=ca_location_id, tax_id=church_tax_id),
                dict(location_id=ca_location_id, tax_id=carbon_tax_id),
                dict(location_id=us_location_id, tax_id=carbon_tax_id),
            ]
        )
    )
    assert ca_church == (ca_location_id, church_tax_id)
    assert ca_carbon == (ca_location_id, carbon_tax_id)
    assert us_carbon == (us_location_id, carbon_tax_id)

    location_taxes = await location_tax_repository.get_all()
    assert location_taxes
    assert len(location_taxes) == 3

    location_taxes = await location_tax_repository.get_all(
        dict(location_id=ca_location_id)
    )
    assert location_taxes
    assert len(location_taxes) == 2

    location_taxes = await location_tax_repository.get_all(dict(tax_id=carbon_tax_id))
    assert location_taxes
    assert len(location_taxes) == 2
