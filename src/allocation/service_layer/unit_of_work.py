# pylint: disable=attribute-defined-outside-init
from __future__ import annotations
from typing import Protocol
import abc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session


from allocation import config
from allocation.adapters import repository
from . import messagebus


class AbstractUnitOfWork(Protocol):
    products: repository.AbstractRepository

    def __enter__(self) -> AbstractUnitOfWork:
        return self

    def __exit__(self, *args):
        self.rollback()


    @abc.abstractmethod
    def commit(self):
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(self):
        raise NotImplementedError


DEFAULT_SESSION_FACTORY = sessionmaker(
    bind=create_engine(
        config.get_postgres_uri(),
        isolation_level="REPEATABLE READ",
    )
)

class UoWEventHandler:
    def __init__(self, uow: AbstractUnitOfWork):
        self._uow = uow

    def commit(self):
        self.publish_events()
        self._uow.commit()

    def publish_events(self):
        for product in self._uow.products.seen:
            while product.events:
                event = product.events.pop(0)
                messagebus.handle(event)
    def rollback(self):
        self._uow.rollback()
    def __enter__(self):
        return self._uow.__enter__()
    def __exit__(self,*args, **kwargs):
        self._uow.__exit__(*args, **kwargs)
    @property
    def products(self):
        return self._uow.products
        

class SqlAlchemyUnitOfWork(AbstractUnitOfWork):
    def __init__(self, session_factory=DEFAULT_SESSION_FACTORY):
        self.session_factory = session_factory

    def __enter__(self):
        self.session = self.session_factory()  # type: Session
        self.products = repository.TrackingRepository(
            repository.SqlAlchemyRepository(self.session)
        )
        return super().__enter__()

    def __exit__(self, *args):
        super().__exit__(*args)
        self.session.close()

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()
