import abc
from allocation.domain import model


class AbstractRepository(abc.ABC):
    def add(self, product: model.Product):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, sku) -> model.Product:
        raise NotImplementedError


class SqlAlchemyRepository(AbstractRepository):
    def __init__(self, session):
        self.session = session

    #def add(self, batch):
    #    self.session.add(batch)
    def add(self, product: model.Product):
        batches = product.batches
        for b in batches:
            res = self.session.query(model.Batch).filter_by(ref=ref).all()
            if not res:
                self.session.add(b)
             

    def get(self, sku):
        batches = self.session.query(model.Batch).filter_by(sku=sku).all()
        return model.Product(sku, batches)

#    def list(self):
#        return self.session.query(model.Batch).all()
