import abc
import model


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError


class SqlRepository(AbstractRepository):
    def __init__(self, session):
        self.session = session

    def add(self, batch):
        eta = 'NULL' if batch.eta is None else batch.eta
        self.session.execute('INSERT INTO batches (reference, sku, _purchased_quantity, eta)'
                             f' VALUES ("{batch.reference}", "{batch.sku}", {batch._purchased_quantity}, {eta})')

        [batch_id] = next(self.session.execute(
            f'SELECT id FROM batches WHERE reference="{batch.reference}"',
        ))

        for orderline in batch._allocations:
            self.session.execute('INSERT INTO order_lines (sku, qty, orderid) '
                                 f'VALUES ("{orderline.sku}", {orderline.qty}, "{orderline.orderid}")')

            [orderline_id] = next(self.session.execute(
                f'SELECT id FROM order_lines WHERE orderid="{orderline.orderid}"',
            ))

            self.session.execute('INSERT INTO allocations (orderline_id, batch_id)'
                                 f'VALUES ({orderline_id}, {batch_id})')

    def get(self, reference) -> model.Batch:
        id_, reference, sku, _purchased_quantity, eta = next(self.session.execute(f'SELECT * FROM batches WHERE reference="{reference}"' ))
        batch =model.Batch(reference, sku, _purchased_quantity, eta) 


        res = self.session.execute(f'SELECT orderline_id FROM allocations WHERE batch_id={id_}' )
        for [orderline_id] in res:
            id2, sku2, qty, orderid  = next(self.session.execute(f'SELECT * FROM order_lines WHERE id={orderline_id}' ))
            orderline = model.OrderLine(orderid, sku, qty)
            batch._allocations.add(orderline)
            

        return batch
