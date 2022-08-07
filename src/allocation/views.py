from allocation.service_layer import unit_of_work


def allocations(orderid: str, uow: unit_of_work.SqlAlchemyUnitOfWork):
    with uow:
        results = uow.session.execute(
            """
            SELECT sku, batchref FROM allocations_view WHERE orderid = :orderid
            """,
            dict(orderid=orderid),
        )
    return [dict(r) for r in results]

def allocation_by_line(orderid: str, sku:str, uow: unit_of_work.SqlAlchemyUnitOfWork):
    with uow:
        results = uow.session.execute(
            """
            SELECT batchref FROM allocations_view WHERE orderid = :orderid AND sku = :sku
            """,
            dict(orderid=orderid, sku=sku),
        )
    return next(results)[0]
