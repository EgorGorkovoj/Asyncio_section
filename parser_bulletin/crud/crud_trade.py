import datetime

from model import SpamixTradingResults
from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from .crud_base import CRUDBase


class TradeCRUD(CRUDBase):
    """
    Класс для работы с моделью 'SpamixTradingResults' через CRUD операции.
    Наследуется от CRUDBase, расширяя функциональность проверкой существующих записей.

    Методы:
        exists_exchange_product_id(session, exchange_product_id, date)
            Проверяет, существует ли запись с указанным 'exchange_product_id' и 'date'.
    """

    async def get_existing_keys(
        self, session: AsyncSession, keys: set[tuple[str, datetime.date]]
    ) -> set[tuple[str, datetime.date]]:
        stmt = select(SpamixTradingResults.exchange_product_id, SpamixTradingResults.date).where(
            tuple_(SpamixTradingResults.exchange_product_id, SpamixTradingResults.date).in_(keys)
        )
        result = await session.execute(stmt)
        return set(result.all())  # type: ignore


crud_trade = TradeCRUD(SpamixTradingResults)
