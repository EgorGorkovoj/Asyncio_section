from datetime import datetime
from decimal import Decimal

from core.constants import LengthConstants, PriceConstants
from sqlalchemy import TIMESTAMP, Date, Numeric, SmallInteger, String, event, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class SpamixTradingResults(Base):
    """
    Модель ORM, представляющая результаты биржевых торгов.
    Содержит информацию о товаре, базисе поставки,
    объёмах торгов и дате сделки.

    Поля:
        exchange_product_id (str): Уникальный код инструмента, из которого
            извлекаются oil_id, delivery_basis_id и delivery_type_id.
        exchange_product_name (str): Полное название инструмента.
        oil_id (str): Код типа нефтепродукта
                      (первые 4 символа exchange_product_id).
        delivery_basis_id (str): Код базиса поставки
                                 (4–6 символы exchange_product_id).
        delivery_basis_name (str): Человекочитаемое имя базиса поставки.
        delivery_type_id (str): Идентификатор типа поставки
                                (последний символ exchange_product_id).
        volume (int | None): Объём договоров.
        total (Decimal | None): Общая стоимость договоров.
        count (int | None): Количество договоров.
        date (datetime.date): Дата торгов.
        created_on (datetime): Дата создания записи
                               (генерируется автоматически).
        updated_on (datetime): Дата обновления записи
                               (обновляется автоматически).

    Примечания:
        - Поля oil_id, delivery_basis_id и delivery_type_id
          заполняются автоматически в обработчике событий
          перед вставкой и обновлением записи.
        - Числовые значения total и volume могут быть NULL.
    """

    exchange_product_id: Mapped[str] = mapped_column(
        String(LengthConstants.EXCHANGE_PRODUCT_ID_LENGTH), nullable=False
    )
    exchange_product_name: Mapped[str] = mapped_column(
        String(LengthConstants.EXCHANGE_PRODUCT_NAME_LENGTH), nullable=False
    )
    oil_id: Mapped[str] = mapped_column(String(LengthConstants.OIL_ID_LENGTH), nullable=False)
    delivery_basis_id: Mapped[str] = mapped_column(
        String(LengthConstants.DELIVERY_BASIS_ID_LENGTH), nullable=False
    )
    delivery_basis_name: Mapped[str] = mapped_column(
        String(LengthConstants.DELIVERY_BASIS_NAME_LENGTH), nullable=False
    )
    delivery_type_id: Mapped[str] = mapped_column(
        String(LengthConstants.DELIVERY_TYPE_ID_LENGTH), nullable=False
    )
    volume: Mapped[int] = mapped_column(SmallInteger, nullable=True)
    total: Mapped[Decimal] = mapped_column(
        Numeric(PriceConstants.PRICE_NUMBER_OF_DIGITS, PriceConstants.PRICE_FRACTIONAL_PART),
        nullable=True,
    )
    count: Mapped[int] = mapped_column(SmallInteger, nullable=True)
    date: Mapped[datetime] = mapped_column(Date, nullable=False)
    created_on: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_on: Mapped[datetime] = mapped_column(
        type_=TIMESTAMP(timezone=True),
        server_default=func.now(),
        onupdate=datetime.now,
    )


@event.listens_for(SpamixTradingResults, 'before_insert')
@event.listens_for(SpamixTradingResults, 'before_update')
def set_oil_id(mapper, connection, target):
    if target.exchange_product_id and len(target.exchange_product_id) >= 7:
        target.oil_id = target.exchange_product_id[:4]
        target.delivery_basis_id = target.exchange_product_id[4:7]
        target.delivery_type_id = target.exchange_product_id[-1]
