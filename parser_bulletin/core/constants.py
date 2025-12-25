class LengthConstants:
    """
    Константы, задающие ограничения по длине
    для различных текстовых полей.

    Атрибуты:
        EXCHANGE_PRODUCT_ID_LENGTH (int): Максимальная длина
                                          идентификатора.
        EXCHANGE_PRODUCT_NAME_LENGTH (int): Максимальная длина
                                            полного названия инструмента.
        OIL_ID (int): Длина кода сырья.
        DELIVERY_BASIS_ID_LENGTH (int): Длина идентификатора базиса поставки.
        DELIVERY_BASIS_NAME_LENGTH (int): Максимальная длина названия
                                          базиса поставки.
        DELIVERY_TYPE_ID_LENGTH (int): Длина идентификатора типа поставки.
    """

    EXCHANGE_PRODUCT_ID_LENGTH: int = 24
    EXCHANGE_PRODUCT_NAME_LENGTH: int = 240
    OIL_ID_LENGTH: int = 4
    DELIVERY_BASIS_ID_LENGTH: int = 3
    DELIVERY_BASIS_NAME_LENGTH: int = 120
    DELIVERY_TYPE_ID_LENGTH: int = 1


class PriceConstants:
    """
    Базовый класс констант для цен.

    Атрибуты:
    - PRICE_NUMBER_OF_DIGITS (int): целая часть цены.
    - PRICE_FRACTIONAL_PART (int): сколько знаков после запятой у цены.
    """

    PRICE_NUMBER_OF_DIGITS: int = 14
    PRICE_FRACTIONAL_PART: int = 2
