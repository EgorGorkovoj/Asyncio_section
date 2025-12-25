from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, declared_attr, mapped_column


class PreBase(DeclarativeBase):
    """Базовая модель проекта"""

    pass


class Base(AsyncAttrs, PreBase):
    """
    Базовая модель проекта. Абстрактная модель.

    Задает наследникам имя таблицы в БД строчными буквами от названия модели.
    """

    __abstract__ = True

    @declared_attr.directive
    def __tablename__(cls) -> str:
        return cls.__name__.lower()

    id: Mapped[int] = mapped_column(primary_key=True, unique=True, autoincrement=True)
