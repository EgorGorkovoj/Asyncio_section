from typing import Generic, Type, TypeVar
from unittest.mock import Base

from core.logger import logger
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

ModelType = TypeVar('ModelType', bound=Base)


class CRUDBase(Generic[ModelType]):
    """
    Базовый класс для операций CRUD с SQLAlchemy моделями.

    Параметры:
        model (Type[ModelType]): Класс SQLAlchemy модели, с которой будут выполняться операции.

    Методы:
        create(session: Session, obj_data: dict) -> ModelType:
            Создает один объект модели, сохраняет его в базе и возвращает созданный экземпляр.
            Если происходит ошибка, транзакция откатывается.

        bulk_create(session: Session, objs_data: list[dict]) -> list[ModelType]:
            Создает несколько объектов одновременно и сохраняет их в базе.
            Возвращает список созданных экземпляров. При ошибке выполняется откат транзакции.

    Примечания:
        - 'ModelType' должен быть наследником Base SQLAlchemy.
        - Для работы методов требуется передать активную SQLAlchemy сессию.
        - Логирование успешных операций и ошибок выполняется через 'logger'.
    """

    def __init__(self, model: Type[ModelType]):
        self.model = model

    async def bulk_create(self, session: AsyncSession, objs_data: list[dict]) -> None:
        """
        Создает несколько объектов модели одновременно и сохраняет их в базе данных.

        Параметры:
            session (Session): Активная сессия SQLAlchemy.
            objs_data (list[dict]): Список словарей с данными для создания объектов модели.
        """
        objs = [self.model(**data) for data in objs_data]
        try:
            session.add_all(objs)
            await session.commit()
            logger.info(f'Данные успешно загружены! Загружено объектов: {len(objs)}')
        except SQLAlchemyError as error:
            await session.rollback()
            logger.error(
                'Произошла ошибка при создании данных в ' f'{self.model.__name__}: {error}!'  # type: ignore
            )
            raise
