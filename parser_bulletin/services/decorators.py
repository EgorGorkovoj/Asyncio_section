import time
from functools import wraps

from core.logger import logger


def measure_time(func):
    """
    Асинхронный декоратор для измерения времени выполнения функции.

    Оборачивает асинхронную функцию, замеряет общее время её выполнения
    и логирует результат через logger.
    """

    @wraps(func)
    async def wrapper(*args, **kwargs):
        start = time.time()
        result = await func(*args, **kwargs)
        logger.info(f'Общее время работы программы: {time.time() - start:.2f} сек')
        return result

    return wrapper
