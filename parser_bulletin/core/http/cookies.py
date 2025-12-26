import os

from aiohttp import CookieJar
from dotenv import load_dotenv

load_dotenv()


def create_cookie_jar() -> CookieJar:
    """
    Создаёт и возвращает объект CookieJar с предустановленными куки для SPIMEX.

    Используется для передачи авторизационных или пользовательских данных при
    выполнении HTTP-запросов через aiohttp. Все куки берутся из переменных
    окружения.

    Куки, устанавливаемые в jar:
        - 'SPIMEX_SM_GUEST_ID'
        - 'SPIMEX_SM_LAST_VISIT'
        - 'PHPSESSID'

    Возвращает:
        CookieJar: объект aiohttp.CookieJar с установленными куки.
    """
    jar = CookieJar(unsafe=True)
    jar.update_cookies(
        {
            'SPIMEX_SM_GUEST_ID': os.getenv('SPIMEX_SM_GUEST_ID'),  # type: ignore
            'SPIMEX_SM_LAST_VISIT': os.getenv('SPIMEX_SM_LAST_VISIT'),
            'PHPSESSID': os.getenv('PHPSESSID'),
        }
    )
    return jar
