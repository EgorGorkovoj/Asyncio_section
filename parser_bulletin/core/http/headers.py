import os

from dotenv import load_dotenv

load_dotenv()

HEADERS = {
    'User-Agent': os.getenv('User-Agent'),
    'Accept': os.getenv('Accept'),
    'Accept-Language': os.getenv('Accept-Language'),
    'Referer': os.getenv('Referer'),
}
