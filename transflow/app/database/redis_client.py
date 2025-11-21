import os
from redis import Redis
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

_redis = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def get_redis_client():
    return _redis
