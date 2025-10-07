import redis
import os
from contextlib import contextmanager
redis_url = os.environ.get("REDIS_URL", "redis://:foobaredUniqnex@localhost:6379/0")
redis_client=redis.StrictRedis.from_url(redis_url)

@contextmanager
def redis_lock(lock_name,timeout=3600):
    lock=redis_client.lock(lock_name,timeout=timeout)
    acquired=lock.acquire(blocking=False)
    try:
        yield acquired
    finally:
        if acquired:
            lock.release()