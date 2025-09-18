import json
import time
from threading import Semaphore
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import redis
from typing import Callable

class QueueWorkerFactory:
    def __init__(self, redis_client: redis.Redis, max_threads: int = 5, site_limit: int = 3, backoff_delay: int = 3, batch_size: int = 5):
        self.r = redis_client
        self.max_threads = max_threads
        self.site_limit = site_limit
        self.backoff_delay = backoff_delay
        self.batch_size = batch_size
        self.site_semaphores = defaultdict(lambda: Semaphore(self.site_limit))

    def create_worker(self, main_queue: str, task_handler: Callable[[dict], None], backoff_zset: str):
        def worker():
            while True:
                item = self.r.blpop(main_queue, timeout=5)
                if not item:
                    continue

                _, task_bytes = item
                task = json.loads(task_bytes)
                site = task["site"]

                sem = self.site_semaphores[site]
                acquired = sem.acquire(blocking=False)
                if not acquired:
                    retry_time = time.time() + self.backoff_delay
                    self.r.zadd(backoff_zset, {task_bytes: retry_time})
                    continue

                try:
                    task_handler(task)
                finally:
                    sem.release()
        return worker

    def create_backoff_worker(self, main_queue: str, backoff_zset: str):
        def backoff_worker():
            while True:
                now = time.time()
                tasks = self.r.zrangebyscore(backoff_zset, 0, now, start=0, num=self.batch_size)
                if not tasks:
                    time.sleep(1)
                    continue

                for t in tasks:
                    self.r.rpush(main_queue, t)
                    self.r.zrem(backoff_zset, t)

                time.sleep(0.1)
        return backoff_worker

    def run(self, main_queue: str, task_handler: Callable[[dict], None], backoff_zset: str):
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            for _ in range(self.max_threads - 1):
                executor.submit(self.create_worker(main_queue, task_handler, backoff_zset))
            executor.submit(self.create_backoff_worker(main_queue, backoff_zset))
