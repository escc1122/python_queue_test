import json
import time
import threading
import contextvars
from typing import Type, Callable, Dict
import redis

# 建立全域 contextvar 用來追蹤 request_id
request_id_var = contextvars.ContextVar("request_id")


# 基礎任務類別
class Task:
    def __init__(self, group_id: str, request_id: str):
        """
        Args:
            group_id (str): 控制同一 group 的並發限制 key
            request_id (str): 用來追蹤請求的唯一 ID
        """
        self.group_id = group_id
        self.request_id = request_id




class QueueWorkerFactory:
    def __init__(self, redis_url: str, namespace: str, group_limits: Dict[str, int], backoff_delay: int = 5):
        """
        Args:
            redis_url (str): Redis 連線 URL
            namespace (str): 隔離用的 namespace，避免 key 衝突
            group_limits (Dict[str, int]): 每個 group_id 的最大並發數
            backoff_delay (int): 無法處理時退避的秒數
        """
        self.r = redis.Redis.from_url(redis_url, decode_responses=True)
        self.namespace = namespace
        self.group_limits = group_limits
        self.backoff_delay = backoff_delay

        # group_id -> Semaphore
        self.group_semaphores: Dict[str, threading.Semaphore] = {
            g: threading.Semaphore(limit) for g, limit in group_limits.items()
        }

        self.stop_event = threading.Event()

    def get_queue_names(self):
        """產生不同的 queue key，避免衝突"""
        return f"{self.namespace}:main_queue", f"{self.namespace}:backoff_zset"

    def create_worker(self, task_type: Type[Task], task_handler: Callable[[Task], None]) -> threading.Thread:
        """
        建立 worker thread
        Args:
            task_type (Type[Task]): 必須繼承 Task 的任務類別
            task_handler (Callable): 任務處理函數
        """
        if not issubclass(task_type, Task):
            raise TypeError("task_type 必須繼承自 Task")

        main_queue, backoff_zset = self.get_queue_names()

        def worker():
            while not self.stop_event.is_set():
                item = self.r.blpop(main_queue, timeout=1)
                if not item:
                    continue

                _, task_bytes = item
                task_dict = json.loads(task_bytes)
                task = task_type(**task_dict)

                # 將 request_id 設到 contextvar
                request_id_var.set(task.request_id)

                group = task.group_id
                sem = self.group_semaphores[group]

                acquired = sem.acquire(blocking=False)
                if not acquired:
                    # 無法取得鎖 → 丟到 backoff queue
                    retry_time = time.time() + self.backoff_delay
                    self.r.zadd(backoff_zset, {task_bytes: retry_time})
                    continue

                try:
                    task_handler(task)
                finally:
                    sem.release()

        t = threading.Thread(target=worker, daemon=True)
        return t

    def create_backoff_worker(self) -> threading.Thread:
        """處理 backoff 任務，符合時間的會丟回 main_queue"""
        main_queue, backoff_zset = self.get_queue_names()

        def backoff_worker():
            while not self.stop_event.is_set():
                now = time.time()
                # 每次只拿 10 筆，避免一次拿光
                tasks = self.r.zrangebyscore(backoff_zset, 0, now, start=0, num=10)
                for t in tasks:
                    self.r.rpush(main_queue, t)
                    self.r.zrem(backoff_zset, t)
                time.sleep(1)

        t = threading.Thread(target=backoff_worker, daemon=True)
        return t

    def stop(self):
        """停止所有 worker"""
        self.stop_event.set()
