from threading import Semaphore, Event
from collections import defaultdict
import json
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Type
from task import Task

class QueueWorkerFactory:
    """
    一個通用的 Queue Worker 工廠
    功能：
    - 從 Redis main_queue 取得任務，並使用 ThreadPoolExecutor 執行
    - 使用 Semaphore 限制每個 site 同時處理任務數量
    - 任務超過限制會放到 backoff_zset 延遲重試
    - 支援 stop_event 停止 worker
    - 支援任務類別 task_type（必須繼承 Task）
    """

    def __init__(self, redis_client, max_threads=5, site_limit=3, backoff_delay=3, batch_size=5, stop_event=None):
        """
        初始化 QueueWorkerFactory

        Args:
            redis_client: redis.Redis 物件
            max_threads: 總共 ThreadPoolExecutor 最大線程數
            site_limit: 每個 site 同時可處理任務數量
            backoff_delay: 任務被放回 backoff_zset 的延遲秒數
            batch_size: backoff_worker 每次處理任務批次大小
            stop_event: threading.Event，可外部控制 worker 停止
        """
        self.r = redis_client
        self.max_threads = max_threads
        self.site_limit = site_limit
        self.backoff_delay = backoff_delay
        self.batch_size = batch_size

        # site_semaphores: defaultdict，每個 site 對應一個 Semaphore
        self.site_semaphores = defaultdict(lambda: Semaphore(self.site_limit))

        # stop_event: 用來停止無限迴圈
        self.stop_event = stop_event or Event()

    def create_worker(
        self,
        main_queue: str,
        task_handler: Callable[[Task], None],
        backoff_zset: str,
        task_type: Type[Task]
    ):
        """
        建立一個 worker function
        從 main_queue 取得任務，並用 Semaphore 控制 site 同時執行數量
        超過限制的任務放到 backoff_zset 延遲重試

        Args:
            main_queue: Redis 主隊列名稱
            task_handler: 處理任務的函數，接收 Task 物件
            backoff_zset: backoff zset 名稱
            task_type: 任務類別 type（必須繼承 Task）

        Returns:
            worker function
        """
        # 驗證 task_type 是否繼承 Task
        if not issubclass(task_type, Task):
            raise TypeError("task_type 必須繼承 Task 類別")

        def worker():
            while not self.stop_event.is_set():
                # 從 main_queue 取得任務，最多等待 1 秒
                item = self.r.blpop(main_queue, timeout=1)
                if not item:
                    continue  # 沒有任務，繼續迴圈

                _, task_bytes = item
                task_dict = json.loads(task_bytes)

                # 將 dict 轉成指定的 task_type
                task = task_type(**task_dict)

                # 取得該 site 的 Semaphore
                site = task.site
                sem = self.site_semaphores[site]

                # 非阻塞方式取得 semaphore
                acquired = sem.acquire(blocking=False)
                if not acquired:
                    # 若已達上限，放回 backoff_zset 延遲處理
                    retry_time = time.time() + self.backoff_delay
                    self.r.zadd(backoff_zset, {task_bytes: retry_time})
                    continue

                try:
                    # 執行任務
                    task_handler(task)
                finally:
                    # 釋放 semaphore
                    sem.release()

        return worker

    def create_backoff_worker(self, main_queue: str, backoff_zset: str):
        """
        建立 backoff_worker
        將 zset 中 score <= 現在時間的任務取出，放回 main_queue

        Args:
            main_queue: Redis 主隊列名稱
            backoff_zset: backoff zset 名稱

        Returns:
            backoff_worker function
        """
        def backoff_worker():
            while not self.stop_event.is_set():
                now = time.time()
                # 取出 score <= now 的任務，限制批次大小
                tasks = self.r.zrangebyscore(backoff_zset, 0, now, start=0, num=self.batch_size)
                if not tasks:
                    time.sleep(0.1)  # 沒有任務，稍微休息
                    continue

                for t in tasks:
                    self.r.rpush(main_queue, t)
                    self.r.zrem(backoff_zset, t)

                time.sleep(0.05)  # 批次之間稍作休息

        return backoff_worker

    def run(
        self,
        main_queue: str,
        task_handler: Callable[[Task], None],
        backoff_zset: str,
        task_type: Type[Task]
    ):
        """
        啟動 ThreadPoolExecutor 執行 worker 與 backoff_worker

        Args:
            main_queue: Redis 主隊列名稱
            task_handler: 處理任務的函數
            backoff_zset: backoff zset 名稱
            task_type: 任務類別 type（必須繼承 Task）
        """
        if not issubclass(task_type, Task):
            raise TypeError("task_type 必須繼承 Task 類別")

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            # 啟動多個 worker
            for _ in range(self.max_threads - 1):
                executor.submit(self.create_worker(main_queue, task_handler, backoff_zset, task_type))
            # 啟動 backoff_worker
            executor.submit(self.create_backoff_worker(main_queue, backoff_zset))
