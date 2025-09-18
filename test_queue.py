import unittest
import json
import time
from threading import Thread, Event
import redis

from main import QueueWorkerFactory
from task import MyTask, Task

# 繼承 MyTask 的新類別
class ExtendedTask(MyTask):
    extra_field: str = "extra"

class TestQueueWorkerFactory(unittest.TestCase):
    def setUp(self):
        self.r = redis.Redis()
        self.main_queue = "test_main_queue"
        self.backoff_zset = "test_backoff_zset"
        self.r.delete(self.main_queue)
        self.r.delete(self.backoff_zset)

        self.stop_event = Event()
        self.factory = QueueWorkerFactory(
            redis_client=self.r,
            max_threads=3,
            site_limit=2,
            backoff_delay=1,
            batch_size=2,
            stop_event=self.stop_event
        )

    def tearDown(self):
        self.stop_event.set()
        self.r.delete(self.main_queue)
        self.r.delete(self.backoff_zset)

    def test_task_processing_with_extended_task(self):
        # 放入 4 個 ExtendedTask 任務
        tasks = [ExtendedTask(site="site_a", task_id=i) for i in range(4)]
        for t in tasks:
            self.r.rpush(self.main_queue, json.dumps(t.__dict__))

        processed = []

        def task_handler(task: ExtendedTask):
            # 可以存取 extra_field
            processed.append((task.task_id, getattr(task, "extra_field", None)))
            time.sleep(0.1)

        # 啟動工廠
        t_worker = Thread(
            target=self.factory.run,
            args=(self.main_queue, task_handler, self.backoff_zset, ExtendedTask)
        )
        t_worker.start()

        # 等待任務完成
        time.sleep(2)

        # 停止 worker
        self.stop_event.set()
        t_worker.join()

        # 驗證任務都被處理
        self.assertCountEqual(
            [t[0] for t in processed],
            [0, 1, 2, 3]
        )
        # 驗證 extra_field
        for _, field in processed:
            self.assertEqual(field, "extra")

        self.assertEqual(self.r.llen(self.main_queue), 0)
        self.assertEqual(self.r.zcard(self.backoff_zset), 0)


if __name__ == "__main__":
    unittest.main()
