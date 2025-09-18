import unittest
import json
import time
from threading import Thread, Event
import redis

from main import QueueWorkerFactory


class TestQueueWorkerFactory(unittest.TestCase):
    def setUp(self):
        self.r = redis.Redis()
        self.main_queue = "test_main_queue"
        self.backoff_zset = "test_backoff_zset"
        self.r.delete(self.main_queue)
        self.r.delete(self.backoff_zset)

        # 停止事件
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

    def test_task_processing(self):
        # 放入 4 個任務
        tasks = [{"site": "site_a", "task_id": i} for i in range(4)]
        for t in tasks:
            self.r.rpush(self.main_queue, json.dumps(t))

        processed = []

        def task_handler(task):
            processed.append(task["task_id"])
            time.sleep(0.1)

        # 啟動 factory.run 在 thread
        t_worker = Thread(target=self.factory.run, args=(self.main_queue, task_handler, self.backoff_zset))
        t_worker.start()

        # 等待任務處理
        time.sleep(2)

        # 停止 worker
        self.stop_event.set()
        t_worker.join()

        # 驗證任務都處理完成
        self.assertCountEqual(processed, [0, 1, 2, 3])
        self.assertEqual(self.r.llen(self.main_queue), 0)
        self.assertEqual(self.r.zcard(self.backoff_zset), 0)

if __name__ == "__main__":
    unittest.main()
