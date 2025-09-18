import unittest
from unittest.mock import patch
import redis
import json
import time
from threading import Thread

from main import QueueWorkerFactory


class TestQueueWorkerFactory(unittest.TestCase):
    def setUp(self):
        # 連線本地 Redis，測試用
        self.r = redis.Redis()
        self.main_queue = "test_main_queue"
        self.backoff_zset = "test_backoff_zset"

        # 清空 Redis key
        self.r.delete(self.main_queue)
        self.r.delete(self.backoff_zset)

        # 建立工廠
        self.factory = QueueWorkerFactory(
            redis_client=self.r,
            max_threads=3,
            site_limit=2,
            backoff_delay=1,
            batch_size=2
        )

    def tearDown(self):
        self.r.delete(self.main_queue)
        self.r.delete(self.backoff_zset)

    def test_task_processing_with_backoff(self):
        # 準備測試任務
        tasks = [{"site": "site_a", "task_id": i} for i in range(4)]
        for t in tasks:
            self.r.rpush(self.main_queue, json.dumps(t))

        processed = []

        # 簡單任務 handler，記錄完成的 task_id
        def task_handler(task):
            processed.append((task["site"], task["task_id"]))
            time.sleep(0.1)  # 模擬處理時間

        # 啟動 worker thread
        t_worker = Thread(target=self.factory.run, args=(self.main_queue, task_handler, self.backoff_zset))
        t_worker.daemon = True
        t_worker.start()

        # 等一段時間讓 worker 處理
        time.sleep(2)

        # 驗證所有任務都被處理
        processed_ids = [tid for _, tid in processed]
        self.assertCountEqual(processed_ids, [0, 1, 2, 3])

        # backoff_zset 應該已清空
        self.assertEqual(self.r.zcard(self.backoff_zset), 0)

        # main_queue 應該也清空
        self.assertEqual(self.r.llen(self.main_queue), 0)

if __name__ == "__main__":
    unittest.main()
