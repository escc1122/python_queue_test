import json
import time
import unittest
import redis

from main import QueueWorkerFactory, request_id_var, Task
from task import ExtendedTask


class QueueWorkerTest(unittest.TestCase):
    def setUp(self):
        self.redis_url = "redis://localhost:6379/0"
        self.r = redis.Redis.from_url(self.redis_url, decode_responses=True)
        self.r.flushdb()

    def test_basic_task_processing(self):
        factory = QueueWorkerFactory(
            redis_url=self.redis_url,
            namespace="unittest1",
            group_limits={"g1": 1}
        )

        processed = []

        def handler(task: Task):
            current_id = request_id_var.get()
            processed.append((task.group_id, current_id))

        worker = factory.create_worker(Task, handler)
        backoff = factory.create_backoff_worker()

        worker.start()
        backoff.start()

        # 推一個任務
        main_queue, _ = factory.get_queue_names()
        self.r.rpush(main_queue, json.dumps({"group_id": "g1", "request_id": "req-1"}))

        time.sleep(2)
        factory.stop()

        self.assertIn(("g1", "req-1"), processed)

    def test_extended_task_processing(self):
        factory = QueueWorkerFactory(
            redis_url=self.redis_url,
            namespace="unittest2",
            group_limits={"g2": 1}
        )

        processed = []

        def handler(task: ExtendedTask):
            current_id = request_id_var.get()
            processed.append((task.group_id, current_id, task.extra))

        worker = factory.create_worker(ExtendedTask, handler)
        backoff = factory.create_backoff_worker()

        worker.start()
        backoff.start()

        # 推一個 ExtendedTask 任務
        main_queue, _ = factory.get_queue_names()
        self.r.rpush(
            main_queue,
            json.dumps({"group_id": "g2", "request_id": "req-2", "extra": "hello"})
        )

        time.sleep(2)
        factory.stop()

        self.assertIn(("g2", "req-2", "hello"), processed)


if __name__ == "__main__":
    unittest.main()
