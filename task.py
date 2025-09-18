# 範例：繼承任務類別，可以加額外欄位
from main import Task


class ExtendedTask(Task):
    def __init__(self, group_id: str, request_id: str, extra: str):
        super().__init__(group_id, request_id)
        self.extra = extra