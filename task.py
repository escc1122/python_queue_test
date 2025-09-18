from dataclasses import dataclass

@dataclass
class Task:
    site: str
    task_id: int

# 可以自定義任務，繼承 Task
@dataclass
class MyTask(Task):
    extra_data: str = ""
