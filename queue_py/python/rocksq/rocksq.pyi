from enum import Enum

def version() -> str: ...

def remove_queue(queue_name: str): ...

def remove_mpmc_queue(queue_name: str): ...

class StartPosition(Enum):
    Oldest=0
    Newest=1
