from typing import Optional
from rocksq import StartPosition

class ResponseVariant:
    @property
    def data(self) -> Optional[list[bytes]]: ...

    @property
    def len(self) -> Optional[int]: ...

    @property
    def size(self) -> Optional[int]: ...


class Response:
    @property
    def is_ready(self) -> bool: ...

    def try_get(self) -> Optional[ResponseVariant]: ...

    def get(self) -> ResponseVariant: ...


class PersistentQueueWithCapacity:
    def __init__(self, path: str, max_elements: int = 1_000_000_000, max_inflight_ops: int = 1_000): ...

    def push(self, items: list[bytes], no_gil: bool = True) -> Response: ...

    @property
    def inflight_ops(self) -> int: ...

    def pop(self, max_elements = 1, no_gil: bool = True) -> Response: ...

    @property
    def disk_size(self) -> Response: ...

    @property
    def payload_size(self) -> Response: ...

    @property
    def len(self) -> Response: ...

class MpmcResponseVariant:
    @property
    def data(self) -> Optional[(list[bytes], bool)]: ...

    @property
    def labels(self) -> Optional[list[str]]: ...

    @property
    def removed_label(self) -> Optional[bool]: ...

    @property
    def len(self) -> Optional[int]: ...

    @property
    def size(self) -> Optional[int]: ...

class MpmcResponse:
    @property
    def is_ready(self) -> bool: ...

    def try_get(self) -> Optional[MpmcResponseVariant]: ...

    def get(self) -> MpmcResponseVariant: ...

class MpmcQueue:
    def __init__(self, path: str, ttl: int, max_inflight_ops: int = 1_000): ...

    def add(self, items: list[bytes], no_gil: bool = True) -> MpmcResponse: ...

    @property
    def inflight_ops(self) -> int: ...

    def next(self, label: str, start_position: StartPosition, max_elements = 1, no_gil: bool = True) -> MpmcResponse: ...

    @property
    def disk_size(self) -> MpmcResponse: ...

    @property
    def len(self) -> MpmcResponse: ...

    @property
    def labels(self) -> MpmcResponse: ...

    def remove_label(self, label: str) -> MpmcResponse: ...
