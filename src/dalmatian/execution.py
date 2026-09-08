from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

PublishOutput = Callable[[str, str, str], int]


@dataclass(frozen=True)
class ExecutionContext:
    query_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    attempt_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    attempt_number: int = 0
    timeout_seconds: int = 300
    can_commit: Callable[[], bool] = lambda: True
    publish_output: PublishOutput | None = None
    kind: str = "sync"
