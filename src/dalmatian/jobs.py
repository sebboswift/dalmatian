from __future__ import annotations

import hashlib
import logging
import random
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from pydantic import TypeAdapter

from dalmatian.errors import BusyError, IdempotencyConflict
from dalmatian.fingerprint import dataset_affinity_key, request_payload_fingerprint
from dalmatian.metadata import MetadataStore, NullMetadataStore
from dalmatian.models import JobRecord, JobStatus, QueryRequest, QueryResult
from dalmatian.observability import QUEUE_WAIT_SECONDS

logger = logging.getLogger(__name__)
_RESULT_ADAPTER = TypeAdapter(QueryResult)


@dataclass(frozen=True)
class ReservedJob:
    id: str
    request: QueryRequest
    attempts: int
    shard: int = 0


class JobQueue(Protocol):
    def enqueue(self, request: QueryRequest, idempotency_key: str | None = None) -> JobRecord: ...
    def get(self, job_id: str) -> JobRecord | None: ...
    def reserve(
        self,
        worker_id: str,
        timeout_seconds: int = 5,
        shard: int = 0,
    ) -> ReservedJob | None: ...
    def heartbeat(self, job_id: str, worker_id: str) -> bool: ...
    def owns(self, job_id: str, worker_id: str) -> bool: ...
    def cancel_requested(self, job_id: str, worker_id: str) -> bool: ...
    def request_cancel(self, job_id: str) -> JobRecord | None: ...
    def complete(self, job_id: str, worker_id: str, result: QueryResult) -> bool: ...
    def publish_output(
        self,
        job_id: str,
        worker_id: str,
        attempt_number: int,
        publication_key: str,
        attempt_id: str,
        manifest_uri: str,
        write_mode: str,
    ) -> int: ...
    def cancel_complete(self, job_id: str, worker_id: str) -> bool: ...
    def fail(
        self,
        job_id: str,
        worker_id: str,
        error: str,
        retryable: bool = False,
        error_class: str | None = None,
    ) -> bool: ...
    def reap_stale(self) -> int: ...
    def promote_retries(self) -> int: ...
    def ping(self) -> bool: ...


class ValkeyJobQueue:
    _ENQUEUE_SCRIPT = """
local job = KEYS[1]
local pending = KEYS[2]
local idem = KEYS[3]
local use_idem = ARGV[1]
local request_hash = ARGV[2]
if use_idem == '1' and redis.call('EXISTS', idem) == 1 then
  local existing_hash = redis.call('HGET', idem, 'request_hash') or ''
  local existing_job = redis.call('HGET', idem, 'job_id') or ''
  if existing_hash ~= request_hash then return {'conflict', existing_job} end
  return {'existing', existing_job}
end
local depth = redis.call('LLEN', KEYS[4]) + redis.call('ZCARD', KEYS[5])
for index = 6, #KEYS do depth = depth + redis.call('LLEN', KEYS[index]) end
if depth >= tonumber(ARGV[9]) then return {'full', ''} end
redis.call('HSET', job,
  'status', 'queued',
  'request', ARGV[3],
  'error', '',
  'result', '',
  'attempts', '0',
  'worker_id', '',
  'lease_expires_ms', '0',
  'cancel_requested', '0',
  'shard', ARGV[4],
  'created_at_ms', ARGV[5],
  'updated_at_ms', ARGV[5],
  'next_attempt_ms', '0')
redis.call('EXPIRE', job, ARGV[6])
redis.call('LPUSH', pending, ARGV[7])
if use_idem == '1' then
  redis.call('HSET', idem, 'request_hash', request_hash, 'job_id', ARGV[7])
  redis.call('EXPIRE', idem, ARGV[8])
end
return {'created', ARGV[7]}
"""

    _PUBLISH_SCRIPT = """
local job = KEYS[1]
local publication = KEYS[2]
if redis.call('HGET', job, 'status') ~= 'running' then return -1 end
if redis.call('HGET', job, 'worker_id') ~= ARGV[1] then return -1 end
if tonumber(redis.call('HGET', job, 'attempts') or '0') ~= tonumber(ARGV[2]) then return -1 end
if redis.call('HGET', job, 'cancel_requested') == '1' then return -1 end
if tonumber(redis.call('HGET', job, 'lease_expires_ms') or '0') <= tonumber(ARGV[7]) then
  return -1
end
local mode = ARGV[3]
if mode == 'append' then
  redis.call('HSET', publication,
    'exists', '1',
    'last_append_query_id', ARGV[4],
    'last_append_attempt_id', ARGV[5],
    'last_append_manifest_uri', ARGV[6],
    'updated_at_ms', ARGV[7])
  return 1
end
if mode == 'error' and redis.call('EXISTS', publication) == 1 then
  local existing_query = redis.call('HGET', publication, 'query_id') or ''
  if existing_query ~= ARGV[4] then return 0 end
end
redis.call('HSET', publication,
  'exists', '1',
  'query_id', ARGV[4],
  'attempt_id', ARGV[5],
  'manifest_uri', ARGV[6],
  'updated_at_ms', ARGV[7])
return 1
"""

    _CLAIM_SCRIPT = """
local key = KEYS[1]
local processing = KEYS[2]
local job_id = ARGV[1]
if redis.call('LPOS', processing, job_id) == false then return nil end
if redis.call('HGET', key, 'status') ~= 'queued' then return nil end
local request = redis.call('HGET', key, 'request')
if not request then redis.call('LREM', processing, 1, job_id); return nil end
local attempts = redis.call('HINCRBY', key, 'attempts', 1)
redis.call('HSET', key, 'status', 'running', 'worker_id', ARGV[2],
  'lease_expires_ms', ARGV[3], 'updated_at_ms', ARGV[4], 'cancel_requested', '0',
  'next_attempt_ms', '0')
redis.call('EXPIRE', key, ARGV[5])
return {request, attempts,
  redis.call('HGET', key, 'shard') or '0',
  redis.call('HGET', key, 'created_at_ms') or ARGV[4]}
"""

    _HEARTBEAT_SCRIPT = """
local key = KEYS[1]
if redis.call('HGET', key, 'status') ~= 'running' then return 0 end
if redis.call('HGET', key, 'worker_id') ~= ARGV[1] then return 0 end
if tonumber(redis.call('HGET', key, 'lease_expires_ms') or '0') <= tonumber(ARGV[3]) then
  return 0
end
redis.call('HSET', key, 'lease_expires_ms', ARGV[2], 'updated_at_ms', ARGV[3])
redis.call('EXPIRE', key, ARGV[4])
return 1
"""

    _COMPLETE_SCRIPT = """
local key = KEYS[1]
local processing = KEYS[2]
if redis.call('HGET', key, 'status') ~= 'running' then return 0 end
if redis.call('HGET', key, 'worker_id') ~= ARGV[1] then return 0 end
if redis.call('HGET', key, 'cancel_requested') == '1' then return 0 end
if tonumber(redis.call('HGET', key, 'lease_expires_ms') or '0') <= tonumber(ARGV[3]) then
  return 0
end
redis.call('HSET', key, 'status', 'succeeded', 'result', ARGV[2], 'error', '',
  'worker_id', '', 'lease_expires_ms', '0', 'updated_at_ms', ARGV[3])
redis.call('LREM', processing, 1, ARGV[4])
redis.call('EXPIRE', key, ARGV[5])
return 1
"""

    _TERMINAL_SCRIPT = """
local key = KEYS[1]
local processing = KEYS[2]
local dead = KEYS[3]
if redis.call('HGET', key, 'status') ~= 'running' then return 0 end
if redis.call('HGET', key, 'worker_id') ~= ARGV[1] then return 0 end
if tonumber(redis.call('HGET', key, 'lease_expires_ms') or '0') <= tonumber(ARGV[4]) then
  return 0
end
redis.call('HSET', key, 'status', ARGV[2], 'error', ARGV[3], 'worker_id', '',
  'lease_expires_ms', '0', 'updated_at_ms', ARGV[4])
redis.call('LREM', processing, 1, ARGV[5])
if ARGV[2] == 'failed' then redis.call('LPUSH', dead, ARGV[5]) end
redis.call('EXPIRE', key, ARGV[6])
return 1
"""

    _RETRY_SCRIPT = """
local key = KEYS[1]
local processing = KEYS[2]
local retry = KEYS[3]
local dead = KEYS[4]
if redis.call('HGET', key, 'status') ~= 'running' then return 0 end
if redis.call('HGET', key, 'worker_id') ~= ARGV[1] then return 0 end
if tonumber(redis.call('HGET', key, 'lease_expires_ms') or '0') <= tonumber(ARGV[5]) then
  return 0
end
local attempts = tonumber(redis.call('HGET', key, 'attempts') or '0')
redis.call('LREM', processing, 1, ARGV[2])
if attempts >= tonumber(ARGV[3]) then
  redis.call('HSET', key, 'status', 'failed', 'error', ARGV[4], 'worker_id', '',
    'lease_expires_ms', '0', 'updated_at_ms', ARGV[5])
  redis.call('LPUSH', dead, ARGV[2])
else
  redis.call('HSET', key, 'status', 'retrying', 'error', ARGV[4], 'worker_id', '',
    'lease_expires_ms', '0', 'updated_at_ms', ARGV[5], 'next_attempt_ms', ARGV[6])
  redis.call('ZADD', retry, ARGV[6], ARGV[2])
end
redis.call('EXPIRE', key, ARGV[7])
return 1
"""

    _REAP_SCRIPT = """
local key = KEYS[1]
local processing = KEYS[2]
local retry = KEYS[3]
local dead = KEYS[4]
local job_id = ARGV[1]
local expected_lease = ARGV[2]
local now_ms = tonumber(ARGV[3])
local max_attempts = tonumber(ARGV[4])
if redis.call('EXISTS', key) == 0 then redis.call('LREM', processing, 1, job_id); return 1 end
local status = redis.call('HGET', key, 'status')
if status == 'queued' then
  redis.call('LREM', processing, 1, job_id)
  redis.call('LPUSH', KEYS[5], job_id)
  return 1
end
if status ~= 'running' then redis.call('LREM', processing, 1, job_id); return 1 end
local lease = redis.call('HGET', key, 'lease_expires_ms') or '0'
if lease ~= expected_lease or tonumber(lease) > now_ms then return 0 end
local attempts = tonumber(redis.call('HGET', key, 'attempts') or '0')
local cancel_requested = redis.call('HGET', key, 'cancel_requested') or '0'
redis.call('LREM', processing, 1, job_id)
if cancel_requested == '1' then
  redis.call('HSET', key, 'status', 'cancelled', 'error', '', 'worker_id', '',
    'lease_expires_ms', '0', 'updated_at_ms', ARGV[3], 'next_attempt_ms', '0')
elseif attempts >= max_attempts then
  redis.call('HSET', key, 'status', 'failed', 'error', 'worker lease expired', 'worker_id', '',
    'lease_expires_ms', '0', 'updated_at_ms', ARGV[3])
  redis.call('LPUSH', dead, job_id)
else
  redis.call('HSET', key, 'status', 'retrying', 'error', 'worker lease expired', 'worker_id', '',
    'lease_expires_ms', '0', 'updated_at_ms', ARGV[3], 'next_attempt_ms', ARGV[5])
  redis.call('ZADD', retry, ARGV[5], job_id)
end
redis.call('EXPIRE', key, ARGV[6])
return 1
"""

    _PROMOTE_SCRIPT = """
local key = KEYS[1]
local retry = KEYS[2]
local pending = KEYS[3]
local job_id = ARGV[1]
local now_ms = tonumber(ARGV[2])
if redis.call('HGET', key, 'status') ~= 'retrying' then
  redis.call('ZREM', retry, job_id)
  return 0
end
local score = redis.call('ZSCORE', retry, job_id)
if not score or tonumber(score) > now_ms then return 0 end
if redis.call('ZREM', retry, job_id) == 0 then return 0 end
redis.call('HSET', key, 'status', 'queued', 'next_attempt_ms', '0', 'updated_at_ms', ARGV[2])
redis.call('LPUSH', pending, job_id)
return 1
"""

    _CANCEL_SCRIPT = """
local key = KEYS[1]
if redis.call('EXISTS', key) == 0 then return -1 end
local status = redis.call('HGET', key, 'status')
if status == 'queued' then
  redis.call('LREM', KEYS[2], 1, ARGV[1])
  redis.call('HSET', key, 'status', 'cancelled', 'updated_at_ms', ARGV[2])
  return 1
end
if status == 'retrying' then
  redis.call('ZREM', KEYS[3], ARGV[1])
  redis.call('HSET', key, 'status', 'cancelled', 'updated_at_ms', ARGV[2])
  return 1
end
if status == 'running' then
  redis.call('HSET', key, 'cancel_requested', '1', 'updated_at_ms', ARGV[2])
  return 2
end
return 0
"""

    def __init__(
        self,
        url: str,
        queue_name: str,
        job_ttl_seconds: int,
        lease_seconds: int,
        max_attempts: int,
        affinity_shards: int = 1,
        max_queue_depth: int = 10000,
        idempotency_ttl_seconds: int | None = None,
        retry_base_seconds: float = 2.0,
        retry_max_seconds: float = 60.0,
        metadata: MetadataStore | None = None,
    ) -> None:
        import valkey

        self.client = valkey.from_url(url, decode_responses=True)
        self.queue_name = queue_name
        self.processing_name = f"{queue_name}:processing"
        self.retry_name = f"{queue_name}:retry"
        self.dead_name = f"{queue_name}:dead"
        self.job_ttl_seconds = job_ttl_seconds
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.affinity_shards = affinity_shards
        self.max_queue_depth = max_queue_depth
        self.idempotency_ttl_seconds = idempotency_ttl_seconds or job_ttl_seconds
        self.retry_base_seconds = retry_base_seconds
        self.retry_max_seconds = retry_max_seconds
        self.metadata = metadata or NullMetadataStore()
        self._enqueue = self.client.register_script(self._ENQUEUE_SCRIPT)
        self._publish_output = self.client.register_script(self._PUBLISH_SCRIPT)
        self._claim = self.client.register_script(self._CLAIM_SCRIPT)
        self._heartbeat = self.client.register_script(self._HEARTBEAT_SCRIPT)
        self._complete = self.client.register_script(self._COMPLETE_SCRIPT)
        self._terminal = self.client.register_script(self._TERMINAL_SCRIPT)
        self._retry = self.client.register_script(self._RETRY_SCRIPT)
        self._reap = self.client.register_script(self._REAP_SCRIPT)
        self._promote = self.client.register_script(self._PROMOTE_SCRIPT)
        self._cancel = self.client.register_script(self._CANCEL_SCRIPT)

    def _key(self, job_id: str) -> str:
        return f"{self.queue_name}:job:{job_id}"

    def _pending(self, shard: int) -> str:
        return f"{self.queue_name}:shard:{shard}"

    def _idempotency_key(self, key: str) -> str:
        digest = hashlib.sha256(key.encode()).hexdigest()
        return f"{self.queue_name}:idempotency:{digest}"

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _dt(ms: str | int | None) -> datetime | None:
        value = int(ms or 0)
        return datetime.fromtimestamp(value / 1000, tz=UTC) if value else None

    def _shard(self, request: QueryRequest) -> int:
        digest = hashlib.sha256(dataset_affinity_key(request).encode()).digest()
        return int.from_bytes(digest[:8], "big") % self.affinity_shards

    def depth(self) -> int:
        pipe = self.client.pipeline(transaction=False)
        for shard in range(self.affinity_shards):
            pipe.llen(self._pending(shard))
        pipe.llen(self.processing_name)
        pipe.zcard(self.retry_name)
        return sum(int(value) for value in pipe.execute())

    def enqueue(
        self,
        request: QueryRequest,
        idempotency_key: str | None = None,
    ) -> JobRecord:
        if idempotency_key is not None and not (1 <= len(idempotency_key) <= 255):
            raise ValueError("Idempotency-Key must be between 1 and 255 characters")
        fingerprint = request_payload_fingerprint(request)
        if idempotency_key is not None:
            existing_binding = self.client.hgetall(self._idempotency_key(idempotency_key))
            if existing_binding:
                if existing_binding.get("request_hash") != fingerprint:
                    raise IdempotencyConflict(
                        "Idempotency-Key is already bound to job "
                        f"{existing_binding.get('job_id', '')} with a different request"
                    )
                existing_job_id = existing_binding.get("job_id", "")
                existing = self.get(existing_job_id)
                if existing is not None:
                    return existing
                raise IdempotencyConflict(
                    "Idempotency-Key is still retained but its job record has expired; "
                    "submit a new request with a new key"
                )
        job_id = str(uuid.uuid4())
        shard = self._shard(request)
        now_ms = self._now_ms()
        request_json = request.model_dump_json(exclude_unset=True)
        idem_redis_key = (
            self._idempotency_key(idempotency_key)
            if idempotency_key is not None
            else f"{self.queue_name}:idempotency:none:{job_id}"
        )
        state, selected_job_id = self._enqueue(
            keys=[
                self._key(job_id),
                self._pending(shard),
                idem_redis_key,
                self.processing_name,
                self.retry_name,
                *(self._pending(index) for index in range(self.affinity_shards)),
            ],
            args=[
                "1" if idempotency_key is not None else "0",
                fingerprint,
                request_json,
                shard,
                now_ms,
                self.job_ttl_seconds,
                job_id,
                self.idempotency_ttl_seconds,
                self.max_queue_depth,
            ],
        )
        if state == "full":
            raise BusyError("async queue is full")
        if state == "conflict":
            raise IdempotencyConflict(
                "Idempotency-Key is already bound to job "
                f"{selected_job_id} with a different request"
            )
        if state == "existing":
            existing = self.get(selected_job_id)
            if existing is None:
                raise IdempotencyConflict(
                    "Idempotency-Key is still retained but its job record has expired; "
                    "submit a new request with a new key"
                )
            return existing
        try:
            self.metadata.record_invocation(job_id, "async", request_json)
        except Exception:
            logger.exception("metadata write failed", extra={"job_id": job_id})
        return JobRecord(
            id=job_id,
            status=JobStatus.QUEUED,
            created_at=self._dt(now_ms),
            updated_at=self._dt(now_ms),
        )

    def get(self, job_id: str) -> JobRecord | None:
        data = self.client.hgetall(self._key(job_id))
        if not data:
            return None
        result = _RESULT_ADAPTER.validate_json(data["result"]) if data.get("result") else None
        return JobRecord(
            id=job_id,
            status=JobStatus(data["status"]),
            attempts=int(data.get("attempts", 0)),
            result=result,
            error=data.get("error") or None,
            created_at=self._dt(data.get("created_at_ms")),
            updated_at=self._dt(data.get("updated_at_ms")),
            next_attempt_at=self._dt(data.get("next_attempt_ms")),
        )

    def promote_retries(self) -> int:
        now = self._now_ms()
        due = self.client.zrangebyscore(self.retry_name, 0, now, start=0, num=100)
        moved = 0
        for job_id in due:
            shard = int(self.client.hget(self._key(job_id), "shard") or 0)
            moved += int(
                bool(
                    self._promote(
                        keys=[self._key(job_id), self.retry_name, self._pending(shard)],
                        args=[job_id, now],
                    )
                )
            )
        return moved

    def reserve(
        self,
        worker_id: str,
        timeout_seconds: int = 5,
        shard: int = 0,
    ) -> ReservedJob | None:
        self.promote_retries()
        primary = shard % self.affinity_shards
        pending = self._pending(primary)
        job_id = self.client.brpoplpush(pending, self.processing_name, timeout_seconds)
        if job_id is None and self.affinity_shards > 1:
            for candidate in range(self.affinity_shards):
                if candidate == primary:
                    continue
                job_id = self.client.rpoplpush(self._pending(candidate), self.processing_name)
                if job_id is not None:
                    break
        if job_id is None:
            return None
        now_ms = self._now_ms()
        claimed = self._claim(
            keys=[self._key(job_id), self.processing_name],
            args=[
                job_id,
                worker_id,
                now_ms + self.lease_seconds * 1000,
                now_ms,
                self.job_ttl_seconds,
            ],
        )
        if not claimed:
            return None
        request_json, attempts, claimed_shard, created_at_ms = claimed
        QUEUE_WAIT_SECONDS.observe(max(0.0, (now_ms - int(created_at_ms)) / 1000))
        try:
            self.metadata.update_invocation(job_id, "running", attempts=int(attempts))
        except Exception:
            logger.exception("metadata write failed", extra={"job_id": job_id})
        return ReservedJob(
            id=job_id,
            request=QueryRequest.model_validate_json(request_json),
            attempts=int(attempts),
            shard=int(claimed_shard),
        )

    def heartbeat(self, job_id: str, worker_id: str) -> bool:
        now_ms = self._now_ms()
        return bool(
            self._heartbeat(
                keys=[self._key(job_id)],
                args=[worker_id, now_ms + self.lease_seconds * 1000, now_ms, self.job_ttl_seconds],
            )
        )

    def owns(self, job_id: str, worker_id: str) -> bool:
        status, owner, lease = self.client.hmget(
            self._key(job_id), "status", "worker_id", "lease_expires_ms"
        )
        return (
            status == JobStatus.RUNNING.value
            and owner == worker_id
            and int(lease or 0) > self._now_ms()
        )

    def cancel_requested(self, job_id: str, worker_id: str) -> bool:
        status, owner, cancel = self.client.hmget(
            self._key(job_id), "status", "worker_id", "cancel_requested"
        )
        return status == JobStatus.RUNNING.value and owner == worker_id and cancel == "1"

    def request_cancel(self, job_id: str) -> JobRecord | None:
        data = self.client.hmget(self._key(job_id), "shard")
        if not data or data[0] is None:
            return None
        result = int(
            self._cancel(
                keys=[self._key(job_id), self._pending(int(data[0])), self.retry_name],
                args=[job_id, self._now_ms()],
            )
        )
        if result == -1:
            return None
        record = self.get(job_id)
        if record and record.status == JobStatus.CANCELLED:
            self._metadata_status(job_id, "cancelled")
        return record

    def complete(self, job_id: str, worker_id: str, result: QueryResult) -> bool:
        ok = bool(
            self._complete(
                keys=[self._key(job_id), self.processing_name],
                args=[
                    worker_id,
                    result.model_dump_json(),
                    self._now_ms(),
                    job_id,
                    self.job_ttl_seconds,
                ],
            )
        )
        if ok:
            self._metadata_status(job_id, "succeeded", result=result)
        return ok

    def publish_output(
        self,
        job_id: str,
        worker_id: str,
        attempt_number: int,
        publication_key: str,
        attempt_id: str,
        manifest_uri: str,
        write_mode: str,
    ) -> int:
        return int(
            self._publish_output(
                keys=[self._key(job_id), publication_key],
                args=[
                    worker_id,
                    attempt_number,
                    write_mode,
                    job_id,
                    attempt_id,
                    manifest_uri,
                    self._now_ms(),
                ],
            )
        )

    def cancel_complete(self, job_id: str, worker_id: str) -> bool:
        ok = bool(
            self._terminal(
                keys=[self._key(job_id), self.processing_name, self.dead_name],
                args=[worker_id, "cancelled", "", self._now_ms(), job_id, self.job_ttl_seconds],
            )
        )
        if ok:
            self._metadata_status(job_id, "cancelled")
        return ok

    def _retry_delay_ms(self, attempts: int) -> int:
        base = min(self.retry_base_seconds * (2 ** max(0, attempts - 1)), self.retry_max_seconds)
        jitter = random.uniform(0.8, 1.2)
        return max(1, int(base * jitter * 1000))

    def fail(
        self,
        job_id: str,
        worker_id: str,
        error: str,
        retryable: bool = False,
        error_class: str | None = None,
    ) -> bool:
        if retryable:
            attempts = int(self.client.hget(self._key(job_id), "attempts") or 0)
            next_ms = self._now_ms() + self._retry_delay_ms(attempts)
            ok = bool(
                self._retry(
                    keys=[self._key(job_id), self.processing_name, self.retry_name, self.dead_name],
                    args=[
                        worker_id,
                        job_id,
                        self.max_attempts,
                        error,
                        self._now_ms(),
                        next_ms,
                        self.job_ttl_seconds,
                    ],
                )
            )
            if ok:
                record = self.get(job_id)
                status = record.status.value if record else "retrying"
                self._metadata_status(job_id, status, error=error, error_class=error_class)
            return ok
        ok = bool(
            self._terminal(
                keys=[self._key(job_id), self.processing_name, self.dead_name],
                args=[worker_id, "failed", error, self._now_ms(), job_id, self.job_ttl_seconds],
            )
        )
        if ok:
            self._metadata_status(job_id, "failed", error=error, error_class=error_class)
        return ok

    def reap_stale(self) -> int:
        now_ms = self._now_ms()
        reaped = 0
        for job_id in self.client.lrange(self.processing_name, 0, -1):
            key = self._key(job_id)
            status, lease, attempts, shard = self.client.hmget(
                key,
                "status",
                "lease_expires_ms",
                "attempts",
                "shard",
            )
            if status == JobStatus.RUNNING.value and int(lease or 0) > now_ms:
                continue
            next_ms = now_ms + self._retry_delay_ms(int(attempts or 1))
            changed = bool(
                self._reap(
                    keys=[
                        key,
                        self.processing_name,
                        self.retry_name,
                        self.dead_name,
                        self._pending(int(shard or 0)),
                    ],
                    args=[
                        job_id,
                        lease or "0",
                        now_ms,
                        self.max_attempts,
                        next_ms,
                        self.job_ttl_seconds,
                    ],
                )
            )
            reaped += int(changed)
            if changed:
                record = self.get(job_id)
                if record:
                    self._metadata_status(job_id, record.status.value, error=record.error)
        self.promote_retries()
        return reaped

    def _metadata_status(
        self,
        job_id: str,
        status: str,
        result: QueryResult | None = None,
        error: str | None = None,
        error_class: str | None = None,
    ) -> None:
        try:
            self.metadata.update_invocation(
                job_id,
                status,
                result=result,
                error=error,
                error_class=error_class,
            )
        except Exception:
            logger.exception("metadata write failed", extra={"job_id": job_id})

    def ping(self) -> bool:
        return bool(self.client.ping())

    def close(self) -> None:
        self.client.close()
