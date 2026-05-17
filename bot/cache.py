"""Redis-backed caches and locks for hot paths.

Two layers live here:

1. **Caches** (`get_question`, `get_access_request`) — read-through helpers.
   They fall back to the DB on a miss and store the result in Redis. Writes
   in the bot must call the corresponding `invalidate_*` so a stale row is
   never served.

2. **Approved-user index** — a Redis SET that mirrors
   `SELECT user_id FROM access_requests WHERE approved = TRUE`. Used by the
   broadcast filters to avoid a full table scan per send.

3. **Answer lock** — short SET-NX lock keyed on `(user_id, question_id)` to
   collapse double-clicks on quiz answer buttons.

All Redis ops fail open: if Redis is unreachable, the bot falls back to the
DB and keeps working (slower, not broken).
"""
import json
import logging
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db import AccessRequest, Question

logger = logging.getLogger(__name__)

# === Tunables ===
QUESTION_TTL = 0          # 0 = no TTL (questions are near-immutable; invalidated on edit)
ACCESS_REQUEST_TTL = 300  # 5 min — short, so stale profile edits self-heal
ANSWER_LOCK_TTL = 5       # seconds — covers human double-click window


# ---------------------------------------------------------------------------
# Questions
# ---------------------------------------------------------------------------

def _qkey(qid: int) -> str:
    return f"q:{qid}"


async def get_question(redis: Redis, session: AsyncSession, q_id: int) -> Question | None:
    """Return a Question, preferring Redis. Falls back to DB and warms cache."""
    try:
        raw = await redis.get(_qkey(q_id))
        if raw:
            data = json.loads(raw)
            return Question(
                id=data["id"],
                question=data["question"],
                options=data["options"],
                correct=data["correct"],
                section=data.get("section", ""),
            )
    except Exception as e:
        logger.warning("cache get_question miss (redis error): %s", e)

    row = (await session.execute(select(Question).where(Question.id == q_id))).scalar()
    if row is None:
        return None
    await _put_question(redis, row)
    return row


async def _put_question(redis: Redis, q: Question) -> None:
    payload = json.dumps({
        "id": q.id,
        "question": q.question,
        "options": q.options,
        "correct": q.correct,
        "section": q.section or "",
    })
    try:
        if QUESTION_TTL:
            await redis.set(_qkey(q.id), payload, ex=QUESTION_TTL)
        else:
            await redis.set(_qkey(q.id), payload)
    except Exception as e:
        logger.warning("cache put_question failed: %s", e)


async def invalidate_question(redis: Redis, q_id: int) -> None:
    try:
        await redis.delete(_qkey(q_id))
    except Exception as e:
        logger.warning("cache invalidate_question failed: %s", e)


async def warmup_questions(redis: Redis, session: AsyncSession) -> int:
    """Load every Question row into Redis. Returns count loaded."""
    rows = (await session.execute(select(Question))).scalars().all()
    for q in rows:
        await _put_question(redis, q)
    logger.info("Question cache warmed: %d entries", len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# AccessRequest
# ---------------------------------------------------------------------------

_AR_FIELDS = (
    "id", "user_id", "username", "full_name", "pib", "study_place",
    "course", "phone", "email", "instagram", "status", "approved",
)


def _arkey(user_id: int) -> str:
    return f"ar:{user_id}"


def _serialize_ar(req: AccessRequest) -> str:
    return json.dumps({f: getattr(req, f) for f in _AR_FIELDS})


def _deserialize_ar(raw: str) -> AccessRequest:
    data = json.loads(raw)
    return AccessRequest(**data)


async def get_access_request(
    redis: Redis, session: AsyncSession, user_id: int
) -> AccessRequest | None:
    """Return AccessRequest by user_id, preferring Redis."""
    try:
        raw = await redis.get(_arkey(user_id))
        if raw:
            return _deserialize_ar(raw)
    except Exception as e:
        logger.warning("cache get_access_request miss (redis error): %s", e)

    row = (await session.execute(
        select(AccessRequest).where(AccessRequest.user_id == user_id)
    )).scalar()
    if row is None:
        return None
    await put_access_request(redis, row)
    return row


async def put_access_request(redis: Redis, req: AccessRequest) -> None:
    try:
        await redis.set(_arkey(req.user_id), _serialize_ar(req), ex=ACCESS_REQUEST_TTL)
    except Exception as e:
        logger.warning("cache put_access_request failed: %s", e)


async def invalidate_access_request(redis: Redis, user_id: int) -> None:
    try:
        await redis.delete(_arkey(user_id))
    except Exception as e:
        logger.warning("cache invalidate_access_request failed: %s", e)


# ---------------------------------------------------------------------------
# Approved-user SET
# ---------------------------------------------------------------------------

APPROVED_SET = "users:approved"


async def warmup_approved_set(redis: Redis, session: AsyncSession) -> int:
    """Rebuild the approved-user SET from the DB."""
    ids = list((await session.execute(
        select(AccessRequest.user_id).where(AccessRequest.approved == True)
    )).scalars().all())
    try:
        async with redis.pipeline(transaction=False) as pipe:
            pipe.delete(APPROVED_SET)
            if ids:
                pipe.sadd(APPROVED_SET, *ids)
            await pipe.execute()
    except Exception as e:
        logger.warning("approved set warmup failed: %s", e)
    logger.info("Approved-users SET warmed: %d members", len(ids))
    return len(ids)


async def add_approved(redis: Redis, user_id: int) -> None:
    try:
        await redis.sadd(APPROVED_SET, user_id)
    except Exception as e:
        logger.warning("add_approved failed: %s", e)


async def remove_approved(redis: Redis, user_id: int) -> None:
    try:
        await redis.srem(APPROVED_SET, user_id)
    except Exception as e:
        logger.warning("remove_approved failed: %s", e)


async def get_approved_ids(redis: Redis, session: AsyncSession) -> list[int]:
    """Fast path: read from Redis SET. Falls back to DB on cache miss."""
    try:
        raw = await redis.smembers(APPROVED_SET)
        if raw:
            return [int(x) for x in raw]
    except Exception as e:
        logger.warning("get_approved_ids cache miss (redis error): %s", e)

    ids = list((await session.execute(
        select(AccessRequest.user_id).where(AccessRequest.approved == True)
    )).scalars().all())
    # Warm the cache so the next call is fast
    try:
        async with redis.pipeline(transaction=False) as pipe:
            pipe.delete(APPROVED_SET)
            if ids:
                pipe.sadd(APPROVED_SET, *ids)
            await pipe.execute()
    except Exception:
        pass
    return ids


# ---------------------------------------------------------------------------
# Answer lock (anti double-click)
# ---------------------------------------------------------------------------

async def acquire_answer_lock(redis: Redis, user_id: int, q_id: Any) -> bool:
    """Acquire a short-lived SET-NX lock for this (user, question).

    Returns True if the caller may process the answer, False if a recent
    duplicate is being processed. Fails open: returns True on Redis error
    (so we never block answers if Redis is down).
    """
    key = f"ansl:{user_id}:{q_id}"
    try:
        return bool(await redis.set(key, "1", nx=True, ex=ANSWER_LOCK_TTL))
    except Exception as e:
        logger.warning("answer lock redis error (fail open): %s", e)
        return True
