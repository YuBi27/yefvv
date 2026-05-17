"""Redis-backed rate limiting middleware for the quiz bot.

Tunable constants live at the top of this file. Restart the bot after editing.
"""
import logging
import time
import uuid
from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Message, TelegramObject
from redis.asyncio import Redis

logger = logging.getLogger(__name__)

# ===========================================================================
# RATE LIMIT CONFIG  --  edit these values to tune protection.
#
# Each bucket maps to (limit, window_seconds):
#   limit  -- maximum events allowed within the window
#   window -- sliding window length in seconds
#
# Admins (ADMIN_IDS) bypass all buckets.
# ===========================================================================

LIMITS: dict[str, tuple[int, int]] = {
    "message_global":  (20, 60),    # any incoming message:        20 per 60s
    "callback_global": (30, 60),    # any inline-button callback:  30 per 60s
    "command":         (5,  30),    # /start /quiz /stats /admin /cancel: 5 per 30s
    "screenshot":      (10, 300),   # photo or document upload:    10 per 5min
    "access_request":  (3,  3600),  # access-request creation:     3  per 1h
}

# User-facing warnings shown when a bucket is exceeded.
# Set value to None to silently drop instead of warning.
WARN_MESSAGES: dict[str, str | None] = {
    "message_global":  "⏳ Не так швидко — зачекай трохи.",
    "callback_global": "Не так швидко",
    "command":         "⏳ Занадто багато команд. Зачекай 30 секунд.",
    "screenshot":      "📸 Достатньо скріншотів. Зачекай 5 хвилин.",
    "access_request":  "⏳ Занадто багато заявок. Спробуй пізніше.",
}

# Minimum gap (seconds) between two warnings to the same user for the same
# bucket. Stops the bot from spamming itself with warnings.
WARN_COOLDOWN_SEC = 30

# Commands that count toward the "command" bucket.
RATELIMITED_COMMANDS = {"/start", "/quiz", "/stats", "/admin", "/cancel"}

# ===========================================================================


async def _check_bucket(redis: Redis, user_id: int, bucket: str) -> bool:
    """Sliding-window check via a Redis sorted set.

    Returns True if the event is allowed, False if it must be blocked.
    Fails open on Redis errors so the bot keeps working if Redis is down.
    """
    if bucket not in LIMITS:
        return True
    limit, window = LIMITS[bucket]
    key = f"rl:{bucket}:{user_id}"
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - window * 1000
    member = f"{now_ms}:{uuid.uuid4().hex[:8]}"
    try:
        async with redis.pipeline(transaction=False) as pipe:
            pipe.zremrangebyscore(key, 0, cutoff)
            pipe.zadd(key, {member: now_ms})
            pipe.zcard(key)
            pipe.expire(key, window + 1)
            results = await pipe.execute()
        count = int(results[2])
        return count <= limit
    except Exception as e:
        logger.warning("Rate-limit Redis error (bucket=%s, user=%s): %s", bucket, user_id, e)
        return True


async def _should_warn(redis: Redis, user_id: int, bucket: str) -> bool:
    """Take a per-(user, bucket) warning lock with TTL = WARN_COOLDOWN_SEC."""
    key = f"rl:warn:{bucket}:{user_id}"
    try:
        return bool(await redis.set(key, "1", nx=True, ex=WARN_COOLDOWN_SEC))
    except Exception:
        return False


class _BaseRateLimit(BaseMiddleware):
    def __init__(self, redis: Redis, admin_ids: set[int]):
        self.redis = redis
        self.admin_ids = admin_ids

    @staticmethod
    def _user_id(event: TelegramObject) -> int | None:
        user = getattr(event, "from_user", None)
        return user.id if user else None


class MessageRateLimit(_BaseRateLimit):
    """Rate-limits all incoming Message updates."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: dict[str, Any],
    ) -> Any:
        user_id = self._user_id(event)
        if user_id is None or user_id in self.admin_ids:
            return await handler(event, data)

        buckets: list[str] = ["message_global"]

        text = (event.text or event.caption or "").strip()
        if text.startswith("/"):
            cmd = text.split()[0].split("@")[0].lower()
            if cmd in RATELIMITED_COMMANDS:
                buckets.append("command")

        # Photos and documents from non-admins only legitimately appear during
        # the access-request flow, so we charge both the screenshot and the
        # access-request buckets.
        if event.photo or event.document:
            buckets.append("screenshot")
            buckets.append("access_request")

        for bucket in buckets:
            if not await _check_bucket(self.redis, user_id, bucket):
                logger.info("Rate-limit hit: user=%s bucket=%s", user_id, bucket)
                warn = WARN_MESSAGES.get(bucket)
                if warn and await _should_warn(self.redis, user_id, bucket):
                    try:
                        await event.answer(warn)
                    except Exception as e:
                        logger.debug("Could not send rate-limit warning: %s", e)
                return

        return await handler(event, data)


class CallbackRateLimit(_BaseRateLimit):
    """Rate-limits all incoming CallbackQuery updates."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: CallbackQuery,
        data: dict[str, Any],
    ) -> Any:
        user_id = self._user_id(event)
        if user_id is None or user_id in self.admin_ids:
            return await handler(event, data)

        if not await _check_bucket(self.redis, user_id, "callback_global"):
            logger.info("Rate-limit hit: user=%s bucket=callback_global", user_id)
            warn = WARN_MESSAGES.get("callback_global")
            if warn:
                try:
                    await event.answer(warn, show_alert=False)
                except Exception as e:
                    logger.debug("Could not answer rate-limited callback: %s", e)
            return

        return await handler(event, data)
