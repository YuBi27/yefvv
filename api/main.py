"""FastAPI service for ЄФВВ bot."""
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import Depends, FastAPI, HTTPException, Query, Security
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from models import AccessRequest, Question, UserResult

import os

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql+asyncpg://bot:bot@postgres:5432/botdb"
)
API_KEY = os.environ.get("API_KEY", "supersecrettoken123")

engine = create_async_engine(DATABASE_URL, echo=False)
session_factory = async_sessionmaker(engine, expire_on_commit=False)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_api_key(key: str = Security(api_key_header)):
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return key


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await engine.dispose()


app = FastAPI(
    title="ЄФВВ Bot API",
    description="REST API для телеграм-бота підготовки до ЄФВВ",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Public endpoints
# ---------------------------------------------------------------------------

@app.get("/health", tags=["System"])
async def health():
    """Перевірка стану сервісу."""
    return {"status": "ok", "service": "efvv-bot-api"}


# @app.get("/api/stats", tags=["Stats"])
# async def get_stats():
#     """Загальна статистика: юзери, результати, питання."""
#     cutoff = datetime.utcnow() - timedelta(days=7)
#     async with session_factory() as session:
#         approved_users = (await session.execute(
#             select(func.count()).select_from(AccessRequest)
#             .where(AccessRequest.approved == True)
#         )).scalar()

#         results_7d = (await session.execute(
#             select(func.count()).select_from(UserResult)
#             .where(UserResult.created_at >= cutoff)
#         )).scalar()

#         total_results = (await session.execute(
#             select(func.count()).select_from(UserResult)
#         )).scalar()

#         total_questions = (await session.execute(
#             select(func.count()).select_from(Question)
#         )).scalar()

#         sections = (await session.execute(
#             select(Question.section, func.count().label("count"))
#             .group_by(Question.section)
#             .order_by(Question.section)
#         )).all()

#     return {
#         "approved_users": approved_users,
#         "results_last_7_days": results_7d,
#         "results_total": total_results,
#         "questions_total": total_questions,
#         "sections": [{"name": s, "count": c} for s, c in sections],
#     }


# @app.get("/api/questions", tags=["Questions"])
# async def get_questions(
#     section: str | None = Query(None, description="Фільтр по розділу"),
#     page: int = Query(1, ge=1),
#     limit: int = Query(20, ge=1, le=100),
# ):
#     """Список питань з пагінацією. Публічний endpoint."""
#     offset = (page - 1) * limit
#     async with session_factory() as session:
#         q = select(Question).order_by(Question.id)
#         if section:
#             q = q.where(Question.section == section)

#         total = (await session.execute(
#             select(func.count()).select_from(q.subquery())
#         )).scalar()

#         rows = (await session.execute(q.offset(offset).limit(limit))).scalars().all()

#     return {
#         "total": total,
#         "page": page,
#         "limit": limit,
#         "pages": (total + limit - 1) // limit,
#         "data": [
#             {
#                 "id": r.id,
#                 "section": r.section,
#                 "question": r.question,
#                 "options": r.options,
#                 "correct": r.correct,
#             }
#             for r in rows
#         ],
#     }


# @app.get("/api/questions/{question_id}", tags=["Questions"])
# async def get_question(question_id: int):
#     """Отримати одне питання за ID."""
#     async with session_factory() as session:
#         q = (await session.execute(
#             select(Question).where(Question.id == question_id)
#         )).scalar()
#     if not q:
#         raise HTTPException(status_code=404, detail="Question not found")
#     return {
#         "id": q.id,
#         "section": q.section,
#         "question": q.question,
#         "options": q.options,
#         "correct": q.correct,
#     }


# # ---------------------------------------------------------------------------
# # Protected endpoints (require X-API-Key)
# # ---------------------------------------------------------------------------

# @app.get("/api/results", tags=["Results"], dependencies=[Depends(require_api_key)])
# async def get_results(
#     page: int = Query(1, ge=1),
#     limit: int = Query(20, ge=1, le=100),
#     days: int = Query(7, ge=1, le=90, description="За скільки днів"),
# ):
#     """Результати тестів за вказаний період. Потребує X-API-Key."""
#     cutoff = datetime.utcnow() - timedelta(days=days)
#     offset = (page - 1) * limit

#     async with session_factory() as session:
#         total = (await session.execute(
#             select(func.count()).select_from(UserResult)
#             .where(UserResult.created_at >= cutoff)
#         )).scalar()

#         rows = (await session.execute(
#             select(UserResult)
#             .where(UserResult.created_at >= cutoff)
#             .order_by(UserResult.created_at.desc())
#             .offset(offset).limit(limit)
#         )).scalars().all()

#         user_ids = [r.user_id for r in rows]
#         profiles = {
#             p.user_id: p
#             for p in (await session.execute(
#                 select(AccessRequest).where(AccessRequest.user_id.in_(user_ids))
#             )).scalars().all()
#         }

#     return {
#         "total": total,
#         "page": page,
#         "limit": limit,
#         "pages": (total + limit - 1) // limit,
#         "days": days,
#         "data": [
#             {
#                 "user_id": r.user_id,
#                 "username": r.username,
#                 "score": r.score,
#                 "total": r.total,
#                 "percent": r.score * 100 // r.total,
#                 "date": r.created_at.isoformat() if r.created_at else None,
#                 "pib": profiles[r.user_id].pib if r.user_id in profiles else "",
#                 "phone": profiles[r.user_id].phone if r.user_id in profiles else "",
#                 "email": profiles[r.user_id].email if r.user_id in profiles else "",
#                 "study_place": profiles[r.user_id].study_place if r.user_id in profiles else "",
#                 "course": profiles[r.user_id].course if r.user_id in profiles else "",
#                 "instagram": profiles[r.user_id].instagram if r.user_id in profiles else "",
#             }
#             for r in rows
#         ],
#     }


# @app.get("/api/users", tags=["Users"], dependencies=[Depends(require_api_key)])
# async def get_users(
#     status: str | None = Query(None, description="pending / approved / rejected"),
#     page: int = Query(1, ge=1),
#     limit: int = Query(20, ge=1, le=100),
# ):
#     """Список користувачів. Потребує X-API-Key."""
#     offset = (page - 1) * limit
#     async with session_factory() as session:
#         q = select(AccessRequest).order_by(AccessRequest.created_at.desc())
#         if status:
#             q = q.where(AccessRequest.status == status)

#         total = (await session.execute(
#             select(func.count()).select_from(q.subquery())
#         )).scalar()

#         rows = (await session.execute(q.offset(offset).limit(limit))).scalars().all()

#     return {
#         "total": total,
#         "page": page,
#         "limit": limit,
#         "pages": (total + limit - 1) // limit,
#         "data": [
#             {
#                 "user_id": r.user_id,
#                 "username": r.username,
#                 "full_name": r.full_name,
#                 "pib": r.pib,
#                 "phone": r.phone,
#                 "email": r.email,
#                 "study_place": r.study_place,
#                 "course": r.course,
#                 "instagram": r.instagram,
#                 "status": r.status,
#                 "approved": r.approved,
#                 "created_at": r.created_at.isoformat() if r.created_at else None,
#             }
#             for r in rows
#         ],
#     }
