"""Database models."""
from datetime import datetime

from sqlalchemy import JSON, BigInteger, Boolean, DateTime, Integer, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Question(Base):
    __tablename__ = "questions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    question: Mapped[str] = mapped_column(String, nullable=False)
    options: Mapped[list] = mapped_column(JSON, nullable=False)
    correct: Mapped[int] = mapped_column(Integer, nullable=False)
    section: Mapped[str] = mapped_column(String, default="")


class UserResult(Base):
    __tablename__ = "user_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    username: Mapped[str] = mapped_column(String, default="")
    score: Mapped[int] = mapped_column(Integer, nullable=False)
    total: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class AccessRequest(Base):
    """Stores access requests + user profile info."""
    __tablename__ = "access_requests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, unique=True)
    username: Mapped[str] = mapped_column(String, default="")
    # Telegram display name (from API)
    full_name: Mapped[str] = mapped_column(String, default="")
    # User-provided profile (filled after approval)
    pib: Mapped[str] = mapped_column(String, default="")
    study_place: Mapped[str] = mapped_column(String, default="")
    course: Mapped[str] = mapped_column(String, default="")
    phone: Mapped[str] = mapped_column(String, default="")
    email: Mapped[str] = mapped_column(String, default="")
    instagram: Mapped[str] = mapped_column(String, default="")
    # pending / approved / rejected
    status: Mapped[str] = mapped_column(String, default="pending")
    approved: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
