"""Authentication module for the CGDA backend."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext

from config import settings

pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
# When auth is disabled, allow requests with no Authorization header (no 401 pre-check).
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login", auto_error=not settings.disable_auth)


@dataclass(frozen=True)
class User:
    username: str
    role: str  # commissioner/admin


_USERS: dict[str, dict] = {
    # Password hashes are computed at process start for MVP simplicity (avoid storing hashes in repo).
    settings.commissioner_username: {
        "password_hash": pwd_context.hash(settings.commissioner_password),
        "role": "commissioner",
    },
    settings.admin_username: {
        "password_hash": pwd_context.hash(settings.admin_password),
        "role": "admin",
    },
    settings.it_head_username: {
        "password_hash": pwd_context.hash(settings.it_head_password),
        "role": "it_head",
    },
}


def verify_password(plain_password: str, password_hash: str) -> bool:
    return pwd_context.verify(plain_password, password_hash)


def authenticate_user(username: str, password: str) -> User | None:
    record = _USERS.get(username)
    if not record:
        return None
    if not verify_password(password, record["password_hash"]):
        return None
    return User(username=username, role=record["role"])


def create_access_token(*, sub: str, role: str) -> str:
    exp = dt.datetime.utcnow() + dt.timedelta(minutes=settings.jwt_exp_minutes)
    payload = {"sub": sub, "role": role, "exp": exp}
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def decode_token(token: str) -> User:
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
        username = payload.get("sub")
        role = payload.get("role")
        if not username or not role:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
        return User(username=username, role=role)
    except JWTError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token") from e


def get_current_user(token: Annotated[str | None, Depends(oauth2_scheme)]) -> User:
    if settings.disable_auth:
        # Default role used throughout the app for read-only dashboards
        return User(username="public", role="commissioner")
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    return decode_token(token)


def require_role(*allowed: str):
    def _dep(user: Annotated[User, Depends(get_current_user)]) -> User:
        if user.role not in allowed:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
        return user

    return _dep


