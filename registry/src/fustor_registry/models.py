from datetime import datetime, UTC
from enum import Enum
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Enum as SQLEnum, JSON
from sqlalchemy.orm import relationship, declarative_base

StateBase = declarative_base()

class UserLevel(str, Enum):
    NORMAL = "normal"
    ADMIN = "admin"

class UserModel(StateBase):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    email = Column(String, unique=True, index=True)
    password = Column(String)
    level = Column(SQLEnum(UserLevel), default=UserLevel.NORMAL)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now(UTC))
    updated_at = Column(DateTime, default=datetime.now(UTC), onupdate=datetime.now(UTC))

    api_keys = relationship("UserAPIKeyModel", back_populates="user")
    datastores = relationship("DatastoreModel", back_populates="owner")

class UserAPIKeyModel(StateBase):
    __tablename__ = "user_api_keys"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    datastore_id = Column(Integer, ForeignKey("datastores.id"), nullable=False) # NEW FIELD
    key = Column(String, unique=True, index=True)
    secret = Column(String) # Hashed secret
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now(UTC))

    user = relationship("UserModel", back_populates="api_keys")
    datastore = relationship("DatastoreModel") # NEW RELATIONSHIP

class TokenBlacklistModel(StateBase):
    __tablename__ = "token_blacklist"

    id = Column(Integer, primary_key=True, index=True)
    token = Column(String, unique=True, index=True)
    expires_at = Column(DateTime, index=True)
    created_at = Column(DateTime, default=datetime.now(UTC))

class DatastoreModel(StateBase):
    __tablename__ = "datastores"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    description = Column(String, nullable=True)
    owner_id = Column(Integer, ForeignKey("users.id"))
    meta = Column(JSON, nullable=True)
    visible = Column(Boolean, default=True)
    allow_concurrent_push = Column(Boolean, default=False) # Re-added
    session_timeout_seconds = Column(Integer, default=30) # Re-added
    created_at = Column(DateTime, default=datetime.now(UTC))
    updated_at = Column(DateTime, default=datetime.now(UTC), onupdate=datetime.now(UTC))

    owner = relationship("UserModel", back_populates="datastores")