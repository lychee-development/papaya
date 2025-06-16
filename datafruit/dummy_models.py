from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime

class TestUser(SQLModel, table=True):
    __tablename__ = "test_users"
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(unique=True)
    email: str = Field(unique=True)
    full_name: Optional[str] = None
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

class TestOutput(SQLModel, table=True):
    __tablename__ = "test_output"
    id: Optional[int] = Field(default=None, primary_key=True)
    count_value: int

class TestPost(SQLModel, table=True):
    __tablename__ = "test_posts"
    id: Optional[int] = Field(default=None, primary_key=True)
    title: str = Field(index=True)
    content: str
    published: bool = Field(default=False)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None

class TestProfile(SQLModel, table=True):
    __tablename__ = "test_profiles"
    id: Optional[int] = Field(default=None, primary_key=True)
    bio: str
    age: int
    salary: Optional[float] = None

class TestComplexModel(SQLModel, table=True):
    __tablename__ = "test_complex"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=100)
    score: float = Field(default=0.0)
    tags: Optional[str] = None
    is_verified: bool = Field(default=False)
    metadata_field: Optional[str] = Field(default=None)