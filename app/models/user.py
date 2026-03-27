from typing import Optional

from pydantic import BaseModel, Field


class UserRegisterRequest(BaseModel):
    """用户注册请求体"""
    username: str = Field(..., min_length=3, max_length=50, description="用户名（3-50字符）")
    password: str = Field(..., min_length=6, max_length=128, description="密码（至少6位）")
    nickname: Optional[str] = Field(None, max_length=50, description="昵称（可选）")


class UserLoginRequest(BaseModel):
    """用户登录请求体"""
    username: str = Field(..., description="用户名")
    password: str = Field(..., description="密码")


class UserInfoResponse(BaseModel):
    """用户信息响应体（不含密码）"""
    id: int
    username: str
    nickname: Optional[str]
    is_active: int
    created_at: str


class LoginResponse(BaseModel):
    """登录成功响应体"""
    access_token: str
    token_type: str = "bearer"
    user: UserInfoResponse
