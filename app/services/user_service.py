"""
用户模块服务层
负责用户注册、登录、密码加密及 JWT Token 签发
"""
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from jose import JWTError, jwt
from passlib.context import CryptContext

from app import config
from app.database import get_conn

logger = logging.getLogger(__name__)

# bcrypt 密码加密上下文
_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# ==================== 密码工具 ====================

def hash_password(plain_password: str) -> str:
    """对明文密码进行 bcrypt 哈希，返回哈希字符串"""
    return _pwd_context.hash(plain_password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """验证明文密码与哈希是否匹配"""
    return _pwd_context.verify(plain_password, hashed_password)


# ==================== JWT 工具 ====================

def create_access_token(payload: Dict[str, Any]) -> str:
    """签发 JWT access token，自动写入过期时间"""
    data = payload.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=config.JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
    data["exp"] = expire
    return jwt.encode(data, config.JWT_SECRET_KEY, algorithm=config.JWT_ALGORITHM)


def decode_access_token(token: str) -> Optional[Dict[str, Any]]:
    """解码并验证 JWT token，失败返回 None"""
    try:
        return jwt.decode(token, config.JWT_SECRET_KEY, algorithms=[config.JWT_ALGORITHM])
    except JWTError:
        return None


# ==================== 用户服务 ====================

class UserService:

    # ---------- 注册 ----------

    def register(self, username: str, password: str, nickname: Optional[str] = None) -> Dict[str, Any]:
        """
        注册新用户。
        - 检查用户名是否已存在
        - 密码 bcrypt 加密后存储
        - 返回新用户信息（不含密码）
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查用户名唯一性
                cur.execute(
                    "SELECT id FROM users WHERE username = %s",
                    (username,),
                )
                if cur.fetchone():
                    raise ValueError(f"用户名 '{username}' 已被注册")

                hashed = hash_password(password)
                cur.execute(
                    "INSERT INTO users (username, hashed_password, nickname) VALUES (%s, %s, %s)",
                    (username, hashed, nickname),
                )
                new_id = cur.lastrowid

        logger.info(f"新用户注册成功: id={new_id}, username={username}")
        return self._get_user_by_id(new_id)

    # ---------- 登录 ----------

    def login(self, username: str, password: str) -> Dict[str, Any]:
        """
        用户登录。
        - 校验用户名与密码
        - 签发 JWT access token
        - 返回 token 及用户信息
        """
        user = self._get_user_by_username(username)
        if not user:
            raise ValueError("用户名或密码错误")
        if not user["is_active"]:
            raise ValueError("该账户已被禁用，请联系管理员")
        if not verify_password(password, user["hashed_password"]):
            raise ValueError("用户名或密码错误")

        token = create_access_token({"sub": str(user["id"]), "username": user["username"]})

        user_info = {k: v for k, v in user.items() if k != "hashed_password"}
        # 将 datetime 转为字符串以便序列化
        if isinstance(user_info.get("created_at"), datetime):
            user_info["created_at"] = user_info["created_at"].strftime("%Y-%m-%d %H:%M:%S")

        logger.info(f"用户登录成功: id={user['id']}, username={username}")
        return {
            "access_token": token,
            "token_type": "bearer",
            "user": user_info,
        }

    # ---------- 内部查询 ----------

    def _get_user_by_id(self, user_id: int) -> Dict[str, Any]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, username, nickname, is_active, created_at "
                    "FROM users WHERE id = %s",
                    (user_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise ValueError(f"用户 id={user_id} 不存在")
                columns = [desc[0] for desc in cur.description]
                user = dict(zip(columns, row))
                if isinstance(user.get("created_at"), datetime):
                    user["created_at"] = user["created_at"].strftime("%Y-%m-%d %H:%M:%S")
                return user

    def _get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, username, hashed_password, nickname, is_active, created_at "
                    "FROM users WHERE username = %s",
                    (username,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, row))


# ==================== 单例工厂 ====================

_user_service: Optional[UserService] = None


def get_user_service() -> UserService:
    global _user_service
    if _user_service is None:
        _user_service = UserService()
    return _user_service
