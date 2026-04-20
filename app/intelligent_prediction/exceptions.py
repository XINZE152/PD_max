"""业务异常（智能预测模块）。"""

from __future__ import annotations

from typing import Any

INTERNAL_SERVER_ERROR_MESSAGE = "服务器内部错误，请稍后再试"


class BusinessException(Exception):
    def __init__(
        self,
        message: str,
        *,
        code: str = "BUSINESS_ERROR",
        status_code: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.status_code = status_code
        self.details = details or {}


class ValidationBusinessException(BusinessException):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, code="VALIDATION_ERROR", status_code=422, details=details)


class NotFoundBusinessException(BusinessException):
    def __init__(self, message: str = "资源不存在") -> None:
        super().__init__(message, code="NOT_FOUND", status_code=404)


class ServiceUnavailableBusinessException(BusinessException):
    def __init__(self, message: str = "服务暂时不可用") -> None:
        super().__init__(message, code="SERVICE_UNAVAILABLE", status_code=503)
