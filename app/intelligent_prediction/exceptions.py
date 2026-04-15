"""業務異常（智能預測模組）。"""

from __future__ import annotations

from typing import Any

INTERNAL_SERVER_ERROR_MESSAGE = "伺服器內部錯誤，請稍後再試"


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
    def __init__(self, message: str = "資源不存在") -> None:
        super().__init__(message, code="NOT_FOUND", status_code=404)


class ServiceUnavailableBusinessException(BusinessException):
    def __init__(self, message: str = "服務暫時不可用") -> None:
        super().__init__(message, code="SERVICE_UNAVAILABLE", status_code=503)
