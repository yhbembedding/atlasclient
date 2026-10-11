"""Lỗi của atlasclient. Lỗi HTTP từ BE được phân theo mã trạng thái để ứng dụng xử lý đúng chỗ."""

from __future__ import annotations

import json
from typing import Any


class AtlasError(Exception):
    """Gốc của mọi lỗi atlasclient."""


class APIConnectionError(AtlasError):
    """Không kết nối được tới API (mạng, DNS, proxy cắt kết nối…)."""


class StreamError(AtlasError):
    """Stream đứt mà không nối lại được. `run_id` (nếu đã biết) và `last_seq` để theo dõi tiếp sau bằng
    `runs.follow(run_id, after_seq=last_seq)`; run vẫn chạy tiếp ở worker."""

    def __init__(self, message: str, *, run_id: str | None = None, last_seq: int = 0) -> None:
        self.run_id, self.last_seq = run_id, last_seq
        super().__init__(message)


class ConversationBusy(AtlasError):
    """Cuộc trò chuyện đang có một lượt chưa xong: các lượt trong một phiên phải gửi lần lượt."""


class RunTimeout(AtlasError, TimeoutError):
    def __init__(self, run_id: str, status: str, timeout: float) -> None:
        self.run_id, self.status = run_id, status
        super().__init__(f"run {run_id} still {status} after {timeout}s")


class APIStatusError(AtlasError):
    """BE trả mã lỗi HTTP. `detail` là thông báo của BE (chuỗi, hoặc danh sách lỗi kiểm dữ liệu)."""

    status_code: int = 0

    def __init__(self, status_code: int, detail: Any, *, headers: dict[str, str] | None = None) -> None:
        self.status_code, self.detail, self.headers = status_code, detail, headers or {}
        super().__init__(f"HTTP {status_code}: {detail}")


class AuthenticationError(APIStatusError):
    """401: thiếu token, token sai hoặc hết hạn."""


class BudgetExceeded(APIStatusError):
    """402: ngân sách cứng (tenant / agent / người dùng) đã hết, run không được tạo."""


class PermissionDenied(APIStatusError):
    """403: token thiếu scope, hoặc vai trò không được chạy agent này."""

    @property
    def missing_scope(self) -> str | None:
        text = str(self.detail)
        return text.removeprefix("missing scope: ") if text.startswith("missing scope: ") else None


class NotFound(APIStatusError):
    """404: không có, hoặc thuộc tenant / người dùng khác (BE không lộ sự tồn tại)."""


class Conflict(APIStatusError):
    """409: trạng thái run không cho thao tác này, hoặc run vừa bị sửa đồng thời (thử lại)."""


class PayloadTooLarge(APIStatusError):
    """413: tệp lớn hơn giới hạn của BE."""


class UnsupportedMediaType(APIStatusError):
    """415: loại tệp không được phép, hoặc nội dung không khớp loại khai báo."""


class InvalidRequest(APIStatusError):
    """422: dữ liệu gửi lên không hợp lệ."""


class RateLimited(APIStatusError):
    """429: vượt giới hạn tốc độ. `retry_after`: số giây nên chờ (theo header Retry-After)."""

    @property
    def retry_after(self) -> float | None:
        value = self.headers.get("retry-after")
        try:
            return float(value) if value is not None else None
        except ValueError:
            return None


class ServerError(APIStatusError):
    """5xx."""


_BY_STATUS: dict[int, type[APIStatusError]] = {
    401: AuthenticationError, 402: BudgetExceeded, 403: PermissionDenied, 404: NotFound, 409: Conflict,
    413: PayloadTooLarge, 415: UnsupportedMediaType, 422: InvalidRequest, 429: RateLimited,
}


def error_for(status_code: int, body: bytes | str, headers: dict[str, str] | None = None) -> APIStatusError:
    text = body.decode(errors="replace") if isinstance(body, bytes) else body
    try:
        data = json.loads(text)
        detail = data.get("detail", data) if isinstance(data, dict) else data
    except ValueError:
        detail = text
    cls = _BY_STATUS.get(status_code) or (ServerError if status_code >= 500 else APIStatusError)
    return cls(status_code, detail, headers={k.lower(): v for k, v in (headers or {}).items()})
