"""Phần dùng chung của client đồng bộ và bất đồng bộ: cấu hình, thân request, retry, đọc SSE."""

from __future__ import annotations

import mimetypes
import os
import random
from collections.abc import AsyncIterator, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any
from urllib.parse import quote

import httpx

from atlasclient.errors import AtlasError, RateLimited, ServerError
from atlasclient.events import ApprovalRequired, ChatEvent, Done, Failed
from atlasclient.types import Decision, Media, MediaRef, Run, RunStatus

DEFAULT_TIMEOUT = 60.0
# BE gửi `: keep-alive` mỗi 15 giây khi run im lặng: quá 60 giây không có byte nào là kết nối đã chết.
STREAM_READ_TIMEOUT = 60.0
USER_AGENT = "atlasclient/0.1.0"

MediaLike = Media | MediaRef | str
FileLike = str | os.PathLike | bytes | IO[bytes]


def resolve_base_url(base_url: str | None) -> str:
    url = base_url or os.environ.get("ATLAS_API_URL")
    if not url:
        raise AtlasError("base_url is required (or set ATLAS_API_URL)")
    return url.rstrip("/")


def resolve_token(token: str | None) -> str | None:
    """Token tường minh, hoặc ATLAS_TOKEN, hoặc nội dung file ATLAS_TOKEN_FILE (như atlasctl)."""
    if token:
        return token
    if os.environ.get("ATLAS_TOKEN"):
        return os.environ["ATLAS_TOKEN"].strip()
    path = os.environ.get("ATLAS_TOKEN_FILE")
    if path:
        return Path(path).expanduser().read_text(encoding="utf-8").strip()
    return None


def seg(value: str) -> str:
    """Một đoạn đường dẫn URL (tên agent, id…), mã hoá mọi ký tự đặc biệt."""
    return quote(str(value), safe="")


def media_ids(media: Iterable[MediaLike] | None) -> list[str] | None:
    if media is None:
        return None
    return [m if isinstance(m, str) else m.id for m in media]


def run_body(input: str, *, session_id: str | None, media: Iterable[MediaLike] | None,
             session_state: dict[str, Any] | None, stream: bool = False) -> dict[str, Any]:
    body: dict[str, Any] = {"input": input}
    ids = media_ids(media)
    for key, value in (("session_id", session_id), ("media", ids), ("session_state", session_state)):
        if value is not None:
            body[key] = value
    if stream:
        body["stream"] = True
    return body


def decisions_body(decisions: Iterable[Decision]) -> dict[str, Any]:
    return {"decisions": [d.to_api() for d in decisions]}


def read_file(file: FileLike, name: str | None, mime: str | None) -> tuple[bytes, str | None, str]:
    """(dữ liệu, tên, mime) cho `POST /v1/media`."""
    if isinstance(file, (str, os.PathLike)):
        path = Path(file)
        data, name = path.read_bytes(), name or path.name
    elif isinstance(file, bytes):
        data = file
    else:
        data = file.read()
        name = name or (Path(file.name).name if isinstance(getattr(file, "name", None), str) else None)
    if mime is None and name:
        mime = mimetypes.guess_type(name)[0]
    if not mime:
        raise AtlasError("cannot guess the file type: pass mime=…")
    return data, name, mime


def stream_timeout(timeout: httpx.Timeout) -> httpx.Timeout:
    return httpx.Timeout(timeout.connect, read=STREAM_READ_TIMEOUT, write=timeout.write, pool=timeout.pool)


# ============================================================================ retry

IDEMPOTENT = frozenset({"GET", "HEAD", "PUT", "DELETE", "OPTIONS"})
RETRY_STATUS = frozenset({429, 502, 503, 504})


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = 2
    backoff: float = 0.5
    max_backoff: float = 8.0

    def retry_on_error(self, method: str, exc: Exception, attempt: int) -> bool:
        if attempt >= self.max_retries:
            return False
        # Chưa kết nối được thì request chưa tới BE: thử lại an toàn với mọi method.
        if isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)):
            return True
        return method in IDEMPOTENT and isinstance(exc, (httpx.ReadError, httpx.RemoteProtocolError,
                                                           httpx.ReadTimeout))

    def retry_on_status(self, method: str, status: int, attempt: int) -> bool:
        if attempt >= self.max_retries or status not in RETRY_STATUS:
            return False
        # 429 bị chặn ở bước xác thực, trước khi handler chạy: POST thử lại không tạo trùng.
        return status == 429 or method in IDEMPOTENT

    def delay(self, attempt: int, error: Exception | None = None) -> float:
        if isinstance(error, RateLimited) and error.retry_after is not None:
            return min(error.retry_after, self.max_backoff * 4)
        base = min(self.backoff * (2 ** attempt), self.max_backoff)
        return base * (0.5 + random.random() / 2)


# ============================================================================ SSE


@dataclass
class SSEFrame:
    event: str | None
    data: str
    id: str | None


class SSEDecoder:
    """Đọc từng dòng của `text/event-stream`. Dòng chú thích (`: keep-alive`) bị bỏ qua."""

    def __init__(self) -> None:
        self._event: str | None = None
        self._data: list[str] = []
        self._id: str | None = None

    def feed(self, line: str) -> SSEFrame | None:
        line = line.rstrip("\r")
        if not line:
            if not self._data:
                self._event = self._id = None
                return None
            frame = SSEFrame(self._event, "\n".join(self._data), self._id)
            self._event, self._data, self._id = None, [], None
            return frame
        if line.startswith(":"):
            return None
        name, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if name == "data":
            self._data.append(value)
        elif name == "event":
            self._event = value
        elif name == "id":
            self._id = value
        return None


def headers_dict(headers: httpx.Headers) -> dict[str, str]:
    return {k.lower(): v for k, v in headers.items()}


def reconnectable(exc: Exception, *, run_id: str | None) -> bool:
    """Stream đứt vì lỗi này thì có nối lại được không (chưa tính 401, xem client)."""
    if run_id is None:
        # Chưa nhận được sự kiện nào: chỉ an toàn khi chắc chắn request tạo run chưa tới BE.
        return isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout, RateLimited))
    return isinstance(exc, (httpx.TransportError, RateLimited, ServerError))


def final_event(run: Run) -> ChatEvent | None:
    """Sự kiện kết thúc dựng từ trạng thái run (khi stream đóng trước khi kịp gửi sự kiện kết thúc)."""
    if run.status is RunStatus.SUCCEEDED:
        return Done(run.id, run.text, run.output, run.usage)
    if run.status is RunStatus.WAITING:
        return ApprovalRequired(run.id, list(run.pending))
    if run.status.terminal:
        return Failed(run.id, run.error or run.status.value, run.status)
    return None


FINAL_CHAT_EVENTS = (ApprovalRequired, Done, Failed)


# Sau sự kiện kết thúc BE còn một truy vấn rồi tự đóng stream. Đọc nốt tới khi đóng thay vì cắt ngang: cắt ngang làm
# server huỷ request giữa truy vấn DB. Gặp frame mới (run đã chạy tiếp ngay) thì thôi, không chờ.
def drain(lines: Iterator[str], decoder: SSEDecoder) -> None:
    try:
        for line in lines:
            if decoder.feed(line) is not None:
                return
    except httpx.TransportError:
        pass


async def adrain(lines: AsyncIterator[str], decoder: SSEDecoder) -> None:
    try:
        async for line in lines:
            if decoder.feed(line) is not None:
                return
    except httpx.TransportError:
        pass
