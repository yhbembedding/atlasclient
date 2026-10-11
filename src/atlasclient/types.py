"""Kiểu dữ liệu trả về từ API của Atlas (bản sao phía client của `atlas.kernel.types` và các thân trả lời API).

Mọi model nhận thêm trường lạ (`extra="allow"`): BE thêm trường mới thì client cũ vẫn đọc được.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class _Model(BaseModel):
    model_config = ConfigDict(extra="allow")


class RunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING = "waiting"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in (RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED)

    @property
    def stopped(self) -> bool:
        """Run không tự chạy tiếp nữa: đã xong, hoặc đang chờ người (duyệt / nhập liệu)."""
        return self.terminal or self is RunStatus.WAITING


class PendingKind(str, Enum):
    CONFIRMATION = "confirmation"
    USER_INPUT = "user_input"
    EXTERNAL = "external_execution"
    # Tool có tác dụng phụ đang chạy thì worker chết: duyệt = chạy lại, từ chối = báo model là không rõ kết quả.
    RECOVERY = "recovery"


class ToolCall(_Model):
    id: str
    name: str
    arguments: dict[str, Any] = Field(default_factory=dict)


class MediaRef(_Model):
    id: str
    kind: str
    mime: str
    name: str | None = None
    size: int = 0
    sha256: str | None = None


class Message(_Model):
    role: str
    content: str | None = None
    media: list[MediaRef] = Field(default_factory=list)
    tool_calls: list[ToolCall] = Field(default_factory=list)
    tool_call_id: str | None = None
    name: str | None = None
    is_error: bool = False
    kind: str | None = None


class Usage(_Model):
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    reasoning_tokens: int = 0
    estimated: bool = False

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.cache_read_tokens + self.cache_write_tokens + self.output_tokens


class PendingToolCall(_Model):
    """Một tool call đang chờ con người (duyệt, nhập liệu) hoặc hệ thống ngoài thực thi."""

    call: ToolCall
    kind: PendingKind | str
    user_input_fields: list[str] = Field(default_factory=list)
    # Run con (thành viên Team / bước Workflow) nơi tool call thực sự đang chờ; None = chính run này.
    origin_run_id: str | None = None

    @property
    def tool_call_id(self) -> str:
        return self.call.id

    @property
    def tool_name(self) -> str:
        return self.call.name

    @property
    def arguments(self) -> dict[str, Any]:
        return self.call.arguments


def _call_id(target: PendingToolCall | str) -> str:
    return target.call.id if isinstance(target, PendingToolCall) else target


class Decision(BaseModel):
    """Quyết định cho một PendingToolCall. Dùng các hàm tạo: `Decision.approve(p)`, `.deny(p)`, `.input(p, …)`,
    `.external(p, result)`."""

    tool_call_id: str
    confirmed: bool = True
    user_input: dict[str, Any] = Field(default_factory=dict)
    result: str | None = None
    note: str | None = None

    @classmethod
    def approve(cls, target: PendingToolCall | str, *, note: str | None = None) -> Decision:
        return cls(tool_call_id=_call_id(target), confirmed=True, note=note)

    @classmethod
    def deny(cls, target: PendingToolCall | str, *, note: str | None = None) -> Decision:
        return cls(tool_call_id=_call_id(target), confirmed=False, note=note)

    @classmethod
    def input(cls, target: PendingToolCall | str, values: dict[str, Any] | None = None, /,
              **fields: Any) -> Decision:
        """USER_INPUT: giá trị người dùng điền, trộn vào tham số tool."""
        return cls(tool_call_id=_call_id(target), user_input={**(values or {}), **fields})

    @classmethod
    def external(cls, target: PendingToolCall | str, result: str) -> Decision:
        """EXTERNAL: kết quả do hệ thống của bạn tự chạy tool rồi trả về."""
        return cls(tool_call_id=_call_id(target), result=result)

    def to_api(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=True)


class Run(_Model):
    """Một lần chạy agent / team / workflow (thân `RunOut` của API)."""

    id: str
    tenant_id: str | None = None
    agent_name: str
    session_id: str
    user_id: str | None = None
    parent_run_id: str | None = None
    status: RunStatus
    input: str = ""
    content: str | None = None
    output: Any = None
    pending: list[PendingToolCall] = Field(default_factory=list)
    usage: Usage = Field(default_factory=Usage)
    step: int = 0
    error: str | None = None
    finish_reason: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    # Chỉ có khi gọi `runs.get(..., include_messages=True)`.
    messages: list[Message] | None = None

    @property
    def text(self) -> str:
        """Câu trả lời cuối của agent ("" khi chưa có)."""
        return self.content or ""

    @property
    def needs_action(self) -> bool:
        """Run đang chờ người: xem `pending`, rồi gửi quyết định."""
        return self.status is RunStatus.WAITING

    @property
    def succeeded(self) -> bool:
        return self.status is RunStatus.SUCCEEDED


class Identity(_Model):
    """`GET /v1/me`: danh tính đã xác minh của token đang dùng."""

    id: str
    tenant_id: str
    kind: str = "user"
    roles: list[str] = Field(default_factory=list)
    scopes: list[str] = Field(default_factory=list)
    via: str | None = None

    def can(self, scope: str) -> bool:
        """Chỉ để ẩn/hiện chức năng trên giao diện. Quyền thật do BE kiểm ở từng request."""
        return "*" in self.scopes or scope in self.scopes


class AgentInfo(_Model):
    name: str
    description: str | None = None
    source: str | None = None  # code | studio | external
    kind: str = "agent"  # agent | team | workflow
    version: int | None = None
    allowed_roles: list[str] | None = None


class WorkflowInfo(_Model):
    name: str
    kind: str  # team | workflow
    description: str | None = None
    source: str | None = None
    version: int | None = None


class SessionInfo(_Model):
    """Một dòng của `GET /v1/sessions`."""

    id: str
    agent_name: str
    user_id: str | None = None
    title: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class Session(_Model):
    id: str
    tenant_id: str | None = None
    agent_name: str
    user_id: str | None = None
    state: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class SessionDetail(_Model):
    """`GET /v1/sessions/{id}`: phiên và các run của nó (cũ nhất trước)."""

    session: Session
    runs: list[Run] = Field(default_factory=list)

    @property
    def turns(self) -> list[tuple[str, str | None]]:
        """(câu người dùng, câu trả lời) của các run cấp cao nhất, để hiển thị lại lịch sử chat."""
        return [(r.input, r.content) for r in self.runs if r.parent_run_id is None]


class Media(_Model):
    id: str
    kind: str
    mime: str
    name: str | None = None
    size: int = 0
    sha256: str | None = None
    tenant_id: str | None = None
    user_id: str | None = None
    created_at: datetime | None = None
