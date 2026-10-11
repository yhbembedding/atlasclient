"""Sự kiện của một run.

Hai tầng:

- **Sự kiện thô** (`RunEvent`): đúng như BE ghi vào event log (`token_delta`, `tool_call_started`, `llm_call`…).
  `parse_event` đọc mọi loại; loại BE mới thêm mà client chưa biết thành `UnknownEvent` thay vì lỗi.
- **Sự kiện chat** (`ChatEvent`): gọn cho giao diện chatbot, do `ChatEventMapper` dịch từ sự kiện thô:
  `TextDelta`, `TextRetracted`, `Thinking`, `ToolStarted`, `ToolFinished`, `MemberStarted`, `MemberFinished`,
  `ApprovalRequired`, `Done`, `Failed`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field

from atlasclient.types import PendingToolCall, RunStatus, Usage

# ============================================================================ sự kiện thô


class _Event(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: str
    run_id: str
    seq: int = 0
    ts: datetime | None = None


class RunStarted(_Event):
    type: Literal["run_started"] = "run_started"
    agent_name: str
    session_id: str


class RunContinued(_Event):
    type: Literal["run_continued"] = "run_continued"


class RunResumed(_Event):
    """Worker nhận lại run từ checkpoint (worker trước chết)."""

    type: Literal["run_resumed"] = "run_resumed"


class TokenDelta(_Event):
    type: Literal["token_delta"] = "token_delta"
    text: str


class TokensRetracted(_Event):
    """Lời gọi model hỏng sau khi đã stream một phần chữ: `text` không thuộc câu trả lời, cắt khỏi cuối."""

    type: Literal["tokens_retracted"] = "tokens_retracted"
    step: int = 0
    text: str


class LLMCall(_Event):
    type: Literal["llm_call"] = "llm_call"
    step: int = 0
    provider: str
    model: str
    key_id: str | None = None
    usage: Usage = Field(default_factory=Usage)
    latency_ms: float = 0.0
    attempt: int = 1
    fallback_index: int = 0
    cost_pico_usd: int | None = None


class LLMError(_Event):
    type: Literal["llm_error"] = "llm_error"
    step: int = 0
    provider: str
    model: str
    error: str
    retryable: bool = False
    attempt: int = 1


class KeyFailover(_Event):
    type: Literal["key_failover"] = "key_failover"
    step: int = 0
    provider: str
    model: str
    from_key_id: str
    reason: str


class ContextShrunk(_Event):
    type: Literal["context_shrunk"] = "context_shrunk"
    step: int = 0
    provider: str
    model: str
    level: int = 0
    dropped_messages: int = 0
    truncated_results: int = 0
    chars_before: int = 0
    chars_after: int = 0


class ContextSummarized(_Event):
    type: Literal["context_summarized"] = "context_summarized"
    step: int = 0
    model: str
    summarized_messages: int = 0
    tokens_before: int = 0
    tokens_after: int = 0


class ToolCallStarted(_Event):
    type: Literal["tool_call_started"] = "tool_call_started"
    tool_call_id: str
    tool_name: str
    arguments: dict[str, Any] = Field(default_factory=dict)


class ToolCallCompleted(_Event):
    type: Literal["tool_call_completed"] = "tool_call_completed"
    tool_call_id: str
    tool_name: str
    result: str = ""
    is_error: bool = False
    duration_ms: float = 0.0
    cached: bool = False
    result_tokens: int = 0


class ToolPolicyDecision(_Event):
    type: Literal["tool_policy_decision"] = "tool_policy_decision"
    tool_call_id: str
    tool_name: str
    decision: str
    reason: str | None = None
    policy: str | None = None


class ApprovalRequested(_Event):
    type: Literal["approval_requested"] = "approval_requested"
    pending: list[PendingToolCall] = Field(default_factory=list)


class RunPaused(_Event):
    type: Literal["run_paused"] = "run_paused"


class GuardrailTriggered(_Event):
    type: Literal["guardrail_triggered"] = "guardrail_triggered"
    guardrail: str
    reason: str


class BudgetBlocked(_Event):
    type: Literal["budget_blocked"] = "budget_blocked"
    budget_id: str
    scope: str
    limit_pico_usd: int = 0
    committed_pico_usd: int = 0
    requested_pico_usd: int = 0


class KeyLeakSuspected(_Event):
    type: Literal["key_leak_suspected"] = "key_leak_suspected"
    source: str
    key_ids: list[str] = Field(default_factory=list)


class RunCompleted(_Event):
    type: Literal["run_completed"] = "run_completed"
    content: str | None = None
    output: Any = None
    usage: Usage = Field(default_factory=Usage)


class RunFailed(_Event):
    type: Literal["run_failed"] = "run_failed"
    error: str
    status: RunStatus = RunStatus.FAILED


# Sự kiện tạm thời (không lưu, không có seq): chỉ có khi đang xem stream.
class ThinkingDelta(_Event):
    type: Literal["thinking_delta"] = "thinking_delta"
    text: str


class ToolCallArgsDelta(_Event):
    type: Literal["tool_call_args_delta"] = "tool_call_args_delta"
    index: int = 0
    tool_call_id: str | None = None
    tool_name: str | None = None
    text: str


class UnknownEvent(_Event):
    """Loại sự kiện client chưa biết (BE mới hơn). Dữ liệu gốc nằm trong các trường thêm."""


RunEvent = Union[  # noqa: UP007 - danh sách dài, dễ đọc hơn X | Y
    RunStarted, RunContinued, RunResumed, TokenDelta, TokensRetracted, LLMCall, LLMError, KeyFailover,
    ContextShrunk, ContextSummarized, ToolCallStarted, ToolCallCompleted, ToolPolicyDecision, ApprovalRequested,
    RunPaused, GuardrailTriggered, BudgetBlocked, KeyLeakSuspected, RunCompleted, RunFailed, ThinkingDelta,
    ToolCallArgsDelta, UnknownEvent,
]

_EVENT_CLASSES: dict[str, type[_Event]] = {
    cls.model_fields["type"].default: cls
    for cls in (RunStarted, RunContinued, RunResumed, TokenDelta, TokensRetracted, LLMCall, LLMError, KeyFailover,
                ContextShrunk, ContextSummarized, ToolCallStarted, ToolCallCompleted, ToolPolicyDecision,
                ApprovalRequested, RunPaused, GuardrailTriggered, BudgetBlocked, KeyLeakSuspected, RunCompleted,
                RunFailed, ThinkingDelta, ToolCallArgsDelta)
}

LIVE_EVENT_TYPES = frozenset({"thinking_delta", "tool_call_args_delta"})
# Sau các sự kiện này run không tự chạy tiếp (BE đóng stream).
TERMINAL_EVENT_TYPES = frozenset({"run_completed", "run_failed", "run_paused"})


def parse_event(raw: dict[str, Any]) -> RunEvent:
    cls = _EVENT_CLASSES.get(str(raw.get("type")), UnknownEvent)
    return cls.model_validate(raw)  # type: ignore[return-value]


# ============================================================================ sự kiện chat

# Tool điều phối của leader trong Team (atlas/team/team.py): hiện thành MemberStarted / MemberFinished.
_DELEGATE_TOOLS = frozenset({"delegate_task_to_member", "delegate_task_to_members", "execute_task"})


@dataclass(frozen=True)
class TextDelta:
    """Một đoạn chữ mới. `snapshot`: toàn bộ chữ đã nhận tới giờ (đã trừ phần bị rút lại)."""

    text: str
    snapshot: str
    type: Literal["text"] = "text"


@dataclass(frozen=True)
class TextRetracted:
    """Phần chữ cuối (`text`) không thuộc câu trả lời (model lỗi giữa chừng rồi gọi lại). Giao diện hiển thị lại
    `snapshot`."""

    text: str
    snapshot: str
    type: Literal["text_retracted"] = "text_retracted"


@dataclass(frozen=True)
class Thinking:
    """Một đoạn suy luận của model (chỉ khi `include_thinking=True`)."""

    text: str
    type: Literal["thinking"] = "thinking"


@dataclass(frozen=True)
class ToolStarted:
    id: str
    name: str
    arguments: dict[str, Any]
    type: Literal["tool_started"] = "tool_started"


@dataclass(frozen=True)
class ToolFinished:
    id: str
    name: str
    result: str
    is_error: bool = False
    duration_ms: float = 0.0
    type: Literal["tool_finished"] = "tool_finished"


@dataclass(frozen=True)
class MemberStarted:
    """Team giao việc cho thành viên. `member` None = giao cho mọi thành viên (chế độ broadcast)."""

    id: str
    member: str | None
    task: str | None
    type: Literal["member_started"] = "member_started"


@dataclass(frozen=True)
class MemberFinished:
    id: str
    member: str | None
    result: str
    is_error: bool = False
    type: Literal["member_finished"] = "member_finished"


@dataclass(frozen=True)
class ApprovalRequired:
    """Run dừng chờ người: gửi quyết định cho từng mục của `pending` (`Conversation.decide` / `runs.decide`)."""

    run_id: str
    pending: list[PendingToolCall]
    type: Literal["approval_required"] = "approval_required"


@dataclass(frozen=True)
class Done:
    run_id: str
    text: str
    output: Any = None
    usage: Usage = field(default_factory=Usage)
    type: Literal["done"] = "done"


@dataclass(frozen=True)
class Failed:
    """Run lỗi hoặc bị huỷ (`status`)."""

    run_id: str
    error: str
    status: RunStatus = RunStatus.FAILED
    type: Literal["failed"] = "failed"


ChatEvent = Union[  # noqa: UP007
    TextDelta, TextRetracted, Thinking, ToolStarted, ToolFinished, MemberStarted, MemberFinished, ApprovalRequired,
    Done, Failed,
]


class ChatEventMapper:
    """Dịch sự kiện thô của MỘT run thành sự kiện chat. Giữ chữ đã gom (`text`) và các yêu cầu duyệt."""

    def __init__(self, *, include_thinking: bool = False) -> None:
        self.include_thinking = include_thinking
        self.text = ""
        self.pending: list[PendingToolCall] = []
        self._members: dict[str, str | None] = {}

    def feed(self, event: RunEvent) -> list[ChatEvent]:
        if isinstance(event, TokenDelta):
            self.text += event.text
            return [TextDelta(event.text, self.text)]
        if isinstance(event, TokensRetracted):
            if event.text and self.text.endswith(event.text):
                self.text = self.text[: -len(event.text)]
            return [TextRetracted(event.text, self.text)]
        if isinstance(event, ThinkingDelta):
            return [Thinking(event.text)] if self.include_thinking else []
        if isinstance(event, ToolCallStarted):
            if event.tool_name in _DELEGATE_TOOLS:
                member = event.arguments.get("member_id") or None
                self._members[event.tool_call_id] = member
                task = event.arguments.get("task") or event.arguments.get("task_id")
                return [MemberStarted(event.tool_call_id, member, task)]
            return [ToolStarted(event.tool_call_id, event.tool_name, dict(event.arguments))]
        if isinstance(event, ToolCallCompleted):
            if event.tool_call_id in self._members:
                return [MemberFinished(event.tool_call_id, self._members.pop(event.tool_call_id), event.result,
                                       event.is_error)]
            return [ToolFinished(event.tool_call_id, event.tool_name, event.result, event.is_error,
                                 event.duration_ms)]
        if isinstance(event, ApprovalRequested):
            self.pending = list(event.pending)
            return []
        if isinstance(event, RunPaused):
            return [ApprovalRequired(event.run_id, list(self.pending))]
        if isinstance(event, RunContinued):
            self.pending = []
            return []
        if isinstance(event, RunCompleted):
            text = event.content if event.content is not None else self.text
            return [Done(event.run_id, text, event.output, event.usage)]
        if isinstance(event, RunFailed):
            return [Failed(event.run_id, event.error, event.status)]
        return []
