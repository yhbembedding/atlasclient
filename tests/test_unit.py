"""Kiểm thử không cần BE: SSE, ánh xạ sự kiện chat, lỗi, retry, nối lại stream (httpx.MockTransport)."""

from __future__ import annotations

import json

import httpx
import pytest

from atlasclient import (
    ApprovalRequired,
    Atlas,
    Decision,
    Done,
    MemberFinished,
    MemberStarted,
    NotFound,
    PermissionDenied,
    RateLimited,
    ServerError,
    StreamError,
    TextDelta,
    TextRetracted,
    ToolStarted,
)
from atlasclient._core import SSEDecoder
from atlasclient.errors import error_for
from atlasclient.events import ChatEventMapper, UnknownEvent, parse_event


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("atlasclient.client.time.sleep", lambda s: None)


def sse(*events: dict, ids: bool = True) -> bytes:
    out = []
    for e in events:
        head = f"id: {e['seq']}\n" if ids and e.get("seq") else ""
        out.append(f"{head}event: {e['type']}\ndata: {json.dumps(e)}\n\n")
    return ": keep-alive\n\n".join(out).encode()


def ev(type_: str, seq: int, **kw) -> dict:
    return {"type": type_, "run_id": "run_1", "seq": seq, **kw}


def run_json(status: str = "succeeded", **kw) -> dict:
    return {"id": "run_1", "agent_name": "bot", "session_id": "ses_1", "status": status, **kw}


class Dropping(httpx.SyncByteStream):
    """Phát vài chunk rồi đứt kết nối như proxy cắt ngang."""

    def __init__(self, body: bytes) -> None:
        self.body = body

    def __iter__(self):
        yield self.body
        raise httpx.ReadError("connection reset")


# ---------------------------------------------------------------------------- SSE + sự kiện


def test_sse_decoder_frames_and_comments():
    d = SSEDecoder()
    lines = [": keep-alive", "", "id: 7", "event: token_delta", 'data: {"a":', "data: 1}", ""]
    frames = [f for f in map(d.feed, lines) if f]
    assert len(frames) == 1
    assert (frames[0].id, frames[0].event, frames[0].data) == ("7", "token_delta", '{"a":\n1}')


def test_unknown_event_type_does_not_break_parsing():
    e = parse_event({"type": "new_thing", "run_id": "r", "seq": 1, "x": 2})
    assert isinstance(e, UnknownEvent) and e.type == "new_thing"


def test_mapper_text_retraction_and_done_falls_back_to_snapshot():
    m = ChatEventMapper()
    out = [x for raw in (ev("token_delta", 1, text="Xin "), ev("token_delta", 2, text="chàoXX"),
                         ev("tokens_retracted", 3, text="chàoXX"), ev("token_delta", 4, text="chào"),
                         ev("run_completed", 5, content=None)) for x in m.feed(parse_event(raw))]
    assert isinstance(out[2], TextRetracted) and out[2].snapshot == "Xin "
    assert isinstance(out[-1], Done) and out[-1].text == "Xin chào"


def test_mapper_team_delegation_becomes_member_events():
    m = ChatEventMapper()
    started = m.feed(parse_event(ev("tool_call_started", 1, tool_call_id="c1", tool_name="delegate_task_to_member",
                                    arguments={"member_id": "writer", "task": "viết"})))
    finished = m.feed(parse_event(ev("tool_call_completed", 2, tool_call_id="c1",
                                     tool_name="delegate_task_to_member", result="xong")))
    plain = m.feed(parse_event(ev("tool_call_started", 3, tool_call_id="c2", tool_name="lookup", arguments={})))
    assert started == [MemberStarted("c1", "writer", "viết")]
    assert finished == [MemberFinished("c1", "writer", "xong")]
    assert isinstance(plain[0], ToolStarted)


def test_mapper_approval_is_emitted_when_run_pauses():
    m = ChatEventMapper()
    pending = [{"call": {"id": "c1", "name": "send_money", "arguments": {"amount": 5}}, "kind": "confirmation"}]
    assert m.feed(parse_event(ev("approval_requested", 1, pending=pending))) == []
    out = m.feed(parse_event(ev("run_paused", 2)))
    assert isinstance(out[0], ApprovalRequired) and out[0].pending[0].tool_name == "send_money"
    assert Decision.approve(out[0].pending[0]).to_api() == {"tool_call_id": "c1", "confirmed": True,
                                                             "user_input": {}}


# ---------------------------------------------------------------------------- lỗi + retry


def test_error_mapping():
    err = error_for(403, b'{"detail": "missing scope: approvals:write"}')
    assert isinstance(err, PermissionDenied) and err.missing_scope == "approvals:write"
    limited = error_for(429, b'{"detail": "slow down"}', {"Retry-After": "3"})
    assert isinstance(limited, RateLimited) and limited.retry_after == 3.0
    assert isinstance(error_for(502, b"bad gateway"), ServerError)


def client(handler) -> Atlas:
    return Atlas("http://atlas", token="tok", transport=httpx.MockTransport(handler))


def test_get_is_retried_but_post_is_not():
    calls: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(f"{req.method} {req.url.path}")
        if req.method == "GET" and calls.count("GET /v1/me") == 1:
            return httpx.Response(503, json={"detail": "db unavailable"})
        if req.method == "POST":
            return httpx.Response(503, json={"detail": "busy"})
        return httpx.Response(200, json={"id": "alice", "tenant_id": "acme", "roles": ["user"],
                                         "scopes": ["runs:read"]})

    atlas = client(handler)
    me = atlas.me()
    assert me.id == "alice" and me.can("runs:read") and not me.can("approvals:write")
    with pytest.raises(ServerError):
        atlas.runs.create("bot", "hi")
    assert calls.count("POST /v1/agents/bot/runs") == 1


def test_with_token_sends_the_other_users_token():
    seen: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req.headers.get("authorization", ""))
        return httpx.Response(404, json={"detail": "run not found"})

    atlas = client(handler)
    with pytest.raises(NotFound):
        atlas.with_token("bob-jwt").runs.get("run_x")
    assert seen == ["Bearer bob-jwt"]


# ---------------------------------------------------------------------------- stream nối lại


def test_stream_reconnects_from_last_event_id():
    requests: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        requests.append(req)
        headers = {"content-type": "text/event-stream"}
        if req.method == "POST":
            assert json.loads(req.content)["stream"] is True
            body = sse(ev("run_started", 1, agent_name="bot", session_id="ses_1"), ev("token_delta", 2, text="Xin "))
            return httpx.Response(200, headers=headers, stream=Dropping(body))
        if req.url.path == "/v1/runs/run_1/stream":
            return httpx.Response(200, headers=headers, content=sse(
                ev("token_delta", 3, text="chào"), ev("run_completed", 4, content="Xin chào", usage={})))
        return httpx.Response(200, json=run_json(content="Xin chào"))

    chat = client(handler).chat("bot")
    events = list(chat.stream("hi"))
    assert [e.text for e in events if isinstance(e, TextDelta)] == ["Xin ", "chào"]
    assert isinstance(events[-1], Done) and events[-1].text == "Xin chào"
    assert chat.session_id == "ses_1" and chat.last_run.text == "Xin chào"
    resume = next(r for r in requests if r.url.path == "/v1/runs/run_1/stream")
    assert resume.url.params["after_seq"] == "2"


def test_stream_refreshes_token_after_401_on_reconnect():
    tokens = iter(["t1", "t2", "t3", "t4"])
    auth: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        auth.append(req.headers["authorization"])
        headers = {"content-type": "text/event-stream"}
        if req.method == "POST":
            return httpx.Response(200, headers=headers, stream=Dropping(sse(ev("run_started", 1, agent_name="bot",
                                                                               session_id="ses_1"))))
        if req.url.path.endswith("/stream"):
            if req.headers["authorization"] == "Bearer t2":
                return httpx.Response(401, json={"detail": "invalid or expired token"})
            return httpx.Response(200, headers=headers, content=sse(ev("run_completed", 2, content="ok", usage={})))
        return httpx.Response(200, json=run_json(content="ok"))

    atlas = Atlas("http://atlas", token_provider=lambda: next(tokens), transport=httpx.MockTransport(handler))
    events = list(atlas.chat("bot").stream("hi"))
    assert isinstance(events[-1], Done)
    assert auth[:3] == ["Bearer t1", "Bearer t2", "Bearer t3"]


def test_stream_drop_before_any_event_is_not_retried():
    posts = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal posts
        posts += 1
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Dropping(b": keep-alive\n\n"))

    with pytest.raises(Exception) as info:
        list(client(handler).chat("bot").stream("hi"))
    assert posts == 1  # không tạo run thứ hai
    assert not isinstance(info.value, StreamError) or info.value.run_id is None
