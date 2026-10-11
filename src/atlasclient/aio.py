"""Client bất đồng bộ `AsyncAtlas`: cùng API với `Atlas` (atlasclient/client.py), các hàm là `async`.

    async with AsyncAtlas(token=user_jwt) as atlas:
        chat = atlas.chat("troluy")
        async for event in chat.stream("xin chào"):
            ...
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from datetime import datetime
from typing import Any

import httpx

from atlasclient import _core
from atlasclient._core import FileLike, MediaLike, RetryPolicy, seg
from atlasclient.client import _stream_error, json_loads
from atlasclient.errors import (
    APIConnectionError,
    AtlasError,
    AuthenticationError,
    ConversationBusy,
    RunTimeout,
    StreamError,
    error_for,
)
from atlasclient.events import (
    LIVE_EVENT_TYPES,
    TERMINAL_EVENT_TYPES,
    ChatEvent,
    ChatEventMapper,
    RunEvent,
    RunStarted,
    parse_event,
)
from atlasclient.types import (
    AgentInfo,
    Decision,
    Identity,
    Media,
    PendingToolCall,
    Run,
    RunStatus,
    SessionDetail,
    SessionInfo,
    WorkflowInfo,
)

TokenProvider = Callable[[], str | Awaitable[str]]


class AsyncAtlas:
    """Bản async của `Atlas`. `token_provider` có thể là hàm thường hoặc coroutine."""

    def __init__(
        self,
        base_url: str | None = None,
        *,
        token: str | None = None,
        token_provider: TokenProvider | None = None,
        timeout: float | httpx.Timeout = _core.DEFAULT_TIMEOUT,
        max_retries: int = 2,
        max_reconnects: int = 5,
        headers: dict[str, str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = _core.resolve_base_url(base_url)
        self._token = None if token_provider else _core.resolve_token(token)
        self._token_provider = token_provider
        self._retry = RetryPolicy(max_retries=max_retries)
        self.max_reconnects = max_reconnects
        self._owns_http = http_client is None
        self._http = http_client or httpx.AsyncClient(
            timeout=timeout, transport=transport, headers={"user-agent": _core.USER_AGENT, **(headers or {})})
        self._init_resources()

    def _init_resources(self) -> None:
        self.agents = AsyncAgents(self)
        self.runs = AsyncRuns(self)
        self.sessions = AsyncSessions(self)
        self.media = AsyncMediaFiles(self)
        self.workflows = AsyncWorkflows(self)

    def with_token(self, token: str | None = None, *, token_provider: TokenProvider | None = None) -> AsyncAtlas:
        """Client cho một người dùng khác, dùng chung kết nối. Đóng client gốc mới đóng kết nối."""
        if not token and token_provider is None:
            raise AtlasError("with_token needs a token or a token_provider")
        clone = copy.copy(self)
        clone._token, clone._token_provider, clone._owns_http = token, token_provider, False
        clone._init_resources()
        return clone

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def __aenter__(self) -> AsyncAtlas:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    # -- tầng HTTP ------------------------------------------------------------------------

    async def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        token: Any = self._token
        if self._token_provider is not None:
            token = self._token_provider()
            if inspect.isawaitable(token):
                token = await token
        out = {"authorization": f"Bearer {token}"} if token else {}
        return {**out, **(extra or {})}

    async def _request(self, method: str, path: str, *, params: dict[str, Any] | None = None, json: Any = None,
                       content: bytes | None = None, headers: dict[str, str] | None = None) -> httpx.Response:
        attempt = 0
        while True:
            try:
                r = await self._http.request(method, self.base_url + path, params=params, json=json,
                                             content=content, headers=await self._headers(headers))
            except httpx.TransportError as exc:
                if self._retry.retry_on_error(method, exc, attempt):
                    await asyncio.sleep(self._retry.delay(attempt))
                    attempt += 1
                    continue
                raise APIConnectionError(f"{method} {path}: {exc!r}") from exc
            if r.status_code < 400:
                return r
            err = error_for(r.status_code, r.content, _core.headers_dict(r.headers))
            if self._retry.retry_on_status(method, r.status_code, attempt):
                await asyncio.sleep(self._retry.delay(attempt, err))
                attempt += 1
                continue
            raise err

    async def _json(self, method: str, path: str, **kw: Any) -> Any:
        r = await self._request(method, path, **kw)
        return r.json() if r.content else None

    async def _events(self, method: str, path: str, *, json: Any = None, after_seq: int = 0,
                      include_live: bool = False) -> AsyncIterator[RunEvent]:
        """Sự kiện SSE của một run; đứt kết nối thì nối lại từ sự kiện cuối đã nhận (Last-Event-ID)."""
        run_id: str | None = None
        last_seq, failures, auth_retried = after_seq, 0, False
        while True:
            params = None if method == "POST" else {"after_seq": last_seq}
            try:
                async with self._http.stream(method, self.base_url + path, json=json, params=params,
                                             headers=await self._headers({"accept": "text/event-stream"}),
                                             timeout=_core.stream_timeout(self._http.timeout)) as r:
                    if r.status_code >= 400:
                        raise error_for(r.status_code, await r.aread(), _core.headers_dict(r.headers))
                    decoder = _core.SSEDecoder()
                    lines = r.aiter_lines()
                    async for line in lines:
                        frame = decoder.feed(line)
                        if frame is None or not frame.data:
                            continue
                        event = parse_event(json_loads(frame.data))
                        run_id = run_id or event.run_id
                        if frame.id and frame.id.isdigit():
                            last_seq = int(frame.id)
                        failures = 0
                        if event.type in LIVE_EVENT_TYPES and not include_live:
                            continue
                        if event.type in TERMINAL_EVENT_TYPES:
                            await _core.adrain(lines, decoder)
                            yield event
                            return
                        yield event
                # BE đóng stream khi run đã dừng; proxy cũng có thể đóng êm giữa chừng: hỏi lại trạng thái.
                if run_id is None or (await self.runs.get(run_id)).status.stopped:
                    return
                exc: Exception = StreamError("stream closed early", run_id=run_id, last_seq=last_seq)
            except Exception as e:
                exc = e
                if isinstance(e, AuthenticationError) and run_id and self._token_provider and not auth_retried:
                    auth_retried = True  # token hết hạn giữa chừng: lấy token mới rồi nối tiếp
                elif not _core.reconnectable(e, run_id=run_id) or failures >= self.max_reconnects:
                    raise _stream_error(e, run_id, last_seq) from e
            if failures >= self.max_reconnects:
                raise _stream_error(exc, run_id, last_seq) from exc
            await asyncio.sleep(self._retry.delay(failures, exc))
            failures += 1
            if run_id is not None:
                method, path, json = "GET", f"/v1/runs/{seg(run_id)}/stream", None

    # -- API cấp cao ------------------------------------------------------------------------

    async def me(self) -> Identity:
        return Identity.model_validate(await self._json("GET", "/v1/me"))

    async def health(self) -> bool:
        try:
            await self._request("GET", "/readyz")
        except AtlasError:
            return False
        return True

    async def run(self, agent: str, input: str, *, session_id: str | None = None,
                  media: Iterable[MediaLike] | None = None, session_state: dict[str, Any] | None = None,
                  timeout: float = 300.0) -> Run:
        run = await self.runs.create(agent, input, session_id=session_id, media=media, session_state=session_state)
        return await self.runs.wait(run.id, timeout=timeout)

    def stream(self, agent: str, input: str, *, session_id: str | None = None,
               media: Iterable[MediaLike] | None = None, session_state: dict[str, Any] | None = None,
               include_thinking: bool = False) -> AsyncIterator[ChatEvent]:
        return AsyncConversation(self, agent, session_id=session_id).stream(
            input, media=media, session_state=session_state, include_thinking=include_thinking)

    def chat(self, agent: str, *, session_id: str | None = None) -> AsyncConversation:
        return AsyncConversation(self, agent, session_id=session_id)


# ============================================================================ tài nguyên


class _Resource:
    def __init__(self, client: AsyncAtlas) -> None:
        self._c = client


class AsyncAgents(_Resource):
    async def list(self) -> list[AgentInfo]:
        return [AgentInfo.model_validate(a) for a in await self._c._json("GET", "/v1/agents")]


class AsyncWorkflows(_Resource):
    async def list(self) -> list[WorkflowInfo]:
        return [WorkflowInfo.model_validate(w) for w in await self._c._json("GET", "/v1/workflows")]

    async def get(self, name: str) -> dict[str, Any]:
        return await self._c._json("GET", f"/v1/workflows/{seg(name)}")


class AsyncRuns(_Resource):
    async def create(self, agent: str, input: str, *, session_id: str | None = None,
                     media: Iterable[MediaLike] | None = None, session_state: dict[str, Any] | None = None) -> Run:
        body = _core.run_body(input, session_id=session_id, media=media, session_state=session_state)
        return Run.model_validate(await self._c._json("POST", f"/v1/agents/{seg(agent)}/runs", json=body))

    async def get(self, run_id: str, *, include_messages: bool = False) -> Run:
        params = {"include_messages": "true"} if include_messages else None
        return Run.model_validate(await self._c._json("GET", f"/v1/runs/{seg(run_id)}", params=params))

    async def list(self, *, session_id: str | None = None, status: RunStatus | str | None = None,
                   agent: str | None = None, limit: int = 50) -> list[Run]:
        params: dict[str, Any] = {"limit": limit}
        for key, value in (("session_id", session_id), ("status", status), ("agent_name", agent)):
            if value is not None:
                params[key] = value.value if isinstance(value, RunStatus) else value
        return [Run.model_validate(r) for r in await self._c._json("GET", "/v1/runs", params=params)]

    async def events(self, run_id: str, *, after_seq: int = 0, limit: int = 500) -> list[RunEvent]:
        data = await self._c._json("GET", f"/v1/runs/{seg(run_id)}/events",
                                   params={"after_seq": after_seq, "limit": limit})
        return [parse_event(e) for e in data]

    async def last_seq(self, run_id: str) -> int:
        seq = 0
        while True:
            page = await self.events(run_id, after_seq=seq, limit=5000)
            if not page:
                return seq
            seq = page[-1].seq

    def stream(self, agent: str, input: str, *, session_id: str | None = None,
               media: Iterable[MediaLike] | None = None, session_state: dict[str, Any] | None = None,
               include_live: bool = False) -> AsyncIterator[RunEvent]:
        body = _core.run_body(input, session_id=session_id, media=media, session_state=session_state, stream=True)
        return self._c._events("POST", f"/v1/agents/{seg(agent)}/runs", json=body, include_live=include_live)

    def follow(self, run_id: str, *, after_seq: int = 0, include_live: bool = False) -> AsyncIterator[RunEvent]:
        return self._c._events("GET", f"/v1/runs/{seg(run_id)}/stream", after_seq=after_seq,
                               include_live=include_live)

    async def wait(self, run_id: str, *, timeout: float = 300.0) -> Run:
        deadline, delay = time.monotonic() + timeout, 0.2
        while True:
            run = await self.get(run_id)
            if run.status.stopped:
                return run
            if time.monotonic() >= deadline:
                raise RunTimeout(run_id, run.status.value, timeout)
            await asyncio.sleep(min(delay, max(0.0, deadline - time.monotonic())))
            delay = min(delay * 1.5, 2.0)

    async def decide(self, run_id: str, decisions: Iterable[Decision]) -> Run:
        body = _core.decisions_body(decisions)
        return Run.model_validate(await self._c._json("POST", f"/v1/runs/{seg(run_id)}/decisions", json=body))

    async def approve_all(self, run_id: str, *, note: str | None = None) -> Run:
        run = await self.get(run_id)
        return await self.decide(run_id, [Decision.approve(p, note=note) for p in run.pending])

    async def deny_all(self, run_id: str, *, note: str | None = None) -> Run:
        run = await self.get(run_id)
        return await self.decide(run_id, [Decision.deny(p, note=note) for p in run.pending])

    async def cancel(self, run_id: str) -> Run:
        return Run.model_validate(await self._c._json("POST", f"/v1/runs/{seg(run_id)}/cancel"))

    async def approvals(self) -> list[Run]:
        return [Run.model_validate(r) for r in await self._c._json("GET", "/v1/approvals")]

    async def orchestration(self, run_id: str) -> dict[str, Any]:
        return await self._c._json("GET", f"/v1/runs/{seg(run_id)}/orchestration")


class AsyncSessions(_Resource):
    async def list(self, *, agent: str | None = None, before: datetime | str | None = None,
                   limit: int = 50) -> list[SessionInfo]:
        params: dict[str, Any] = {"limit": limit}
        if agent:
            params["agent"] = agent
        if before is not None:
            params["before"] = before.isoformat() if isinstance(before, datetime) else before
        return [SessionInfo.model_validate(s) for s in await self._c._json("GET", "/v1/sessions", params=params)]

    async def get(self, session_id: str) -> SessionDetail:
        return SessionDetail.model_validate(await self._c._json("GET", f"/v1/sessions/{seg(session_id)}"))

    async def delete(self, session_id: str) -> None:
        await self._c._request("DELETE", f"/v1/sessions/{seg(session_id)}")


class AsyncMediaFiles(_Resource):
    async def upload(self, file: FileLike, *, name: str | None = None, mime: str | None = None) -> Media:
        data, name, mime = await asyncio.to_thread(_core.read_file, file, name, mime)
        params = {"name": name} if name else None
        return Media.model_validate(await self._c._json("POST", "/v1/media", params=params, content=data,
                                                        headers={"content-type": mime}))

    async def get(self, media_id: str) -> Media:
        return Media.model_validate(await self._c._json("GET", f"/v1/media/{seg(media_id)}"))

    async def content(self, media_id: str) -> bytes:
        return (await self._c._request("GET", f"/v1/media/{seg(media_id)}/content")).content

    async def delete(self, media_id: str) -> None:
        await self._c._request("DELETE", f"/v1/media/{seg(media_id)}")


# ============================================================================ hội thoại


class AsyncConversation:
    """Bản async của `Conversation` (atlasclient/client.py)."""

    def __init__(self, client: AsyncAtlas, agent: str, *, session_id: str | None = None) -> None:
        self.client, self.agent, self.session_id = client, agent, session_id
        self.last_run: Run | None = None
        self.pending: list[PendingToolCall] = []
        self._active_run: str | None = None
        self._last_seq: int | None = None
        self._turn: int | None = None
        self._turns = 0

    @property
    def last_run_id(self) -> str | None:
        return self.last_run.id if self.last_run else None

    def _begin(self) -> int:
        if self._turn is not None:
            raise ConversationBusy(f"a turn is already running in session {self.session_id}")
        self._turns += 1
        self._turn = self._turns
        return self._turn

    def _end(self, turn: int) -> None:
        if self._turn == turn:
            self._turn = None

    def _finish(self, run: Run) -> Run:
        self.last_run, self.session_id, self._active_run = run, run.session_id, None
        self.pending = list(run.pending) if run.needs_action else []
        return run

    async def send(self, text: str, *, media: Iterable[MediaLike] | None = None,
                   session_state: dict[str, Any] | None = None, timeout: float = 300.0) -> Run:
        turn = self._begin()
        try:
            run = await self.client.runs.create(self.agent, text, session_id=self.session_id, media=media,
                                                session_state=session_state)
            self.session_id, self._active_run, self._last_seq = run.session_id, run.id, None
            return self._finish(await self.client.runs.wait(run.id, timeout=timeout))
        finally:
            self._end(turn)

    async def decide(self, decisions: Iterable[Decision], *, timeout: float = 300.0) -> Run:
        turn = self._begin()
        try:
            run_id = self._waiting_run()
            await self.client.runs.decide(run_id, decisions)
            self._active_run, self._last_seq = run_id, None
            return self._finish(await self.client.runs.wait(run_id, timeout=timeout))
        finally:
            self._end(turn)

    async def approve_all(self, *, note: str | None = None, timeout: float = 300.0) -> Run:
        return await self.decide([Decision.approve(p, note=note) for p in self.pending], timeout=timeout)

    async def deny_all(self, *, note: str | None = None, timeout: float = 300.0) -> Run:
        return await self.decide([Decision.deny(p, note=note) for p in self.pending], timeout=timeout)

    async def stream(self, text: str, *, media: Iterable[MediaLike] | None = None,
                     session_state: dict[str, Any] | None = None,
                     include_thinking: bool = False) -> AsyncIterator[ChatEvent]:
        turn = self._begin()
        try:
            events = self.client.runs.stream(self.agent, text, session_id=self.session_id, media=media,
                                             session_state=session_state, include_live=include_thinking)
            async for event in self._drive(turn, events, ChatEventMapper(include_thinking=include_thinking), None):
                yield event
        finally:
            self._end(turn)

    async def stream_text(self, text: str, **kw: Any) -> AsyncIterator[str]:
        async for event in self.stream(text, **kw):
            if event.type == "text":
                yield event.text

    async def decide_stream(self, decisions: Iterable[Decision], *,
                            include_thinking: bool = False) -> AsyncIterator[ChatEvent]:
        turn = self._begin()
        try:
            run_id = self._waiting_run()
            seq = self._last_seq if self._last_seq is not None else await self.client.runs.last_seq(run_id)
            await self.client.runs.decide(run_id, decisions)
            self._active_run = run_id
            events = self.client.runs.follow(run_id, after_seq=seq, include_live=include_thinking)
            async for event in self._drive(turn, events, ChatEventMapper(include_thinking=include_thinking), run_id):
                yield event
        finally:
            self._end(turn)

    def approve_all_stream(self, *, note: str | None = None,
                           include_thinking: bool = False) -> AsyncIterator[ChatEvent]:
        return self.decide_stream([Decision.approve(p, note=note) for p in self.pending],
                                  include_thinking=include_thinking)

    async def _drive(self, turn: int, events: AsyncIterator[RunEvent], mapper: ChatEventMapper,
                     run_id: str | None) -> AsyncIterator[ChatEvent]:
        final: ChatEvent | None = None
        async for raw in events:
            if run_id is None:
                run_id = self._active_run = raw.run_id
            if isinstance(raw, RunStarted):
                self.session_id = raw.session_id
            if raw.seq:
                self._last_seq = raw.seq
            for event in mapper.feed(raw):
                if isinstance(event, _core.FINAL_CHAT_EVENTS):
                    final = event
                else:
                    yield event
        if run_id is None:
            raise StreamError("stream ended before the run started")
        run = self._finish(await self.client.runs.get(run_id))
        final = final or _core.final_event(run)
        if final is None:
            raise StreamError(f"stream of run {run_id} ended while the run is {run.status.value}", run_id=run_id,
                              last_seq=self._last_seq or 0)
        self._end(turn)
        yield final

    async def cancel(self) -> Run | None:
        run_id = self._active_run
        return await self.client.runs.cancel(run_id) if run_id else None

    async def resume(self, run_id: str) -> Run:
        run = self._finish(await self.client.runs.get(run_id))
        self._active_run = None if run.status.stopped else run.id
        return run

    async def history(self) -> SessionDetail | None:
        return await self.client.sessions.get(self.session_id) if self.session_id else None

    def reset(self) -> None:
        self.session_id, self.last_run, self.pending, self._active_run, self._last_seq = None, None, [], None, None

    def _waiting_run(self) -> str:
        if self.last_run is None or not self.pending:
            raise AtlasError("nothing is waiting for a decision in this conversation")
        return self.last_run.id
