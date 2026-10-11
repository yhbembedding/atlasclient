"""Client đồng bộ `Atlas`. Bản bất đồng bộ (`AsyncAtlas`, cùng API với `await`) ở `atlasclient/aio.py`."""

from __future__ import annotations

import copy
import json
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from datetime import datetime
from typing import Any

import httpx

from atlasclient import _core
from atlasclient._core import FileLike, MediaLike, RetryPolicy, seg
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


class Atlas:
    """Client của API Atlas.

        atlas = Atlas("https://atlas.example.com", token=user_jwt)       # hoặc đọc ATLAS_API_URL / ATLAS_TOKEN
        print(atlas.run("troluy", "xin chào").text)

        chat = atlas.chat("troluy")                                      # giữ session_id giữa các lượt
        for chunk in chat.stream_text("tôi tên Hà"):
            print(chunk, end="")

    Mỗi request mang token của đúng người dùng cuối: BE kiểm vai trò, scope, policy tool, memory theo người đó.
    Ứng dụng nhiều người dùng tạo một `Atlas` dùng chung rồi `atlas.with_token(token_của_người_dùng)` mỗi request
    (dùng chung connection pool).
    """

    def __init__(
        self,
        base_url: str | None = None,
        *,
        token: str | None = None,
        token_provider: Callable[[], str] | None = None,
        timeout: float | httpx.Timeout = _core.DEFAULT_TIMEOUT,
        max_retries: int = 2,
        max_reconnects: int = 5,
        headers: dict[str, str] | None = None,
        transport: httpx.BaseTransport | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.base_url = _core.resolve_base_url(base_url)
        self._token = None if token_provider else _core.resolve_token(token)
        self._token_provider = token_provider
        self._retry = RetryPolicy(max_retries=max_retries)
        self.max_reconnects = max_reconnects
        self._owns_http = http_client is None
        self._http = http_client or httpx.Client(
            timeout=timeout, transport=transport, headers={"user-agent": _core.USER_AGENT, **(headers or {})})
        self._init_resources()

    def _init_resources(self) -> None:
        self.agents = Agents(self)
        self.runs = Runs(self)
        self.sessions = Sessions(self)
        self.media = MediaFiles(self)
        self.workflows = Workflows(self)

    def with_token(self, token: str | None = None, *, token_provider: Callable[[], str] | None = None) -> Atlas:
        """Client cho một người dùng khác, dùng chung kết nối. Đóng client gốc mới đóng kết nối."""
        if not token and token_provider is None:
            raise AtlasError("with_token needs a token or a token_provider")
        clone = copy.copy(self)
        clone._token, clone._token_provider, clone._owns_http = token, token_provider, False
        clone._init_resources()
        return clone

    def close(self) -> None:
        if self._owns_http:
            self._http.close()

    def __enter__(self) -> Atlas:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # -- tầng HTTP ------------------------------------------------------------------------

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        token = self._token_provider() if self._token_provider else self._token
        out = {"authorization": f"Bearer {token}"} if token else {}
        return {**out, **(extra or {})}

    def _request(self, method: str, path: str, *, params: dict[str, Any] | None = None, json: Any = None,
                 content: bytes | None = None, headers: dict[str, str] | None = None) -> httpx.Response:
        attempt = 0
        while True:
            try:
                r = self._http.request(method, self.base_url + path, params=params, json=json, content=content,
                                       headers=self._headers(headers))
            except httpx.TransportError as exc:
                if self._retry.retry_on_error(method, exc, attempt):
                    time.sleep(self._retry.delay(attempt))
                    attempt += 1
                    continue
                raise APIConnectionError(f"{method} {path}: {exc!r}") from exc
            if r.status_code < 400:
                return r
            err = error_for(r.status_code, r.content, _core.headers_dict(r.headers))
            if self._retry.retry_on_status(method, r.status_code, attempt):
                time.sleep(self._retry.delay(attempt, err))
                attempt += 1
                continue
            raise err

    def _json(self, method: str, path: str, **kw: Any) -> Any:
        r = self._request(method, path, **kw)
        return r.json() if r.content else None

    def _events(self, method: str, path: str, *, json: Any = None, after_seq: int = 0,
                include_live: bool = False) -> Iterator[RunEvent]:
        """Sự kiện SSE của một run; đứt kết nối thì nối lại từ sự kiện cuối đã nhận (Last-Event-ID)."""
        run_id: str | None = None
        last_seq, failures, auth_retried = after_seq, 0, False
        while True:
            params = None if method == "POST" else {"after_seq": last_seq}
            try:
                with self._http.stream(method, self.base_url + path, json=json, params=params,
                                       headers=self._headers({"accept": "text/event-stream"}),
                                       timeout=_core.stream_timeout(self._http.timeout)) as r:
                    if r.status_code >= 400:
                        raise error_for(r.status_code, r.read(), _core.headers_dict(r.headers))
                    decoder = _core.SSEDecoder()
                    lines = r.iter_lines()
                    for line in lines:
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
                            _core.drain(lines, decoder)
                            yield event
                            return
                        yield event
                # BE đóng stream khi run đã dừng; proxy cũng có thể đóng êm giữa chừng: hỏi lại trạng thái.
                if run_id is None or self.runs.get(run_id).status.stopped:
                    return
                exc: Exception = StreamError("stream closed early", run_id=run_id, last_seq=last_seq)
            except Exception as e:
                exc = e
                if isinstance(e, AuthenticationError) and run_id and self._token_provider and not auth_retried:
                    auth_retried = True  # token hết hạn giữa chừng: lấy token mới rồi nối tiếp
                elif not _core.reconnectable(e, run_id=run_id) \
                        or failures >= self.max_reconnects:
                    raise _stream_error(e, run_id, last_seq) from e
            if failures >= self.max_reconnects:
                raise _stream_error(exc, run_id, last_seq) from exc
            time.sleep(self._retry.delay(failures, exc))
            failures += 1
            if run_id is not None:
                method, path, json = "GET", f"/v1/runs/{seg(run_id)}/stream", None

    # -- API cấp cao ------------------------------------------------------------------------

    def me(self) -> Identity:
        """Danh tính của token đang dùng (id, tenant, roles, scopes): cách nhanh để kiểm URL + token."""
        return Identity.model_validate(self._json("GET", "/v1/me"))

    def health(self) -> bool:
        """BE sẵn sàng (API + DB). Không cần token."""
        try:
            self._request("GET", "/readyz")
        except AtlasError:
            return False
        return True

    def run(self, agent: str, input: str, *, session_id: str | None = None, media: Iterable[MediaLike] | None = None,
            session_state: dict[str, Any] | None = None, timeout: float = 300.0) -> Run:
        """Chạy agent / team / workflow và đợi tới khi xong hoặc chờ duyệt (`run.needs_action`)."""
        run = self.runs.create(agent, input, session_id=session_id, media=media, session_state=session_state)
        return self.runs.wait(run.id, timeout=timeout)

    def stream(self, agent: str, input: str, *, session_id: str | None = None,
               media: Iterable[MediaLike] | None = None, session_state: dict[str, Any] | None = None,
               include_thinking: bool = False) -> Iterator[ChatEvent]:
        """Một lượt, phát sự kiện chat. Hội thoại nhiều lượt dùng `chat()`."""
        return Conversation(self, agent, session_id=session_id).stream(
            input, media=media, session_state=session_state, include_thinking=include_thinking)

    def chat(self, agent: str, *, session_id: str | None = None) -> Conversation:
        """Cuộc trò chuyện với agent. `session_id` None: phiên mới (BE tạo id ở lượt đầu)."""
        return Conversation(self, agent, session_id=session_id)


def json_loads(text: str) -> dict[str, Any]:
    data = json.loads(text)
    if not isinstance(data, dict):
        raise StreamError(f"unexpected SSE payload: {text[:200]}")
    return data


def _stream_error(exc: Exception, run_id: str | None, last_seq: int) -> Exception:
    if isinstance(exc, (httpx.TransportError, StreamError)):
        if run_id is None:
            return APIConnectionError(f"stream failed before the run started: {exc!r}")
        return StreamError(f"stream of run {run_id} lost: {exc!r}", run_id=run_id, last_seq=last_seq)
    return exc


# ============================================================================ tài nguyên


class _Resource:
    def __init__(self, client: Atlas) -> None:
        self._c = client


class Agents(_Resource):
    def list(self) -> list[AgentInfo]:
        """Agent / team / workflow người gọi chạy được (BE đã lọc theo vai trò)."""
        return [AgentInfo.model_validate(a) for a in self._c._json("GET", "/v1/agents")]


class Workflows(_Resource):
    def list(self) -> list[WorkflowInfo]:
        return [WorkflowInfo.model_validate(w) for w in self._c._json("GET", "/v1/workflows")]

    def get(self, name: str) -> dict[str, Any]:
        """Cấu trúc: thành viên + chế độ (team) hoặc cây bước (workflow)."""
        return self._c._json("GET", f"/v1/workflows/{seg(name)}")


class Runs(_Resource):
    def create(self, agent: str, input: str, *, session_id: str | None = None,
               media: Iterable[MediaLike] | None = None, session_state: dict[str, Any] | None = None) -> Run:
        """Xếp run vào hàng đợi (trả về ngay, status `queued`)."""
        body = _core.run_body(input, session_id=session_id, media=media, session_state=session_state)
        return Run.model_validate(self._c._json("POST", f"/v1/agents/{seg(agent)}/runs", json=body))

    def get(self, run_id: str, *, include_messages: bool = False) -> Run:
        params = {"include_messages": "true"} if include_messages else None
        return Run.model_validate(self._c._json("GET", f"/v1/runs/{seg(run_id)}", params=params))

    def list(self, *, session_id: str | None = None, status: RunStatus | str | None = None,
             agent: str | None = None, limit: int = 50) -> list[Run]:
        params: dict[str, Any] = {"limit": limit}
        for key, value in (("session_id", session_id), ("status", status), ("agent_name", agent)):
            if value is not None:
                params[key] = value.value if isinstance(value, RunStatus) else value
        return [Run.model_validate(r) for r in self._c._json("GET", "/v1/runs", params=params)]

    def events(self, run_id: str, *, after_seq: int = 0, limit: int = 500) -> list[RunEvent]:
        """Sự kiện đã lưu của run (một trang, theo seq tăng dần)."""
        data = self._c._json("GET", f"/v1/runs/{seg(run_id)}/events", params={"after_seq": after_seq, "limit": limit})
        return [parse_event(e) for e in data]

    def last_seq(self, run_id: str) -> int:
        seq = 0
        while True:
            page = self.events(run_id, after_seq=seq, limit=5000)
            if not page:
                return seq
            seq = page[-1].seq

    def stream(self, agent: str, input: str, *, session_id: str | None = None,
               media: Iterable[MediaLike] | None = None, session_state: dict[str, Any] | None = None,
               include_live: bool = False) -> Iterator[RunEvent]:
        """Tạo run ở chế độ stream, phát sự kiện thô (token_delta thật từ worker)."""
        body = _core.run_body(input, session_id=session_id, media=media, session_state=session_state, stream=True)
        return self._c._events("POST", f"/v1/agents/{seg(agent)}/runs", json=body, include_live=include_live)

    def follow(self, run_id: str, *, after_seq: int = 0, include_live: bool = False) -> Iterator[RunEvent]:
        """Theo dõi run đã có từ sau sự kiện `after_seq` tới khi run dừng."""
        return self._c._events("GET", f"/v1/runs/{seg(run_id)}/stream", after_seq=after_seq,
                               include_live=include_live)

    def wait(self, run_id: str, *, timeout: float = 300.0) -> Run:
        """Đợi tới khi run xong hoặc chờ duyệt; quá `timeout` giây thì `RunTimeout`."""
        deadline, delay = time.monotonic() + timeout, 0.2
        while True:
            run = self.get(run_id)
            if run.status.stopped:
                return run
            if time.monotonic() >= deadline:
                raise RunTimeout(run_id, run.status.value, timeout)
            time.sleep(min(delay, max(0.0, deadline - time.monotonic())))
            delay = min(delay * 1.5, 2.0)

    def decide(self, run_id: str, decisions: Iterable[Decision]) -> Run:
        """Gửi quyết định cho các tool call đang chờ; run được xếp hàng chạy tiếp (chưa đợi)."""
        body = _core.decisions_body(decisions)
        return Run.model_validate(self._c._json("POST", f"/v1/runs/{seg(run_id)}/decisions", json=body))

    def approve_all(self, run_id: str, *, note: str | None = None) -> Run:
        run = self.get(run_id)
        return self.decide(run_id, [Decision.approve(p, note=note) for p in run.pending])

    def deny_all(self, run_id: str, *, note: str | None = None) -> Run:
        run = self.get(run_id)
        return self.decide(run_id, [Decision.deny(p, note=note) for p in run.pending])

    def cancel(self, run_id: str) -> Run:
        return Run.model_validate(self._c._json("POST", f"/v1/runs/{seg(run_id)}/cancel"))

    def approvals(self) -> list[Run]:
        """Run cấp cao nhất đang chờ người gọi duyệt."""
        return [Run.model_validate(r) for r in self._c._json("GET", "/v1/approvals")]

    def orchestration(self, run_id: str) -> dict[str, Any]:
        """Tiến độ của run team / workflow: run con, task, tương tác thành viên, các bước."""
        return self._c._json("GET", f"/v1/runs/{seg(run_id)}/orchestration")


class Sessions(_Resource):
    def list(self, *, agent: str | None = None, before: datetime | str | None = None,
             limit: int = 50) -> list[SessionInfo]:
        """Phiên của người gọi, mới nhất trước. Trang sau: `before=<updated_at của dòng cuối>`."""
        params: dict[str, Any] = {"limit": limit}
        if agent:
            params["agent"] = agent
        if before is not None:
            params["before"] = before.isoformat() if isinstance(before, datetime) else before
        return [SessionInfo.model_validate(s) for s in self._c._json("GET", "/v1/sessions", params=params)]

    def get(self, session_id: str) -> SessionDetail:
        return SessionDetail.model_validate(self._c._json("GET", f"/v1/sessions/{seg(session_id)}"))

    def delete(self, session_id: str) -> None:
        """Xoá phiên khỏi danh sách (và ký ức gắn với phiên). Phiên còn run đang chạy: `Conflict`."""
        self._c._request("DELETE", f"/v1/sessions/{seg(session_id)}")


class MediaFiles(_Resource):
    def upload(self, file: FileLike, *, name: str | None = None, mime: str | None = None) -> Media:
        """Tải tệp lên (đường dẫn, bytes hoặc file mở ở chế độ nhị phân) để đính kèm vào run qua `media=[…]`."""
        data, name, mime = _core.read_file(file, name, mime)
        params = {"name": name} if name else None
        return Media.model_validate(self._c._json("POST", "/v1/media", params=params, content=data,
                                                  headers={"content-type": mime}))

    def get(self, media_id: str) -> Media:
        return Media.model_validate(self._c._json("GET", f"/v1/media/{seg(media_id)}"))

    def content(self, media_id: str) -> bytes:
        return self._c._request("GET", f"/v1/media/{seg(media_id)}/content").content

    def delete(self, media_id: str) -> None:
        self._c._request("DELETE", f"/v1/media/{seg(media_id)}")


# ============================================================================ hội thoại




class Conversation:
    """Hội thoại nhiều lượt với một agent: giữ `session_id`, gửi các lượt lần lượt, xử lý chờ duyệt.

    Agent nhớ những gì tuỳ cấu hình agent ở BE (lịch sử, tóm tắt phiên, memory); client chỉ giữ phiên.
    Một đối tượng chỉ chạy một lượt mỗi lúc (lượt thứ hai trong khi lượt đầu chưa xong: `ConversationBusy`).
    """

    def __init__(self, client: Atlas, agent: str, *, session_id: str | None = None) -> None:
        self.client, self.agent, self.session_id = client, agent, session_id
        self.last_run: Run | None = None
        self.pending: list[PendingToolCall] = []
        self._active_run: str | None = None
        self._last_seq: int | None = None
        self._guard = threading.Lock()
        self._turn: int | None = None
        self._turns = 0

    @property
    def last_run_id(self) -> str | None:
        return self.last_run.id if self.last_run else None

    def _begin(self) -> int:
        with self._guard:
            if self._turn is not None:
                raise ConversationBusy(f"a turn is already running in session {self.session_id}")
            self._turns += 1
            self._turn = self._turns
            return self._turn

    def _end(self, turn: int) -> None:
        with self._guard:
            if self._turn == turn:
                self._turn = None

    def _finish(self, run: Run) -> Run:
        self.last_run, self.session_id, self._active_run = run, run.session_id, None
        self.pending = list(run.pending) if run.needs_action else []
        return run

    # -- không stream ---------------------------------------------------------------------

    def send(self, text: str, *, media: Iterable[MediaLike] | None = None,
             session_state: dict[str, Any] | None = None, timeout: float = 300.0) -> Run:
        """Gửi một lượt và đợi câu trả lời (`run.text`), hoặc tới khi cần duyệt (`run.needs_action`)."""
        turn = self._begin()
        try:
            run = self.client.runs.create(self.agent, text, session_id=self.session_id, media=media,
                                          session_state=session_state)
            self.session_id, self._active_run, self._last_seq = run.session_id, run.id, None
            return self._finish(self.client.runs.wait(run.id, timeout=timeout))
        finally:
            self._end(turn)

    def decide(self, decisions: Iterable[Decision], *, timeout: float = 300.0) -> Run:
        """Gửi quyết định cho lượt đang chờ rồi đợi kết quả."""
        turn = self._begin()
        try:
            run_id = self._waiting_run()
            self.client.runs.decide(run_id, decisions)
            self._active_run, self._last_seq = run_id, None
            return self._finish(self.client.runs.wait(run_id, timeout=timeout))
        finally:
            self._end(turn)

    def approve_all(self, *, note: str | None = None, timeout: float = 300.0) -> Run:
        return self.decide([Decision.approve(p, note=note) for p in self.pending], timeout=timeout)

    def deny_all(self, *, note: str | None = None, timeout: float = 300.0) -> Run:
        return self.decide([Decision.deny(p, note=note) for p in self.pending], timeout=timeout)

    # -- stream ---------------------------------------------------------------------------

    def stream(self, text: str, *, media: Iterable[MediaLike] | None = None,
               session_state: dict[str, Any] | None = None, include_thinking: bool = False) -> Iterator[ChatEvent]:
        """Gửi một lượt, phát sự kiện chat. Luôn kết thúc bằng đúng một `Done`, `Failed` hoặc `ApprovalRequired`."""
        turn = self._begin()
        try:
            events = self.client.runs.stream(self.agent, text, session_id=self.session_id, media=media,
                                             session_state=session_state, include_live=include_thinking)
            yield from self._drive(turn, events, ChatEventMapper(include_thinking=include_thinking), None)
        finally:
            self._end(turn)

    def stream_text(self, text: str, **kw: Any) -> Iterator[str]:
        """Chỉ các đoạn chữ của câu trả lời. Câu trả lời cuối chính xác ở `last_run.text` (kể cả khi model lỗi giữa
        chừng và một phần chữ đã phát bị rút lại)."""
        for event in self.stream(text, **kw):
            if event.type == "text":
                yield event.text

    def decide_stream(self, decisions: Iterable[Decision], *, include_thinking: bool = False) -> Iterator[ChatEvent]:
        """Gửi quyết định rồi phát sự kiện của phần chạy tiếp."""
        turn = self._begin()
        try:
            run_id = self._waiting_run()
            seq = self._last_seq if self._last_seq is not None else self.client.runs.last_seq(run_id)
            self.client.runs.decide(run_id, decisions)
            self._active_run = run_id
            events = self.client.runs.follow(run_id, after_seq=seq, include_live=include_thinking)
            yield from self._drive(turn, events, ChatEventMapper(include_thinking=include_thinking), run_id)
        finally:
            self._end(turn)

    def approve_all_stream(self, *, note: str | None = None, include_thinking: bool = False) -> Iterator[ChatEvent]:
        return self.decide_stream([Decision.approve(p, note=note) for p in self.pending],
                                  include_thinking=include_thinking)

    def _drive(self, turn: int, events: Iterator[RunEvent], mapper: ChatEventMapper,
               run_id: str | None) -> Iterator[ChatEvent]:
        final: ChatEvent | None = None
        for raw in events:
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
        run = self._finish(self.client.runs.get(run_id))
        final = final or _core.final_event(run)
        if final is None:
            raise StreamError(f"stream of run {run_id} ended while the run is {run.status.value}", run_id=run_id,
                              last_seq=self._last_seq or 0)
        # Mở khoá TRƯỚC sự kiện cuối: ứng dụng gọi decide_stream() ngay trong vòng lặp vẫn được.
        self._end(turn)
        yield final

    # -- khác -----------------------------------------------------------------------------

    def cancel(self) -> Run | None:
        """Huỷ lượt đang chạy (gọi được từ luồng khác trong lúc đang chờ / đang stream)."""
        run_id = self._active_run
        return self.client.runs.cancel(run_id) if run_id else None

    def resume(self, run_id: str) -> Run:
        """Nhận lại một lượt đã có (vd. lượt đang chờ duyệt, tạo ở request HTTP khác) để `decide` / `cancel` tiếp."""
        run = self._finish(self.client.runs.get(run_id))
        self._active_run = None if run.status.stopped else run.id
        return run

    def history(self) -> SessionDetail | None:
        """Phiên và các lượt đã có (để hiển thị lại lịch sử chat)."""
        return self.client.sessions.get(self.session_id) if self.session_id else None

    def reset(self) -> None:
        """Bắt đầu phiên mới (lượt sau BE tạo session_id mới)."""
        self.session_id, self.last_run, self.pending, self._active_run, self._last_seq = None, None, [], None, None

    def _waiting_run(self) -> str:
        if self.last_run is None or not self.pending:
            raise AtlasError("nothing is waiting for a decision in this conversation")
        return self.last_run.id
