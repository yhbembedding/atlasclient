"""Kiểm thử với Atlas thật (API + worker trong cùng tiến trình, SQLite): cần gói `atlas` (atlasruntime).

    PYTHONPATH=src ../atlasruntime/.venv/bin/python -m pytest
"""

from __future__ import annotations

import asyncio
import os
import time

import httpx
import pytest

os.environ.setdefault("ATLAS_ENV", "test")
atlas_pkg = pytest.importorskip("atlas")
pytest.importorskip("fastapi")

import jwt as pyjwt  # noqa: E402
from atlas import Agent, FunctionModel, tool  # noqa: E402
from atlas import Atlas as AtlasPlatform  # noqa: E402
from atlas.auth import JWTVerifier  # noqa: E402
from atlas.db.migrate import upgrade  # noqa: E402
from atlas.kernel.types import Role, ToolCall, new_id  # noqa: E402
from atlas.team import Team  # noqa: E402

from atlasclient import (  # noqa: E402
    ApprovalRequired,
    AsyncAtlas,
    Decision,
    Done,
    MemberFinished,
    MemberStarted,
    NotFound,
    PermissionDenied,
    RunStatus,
    TextDelta,
    ToolFinished,
    ToolStarted,
)

SECRET = "integration-secret-long-enough-for-hs256"


# ---------------------------------------------------------------------------- agent mẫu


@tool
def lookup(q: str) -> str:
    """Tra cứu."""
    return f"found:{q}"


@tool(requires_confirmation=True, effect="external", risk="high")
def send_money(to: str, amount: int) -> str:
    """Chuyển tiền (cần duyệt)."""
    return f"sent {amount} to {to}"


def _turn(messages):
    start = max(i for i, m in enumerate(messages) if m.role == Role.USER)
    return messages[start], [m for m in messages[start:] if m.role == Role.TOOL]


def echo_agent() -> Agent:
    def fn(messages, tools):
        user, results = _turn(messages)
        if not results:
            return ToolCall(id=new_id("call"), name="lookup", arguments={"q": user.content})
        earlier = sum(1 for m in messages if m.role == Role.USER) - 1
        return f"done:{results[-1].content} (history:{earlier})"

    return Agent(name="echo", model=FunctionModel(fn), tools=[lookup], description="echo")


def banker() -> Agent:
    def fn(messages, tools):
        _, results = _turn(messages)
        if not results:
            return ToolCall(id=new_id("call"), name="send_money", arguments={"to": "bob", "amount": 5})
        return f"result:{results[-1].content}"

    return Agent(name="banker", model=FunctionModel(fn), tools=[send_money])


def team() -> Team:
    def leader(messages, tools):
        user, results = _turn(messages)
        if not results:
            return ToolCall(id=new_id("call"), name="delegate_task_to_member",
                            arguments={"member_id": "writer", "task": f"write about {user.content}"})
        return f"team:{results[-1].content}"

    writer = Agent(name="writer", model=FunctionModel(lambda m, t: "WROTE<" + _turn(m)[0].content + ">"),
                   description="writes")
    return Team(name="crew", model=FunctionModel(leader), members=[writer], description="crew")


# ---------------------------------------------------------------------------- môi trường


def token(sub: str, roles: list[str], tenant: str = "acme") -> str:
    return pyjwt.encode({"sub": sub, "tenant": tenant, "roles": roles, "exp": int(time.time()) + 600}, SECRET,
                        algorithm="HS256")


@pytest.fixture
async def env(tmp_path):
    url = f"sqlite:///{tmp_path}/atlas.db"
    await asyncio.to_thread(upgrade, url)
    platform = AtlasPlatform(database_url=url, agents=[echo_agent(), banker(), team()],
                             jwt=JWTVerifier(secret=SECRET))
    await platform.auth.create_tenant("acme")
    stop = asyncio.Event()

    async def work() -> None:  # worker chạy song song như production
        w = platform.worker(worker_id="it-worker")
        while not stop.is_set():
            if not await w.run_until_idle():
                await asyncio.sleep(0.02)

    task = asyncio.create_task(work())
    transport = httpx.ASGITransport(app=platform.api(sse_poll_interval=0.02))
    base = AsyncAtlas("http://atlas", transport=transport)
    try:
        yield base
    finally:
        stop.set()
        await task
        await base.aclose()


# ---------------------------------------------------------------------------- test


async def test_identity_and_agent_list_follow_roles(env):
    alice = env.with_token(token("alice", ["user"]))
    me = await alice.me()
    assert (me.id, me.tenant_id, me.roles) == ("alice", "acme", ["user"])
    assert {a.name for a in await alice.agents.list()} >= {"echo", "banker", "crew"}
    with pytest.raises(PermissionDenied) as info:  # auditor chỉ đọc: không tạo run được
        await env.with_token(token("audra", ["auditor"])).runs.create("echo", "x")
    assert info.value.missing_scope == "runs:write"


async def test_run_and_multi_turn_session(env):
    alice = env.with_token(token("alice", ["user"]))
    run = await alice.run("echo", "mot")
    assert run.status is RunStatus.SUCCEEDED and run.text == "done:found:mot (history:0)"
    chat = alice.chat("echo")
    first = await chat.send("a")
    second = await chat.send("b")
    assert first.session_id == second.session_id == chat.session_id
    assert second.text.endswith("(history:1)")  # BE nạp lịch sử của phiên
    detail = await chat.history()
    assert detail.turns == [("a", first.text), ("b", second.text)]
    assert chat.session_id in {s.id for s in await alice.sessions.list(agent="echo")}


async def test_stream_events(env):
    alice = env.with_token(token("alice", ["user"]))
    chat = alice.chat("echo")
    events = [e async for e in chat.stream("xin chao")]
    kinds = [type(e) for e in events]
    assert ToolStarted in kinds and ToolFinished in kinds
    assert isinstance(events[-1], Done) and events[-1].text == "done:found:xin chao (history:0)"
    texts = [e for e in events if isinstance(e, TextDelta)]
    assert not texts or texts[-1].snapshot == events[-1].text
    assert chat.session_id and chat.last_run.succeeded


async def test_approval_flow_streams_the_continuation(env):
    alice = env.with_token(token("alice", ["user"]))
    chat = alice.chat("banker")
    events = [e async for e in chat.stream("chuyen tien")]
    assert isinstance(events[-1], ApprovalRequired)
    assert chat.pending[0].tool_name == "send_money" and chat.pending[0].arguments == {"to": "bob", "amount": 5}
    rest = [e async for e in chat.decide_stream([Decision.approve(p) for p in chat.pending])]
    assert isinstance(rest[-1], Done) and rest[-1].text == "result:sent 5 to bob"
    assert not chat.pending


async def test_deny_without_stream(env):
    alice = env.with_token(token("alice", ["user"]))
    chat = alice.chat("banker")
    waiting = await chat.send("chuyen tien")
    assert waiting.needs_action
    done = await chat.deny_all(note="không cho")
    assert done.status is RunStatus.SUCCEEDED and "sent" not in done.text


async def test_team_member_events(env):
    alice = env.with_token(token("alice", ["user"]))
    events = [e async for e in alice.stream("crew", "mua")]
    assert [(e.member, e.task) for e in events if isinstance(e, MemberStarted)] == [("writer", "write about mua")]
    assert any(isinstance(e, MemberFinished) and e.result == "WROTE<write about mua>" for e in events)
    assert isinstance(events[-1], Done) and events[-1].text == "team:WROTE<write about mua>"


async def test_other_users_cannot_see_runs_or_sessions(env):
    alice = env.with_token(token("alice", ["user"]))
    carol = env.with_token(token("carol", ["user"]))
    run = await alice.run("echo", "bi mat")
    with pytest.raises(NotFound):
        await carol.runs.get(run.id)
    with pytest.raises(NotFound):
        await carol.sessions.get(run.session_id)
    assert run.session_id not in {s.id for s in await carol.sessions.list()}


async def test_media_upload_and_attach(env):
    alice = env.with_token(token("alice", ["user"]))
    media = await alice.media.upload(b"ghi chu", name="note.txt")
    assert media.mime == "text/plain" and await alice.media.content(media.id) == b"ghi chu"
    run = await alice.run("echo", "doc file", media=[media])
    assert run.succeeded
    with pytest.raises(NotFound):
        await env.with_token(token("carol", ["user"])).media.get(media.id)


async def test_delete_session(env):
    alice = env.with_token(token("alice", ["user"]))
    run = await alice.run("echo", "x")
    await alice.sessions.delete(run.session_id)
    with pytest.raises(NotFound):
        await alice.sessions.get(run.session_id)


# ---------------------------------------------------------------------------- client đồng bộ, server thật


@pytest.fixture
def live_url(tmp_path):
    """Atlas chạy thật (uvicorn + worker) trong một luồng riêng; client đồng bộ gọi qua TCP."""
    import socket
    import threading

    import uvicorn

    url = f"sqlite:///{tmp_path}/atlas.db"
    upgrade(url)
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    ready, stop = threading.Event(), threading.Event()

    async def main() -> None:
        platform = AtlasPlatform(database_url=url, agents=[echo_agent(), banker()], jwt=JWTVerifier(secret=SECRET))
        await platform.auth.create_tenant("acme")
        server = uvicorn.Server(uvicorn.Config(platform.api(sse_poll_interval=0.02), host="127.0.0.1", port=port,
                                               log_level="warning", lifespan="off"))
        serving = asyncio.create_task(server.serve())
        w = platform.worker(worker_id="live-worker")
        while not server.started:
            await asyncio.sleep(0.01)
        ready.set()
        while not stop.is_set():
            if not await w.run_until_idle():
                await asyncio.sleep(0.02)
        server.should_exit = True
        await serving

    thread = threading.Thread(target=lambda: asyncio.run(main()), daemon=True)
    thread.start()
    assert ready.wait(30), "atlas did not start"
    yield f"http://127.0.0.1:{port}"
    stop.set()
    thread.join(10)


def test_sync_client_end_to_end(live_url):
    from atlasclient import Atlas

    with Atlas(live_url) as base:
        assert base.health()
        alice = base.with_token(token("alice", ["user"]))
        chat = alice.chat("echo")
        events = list(chat.stream("mot"))
        assert isinstance(events[-1], Done) and events[-1].text == "done:found:mot (history:0)"
        assert chat.send("hai").text.endswith("(history:1)")

        bank = alice.chat("banker")
        assert isinstance(list(bank.stream("chuyen"))[-1], ApprovalRequired)
        rest = list(bank.approve_all_stream())
        assert isinstance(rest[-1], Done) and rest[-1].text == "result:sent 5 to bob"
