"""Atlas demo để chạy các ví dụ mà không cần khoá LLM: API + worker trong một tiến trình, model giả, SQLite.

    cd atlasclient
    ../atlasruntime/.venv/bin/python examples/demo_server.py        # in ra ATLAS_API_URL và token của từng người

Agent có sẵn:
    troluy    trợ lý: trả lời từng chữ (stream), nhớ số lượt trong phiên, gọi tool `lookup` khi câu hỏi có "tra cứu"
    banker    gọi tool `send_money` cần người duyệt (human-in-the-loop)
    crew      team: leader giao việc cho `researcher` rồi `writer`, tổng hợp câu trả lời
    pipeline  workflow hai bước: research → write

Người dùng (JWT HS256, hạn 12 giờ): alice, bob (vai trò `user`), audra (`auditor`: chỉ đọc).
Cần atlasruntime: chạy bằng python của atlasruntime/.venv (hoặc cài `atlas[platform]`).
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import time

os.environ.setdefault("ATLAS_ENV", "dev")

import jwt
import uvicorn
from atlas import Agent, Atlas, FunctionModel, tool
from atlas.auth import JWTVerifier
from atlas.db.migrate import upgrade
from atlas.kernel.types import Role, ToolCall, new_id
from atlas.llm.base import ResponseDone, TextDelta
from atlas.team import Team
from atlas.workflow import Step, Workflow

SECRET = os.environ.get("DEMO_JWT_SECRET", "demo-secret-change-me-at-least-32-bytes")
PORT = int(os.environ.get("DEMO_PORT", "8020"))
USERS = {"alice": ["user"], "bob": ["user"], "audra": ["auditor"]}


class WordModel(FunctionModel):
    """FunctionModel phát câu trả lời từng chữ như model thật."""

    async def stream(self, messages, tools, **kw):
        response = await self.complete(messages, tools, **kw)
        words = (response.message.content or "").split(" ")
        for i, word in enumerate(words if response.message.content else []):
            await asyncio.sleep(0.03)
            yield TextDelta(word if i == 0 else " " + word)
        yield ResponseDone(response)


def turn(messages):
    """(câu hỏi của lượt này, kết quả tool của lượt này, số lượt trước trong phiên)."""
    users = [i for i, m in enumerate(messages) if m.role == Role.USER]
    start = users[-1]
    return messages[start].content or "", [m for m in messages[start:] if m.role == Role.TOOL], len(users) - 1


@tool
def lookup(q: str) -> str:
    """Tra cứu thông tin nội bộ."""
    return f"đơn {q.split()[-1]}: đang giao, dự kiến ngày mai"


@tool(requires_confirmation=True, effect="external", risk="high")
def send_money(to: str, amount: int) -> str:
    """Chuyển tiền (cần người duyệt)."""
    return f"đã chuyển {amount:,} VND cho {to}"


def troluy() -> Agent:
    def fn(messages, tools):
        question, results, earlier = turn(messages)
        if "tra cứu" in question.lower() and not results:
            return ToolCall(id=new_id("call"), name="lookup", arguments={"q": question})
        if results:
            return f"Kết quả tra cứu: {results[-1].content}."
        return f"Bạn vừa nói: «{question}». Trong phiên này mình đã nói chuyện {earlier} lượt trước đó."

    return Agent(name="troluy", model=WordModel(fn), tools=[lookup], description="Trợ lý demo")


def banker() -> Agent:
    def fn(messages, tools):
        _, results, _ = turn(messages)
        if not results:
            return ToolCall(id=new_id("call"), name="send_money", arguments={"to": "Bình", "amount": 2_000_000})
        return f"Xong: {results[-1].content}."

    return Agent(name="banker", model=WordModel(fn), tools=[send_money], description="Chuyển tiền (cần duyệt)")


def researcher() -> Agent:
    return Agent(name="researcher", model=WordModel(lambda m, t: f"số liệu về «{turn(m)[0]}»: tăng 12%"),
                 description="tìm số liệu")


def writer() -> Agent:
    # Bước sau của workflow nhận cả yêu cầu gốc lẫn kết quả bước trước: lấy dòng cuối (kết quả bước trước).
    return Agent(name="writer", model=WordModel(lambda m, t: f"báo cáo ngắn: {turn(m)[0].strip().splitlines()[-1]}"),
                 description="viết báo cáo")


def crew() -> Team:
    def leader(messages, tools):
        question, results, _ = turn(messages)
        if not results:
            return ToolCall(id=new_id("call"), name="delegate_task_to_member",
                            arguments={"member_id": "researcher", "task": question})
        if len(results) == 1:
            return ToolCall(id=new_id("call"), name="delegate_task_to_member",
                            arguments={"member_id": "writer", "task": results[0].content})
        return f"Tổng hợp của team: {results[-1].content}"

    return Team(name="crew", model=WordModel(leader), members=[researcher(), writer()],
                description="Team: researcher + writer")


def pipeline() -> Workflow:
    return Workflow(name="pipeline", description="Workflow: research → write",
                    steps=[Step("research", agent=researcher()), Step("write", agent=writer())])


def token(user: str) -> str:
    claims = {"sub": user, "tenant": "demo", "roles": USERS[user], "exp": int(time.time()) + 12 * 3600}
    return jwt.encode(claims, SECRET, algorithm="HS256")


async def main() -> None:
    url = f"sqlite:///{tempfile.mkdtemp(prefix='atlas-demo-')}/atlas.db"
    await asyncio.to_thread(upgrade, url)
    atlas = Atlas(database_url=url, agents=[troluy(), banker(), crew(), pipeline()], jwt=JWTVerifier(secret=SECRET))
    await atlas.auth.create_tenant("demo")
    server = uvicorn.Server(uvicorn.Config(atlas.api(), host="127.0.0.1", port=PORT, log_level="warning"))
    serving = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)

    print(f"export ATLAS_API_URL=http://127.0.0.1:{PORT}")
    for user in USERS:
        print(f"export ATLAS_TOKEN_{user.upper()}={token(user)}")
    print("export ATLAS_TOKEN=$ATLAS_TOKEN_ALICE")
    print(f"# API docs: http://127.0.0.1:{PORT}/docs · Ctrl+C để dừng", flush=True)

    worker = atlas.worker(worker_id="demo-worker")
    while not serving.done():
        if not await worker.run_until_idle():
            await asyncio.sleep(0.05)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
