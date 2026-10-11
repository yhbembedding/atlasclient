"""Backend chatbot web (FastAPI) dùng atlasclient, kèm một trang HTML tối giản để thử.

    pip install fastapi uvicorn
    export ATLAS_API_URL=http://127.0.0.1:8020
    python examples/05_fastapi_chatbot.py                   # cổng 8030 (PORT=…)
    # mở http://127.0.0.1:8030, dán token của người dùng (demo_server.py in ra)

Nguyên tắc:
- Mỗi request mang token của NGƯỜI ĐANG CHAT (ở đây: header Authorization do trình duyệt gửi; ứng dụng thật lấy
  từ phiên đăng nhập SSO). Backend chỉ chuyển tiếp: `atlas.with_token(token)`. BE Atlas kiểm vai trò, agent được
  chạy, quyền duyệt tool; phiên, memory, chi phí tính theo đúng người đó.
- Backend không giữ trạng thái: `session_id`, `run_id` đi theo từng request (trình duyệt giữ).

API cho frontend:
    GET    /api/me, /api/agents
    POST   /api/chat                    {agent, message, session_id?}  → SSE sự kiện chat
    POST   /api/runs/{run_id}/decisions {decisions: [{tool_call_id, approve, note?}]} → SSE phần chạy tiếp
    POST   /api/runs/{run_id}/cancel
    GET    /api/conversations?agent=…, GET/DELETE /api/conversations/{session_id}
"""

from __future__ import annotations

import dataclasses
import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from enum import Enum
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel

from atlasclient import APIStatusError, AsyncAtlas, AsyncConversation, AtlasError, ChatEvent, Decision

# Một client cho cả ứng dụng (dùng chung connection pool); mỗi request dùng `with_token` của người gọi.
atlas = AsyncAtlas(os.environ.get("ATLAS_API_URL", "http://127.0.0.1:8020"))


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    yield
    await atlas.aclose()


app = FastAPI(title="Chatbot demo", lifespan=lifespan)


def user_client(authorization: str | None) -> AsyncAtlas:
    """Client của người đang gọi. Không có token thì từ chối luôn, không gọi Atlas bằng quyền của backend."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "missing bearer token")
    return atlas.with_token(authorization[7:].strip())


async def call(aw: Any) -> Any:
    """Lỗi của Atlas → cùng mã HTTP cho frontend (401, 403, 404, 402…)."""
    try:
        return await aw
    except APIStatusError as exc:
        raise HTTPException(exc.status_code, exc.detail) from None
    except AtlasError as exc:
        raise HTTPException(502, str(exc)) from None


# ---------------------------------------------------------------------------- SSE ra trình duyệt


def jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, list):
        return [jsonable(v) for v in value]
    return value


def event_json(ev: ChatEvent) -> dict[str, Any]:
    return {f.name: jsonable(getattr(ev, f.name)) for f in dataclasses.fields(ev)}


def sse(conv: AsyncConversation, events: AsyncIterator[ChatEvent]) -> StreamingResponse:
    async def body() -> AsyncIterator[str]:
        try:
            async for ev in events:
                yield f"data: {json.dumps(event_json(ev), ensure_ascii=False)}\n\n"
            # Trình duyệt giữ session_id để lượt sau tiếp đúng phiên (lượt đầu BE mới tạo id).
            yield f"data: {json.dumps({'type': 'session', 'session_id': conv.session_id})}\n\n"
        except APIStatusError as exc:
            yield f"data: {json.dumps({'type': 'error', 'status': exc.status_code, 'detail': str(exc.detail)})}\n\n"
        except AtlasError as exc:
            yield f"data: {json.dumps({'type': 'error', 'status': 502, 'detail': str(exc)})}\n\n"

    return StreamingResponse(body(), media_type="text/event-stream",
                             headers={"cache-control": "no-cache", "x-accel-buffering": "no"})


# ---------------------------------------------------------------------------- API


class ChatIn(BaseModel):
    agent: str
    message: str
    session_id: str | None = None


class DecisionIn(BaseModel):
    tool_call_id: str
    approve: bool
    note: str | None = None


class DecisionsIn(BaseModel):
    decisions: list[DecisionIn]


@app.get("/api/me")
async def me(authorization: str | None = Header(None)) -> Any:
    return await call(user_client(authorization).me())


@app.get("/api/agents")
async def agents(authorization: str | None = Header(None)) -> Any:
    return await call(user_client(authorization).agents.list())


@app.post("/api/chat")
async def chat(body: ChatIn, authorization: str | None = Header(None)) -> StreamingResponse:
    conv = user_client(authorization).chat(body.agent, session_id=body.session_id)
    return sse(conv, conv.stream(body.message))


@app.post("/api/runs/{run_id}/decisions")
async def decide(run_id: str, body: DecisionsIn, authorization: str | None = Header(None)) -> StreamingResponse:
    user = user_client(authorization)
    run = await call(user.runs.get(run_id))
    conv = user.chat(run.agent_name, session_id=run.session_id)
    await call(conv.resume(run_id))  # lượt đang chờ duyệt được tạo ở request trước
    decisions = [Decision.approve(d.tool_call_id, note=d.note) if d.approve else
                 Decision.deny(d.tool_call_id, note=d.note) for d in body.decisions]
    return sse(conv, conv.decide_stream(decisions))


@app.post("/api/runs/{run_id}/cancel")
async def cancel(run_id: str, authorization: str | None = Header(None)) -> Any:
    return await call(user_client(authorization).runs.cancel(run_id))


@app.get("/api/conversations")
async def conversations(agent: str | None = None, authorization: str | None = Header(None)) -> Any:
    return await call(user_client(authorization).sessions.list(agent=agent))


@app.get("/api/conversations/{session_id}")
async def conversation(session_id: str, authorization: str | None = Header(None)) -> Any:
    detail = await call(user_client(authorization).sessions.get(session_id))
    return {"session_id": session_id, "agent": detail.session.agent_name,
            "turns": [{"user": q, "assistant": a} for q, a in detail.turns]}


@app.delete("/api/conversations/{session_id}", status_code=204)
async def delete_conversation(session_id: str, authorization: str | None = Header(None)) -> None:
    await call(user_client(authorization).sessions.delete(session_id))


# ---------------------------------------------------------------------------- trang thử


PAGE = """<!doctype html><html lang="vi"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Chatbot demo</title>
<style>
 body{font:15px system-ui,sans-serif;max-width:760px;margin:0 auto;padding:16px;color:#1d1d1f;background:#fafafa}
 input,select,button{font:inherit;padding:6px 10px;border:1px solid #ccc;border-radius:6px}
 button{cursor:pointer;background:#fff} #log{background:#fff;border:1px solid #ddd;border-radius:8px;
 padding:12px;height:60vh;overflow:auto;margin:12px 0} .u{color:#0a58ca;margin-top:10px} .b{white-space:pre-wrap}
 .s{color:#777;font-size:13px} .row{display:flex;gap:8px;flex-wrap:wrap} .row input{flex:1;min-width:0}
</style></head><body>
<div class="row"><input id="tok" placeholder="Token của người dùng (JWT / atlas_pat_…)">
 <button onclick="connect()">Kết nối</button></div>
<div class="row" style="margin-top:8px"><select id="agent"></select><span id="who" class="s"></span>
 <button onclick="newChat()">Phiên mới</button></div>
<div id="log"></div>
<form class="row" onsubmit="send(event)"><input id="msg" placeholder="Nhập tin nhắn…" autocomplete="off">
 <button>Gửi</button></form>
<script>
let session = null;
const $ = id => document.getElementById(id);
const hdr = () => ({"authorization": "Bearer " + $("tok").value.trim(), "content-type": "application/json"});
function line(cls, text) { const d = document.createElement("div"); d.className = cls; d.textContent = text;
  $("log").append(d); $("log").scrollTop = 1e9; return d; }
async function connect() {
  const me = await fetch("/api/me", {headers: hdr()});
  if (!me.ok) { $("who").textContent = "token không hợp lệ (" + me.status + ")"; return; }
  const m = await me.json(); $("who").textContent = m.id + " · " + m.roles.join(",");
  const agents = await (await fetch("/api/agents", {headers: hdr()})).json();
  $("agent").innerHTML = agents.map(a => `<option value="${a.name}">${a.name} [${a.kind}]</option>`).join("");
}
function newChat() { session = null; $("log").innerHTML = ""; }
async function stream(url, body) {
  const res = await fetch(url, {method: "POST", headers: hdr(), body: JSON.stringify(body)});
  if (!res.ok) { line("s", "lỗi " + res.status + ": " + await res.text()); return; }
  const out = line("b", ""), reader = res.body.getReader(), dec = new TextDecoder(); let buf = "";
  for (;;) {
    const {value, done} = await reader.read(); if (done) break;
    buf += dec.decode(value, {stream: true});
    let i; while ((i = buf.indexOf("\\n\\n")) >= 0) {
      const raw = buf.slice(0, i); buf = buf.slice(i + 2);
      if (raw.startsWith("data: ")) handle(JSON.parse(raw.slice(6)), out);
    }
  }
}
function handle(ev, out) {
  if (ev.type === "text" || ev.type === "text_retracted") out.textContent = ev.snapshot;
  else if (ev.type === "tool_started") line("s", "· gọi " + ev.name);
  else if (ev.type === "member_started") line("s", "· giao cho " + (ev.member || "mọi thành viên") + ": " + ev.task);
  else if (ev.type === "done") out.textContent = ev.text;
  else if (ev.type === "session") session = ev.session_id;
  else if (ev.type === "failed" || ev.type === "error") line("s", "✗ " + (ev.error || ev.detail));
  else if (ev.type === "approval_required") askApproval(ev);
}
function askApproval(ev) {
  for (const p of ev.pending) {
    const d = line("s", "⚠ cần duyệt: " + p.call.name + " " + JSON.stringify(p.call.arguments) + " ");
    for (const [label, ok] of [["Đồng ý", true], ["Từ chối", false]]) {
      const b = document.createElement("button"); b.textContent = label; d.append(b);
      b.onclick = () => { d.querySelectorAll("button").forEach(x => x.remove());
        stream(`/api/runs/${ev.run_id}/decisions`, {decisions: [{tool_call_id: p.call.id, approve: ok}]}); };
    }
  }
}
async function send(e) {
  e.preventDefault(); const text = $("msg").value.trim(); if (!text) return; $("msg").value = "";
  line("u", "bạn: " + text);
  await stream("/api/chat", {agent: $("agent").value, message: text, session_id: session});
}
</script></body></html>"""


@app.get("/", response_class=HTMLResponse)
async def page() -> str:
    return PAGE


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "8030")))
