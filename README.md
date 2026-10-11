# atlasclient

Client Python để ứng dụng khác (chatbot, backend, job…) gọi **agent, team, workflow** của Atlas qua API.
Chỉ phụ thuộc `httpx` và `pydantic`, không cần cài `atlas`. Python ≥ 3.10. Có bản đồng bộ (`Atlas`) và bất đồng bộ
(`AsyncAtlas`) cùng API.

```bash
pip install -e ./atlasclient
```

## Cấu hình

| | |
|---|---|
| `base_url` / `ATLAS_API_URL` | Địa chỉ API Atlas, không kèm `/v1` (dev: `http://localhost:8010`, prod: `https://<ingress.host>`) |
| `token` / `ATLAS_TOKEN` / `ATLAS_TOKEN_FILE` | Token của **người dùng cuối** (JWT từ SSO, hoặc PAT `atlas_pat_…`) |
| `token_provider` | Hàm trả token, gọi trước mỗi request (làm mới JWT). Bản async nhận cả coroutine. |

**Mỗi request mang token của đúng người đang chat.** BE kiểm vai trò, scope, agent được chạy, policy tool, phiên và
memory theo người đó. Đừng dùng chung một token service cho mọi người dùng: vai trò `service` không duyệt tool được,
và phiên, memory, chi phí của mọi người sẽ bị trộn vào nhau. Client không bao giờ tự gửi vai trò.

```python
from atlasclient import Atlas

atlas = Atlas("https://atlas.example.com")           # một đối tượng cho cả ứng dụng (dùng chung kết nối)
user = atlas.with_token(request.user.access_token)   # mỗi request: client của người dùng đó
print(user.me().roles)                               # kiểm nhanh URL + token
```

## Dùng

```python
# Một câu hỏi, một câu trả lời (agent, team, workflow đều như nhau)
run = user.run("troluy", "xin chào")
print(run.text, run.status, run.usage.total_tokens)

# Chatbot nhiều lượt: Conversation giữ session_id
chat = user.chat("troluy")                     # hoặc user.chat("troluy", session_id="ses_…") để tiếp phiên cũ
for chunk in chat.stream_text("tôi tên Hà"):
    print(chunk, end="", flush=True)
print(chat.send("tôi tên gì?").text)
print(chat.session_id)                         # lưu lại để lần sau tiếp phiên

# Sự kiện cho giao diện
for ev in chat.stream("tra cứu đơn 123"):
    match ev.type:
        case "text":              ui.set_text(ev.snapshot)          # snapshot: toàn bộ chữ tới giờ
        case "text_retracted":    ui.set_text(ev.snapshot)          # model lỗi giữa chừng, phần cuối bị rút lại
        case "tool_started":      ui.status(f"đang gọi {ev.name}…")
        case "member_started":    ui.status(f"đang hỏi {ev.member}…")  # thành viên của team
        case "approval_required": ui.ask_approval(ev.pending)
        case "done":              ui.finish(ev.text)
        case "failed":            ui.error(ev.error)
```

`stream()` luôn kết thúc bằng đúng một trong ba sự kiện `done`, `failed`, `approval_required`.
Muốn nhận cả suy luận của model: `stream(..., include_thinking=True)` (sự kiện `thinking`).

### Duyệt tool / nhập liệu (human-in-the-loop)

Agent gọi tool cần duyệt thì run dừng ở `waiting`:

```python
run = chat.send("chuyển 2 triệu cho Bình")
if run.needs_action:
    for p in chat.pending:
        print(p.kind, p.tool_name, p.arguments)
    run = chat.approve_all()                       # hoặc chat.deny_all(note="không cho")
    # từng mục: chat.decide([Decision.approve(p1), Decision.deny(p2), Decision.input(p3, otp="123456")])

# Bản stream: gửi quyết định rồi xem tiếp phần chạy sau
for ev in chat.decide_stream([Decision.approve(p) for p in chat.pending]):
    ...
```

Quyền duyệt cũng do BE kiểm (`approvals:write`). Thành viên của team hay bước của workflow cần duyệt thì yêu cầu hiện ở
run cha, client xử lý như run thường.

### Tệp đính kèm

```python
m = user.media.upload("hoa_don.pdf")               # đường dẫn, bytes hoặc file mở ở chế độ nhị phân
chat.send("đọc hoá đơn này", media=[m])            # tối đa 10 tệp mỗi lượt
```

### Lịch sử chat, agent

```python
user.agents.list()                                 # agent / team / workflow người này chạy được (BE đã lọc theo vai trò)
user.sessions.list(agent="troluy")                 # sidebar: phiên của chính người này, mới nhất trước
user.sessions.get(session_id).turns                # [(câu hỏi, câu trả lời), …]
user.sessions.delete(session_id)
user.runs.orchestration(run_id)                    # tiến độ của team / workflow
```

### Async

```python
from atlasclient import AsyncAtlas

async with AsyncAtlas() as atlas:
    chat = atlas.with_token(jwt).chat("troluy")
    async for ev in chat.stream("xin chào"):
        ...
```

### Backend web phi trạng thái

Mỗi request HTTP tạo `Conversation` mới từ `session_id` do trình duyệt giữ. Lượt đang chờ duyệt tạo ở request trước
thì nhận lại bằng `resume`:

```python
conv = user.chat(run.agent_name, session_id=run.session_id)
await conv.resume(run_id)
async for ev in conv.decide_stream([Decision.approve(tool_call_id)]):
    ...
```

Xem `examples/05_fastapi_chatbot.py`.

## Ví dụ

Thư mục `examples/` (kèm `demo_server.py`: Atlas demo với model giả, chạy không cần khoá LLM). Xem `examples/README.md`.

## Agent nhớ những gì?

Hoàn toàn do **cấu hình agent ở BE** (Studio): lịch sử `num_history_runs`, `session-summary`, `memory-session`…
Client chỉ cần gửi cùng `session_id` (Conversation tự làm).

## Lỗi

| Lỗi | Khi nào |
|---|---|
| `AuthenticationError` (401) | thiếu token, token sai hoặc hết hạn |
| `BudgetExceeded` (402) | hết ngân sách cứng, run không được tạo |
| `PermissionDenied` (403) | thiếu scope (`err.missing_scope`) hoặc vai trò không được chạy agent |
| `NotFound` (404) | không có, hoặc thuộc người dùng / tenant khác |
| `Conflict` (409) | trạng thái run không cho thao tác (vd. xoá phiên đang chạy) |
| `RateLimited` (429) | vượt giới hạn tốc độ (`err.retry_after`) |
| `InvalidRequest` (422), `PayloadTooLarge` (413), `UnsupportedMediaType` (415), `ServerError` (5xx) | |
| `APIConnectionError`, `StreamError`, `RunTimeout`, `ConversationBusy` | lỗi phía kết nối / client |

Tất cả kế thừa `AtlasError`.

## Độ bền

- GET/DELETE được thử lại khi gặp 429/502/503/504 và lỗi mạng. POST tạo run **không** được thử lại, trừ khi chắc chắn
  request chưa tới BE (lỗi kết nối, 429), để không tạo run trùng.
- Stream SSE đứt (mạng, proxy) thì tự nối lại từ sự kiện cuối đã nhận (`after_seq`), tối đa `max_reconnects` lần.
  Token hết hạn lúc nối lại thì gọi `token_provider` lấy token mới. Stream đứt trước khi nhận được sự kiện nào thì báo
  lỗi (không biết run nào để nối lại).
- Một `Conversation` chạy một lượt mỗi lúc (`ConversationBusy`). Nhiều request HTTP của cùng một phiên thì ứng dụng tự
  xếp hàng.
- Proxy đứng trước Atlas phải tắt buffering cho SSE và để read timeout lớn hơn 15 giây (BE gửi keep-alive mỗi 15 giây).

## Kiểm thử

```bash
cd atlasclient
PYTHONPATH=src ../atlasruntime/.venv/bin/python -m pytest      # unit + tích hợp với Atlas thật (SQLite)
```
