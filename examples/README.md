# Ví dụ dùng atlasclient

| File | Nội dung |
|---|---|
| `01_quickstart.py` | Kiểm tra kết nối (`me`), liệt kê agent, chạy một câu, hội thoại nhiều lượt, stream chữ |
| `02_chat_cli.py` | Chatbot trong terminal: stream, hiện tool / thành viên team đang chạy, duyệt tool, tiếp phiên cũ, Ctrl+C huỷ lượt |
| `03_approvals.py` | Duyệt tool theo luật nghiệp vụ (không tương tác): bản không stream, bản stream, hàng chờ duyệt |
| `04_team_workflow_async.py` | `AsyncAtlas`: team (thành viên nào đang làm), workflow (tiến độ từng bước), chạy song song, đính kèm tệp |
| `05_fastapi_chatbot.py` | Backend chatbot web: chuyển tiếp token của người dùng, SSE ra trình duyệt, duyệt tool qua request khác, lịch sử phiên, kèm trang HTML để thử |
| `demo_server.py` | Atlas demo (API + worker, model giả, SQLite) để chạy các ví dụ trên mà không cần khoá LLM |

## Chạy thử với Atlas demo

```bash
cd atlasclient
PY=../atlasruntime/.venv/bin/python        # python có sẵn atlas (cho demo_server) và httpx, pydantic, fastapi

# Cửa sổ 1: Atlas demo. In ra ATLAS_API_URL và token của alice, bob (user), audra (auditor).
$PY examples/demo_server.py

# Cửa sổ 2: dán các dòng `export …` vừa in ra, rồi:
export PYTHONPATH=src                      # hoặc: pip install -e .
$PY examples/01_quickstart.py
$PY examples/02_chat_cli.py troluy         # thử: "tra cứu đơn 123", /history, /sessions
$PY examples/02_chat_cli.py banker         # tool cần duyệt
$PY examples/02_chat_cli.py crew           # team
$PY examples/03_approvals.py
$PY examples/04_team_workflow_async.py
$PY examples/05_fastapi_chatbot.py         # mở http://127.0.0.1:8030, dán token của alice
ATLAS_TOKEN=$ATLAS_TOKEN_AUDRA $PY examples/01_quickstart.py   # auditor: xem được, không chạy được (403)
```

## Chạy với Atlas thật

Đặt `ATLAS_API_URL` (địa chỉ API, không kèm `/v1`) và `ATLAS_TOKEN` (JWT của người dùng từ SSO, hoặc PAT
`atlas_pat_…` của tài khoản dịch vụ), rồi truyền tên agent của bạn: `python examples/02_chat_cli.py <agent>`.
Agent nhớ gì giữa các lượt tuỳ cấu hình agent ở Atlas (lịch sử, tóm tắt phiên, memory).
