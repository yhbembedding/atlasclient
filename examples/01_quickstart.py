"""Bắt đầu nhanh: kiểm tra kết nối, xem agent, chạy một câu, hội thoại nhiều lượt, stream chữ.

    export ATLAS_API_URL=http://127.0.0.1:8020  ATLAS_TOKEN=<jwt hoặc atlas_pat_…>   # demo: chạy demo_server.py
    python examples/01_quickstart.py [agent]                                         # mặc định: troluy
"""

from __future__ import annotations

import sys

from atlasclient import Atlas, AtlasError, PermissionDenied

AGENT = sys.argv[1] if len(sys.argv) > 1 else "troluy"


def main() -> None:
    with Atlas() as atlas:  # đọc ATLAS_API_URL, ATLAS_TOKEN (hoặc ATLAS_TOKEN_FILE)
        # 1. Token này là ai, có quyền gì (cách nhanh nhất để kiểm URL + token)
        me = atlas.me()
        print(f"Đăng nhập: {me.id} · tenant {me.tenant_id} · vai trò {me.roles}")

        # 2. Agent / team / workflow người này được chạy (BE đã lọc theo vai trò)
        for a in atlas.agents.list():
            print(f"  - {a.name:<10} [{a.kind}] {a.description or ''}")

        # 3. Một câu hỏi, đợi câu trả lời
        run = atlas.run(AGENT, "xin chào")
        print(f"\n[{run.status.value}] {run.text}  ({run.usage.total_tokens} token)")

        # 4. Hội thoại nhiều lượt: Conversation tự giữ session_id, BE nạp lịch sử của phiên
        chat = atlas.chat(AGENT)
        print("\nbạn> tôi tên Hà")
        print("bot>", chat.send("tôi tên Hà").text)
        print("bạn> nhắc lại giúp tôi")
        print("bot> ", end="")
        for chunk in chat.stream_text("nhắc lại giúp tôi"):  # chữ hiện dần
            print(chunk, end="", flush=True)
        print(f"\n(phiên {chat.session_id})")


if __name__ == "__main__":
    try:
        main()
    except PermissionDenied as exc:
        sys.exit(f"Không đủ quyền: {exc.detail}")
    except AtlasError as exc:
        sys.exit(f"Lỗi: {exc}")
