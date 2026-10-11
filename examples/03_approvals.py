"""Duyệt tool (human-in-the-loop) không cần tương tác: hữu ích cho backend tự quyết theo luật nghiệp vụ.

    python examples/03_approvals.py [agent]        # mặc định: banker (demo_server.py)

Agent gọi tool cần duyệt thì run dừng ở `waiting`; gửi quyết định thì run chạy tiếp. Quyền duyệt do BE kiểm
(`approvals:write`): người không có quyền gửi quyết định sẽ nhận `PermissionDenied`.
"""

from __future__ import annotations

import sys

from atlasclient import Atlas, Decision, PendingKind

AGENT = sys.argv[1] if len(sys.argv) > 1 else "banker"
LIMIT_VND = 5_000_000


def decide(pending) -> list[Decision]:
    """Luật mẫu: chuyển tiền dưới hạn mức thì duyệt, còn lại từ chối."""
    out = []
    for p in pending:
        if p.kind == PendingKind.CONFIRMATION and p.arguments.get("amount", 0) <= LIMIT_VND:
            out.append(Decision.approve(p, note="dưới hạn mức"))
        else:
            out.append(Decision.deny(p, note="vượt hạn mức, cần người duyệt"))
    return out


def main() -> None:
    with Atlas() as atlas:
        # Cách 1: không stream
        chat = atlas.chat(AGENT)
        run = chat.send("chuyển 2 triệu cho Bình")
        print(f"1) trạng thái: {run.status.value}")
        if run.needs_action:
            for p in chat.pending:
                print(f"   chờ duyệt: {p.tool_name} {p.arguments}")
            run = chat.decide(decide(chat.pending))
        print(f"   kết quả: {run.text}")

        # Cách 2: stream, gửi quyết định ngay khi nhận sự kiện approval_required rồi xem tiếp phần chạy sau
        chat = atlas.chat(AGENT)
        print("2) ", end="")
        for ev in chat.stream("chuyển 2 triệu cho Bình"):
            if ev.type == "approval_required":
                print(f"[chờ duyệt {len(ev.pending)} tool] ", end="")
                for after in chat.decide_stream(decide(ev.pending)):
                    if after.type == "text":
                        print(after.text, end="", flush=True)
                print()

        # Cách 3: hàng chờ duyệt của cả tài khoản (vd. trang "việc cần duyệt" của ứng dụng)
        waiting = atlas.runs.approvals()
        print(f"3) còn {len(waiting)} run đang chờ duyệt")
        for r in waiting:
            atlas.runs.deny_all(r.id, note="dọn dẹp ví dụ")


if __name__ == "__main__":
    main()
