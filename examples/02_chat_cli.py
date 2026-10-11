"""Chatbot trong terminal: stream chữ, hiện tool / thành viên team đang chạy, duyệt tool, tiếp phiên cũ.

    python examples/02_chat_cli.py [agent] [--session ses_…]

Lệnh: /new (phiên mới) · /sessions (phiên gần đây) · /resume <id> · /history · /exit
Ctrl+C khi bot đang trả lời: huỷ lượt đó.
"""

from __future__ import annotations

import argparse
import threading

from atlasclient import (
    Atlas,
    AtlasError,
    Conversation,
    Decision,
    PendingKind,
    PendingToolCall,
)


def ask_decisions(pending: list[PendingToolCall]) -> list[Decision]:
    """Hỏi người dùng từng tool call đang chờ."""
    decisions = []
    for p in pending:
        print(f"\n  ⚠ cần duyệt: {p.tool_name}({p.arguments})")
        if p.kind == PendingKind.USER_INPUT:
            values = {f: input(f"    {f}: ") for f in p.user_input_fields}
            decisions.append(Decision.input(p, values))
        elif input("    đồng ý? [y/N] ").strip().lower() == "y":
            decisions.append(Decision.approve(p))
        else:
            decisions.append(Decision.deny(p, note=input("    lý do: ") or None))
    return decisions


def show(events) -> list[PendingToolCall]:
    """In sự kiện của một lượt; trả về các tool call đang chờ duyệt (nếu có)."""
    print("bot> ", end="", flush=True)
    for ev in events:
        if ev.type == "text":
            print(ev.text, end="", flush=True)
        elif ev.type == "text_retracted":  # model lỗi giữa chừng: in lại phần chữ còn giữ
            print(f"\r\033[Kbot> {ev.snapshot}", end="", flush=True)
        elif ev.type == "tool_started":
            print(f"\n  · gọi {ev.name}({ev.arguments})", flush=True)
        elif ev.type == "member_started":
            print(f"\n  · giao cho {ev.member or 'mọi thành viên'}: {ev.task}", flush=True)
        elif ev.type == "member_finished":
            print(f"  · {ev.member} xong", flush=True)
        elif ev.type == "approval_required":
            return ev.pending
        elif ev.type == "done":
            print()
        elif ev.type == "failed":
            print(f"\n  ✗ {ev.status.value}: {ev.error}")
    return []


def run_turn(chat: Conversation, text: str) -> None:
    try:
        pending = show(chat.stream(text))
        while pending:  # có thể cần duyệt nhiều lần trong một lượt
            pending = show(chat.decide_stream(ask_decisions(pending)))
    except KeyboardInterrupt:
        threading.Thread(target=chat.cancel).start()
        print("\n  (đã huỷ lượt này)")


def command(atlas: Atlas, chat: Conversation, line: str) -> bool:
    cmd, _, arg = line.partition(" ")
    if cmd == "/exit":
        return False
    if cmd == "/new":
        chat.reset()
        print("  (phiên mới)")
    elif cmd == "/sessions":
        for s in atlas.sessions.list(agent=chat.agent, limit=10):
            print(f"  {s.id}  {s.updated_at:%d/%m %H:%M}  {s.title or ''}")
    elif cmd == "/resume" and arg:
        chat.reset()
        chat.session_id = arg.strip()
        print(f"  (tiếp phiên {chat.session_id})")
    elif cmd == "/history" and chat.session_id:
        for question, answer in chat.history().turns:
            print(f"  bạn> {question}\n  bot> {answer}")
    else:
        print("  /new · /sessions · /resume <id> · /history · /exit")
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("agent", nargs="?", default="troluy")
    parser.add_argument("--session")
    args = parser.parse_args()

    with Atlas() as atlas:
        chat = atlas.chat(args.agent, session_id=args.session)
        print(f"Chat với {args.agent} ({atlas.me().id}). /help để xem lệnh.")
        while True:
            try:
                line = input("bạn> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                return
            if not line:
                continue
            try:
                if line.startswith("/"):
                    if not command(atlas, chat, line):
                        return
                else:
                    run_turn(chat, line)
            except AtlasError as exc:
                print(f"  ✗ {exc}")


if __name__ == "__main__":
    main()
