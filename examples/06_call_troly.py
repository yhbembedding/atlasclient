"""Gọi agent `troly` bằng PAT của người dùng.

    export ATLAS_API_URL=http://localhost:8010
    export ATLAS_TOKEN=atlas_pat_...                 # hoặc ATLAS_TOKEN_FILE=/đường/dẫn/file/chứa/token
    PYTHONPATH=src python examples/06_call_troly.py "xin chào"

Đổi agent bằng ATLAS_AGENT (mặc định `troly`).
"""

from __future__ import annotations

import os
import sys

from atlasclient import Atlas, AtlasError

AGENT = os.environ.get("ATLAS_AGENT", "troly")


def main() -> None:
    question = " ".join(sys.argv[1:]) or "xin chào"
    # Atlas() tự đọc ATLAS_API_URL và ATLAS_TOKEN / ATLAS_TOKEN_FILE.
    with Atlas() as atlas:
        print("người gọi:", atlas.me().id)  # kiểm nhanh URL + token

        # 1) Một câu hỏi, một câu trả lời
        run = atlas.run(AGENT, question)
        print(f"[{run.status}] {run.text}")
        print("token dùng:", run.usage.total_tokens)

        # 2) Chat nhiều lượt, in dần từng chữ
        chat = atlas.chat(AGENT)
        for chunk in chat.stream_text("tôi tên Hà"):
            print(chunk, end="", flush=True)
        print()
        print(chat.send("tôi tên gì?").text)
        print("session_id:", chat.session_id)  # lưu lại để tiếp phiên lần sau


if __name__ == "__main__":
    try:
        main()
    except AtlasError as e:
        sys.exit(f"{type(e).__name__}: {e}")
