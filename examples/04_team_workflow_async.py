"""Bản async: team (xem thành viên nào đang làm), workflow (xem tiến độ từng bước), chạy song song, đính kèm tệp.

    python examples/04_team_workflow_async.py [team] [workflow]     # mặc định: crew, pipeline (demo_server.py)
"""

from __future__ import annotations

import asyncio
import sys

from atlasclient import AsyncAtlas

TEAM = sys.argv[1] if len(sys.argv) > 1 else "crew"
WORKFLOW = sys.argv[2] if len(sys.argv) > 2 else "pipeline"


async def team_demo(atlas: AsyncAtlas) -> None:
    print(f"== Team {TEAM}")
    async for ev in atlas.stream(TEAM, "doanh thu quý 3"):
        if ev.type == "member_started":
            print(f"  → {ev.member}: {ev.task}")
        elif ev.type == "member_finished":
            print(f"  ← {ev.member}: {ev.result}")
        elif ev.type == "done":
            print(f"  = {ev.text}")
        elif ev.type == "failed":
            print(f"  ✗ {ev.error}")


async def workflow_demo(atlas: AsyncAtlas) -> None:
    print(f"\n== Workflow {WORKFLOW}")
    graph = await atlas.workflows.get(WORKFLOW)
    print("  các bước:", [s.get("name") for s in graph.get("steps", [])])
    run = await atlas.run(WORKFLOW, "chi phí marketing")
    print(f"  [{run.status.value}] {run.text}")
    progress = await atlas.runs.orchestration(run.id)
    for step in progress.get("steps", []):
        print(f"  bước {step['path']} {step['step_name']}: {step['status']}")


async def parallel_demo(atlas: AsyncAtlas) -> None:
    print("\n== Chạy song song 3 câu (mỗi câu một phiên riêng)")
    questions = ["xin chào", "tra cứu đơn 1001", "tra cứu đơn 1002"]
    runs = await asyncio.gather(*(atlas.run("troluy", q) for q in questions))
    for q, r in zip(questions, runs, strict=True):
        print(f"  {q!r:>20} → {r.text}")


async def media_demo(atlas: AsyncAtlas) -> None:
    print("\n== Đính kèm tệp")
    media = await atlas.media.upload(b"STT,San pham,So luong\n1,Ao,3\n", name="don_hang.csv")
    print(f"  đã tải {media.name} ({media.mime}, {media.size} byte) → {media.id}")
    run = await atlas.run("troluy", "đọc tệp đính kèm", media=[media])
    print(f"  {run.text}")
    await atlas.media.delete(media.id)


async def main() -> None:
    async with AsyncAtlas() as atlas:
        await team_demo(atlas)
        await workflow_demo(atlas)
        await parallel_demo(atlas)
        await media_demo(atlas)


if __name__ == "__main__":
    asyncio.run(main())
