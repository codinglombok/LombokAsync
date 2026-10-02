"""Runs the shared cross-language vectors (vectors/lombokasync-vectors-v1.json)."""

import asyncio
import json
import pathlib

import pytest

import lombokasync as la

DOC = json.loads((pathlib.Path(__file__).resolve().parents[2] / "vectors" / "lombokasync-vectors-v1.json").read_text(encoding="utf-8"))
DEADLINE = DOC["timeout_deadline_ms"] / 1000


def task(spec):
    if spec.get("never"):
        return asyncio.get_running_loop().create_future()

    async def body():
        for _ in range(spec.get("yields", 0)):
            await la.yield_now()
        if "error" in spec:
            raise RuntimeError(spec["error"])
        return spec["value"]

    return body()


def run_mpsc(ops):
    out, senders, rx = [], [], None
    for op in ops:
        name = op[0]
        if name == "new":
            try:
                tx, rx = la.mpsc_channel(op[1])
            except la.InvalidCapacity:
                out.append("invalid_capacity")
                break
            senders.append(tx)
            out.append("ok")
        elif name == "clone":
            senders.append(senders[op[1]].clone())
            out.append({"sender": len(senders) - 1})
        elif name == "send":
            try:
                senders[op[1]].try_send(op[2])
                out.append("ok")
            except la.ChannelFull:
                out.append("full")
            except la.ChannelClosed:
                out.append("closed")
        elif name == "recv":
            try:
                out.append({"value": rx.try_recv()})
            except la.ChannelEmpty:
                out.append("empty")
            except la.ChannelClosed:
                out.append("closed")
        elif name == "drop":
            senders[op[1]].close()
            out.append("ok")
        elif name == "close":
            rx.close()
            out.append("ok")
        elif name == "len":
            out.append({"len": len(rx)})
        else:
            raise ValueError(name)
    return out


def run_oneshot(ops):
    tx, rx = la.oneshot_channel()
    out = []
    for op in ops:
        name = op[0]
        if name == "new":
            out.append("ok")
        elif name == "send":
            try:
                tx.send(op[1])
                out.append("ok")
            except la.AlreadySent:
                out.append("already_sent")
            except la.ChannelClosed:
                out.append("closed")
        elif name == "recv":
            try:
                out.append({"value": rx.try_recv()})
            except la.ChannelEmpty:
                out.append("empty")
            except la.ChannelClosed:
                out.append("closed")
        elif name == "drop_tx":
            tx.close()
            out.append("ok")
        elif name == "close":
            rx.close()
            out.append("ok")
        else:
            raise ValueError(name)
    return out


async def run_case(case):
    kind = case["kind"]
    if kind == "mpsc":
        return run_mpsc(case["ops"])
    if kind == "oneshot":
        return run_oneshot(case["ops"])
    try:
        if kind == "join_all":
            return {"ok": await la.join_all([task(t) for t in case["tasks"]])}
        if kind == "select":
            index, value = await la.select([task(t) for t in case["tasks"]])
            return {"index": index, "value": value}
        if kind == "timeout":
            try:
                return {"ok": await la.timeout(DEADLINE, task(case["task"]))}
            except la.Timeout:
                return "timeout"
    except RuntimeError as e:
        return {"error": str(e)}
    raise ValueError(kind)


def test_at_least_100_cases():
    assert len(DOC["cases"]) >= 100


@pytest.mark.parametrize("case", DOC["cases"], ids=[c["id"] for c in DOC["cases"]])
def test_vector(case):
    assert asyncio.run(run_case(case)) == case["expected"]
