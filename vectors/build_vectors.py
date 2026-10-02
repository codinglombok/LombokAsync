#!/usr/bin/env python3
"""Builds vectors/lombokasync-vectors-v1.json (GP-11).

Expected outputs come from a small reference model of SPEC_LombokAsync sections
3-5 written in this file from the SPEC (plain state machines, no asyncio), plus
hand-written cases whose expected output is typed out and checked against that
model. Every language port must reproduce every case exactly.

After editing, run:

    python3 vectors/build_vectors.py
    sha256sum vectors/lombokasync-vectors-v1.json > vectors/SHA256SUMS

and update the hash in docs/SPEC_LombokAsync_v<version>.md.
"""
import json
import pathlib
import random

# ---------------------------------------------------------------------------
# Reference model (SPEC section 3: mpsc channel)
# ---------------------------------------------------------------------------


def run_mpsc(ops):
    out = []
    queue, cap, live, rx_closed = [], None, set(), False
    next_id = 1
    for op in ops:
        name = op[0]
        if name == "new":
            cap = op[1]
            if cap is not None and cap < 1:
                out.append("invalid_capacity")
                break
            live = {0}
            out.append("ok")
        elif name == "clone":
            assert op[1] in live, "vectors never use a dropped sender"
            live.add(next_id)
            out.append({"sender": next_id})
            next_id += 1
        elif name == "send":
            assert op[1] in live, "vectors never use a dropped sender"
            if rx_closed:
                out.append("closed")
            elif cap is not None and len(queue) >= cap:
                out.append("full")
            else:
                queue.append(op[2])
                out.append("ok")
        elif name == "recv":
            if queue:
                out.append({"value": queue.pop(0)})
            elif rx_closed or not live:
                out.append("closed")
            else:
                out.append("empty")
        elif name == "drop":
            assert op[1] in live, "vectors never drop a sender twice"
            live.discard(op[1])
            out.append("ok")
        elif name == "close":
            rx_closed = True
            out.append("ok")
        elif name == "len":
            out.append({"len": len(queue)})
        else:
            raise ValueError(name)
    return out


# ---------------------------------------------------------------------------
# Reference model (SPEC section 4: oneshot channel)
# ---------------------------------------------------------------------------


def run_oneshot(ops):
    out = []
    value, has_value, tx_used, tx_dropped, rx_closed, taken = None, False, False, False, False, False
    for op in ops:
        name = op[0]
        if name == "new":
            out.append("ok")
        elif name == "send":
            assert not tx_dropped, "vectors never send after drop_tx"
            if tx_used:
                out.append("already_sent")
            elif rx_closed:
                tx_used = True
                out.append("closed")
            else:
                tx_used = True
                value, has_value = op[1], True
                out.append("ok")
        elif name == "recv":
            if has_value and not taken:
                taken = True
                out.append({"value": value})
            elif taken or rx_closed or tx_dropped:
                out.append("closed")
            else:
                out.append("empty")
        elif name == "drop_tx":
            tx_dropped = True
            out.append("ok")
        elif name == "close":
            rx_closed = True
            out.append("ok")
        else:
            raise ValueError(name)
    return out


# ---------------------------------------------------------------------------
# Reference model (SPEC section 5: combinators)
# ---------------------------------------------------------------------------


def run_join_all(tasks):
    failed = [t for t in tasks if "error" in t]
    if failed:
        return {"error": failed[0]["error"]}
    return {"ok": [t["value"] for t in tasks]}


def run_select(tasks):
    for i, t in enumerate(tasks):
        if t.get("never"):
            continue
        if "error" in t:
            return {"error": t["error"]}
        return {"index": i, "value": t["value"]}
    raise ValueError("select vectors always have a ready task")


def run_timeout(task):
    if task.get("never"):
        return "timeout"
    if "error" in task:
        return {"error": task["error"]}
    return {"ok": task["value"]}


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

cases = []


def add(kind, body, expected, note=None):
    case = {"id": f"{kind}-{sum(1 for c in cases if c['kind'] == kind) + 1:03d}", "kind": kind}
    if note:
        case["note"] = note
    case.update(body)
    case["expected"] = expected
    cases.append(case)


def hand_mpsc(ops, expected, note):
    model = run_mpsc(ops)
    assert model == expected, f"hand-written mpsc case disagrees with SPEC model: {note}\n{model}\n{expected}"
    add("mpsc", {"ops": ops}, expected, note)


def hand_oneshot(ops, expected, note):
    model = run_oneshot(ops)
    assert model == expected, f"hand-written oneshot case disagrees with SPEC model: {note}\n{model}\n{expected}"
    add("oneshot", {"ops": ops}, expected, note)


def hand(kind, body, expected, note):
    model = {"join_all": lambda: run_join_all(body["tasks"]),
             "select": lambda: run_select(body["tasks"]),
             "timeout": lambda: run_timeout(body["task"])}[kind]()
    assert model == expected, f"hand-written {kind} case disagrees with SPEC model: {note}\n{model}\n{expected}"
    add(kind, body, expected, note)


# --- mpsc, hand-written -----------------------------------------------------
hand_mpsc([["new", None], ["send", 0, 1], ["send", 0, 2], ["recv"], ["recv"], ["recv"]],
          ["ok", "ok", "ok", {"value": 1}, {"value": 2}, "empty"], "FIFO order, then empty")
hand_mpsc([["new", None], ["recv"]], ["ok", "empty"], "empty while a sender is alive")
hand_mpsc([["new", None], ["drop", 0], ["recv"]], ["ok", "ok", "closed"], "closed when every sender is dropped")
hand_mpsc([["new", None], ["send", 0, "a"], ["drop", 0], ["recv"], ["recv"]],
          ["ok", "ok", "ok", {"value": "a"}, "closed"], "buffered values drain before closed")
hand_mpsc([["new", 1], ["send", 0, 1], ["send", 0, 2], ["recv"], ["send", 0, 3], ["recv"]],
          ["ok", "ok", "full", {"value": 1}, "ok", {"value": 3}], "capacity 1")
hand_mpsc([["new", 2], ["send", 0, 1], ["send", 0, 2], ["send", 0, 3], ["len"]],
          ["ok", "ok", "ok", "full", {"len": 2}], "full keeps the queue unchanged")
hand_mpsc([["new", 0]], ["invalid_capacity"], "capacity 0 is invalid")
hand_mpsc([["new", -3]], ["invalid_capacity"], "negative capacity is invalid")
hand_mpsc([["new", None], ["clone", 0], ["send", 1, "x"], ["drop", 0], ["recv"], ["recv"]],
          ["ok", {"sender": 1}, "ok", "ok", {"value": "x"}, "empty"], "a clone keeps the channel open")
hand_mpsc([["new", None], ["clone", 0], ["drop", 0], ["drop", 1], ["recv"]],
          ["ok", {"sender": 1}, "ok", "ok", "closed"], "closed after the last clone is dropped")
hand_mpsc([["new", None], ["clone", 0], ["clone", 1], ["send", 2, 3], ["send", 0, 1], ["send", 1, 2], ["recv"], ["recv"], ["recv"]],
          ["ok", {"sender": 1}, {"sender": 2}, "ok", "ok", "ok", {"value": 3}, {"value": 1}, {"value": 2}],
          "one queue for every sender, FIFO by send order")
hand_mpsc([["new", None], ["close"], ["send", 0, 1], ["recv"]], ["ok", "ok", "closed", "closed"], "send after receiver close")
hand_mpsc([["new", None], ["send", 0, 1], ["close"], ["send", 0, 2], ["recv"], ["recv"]],
          ["ok", "ok", "ok", "closed", {"value": 1}, "closed"], "receiver close keeps buffered values")
hand_mpsc([["new", 1], ["send", 0, 1], ["close"], ["send", 0, 2]],
          ["ok", "ok", "ok", "closed"], "closed takes precedence over full")
hand_mpsc([["new", None], ["close"], ["close"], ["recv"]], ["ok", "ok", "ok", "closed"], "close is idempotent")
hand_mpsc([["new", None], ["len"], ["send", 0, 1], ["len"], ["recv"], ["len"]],
          ["ok", {"len": 0}, "ok", {"len": 1}, {"value": 1}, {"len": 0}], "len follows the queue")
hand_mpsc([["new", None], ["send", 0, ""], ["send", 0, 0], ["recv"], ["recv"]],
          ["ok", "ok", "ok", {"value": ""}, {"value": 0}], "empty string and zero are values")
hand_mpsc([["new", None], ["send", 0, "é中😀"], ["recv"]],
          ["ok", "ok", {"value": "é中😀"}], "non-ASCII string values")
hand_mpsc([["new", None], ["send", 0, -9007199254740991], ["send", 0, 9007199254740991], ["recv"], ["recv"]],
          ["ok", "ok", "ok", {"value": -9007199254740991}, {"value": 9007199254740991}], "safe integer range")
hand_mpsc([["new", 3], ["send", 0, 1], ["send", 0, 2], ["send", 0, 3], ["recv"], ["recv"], ["send", 0, 4], ["send", 0, 5], ["send", 0, 6], ["len"]],
          ["ok", "ok", "ok", "ok", {"value": 1}, {"value": 2}, "ok", "ok", "full", {"len": 3}], "capacity reused after receives")
hand_mpsc([["new", None], ["clone", 0], ["drop", 1], ["send", 0, 1], ["recv"], ["recv"]],
          ["ok", {"sender": 1}, "ok", "ok", {"value": 1}, "empty"], "dropping a clone keeps the original")
hand_mpsc([["new", 1], ["drop", 0], ["recv"], ["len"]], ["ok", "ok", "closed", {"len": 0}], "bounded closed when senders are gone")

# --- mpsc, generated from the SPEC model with a fixed seed --------------------
rng = random.Random(20261002)
VALUES = [0, 1, 2, 7, 42, -1, 1000, "a", "b", "hello", "", "x y", "é"]
for n in range(60):
    cap = rng.choice([None, None, 1, 2, 3, 5])
    ops = [["new", cap]]
    live = [0]
    next_id = 1
    for _ in range(rng.randint(4, 24)):
        r = rng.random()
        if live and r < 0.45:
            ops.append(["send", rng.choice(live), rng.choice(VALUES)])
        elif r < 0.75:
            ops.append(["recv"])
        elif live and r < 0.83:
            ops.append(["clone", rng.choice(live)])
            live.append(next_id)
            next_id += 1
        elif live and r < 0.90:
            s = rng.choice(live)
            ops.append(["drop", s])
            live.remove(s)
        elif r < 0.95:
            ops.append(["len"])
        else:
            ops.append(["close"])
    add("mpsc", {"ops": ops}, run_mpsc(ops), "generated")

# --- oneshot, hand-written ------------------------------------------------------
hand_oneshot([["new"], ["recv"], ["send", 5], ["recv"], ["recv"]],
             ["ok", "empty", "ok", {"value": 5}, "closed"], "empty, then value, then closed")
hand_oneshot([["new"], ["send", "v"], ["send", "w"], ["recv"]],
             ["ok", "ok", "already_sent", {"value": "v"}], "second send is rejected")
hand_oneshot([["new"], ["drop_tx"], ["recv"]], ["ok", "ok", "closed"], "sender dropped without a value")
hand_oneshot([["new"], ["send", 1], ["drop_tx"], ["recv"]], ["ok", "ok", "ok", {"value": 1}], "value survives sender drop")
hand_oneshot([["new"], ["close"], ["send", 1]], ["ok", "ok", "closed"], "send after receiver close")
hand_oneshot([["new"], ["send", 1], ["close"], ["recv"]], ["ok", "ok", "ok", {"value": 1}], "value sent before close is kept")
hand_oneshot([["new"], ["close"], ["recv"]], ["ok", "ok", "closed"], "recv after close without value")
hand_oneshot([["new"], ["close"], ["send", 1], ["send", 2]], ["ok", "ok", "closed", "already_sent"], "a failed send still uses the sender")
hand_oneshot([["new"], ["send", ""], ["recv"]], ["ok", "ok", {"value": ""}], "empty string value")
hand_oneshot([["new"], ["send", 0], ["recv"]], ["ok", "ok", {"value": 0}], "zero value")
hand_oneshot([["new"], ["recv"], ["recv"], ["drop_tx"], ["recv"]], ["ok", "empty", "empty", "ok", "closed"], "empty is repeatable")
hand_oneshot([["new"], ["close"], ["close"], ["recv"]], ["ok", "ok", "ok", "closed"], "close is idempotent")
hand_oneshot([["new"], ["send", "中"], ["close"], ["recv"], ["recv"]],
             ["ok", "ok", "ok", {"value": "中"}, "closed"], "taken once")

# --- join_all -----------------------------------------------------------------------
J = "join_all"
hand(J, {"tasks": []}, {"ok": []}, "empty list")
hand(J, {"tasks": [{"value": 1}]}, {"ok": [1]}, "single ready task")
hand(J, {"tasks": [{"yields": 3, "value": "a"}, {"yields": 0, "value": "b"}, {"yields": 1, "value": "c"}]},
     {"ok": ["a", "b", "c"]}, "input order, not completion order")
hand(J, {"tasks": [{"yields": 5, "value": 1}, {"yields": 4, "value": 2}, {"yields": 3, "value": 3}, {"yields": 2, "value": 4}, {"yields": 1, "value": 5}]},
     {"ok": [1, 2, 3, 4, 5]}, "reverse completion order")
hand(J, {"tasks": [{"value": 1}, {"error": "boom"}]}, {"error": "boom"}, "one failure")
hand(J, {"tasks": [{"yields": 4, "error": "first"}, {"yields": 0, "error": "second"}]},
     {"error": "first"}, "lowest index failure wins, not the earliest")
hand(J, {"tasks": [{"value": 1}, {"yields": 2, "error": "e2"}, {"yields": 1, "error": "e3"}, {"value": 4}]},
     {"error": "e2"}, "lowest index among several failures")
hand(J, {"tasks": [{"yields": 2, "value": ""}, {"value": 0}]}, {"ok": ["", 0]}, "falsy values kept")
for n in range(12):
    tasks = []
    for i in range(rng.randint(1, 8)):
        t = {"yields": rng.randint(0, 6)}
        if rng.random() < 0.15:
            t["error"] = f"err{i}"
        else:
            t["value"] = rng.choice(VALUES)
        tasks.append(t)
    add(J, {"tasks": tasks}, run_join_all(tasks), "generated")

# --- select -----------------------------------------------------------------------------
S = "select"
hand(S, {"tasks": [{"value": 1}, {"never": True}]}, {"index": 0, "value": 1}, "first ready")
hand(S, {"tasks": [{"never": True}, {"value": "b"}]}, {"index": 1, "value": "b"}, "second ready")
hand(S, {"tasks": [{"value": "a"}, {"value": "b"}]}, {"index": 0, "value": "a"}, "tie goes to the lowest index")
hand(S, {"tasks": [{"never": True}, {"never": True}, {"value": 3}, {"value": 4}]}, {"index": 2, "value": 3}, "lowest ready index")
hand(S, {"tasks": [{"value": 9}]}, {"index": 0, "value": 9}, "single task")
hand(S, {"tasks": [{"never": True}, {"error": "bad"}]}, {"error": "bad"}, "a ready failure is the result")
hand(S, {"tasks": [{"error": "bad"}, {"value": 1}]}, {"error": "bad"}, "ready failure at lower index wins")
hand(S, {"tasks": [{"value": 1}, {"error": "bad"}]}, {"index": 0, "value": 1}, "ready value at lower index wins")
hand(S, {"tasks": [{"never": True}, {"value": ""}]}, {"index": 1, "value": ""}, "empty string value")
for n in range(10):
    k = rng.randint(1, 6)
    tasks = [{"never": True} for _ in range(k)]
    for i in rng.sample(range(k), rng.randint(1, k)):
        tasks[i] = {"value": rng.choice(VALUES)}
    add(S, {"tasks": tasks}, run_select(tasks), "generated")

# --- timeout ------------------------------------------------------------------------------
T = "timeout"
hand(T, {"task": {"value": 1}}, {"ok": 1}, "ready value")
hand(T, {"task": {"never": True}}, "timeout", "never completes")
hand(T, {"task": {"error": "late"}}, {"error": "late"}, "failure passes through")
hand(T, {"task": {"yields": 5, "value": "slow"}}, {"ok": "slow"}, "yields well inside the deadline")
hand(T, {"task": {"yields": 2, "error": "x"}}, {"error": "x"}, "failure after yields")
hand(T, {"task": {"value": ""}}, {"ok": ""}, "empty string value")
hand(T, {"task": {"value": 0}}, {"ok": 0}, "zero value")

doc = {
    "name": "lombokasync-vectors",
    "version": 1,
    "spec": "docs/SPEC_LombokAsync (sections 3-6)",
    "kinds": {
        "mpsc": "apply ops in order to one channel; expected[i] is the outcome of ops[i]",
        "oneshot": "apply ops in order to one oneshot channel; expected[i] is the outcome of ops[i]",
        "join_all": "join_all over tasks; expected is {ok: values} or {error: message}",
        "select": "select over tasks; expected is {index, value} or {error: message}",
        "timeout": "timeout(deadline_ms, task); expected is {ok: value}, \"timeout\" or {error: message}",
    },
    "timeout_deadline_ms": 50,
    "cases": cases,
}
out = pathlib.Path(__file__).with_name("lombokasync-vectors-v1.json")
out.write_text(json.dumps(doc, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
print(f"{len(cases)} cases -> {out}")
