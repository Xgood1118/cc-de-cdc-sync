# -*- coding: utf-8 -*-
"""变更日志读取与应用。

骨架版本：逐行读 JSON 事件，只实现 insert，按到达顺序直接写库。
"""
import json
import sqlite3


def read_wal(path):
    """读取 JSONL 变更日志，返回事件列表（按文件顺序，未排序）。"""
    events = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            events.append(json.loads(line))
    return events


def apply_event(conn, ev):
    """应用单条事件。骨架只支持 insert。"""
    op = ev.get("op")
    table = ev.get("table")
    data = ev.get("data") or {}
    if op == "insert" and data:
        cols = ",".join(data.keys())
        marks = ",".join("?" * len(data))
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, cols, marks)
        conn.execute(sql, list(data.values()))
        return 1
    return 0


def apply_all(conn, events):
    """按到达顺序应用全部事件，返回成功条数。"""
    applied = 0
    for ev in events:
        applied += apply_event(conn, ev)
    conn.commit()
    return applied


def ensure_schema(conn, schema_path):
    with open(schema_path, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.commit()
