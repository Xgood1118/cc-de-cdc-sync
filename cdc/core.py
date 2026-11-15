# -*- coding: utf-8 -*-
"""变更日志（WAL）读取与应用。

职责：
- read_wal：按文件顺序读取 JSONL 事件，做基本格式校验（不排序、不去重）。
- apply_event：应用单条 DML 事件（insert/update/delete/upsert），自动扩展
  未知列，表不存在时抛出可定位的 EventRejectedError。
- apply_all：排序、去重、冲突检测、事务分组与原子应用、幂等重放、统计。

只用 Python 标准库（json/sqlite3/datetime）。
"""
import json
import sqlite3
from datetime import datetime, timezone

CONTROL_OPS = ("begin", "commit")
DML_OPS = ("insert", "update", "delete", "upsert")

# 落库已应用 seq 的元数据表，以及表结构自动扩展的变更记录表。
_APPLIED_SEQ_DDL = """
CREATE TABLE IF NOT EXISTS cdc_applied_seq (
  seq        INTEGER PRIMARY KEY,
  tx_id      INTEGER,
  op         TEXT NOT NULL,
  applied_at TEXT NOT NULL
)
"""
_SCHEMA_CHANGES_DDL = """
CREATE TABLE IF NOT EXISTS cdc_schema_changes (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name  TEXT NOT NULL,
  column_name TEXT NOT NULL,
  column_type TEXT NOT NULL,
  source_seq  INTEGER,
  changed_at  TEXT NOT NULL
)
"""


class CDCError(Exception):
    """CDC 同步过程中所有可预期错误的基类。"""


class WALFormatError(CDCError):
    """输入的 WAL 事件格式非法（退出码 2）。"""


class SchemaError(CDCError):
    """schema.sql 无法读取或执行（退出码 2）。"""


class ConflictError(CDCError):
    """同一 seq 出现内容不一致的事件（退出码 1）。"""


class EventRejectedError(CDCError):
    """事件被拒收，例如目标表不存在（退出码 1）。"""


def _now():
    return datetime.now(timezone.utc).isoformat()


def _quote_ident(name):
    """对表名/列名做双引号转义，避免注入与语法错误。"""
    return '"' + str(name).replace('"', '""') + '"'


def _canonical(ev):
    return json.dumps(ev, sort_keys=True, ensure_ascii=False, default=str)


# ---------------------------------------------------------------------------
# 读取
# ---------------------------------------------------------------------------
def read_wal(path):
    """读取 JSONL 变更日志，返回事件列表（保持文件顺序，不排序、不去重）。"""
    events = []
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError as exc:
                raise WALFormatError(
                    "第 %d 行不是合法 JSON: %s" % (lineno, exc)
                ) from exc
            if not isinstance(ev, dict):
                raise WALFormatError("第 %d 行事件必须是 JSON 对象" % lineno)
            seq = ev.get("seq")
            if not isinstance(seq, int) or isinstance(seq, bool):
                raise WALFormatError("第 %d 行缺少整型 seq 字段" % lineno)
            if not isinstance(ev.get("op"), str) or not ev["op"]:
                raise WALFormatError("第 %d 行(seq=%s)缺少字符串 op 字段"
                                     % (lineno, seq))
            events.append(ev)
    return events


# ---------------------------------------------------------------------------
# 排序、去重、冲突检测
# ---------------------------------------------------------------------------
def normalize_events(events):
    """按 seq 升序排列；重复 seq 内容一致只保留一条，不一致则记为冲突。

    返回 (ordered_events, conflicts)：
    - ordered_events：去掉重复项与冲突项后、按 seq 升序的事件列表。
    - conflicts：[(seq, first_event, other_event), ...]，冲突 seq 不会被应用。
    """
    by_seq = {}
    conflict_seqs = set()
    conflicts = []
    for ev in events:
        seq = ev["seq"]
        if seq in conflict_seqs:
            conflicts.append((seq, by_seq.get(seq), ev))
            continue
        if seq in by_seq:
            if _canonical(by_seq[seq]) != _canonical(ev):
                conflict_seqs.add(seq)
                conflicts.append((seq, by_seq[seq], ev))
            # 内容完全一致：上游重放，保留第一条即可。
            continue
        by_seq[seq] = ev
    ordered = sorted(
        (ev for seq, ev in by_seq.items() if seq not in conflict_seqs),
        key=lambda e: e["seq"],
    )
    return ordered, conflicts


def group_events(events):
    """把排序后的事件流切分为应用组。

    返回 (groups, incomplete_tx, incomplete_info)：
    - groups：[("single", ev), ...] 或 [("tx", {"tx": id, "events": [...]}), ...]
    - begin/commit 之间的事件构成一个事务组；begin 后没有 commit 的事务整体
      丢弃并计入 incomplete_tx；没有 begin 的孤立事件各自成为单条事务组。
    - 事务边界同时参考事件的 tx 标识：未提交事务之后出现 tx 不同的事件时，
      前一事务视为隐式结束（不完整）；没有 tx 标识的事件仍留在当前事务内。
    """
    groups = []
    current = None
    incomplete_tx = 0
    incomplete_info = []

    def close_open():
        nonlocal current, incomplete_tx
        if current is not None:
            incomplete_tx += 1
            incomplete_info.append(current)
            current = None

    for ev in events:
        op = ev["op"]
        if op == "begin":
            close_open()
            current = {"tx": ev.get("tx"), "events": []}
        elif op == "commit":
            if current is None:
                # 孤立 commit：没有可提交的事务，忽略控制事件。
                continue
            groups.append(("tx", current))
            current = None
        else:
            tx_of_ev = ev.get("tx")
            if (current is not None and tx_of_ev is not None
                    and tx_of_ev != current["tx"]):
                close_open()
            if current is not None:
                current["events"].append(ev)
            else:
                groups.append(("single", ev))
    close_open()
    return groups, incomplete_tx, incomplete_info


# ---------------------------------------------------------------------------
# 表结构
# ---------------------------------------------------------------------------
def ensure_cdc_tables(conn):
    """创建 CDC 自己的元数据表（幂等）。"""
    conn.execute(_APPLIED_SEQ_DDL)
    conn.execute(_SCHEMA_CHANGES_DDL)


def ensure_schema(conn, schema_path):
    """执行 schema.sql 并准备 CDC 元数据表。"""
    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            script = f.read()
    except OSError as exc:
        raise SchemaError("无法读取 schema 文件 %s: %s" % (schema_path, exc)) from exc
    try:
        conn.executescript(script)
        ensure_cdc_tables(conn)
        conn.commit()
    except sqlite3.DatabaseError as exc:
        raise SchemaError("schema.sql 执行失败: %s" % exc) from exc


def _table_exists(conn, table):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(conn, table):
    """返回 {列名: pragma 行}。"""
    rows = conn.execute("PRAGMA table_info(%s)" % _quote_ident(table)).fetchall()
    return {row[1]: row for row in rows}


def _primary_keys(conn, table):
    return [name for name, row in _table_columns(conn, table).items() if row[5] > 0]


def _infer_sql_type(value):
    if isinstance(value, bool):
        return "INTEGER"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "REAL"
    if isinstance(value, (bytes, bytearray)):
        return "BLOB"
    # str、None 及其它类型统一落到 TEXT（列允许 NULL）。
    return "TEXT"


def _ensure_table(conn, ev):
    table = ev.get("table")
    if not isinstance(table, str) or not table:
        raise WALFormatError("seq=%s 的事件缺少 table 字段" % ev.get("seq"))
    if not _table_exists(conn, table):
        raise EventRejectedError(
            "seq=%s 目标表 %r 不存在，事件被拒收" % (ev.get("seq"), table)
        )
    return table


def _ensure_columns(conn, table, values, seq):
    """values 中出现而表里没有的列：自动 ALTER 扩展并记录变更，不静默丢列。"""
    existing = _table_columns(conn, table)
    for col, value in values.items():
        if col in existing:
            continue
        col_type = _infer_sql_type(value)
        conn.execute(
            "ALTER TABLE %s ADD COLUMN %s %s"
            % (_quote_ident(table), _quote_ident(col), col_type)
        )
        conn.execute(
            "INSERT INTO cdc_schema_changes"
            "(table_name, column_name, column_type, source_seq, changed_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (table, col, col_type, seq, _now()),
        )


# ---------------------------------------------------------------------------
# 单条事件
# ---------------------------------------------------------------------------
def _insert(conn, table, data):
    cols = list(data)
    marks = ",".join("?" * len(cols))
    sql = "INSERT INTO %s (%s) VALUES (%s)" % (
        _quote_ident(table),
        ",".join(_quote_ident(c) for c in cols),
        marks,
    )
    return conn.execute(sql, list(data.values()))


def _update(conn, table, key, data):
    set_sql = ",".join("%s=?" % _quote_ident(c) for c in data)
    where_sql = " AND ".join("%s=?" % _quote_ident(c) for c in key)
    sql = "UPDATE %s SET %s WHERE %s" % (
        _quote_ident(table), set_sql, where_sql)
    params = list(data.values()) + list(key.values())
    return conn.execute(sql, params)


def _delete(conn, table, key):
    where_sql = " AND ".join("%s=?" % _quote_ident(c) for c in key)
    sql = "DELETE FROM %s WHERE %s" % (_quote_ident(table), where_sql)
    return conn.execute(sql, list(key.values()))


def _upsert(conn, table, data):
    cols = list(data)
    marks = ",".join("?" * len(cols))
    sql = "INSERT INTO %s (%s) VALUES (%s)" % (
        _quote_ident(table),
        ",".join(_quote_ident(c) for c in cols),
        marks,
    )
    pks = _primary_keys(conn, table)
    update_cols = [c for c in cols if c not in pks]
    if pks and update_cols:
        sql += " ON CONFLICT(%s) DO UPDATE SET %s" % (
            ",".join(_quote_ident(c) for c in pks),
            ",".join("%s=excluded.%s" % (_quote_ident(c), _quote_ident(c))
                     for c in update_cols),
        )
    elif pks:
        sql += " ON CONFLICT(%s) DO NOTHING" % ",".join(
            _quote_ident(c) for c in pks)
    return conn.execute(sql, list(data.values()))


def apply_event(conn, ev):
    """应用单条事件，返回受影响行数。

    - begin/commit 等控制事件返回 0；
    - delete/update 未命中任何行时返回 0（调用方据此计 skipped）；
    - 其余 DML 成功返回 1；
    - 表不存在抛 EventRejectedError，未知列自动扩展并记录到
      cdc_schema_changes，格式问题抛 WALFormatError。
    """
    op = ev.get("op")
    if op in CONTROL_OPS:
        return 0
    if op not in DML_OPS:
        raise WALFormatError("seq=%s 出现不支持的 op=%r"
                             % (ev.get("seq"), op))

    table = _ensure_table(conn, ev)
    data = ev.get("data") or {}
    key = ev.get("key") or {}
    if not isinstance(data, dict) or not isinstance(key, dict):
        raise WALFormatError("seq=%s 的 data/key 必须是对象" % ev.get("seq"))

    _ensure_columns(conn, table, dict(key, **data), ev.get("seq"))

    if op in ("update", "delete") and not key:
        raise WALFormatError("seq=%s 的 %s 操作缺少 key"
                             % (ev.get("seq"), op))
    if op in ("insert", "upsert") and not data:
        raise WALFormatError("seq=%s 的 %s 操作缺少 data"
                             % (ev.get("seq"), op))

    if op == "insert":
        cur = _insert(conn, table, data)
    elif op == "update":
        cur = _update(conn, table, key, data)
    elif op == "delete":
        cur = _delete(conn, table, key)
    else:
        cur = _upsert(conn, table, data)
    return cur.rowcount


# ---------------------------------------------------------------------------
# 整批应用
# ---------------------------------------------------------------------------
def apply_all(conn, events):
    """排序、去重后按事务原子应用全部事件，返回统计字典。

    统计键：applied / skipped / replayed / incomplete_tx / conflict，
    另带 errors（冲突与被拒收/回滚事务的可定位信息，供 CLI 决定退出码）。
    已应用的 seq 落在 cdc_applied_seq，整批重放时跳过且结果保持一致。
    """
    stats = {key: 0 for key in
             ("applied", "skipped", "replayed", "incomplete_tx", "conflict")}
    errors = []

    ensure_cdc_tables(conn)
    ordered, conflicts = normalize_events(events)
    conflict_seqs = set()
    for seq, first, other in conflicts:
        conflict_seqs.add(seq)
        errors.append(
            "冲突: seq=%s 出现内容不一致的事件，两条均不应用: %s != %s"
            % (seq, _canonical(first), _canonical(other))
        )
    stats["conflict"] = len(conflict_seqs)

    groups, incomplete_tx, incomplete_info = group_events(ordered)
    stats["incomplete_tx"] = incomplete_tx
    for tx in incomplete_info:
        errors.append(
            "不完整事务 tx=%s 缺少 commit，整段跳过(seq=%s)"
            % (tx.get("tx"), [e["seq"] for e in tx["events"]])
        )

    applied_set = {row[0] for row in conn.execute(
        "SELECT seq FROM cdc_applied_seq")}

    for kind, payload in groups:
        if kind == "single":
            tx_id = payload.get("tx")
            dml_events = [payload]
        else:
            tx_id = payload.get("tx")
            dml_events = [e for e in payload["events"]
                          if e.get("op") not in CONTROL_OPS]
        if not dml_events:
            continue

        local = {"applied": 0, "skipped": 0, "replayed": 0}
        pending_records = []
        try:
            # with conn：正常退出提交，抛异常时整段回滚，杜绝半截数据；
            # DML、ALTER 与 seq 落库在同一个事务里。
            with conn:
                for ev in dml_events:
                    seq = ev["seq"]
                    if seq in applied_set:
                        local["replayed"] += 1
                        continue
                    rowcount = apply_event(conn, ev)
                    if rowcount and rowcount > 0:
                        local["applied"] += 1
                    else:
                        local["skipped"] += 1
                    pending_records.append((seq, ev.get("tx"), ev.get("op")))
                for seq, tx_field, op in pending_records:
                    conn.execute(
                        "INSERT OR IGNORE INTO cdc_applied_seq"
                        "(seq, tx_id, op, applied_at) VALUES (?, ?, ?, ?)",
                        (seq, tx_field, op, _now()),
                    )
        except Exception as exc:  # 整组回滚后继续处理后续组
            errors.append(
                "事务(tx=%s, seqs=%s)应用失败已整体回滚: %s"
                % (tx_id, [e["seq"] for e in dml_events], exc)
            )
            continue

        for key_name in ("applied", "skipped", "replayed"):
            stats[key_name] += local[key_name]
        applied_set.update(seq for seq, _tx, _op in pending_records)

    stats["errors"] = errors
    return stats
