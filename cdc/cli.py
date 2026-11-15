# -*- coding: utf-8 -*-
"""命令行入口：python -m cdc.cli wal.jsonl --db out.db

退出码：
- 0：全部成功；
- 1：存在 seq 冲突或被拒收/回滚的事件；
- 2：输入（WAL 读取/格式）或 schema 错误。
"""
import argparse
import sqlite3
import sys

from .core import (
    CDCError,
    EventRejectedError,
    SchemaError,
    WALFormatError,
    apply_all,
    ensure_cdc_tables,
    ensure_schema,
    read_wal,
)

STATS_KEYS = ("applied", "skipped", "replayed", "incomplete_tx", "conflict")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="cdc",
                                 description="变更日志(WAL)同步器")
    ap.add_argument("wal")
    ap.add_argument("--db", default="cdc_out.db")
    ap.add_argument("--schema", default="schema.sql")
    args = ap.parse_args(argv)

    # 1) 读 WAL：文件/JSON/字段格式问题 -> 退出码 2
    try:
        events = read_wal(args.wal)
    except (OSError, WALFormatError) as exc:
        sys.stderr.write("读取变更日志失败: %s\n" % exc)
        return 2

    conn = sqlite3.connect(args.db)
    try:
        # 2) 建表：schema.sql 不可读或执行失败 -> 退出码 2
        try:
            ensure_schema(conn, args.schema)
            ensure_cdc_tables(conn)
        except SchemaError as exc:
            sys.stderr.write("schema 错误: %s\n" % exc)
            return 2

        # 3) 应用：冲突/拒收/回滚不中断整批，靠 errors 决定退出码 1
        try:
            stats = apply_all(conn, events)
        except WALFormatError as exc:
            sys.stderr.write("输入格式错误: %s\n" % exc)
            return 2
        except EventRejectedError as exc:
            sys.stderr.write("事件被拒收: %s\n" % exc)
            return 1
        except (CDCError, sqlite3.DatabaseError) as exc:
            sys.stderr.write("应用失败: %s\n" % exc)
            return 1
    finally:
        conn.close()

    for msg in stats.get("errors", []):
        sys.stderr.write(msg + "\n")

    line = " ".join("%s=%d" % (k, stats[k]) for k in STATS_KEYS)
    print("事件 %d 条，%s" % (len(events), line))
    return 1 if stats.get("errors") else 0


if __name__ == "__main__":
    sys.exit(main())
