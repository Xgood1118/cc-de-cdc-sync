# -*- coding: utf-8 -*-
"""命令行入口：python -m cdc.cli wal.jsonl --db out.db"""
import argparse
import sqlite3
import sys

from .core import apply_all, ensure_schema, read_wal


def main(argv=None):
    ap = argparse.ArgumentParser(prog="cdc")
    ap.add_argument("wal")
    ap.add_argument("--db", default="cdc_out.db")
    ap.add_argument("--schema", default="schema.sql")
    args = ap.parse_args(argv)
    try:
        events = read_wal(args.wal)
    except Exception as exc:
        sys.stderr.write("读取变更日志失败: %s\n" % exc)
        return 2
    conn = sqlite3.connect(args.db)
    try:
        ensure_schema(conn, args.schema)
        n = apply_all(conn, events)
    except Exception as exc:
        sys.stderr.write("应用失败: %s\n" % exc)
        conn.close()
        return 1
    conn.close()
    print("事件 %d 条，应用 %d 条" % (len(events), n))
    return 0


if __name__ == "__main__":
    sys.exit(main())
