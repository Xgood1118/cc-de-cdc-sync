# -*- coding: utf-8 -*-
import os
import sqlite3
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from cdc.core import apply_event, ensure_schema, read_wal


def wal():
    return os.path.join(ROOT, "wal.jsonl")


def schema():
    return os.path.join(ROOT, "schema.sql")


class ReaderTest(unittest.TestCase):
    def test_event_count(self):
        events = read_wal(wal())
        self.assertEqual(len(events), 13)

    def test_order_kept_as_file(self):
        events = read_wal(wal())
        self.assertEqual(events[0]["op"], "begin")
        self.assertEqual(events[7]["seq"], 9)

    def test_duplicate_present(self):
        events = read_wal(wal())
        inserts = [e for e in events if e.get("op") == "insert" and e["data"]["id"] == 1]
        self.assertEqual(len(inserts), 2)


class ApplyTest(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        ensure_schema(self.conn, schema())

    def tearDown(self):
        self.conn.close()

    def test_single_insert(self):
        n = apply_event(self.conn, {"op": "insert", "table": "users",
                                    "data": {"id": 7, "name": "测试", "city": "长春"}})
        self.assertEqual(n, 1)
        rows = self.conn.execute("select count(*) from users").fetchone()[0]
        self.assertEqual(rows, 1)

    def test_commit_ignored(self):
        n = apply_event(self.conn, {"op": "commit", "tx": 1})
        self.assertEqual(n, 0)


if __name__ == "__main__":
    unittest.main()
