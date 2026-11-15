# -*- coding: utf-8 -*-
"""CDC 同步器的完整行为测试（仅用标准库 unittest，pytest 可直接收集）。

覆盖：乱序、重复 seq 去重、内容冲突、update 局部列、delete 未命中、
upsert、事务原子性回滚、不完整事务跳过、孤立事件、重放幂等、未知列自动
扩展、表不存在拒收，以及 CLI 端到端的统计与退出码。
"""
import contextlib
import io
import json
import os
import sqlite3
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import sys
sys.path.insert(0, ROOT)

from cdc.cli import main
from cdc.core import (
    EventRejectedError,
    WALFormatError,
    apply_all,
    apply_event,
    ensure_schema,
    group_events,
    normalize_events,
    read_wal,
)

SCHEMA = os.path.join(ROOT, "schema.sql")
WAL = os.path.join(ROOT, "wal.jsonl")


def ev(seq, op, **kw):
    d = {"seq": seq, "op": op}
    d.update(kw)
    return d


def fresh_conn():
    conn = sqlite3.connect(":memory:")
    ensure_schema(conn, SCHEMA)
    return conn


def rows(conn):
    return conn.execute(
        "SELECT id, name, city FROM users ORDER BY id").fetchall()


class OrderingAndDedupTest(unittest.TestCase):
    def test_out_of_order_applied_by_seq(self):
        conn = fresh_conn()
        events = [
            ev(5, "update", table="users", key={"id": 1}, data={"city": "沈阳"}),
            ev(2, "insert", table="users",
               data={"id": 1, "name": "张三", "city": "长春"}),
        ]
        stats = apply_all(conn, events)
        self.assertEqual(stats["conflict"], 0)
        self.assertEqual(rows(conn), [(1, "张三", "沈阳")])
        conn.close()

    def test_duplicate_seq_identical_applied_once(self):
        conn = fresh_conn()
        payload = {"id": 1, "name": "张三", "city": "长春"}
        events = [
            ev(2, "insert", table="users", data=dict(payload)),
            ev(2, "insert", table="users", data=dict(payload)),
        ]
        stats = apply_all(conn, events)
        self.assertEqual(stats["applied"], 1)
        self.assertEqual(stats["conflict"], 0)
        self.assertEqual(len(rows(conn)), 1)
        conn.close()

    def test_duplicate_seq_conflict_neither_applied(self):
        conn = fresh_conn()
        events = [
            ev(2, "insert", table="users",
               data={"id": 1, "name": "张三", "city": "长春"}),
            ev(2, "insert", table="users",
               data={"id": 1, "name": "另一个人", "city": "长春"}),
        ]
        stats = apply_all(conn, events)
        self.assertEqual(stats["conflict"], 1)
        self.assertEqual(stats["applied"], 0)
        self.assertTrue(any("冲突" in m for m in stats["errors"]))
        self.assertEqual(len(rows(conn)), 0)
        conn.close()

    def test_normalize_dedup_and_sort(self):
        events = [
            ev(3, "commit", tx=1),
            ev(2, "insert", tx=1, table="users", data={"id": 1}),
            ev(2, "insert", tx=1, table="users", data={"id": 1}),
        ]
        ordered, conflicts = normalize_events(events)
        self.assertEqual([e["seq"] for e in ordered], [2, 3])
        self.assertEqual(conflicts, [])


class OpSemanticsTest(unittest.TestCase):
    def setUp(self):
        self.conn = fresh_conn()

    def tearDown(self):
        self.conn.close()

    def test_update_only_given_columns(self):
        apply_event(self.conn, ev(1, "insert", table="users",
                                  data={"id": 10, "name": "钱九", "city": "吉林"}))
        n = apply_event(self.conn, ev(2, "update", table="users",
                                      key={"id": 10}, data={"city": "白山"}))
        self.assertEqual(n, 1)
        self.assertEqual(rows(self.conn), [(10, "钱九", "白山")])

    def test_update_missing_row_is_skipped(self):
        n = apply_event(self.conn, ev(1, "update", table="users",
                                      key={"id": 99}, data={"city": "x"}))
        self.assertEqual(n, 0)

    def test_delete_missing_row_is_skipped_not_error(self):
        n = apply_event(self.conn, ev(1, "delete", table="users",
                                      key={"id": 99}))
        self.assertEqual(n, 0)
        stats = apply_all(self.conn, [
            ev(1, "delete", table="users", key={"id": 99})])
        self.assertEqual(stats["skipped"], 1)
        self.assertEqual(stats["applied"], 0)

    def test_delete_existing_row(self):
        apply_event(self.conn, ev(1, "insert", table="users",
                                  data={"id": 11, "name": "甲", "city": "乙"}))
        n = apply_event(self.conn, ev(2, "delete", table="users",
                                      key={"id": 11}))
        self.assertEqual(n, 1)
        self.assertEqual(rows(self.conn), [])

    def test_upsert_inserts_then_updates(self):
        apply_event(self.conn, ev(1, "upsert", table="users",
                                  data={"id": 12, "name": "丙", "city": "丁"}))
        apply_event(self.conn, ev(2, "upsert", table="users",
                                  data={"id": 12, "name": "戊", "city": "己"}))
        self.assertEqual(rows(self.conn), [(12, "戊", "己")])

    def test_unknown_op_rejected(self):
        with self.assertRaises(WALFormatError):
            apply_event(self.conn, ev(1, "merge", table="users", data={}))


class TransactionTest(unittest.TestCase):
    def test_committed_tx_applied_atomically(self):
        conn = fresh_conn()
        events = [
            ev(1, "begin", tx=1),
            ev(2, "insert", tx=1, table="users",
               data={"id": 1, "name": "a", "city": "x"}),
            ev(3, "insert", tx=1, table="users",
               data={"id": 2, "name": "b", "city": "y"}),
            ev(4, "commit", tx=1),
        ]
        stats = apply_all(conn, events)
        self.assertEqual(stats["applied"], 2)
        self.assertEqual(len(rows(conn)), 2)
        conn.close()

    def test_failed_tx_fully_rolled_back(self):
        conn = fresh_conn()
        events = [
            ev(1, "begin", tx=1),
            ev(2, "insert", tx=1, table="users",
               data={"id": 1, "name": "a", "city": "x"}),
            ev(3, "insert", tx=1, table="users",
               data={"id": 1, "name": "dup", "city": "y"}),  # 主键冲突
            ev(4, "commit", tx=1),
            # 后续孤立事件不受影响，照常应用
            ev(5, "insert", table="users",
               data={"id": 9, "name": "ok", "city": "z"}),
        ]
        stats = apply_all(conn, events)
        self.assertEqual(rows(conn), [(9, "ok", "z")])  # 无半截数据
        self.assertEqual(stats["applied"], 1)
        self.assertTrue(stats["errors"])
        # 回滚事务的 seq 不能被记成已应用
        applied = {r[0] for r in conn.execute("SELECT seq FROM cdc_applied_seq")}
        self.assertEqual(applied, {5})
        conn.close()

    def test_incomplete_tx_skipped_and_counted(self):
        conn = fresh_conn()
        events = [
            ev(1, "begin", tx=7),
            ev(2, "insert", tx=7, table="users",
               data={"id": 1, "name": "a", "city": "x"}),
            ev(3, "insert", tx=7, table="users",
               data={"id": 2, "name": "b", "city": "y"}),
            # 没有 commit；后续事件属于另一个 tx，隐式结束前一不完整事务
            ev(4, "insert", tx=8, table="users",
               data={"id": 8, "name": "orphan", "city": "z"}),
        ]
        stats = apply_all(conn, events)
        self.assertEqual(stats["incomplete_tx"], 1)
        self.assertEqual(stats["applied"], 1)  # 孤立事件照常
        self.assertEqual(rows(conn), [(8, "orphan", "z")])
        conn.close()

    def test_orphan_events_treated_as_single_tx(self):
        conn = fresh_conn()
        stats = apply_all(conn, [
            ev(7, "insert", table="users",
               data={"id": 4, "name": "赵六", "city": "长春"}),
        ])
        self.assertEqual(stats["applied"], 1)
        conn.close()

    def test_group_events_structure(self):
        groups, incomplete, info = group_events([
            ev(1, "begin", tx=1),
            ev(2, "insert", tx=1, table="users", data={}),
            ev(3, "commit", tx=1),
            ev(4, "insert", table="users", data={}),
            ev(5, "begin", tx=2),
            ev(6, "insert", tx=2, table="users", data={}),
        ])
        self.assertEqual(len(groups), 2)
        self.assertEqual(groups[0][0], "tx")
        self.assertEqual(groups[1][0], "single")
        self.assertEqual(incomplete, 1)
        self.assertEqual(info[0]["tx"], 2)


class ReplayIdempotencyTest(unittest.TestCase):
    def _events(self):
        return [
            ev(1, "begin", tx=1),
            ev(2, "insert", tx=1, table="users",
               data={"id": 1, "name": "a", "city": "x"}),
            ev(3, "insert", tx=1, table="users",
               data={"id": 2, "name": "b", "city": "y"}),
            ev(4, "commit", tx=1),
            ev(5, "update", table="users", key={"id": 1},
               data={"city": "沈阳"}),
            ev(6, "delete", table="users", key={"id": 2}),
        ]

    def test_replay_same_connection_no_change(self):
        conn = fresh_conn()
        events = self._events()
        first = apply_all(conn, events)
        self.assertEqual(first["replayed"], 0)
        second = apply_all(conn, events)
        self.assertEqual(second["applied"], 0)
        self.assertEqual(second["replayed"], first["applied"])
        self.assertEqual(rows(conn), [(1, "a", "沈阳")])
        conn.close()

    def test_replay_new_connection_persisted(self):
        fd, dbpath = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            events = self._events()
            c1 = sqlite3.connect(dbpath)
            ensure_schema(c1, SCHEMA)
            apply_all(c1, events)
            c1.close()

            c2 = sqlite3.connect(dbpath)
            stats = apply_all(c2, events)
            self.assertEqual(stats["applied"], 0)
            self.assertEqual(stats["replayed"], 4)
            self.assertEqual(
                c2.execute("SELECT id, name, city FROM users").fetchall(),
                [(1, "a", "沈阳")])
            c2.close()
        finally:
            os.unlink(dbpath)


class SchemaEvolutionTest(unittest.TestCase):
    def test_unknown_column_auto_extended_and_recorded(self):
        conn = fresh_conn()
        apply_all(conn, [
            ev(10, "insert", table="users",
               data={"id": 5, "name": "孙七", "city": "北京", "vip": 1}),
        ])
        cols = {r[1] for r in conn.execute("PRAGMA table_info(users)")}
        self.assertIn("vip", cols)
        change = conn.execute(
            "SELECT table_name, column_name, column_type, source_seq "
            "FROM cdc_schema_changes").fetchall()
        self.assertEqual(change, [("users", "vip", "INTEGER", 10)])
        row = conn.execute("SELECT id, vip FROM users WHERE id=5").fetchone()
        self.assertEqual(row, (5, 1))
        conn.close()

    def test_missing_table_raises_locatable_error(self):
        conn = fresh_conn()
        with self.assertRaises(EventRejectedError) as ctx:
            apply_event(conn, ev(1, "insert", table="ghost",
                                 data={"id": 1}))
        self.assertIn("ghost", str(ctx.exception))
        self.assertIn("seq=1", str(ctx.exception))
        conn.close()

    def test_missing_table_recorded_by_apply_all(self):
        conn = fresh_conn()
        stats = apply_all(conn, [
            ev(1, "insert", table="ghost", data={"id": 1}),
            ev(2, "insert", table="users",
               data={"id": 1, "name": "a", "city": "b"}),
        ])
        self.assertTrue(stats["errors"])
        self.assertEqual(rows(conn), [(1, "a", "b")])
        conn.close()


class CliEndToEndTest(unittest.TestCase):
    def _run_cli(self, wal_path, db_path, schema_path=SCHEMA):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = main([wal_path, "--db", db_path, "--schema", schema_path])
        return code, buf.getvalue()

    def test_shipped_wal_final_state_and_stats(self):
        fd, dbpath = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            code, out = self._run_cli(WAL, dbpath)
            self.assertEqual(code, 0)
            for key in ("applied", "skipped", "replayed",
                        "incomplete_tx", "conflict"):
                self.assertIn(key + "=", out)
            self.assertIn("applied=8", out)

            conn = sqlite3.connect(dbpath)
            data = conn.execute(
                "SELECT id, name, city FROM users ORDER BY id").fetchall()
            self.assertEqual(data, [
                (1, "张三", "沈阳"),
                (3, "王五", "四平"),
                (4, "赵六", "长春"),
                (5, "孙七", "北京"),
                (6, "周八", "通化"),
            ])
            self.assertEqual(len(data), 5)
            # 第二次整批重放：全部命中幂等记录，数据不变
            code2, out2 = self._run_cli(WAL, dbpath)
            self.assertEqual(code2, 0)
            self.assertIn("replayed=8", out2)
            self.assertIn("applied=0", out2)
            data2 = conn.execute(
                "SELECT id, name, city FROM users ORDER BY id").fetchall()
            self.assertEqual(data, data2)
            conn.close()
        finally:
            os.unlink(dbpath)

    def test_conflict_exit_code_1(self):
        tmp = tempfile.NamedTemporaryFile(
            "w", suffix=".jsonl", delete=False, encoding="utf-8")
        try:
            tmp.write(json.dumps(ev(1, "insert", table="users",
                                    data={"id": 1, "name": "a"}),
                                 ensure_ascii=False) + "\n")
            tmp.write(json.dumps(ev(1, "insert", table="users",
                                    data={"id": 1, "name": "b"}),
                                 ensure_ascii=False) + "\n")
            tmp.close()
            fd, dbpath = tempfile.mkstemp(suffix=".db")
            os.close(fd)
            try:
                code, _ = self._run_cli(tmp.name, dbpath)
                self.assertEqual(code, 1)
            finally:
                os.unlink(dbpath)
        finally:
            os.unlink(tmp.name)

    def test_bad_input_exit_code_2(self):
        tmp = tempfile.NamedTemporaryFile(
            "w", suffix=".jsonl", delete=False, encoding="utf-8")
        tmp.write("{not json\n")
        tmp.close()
        fd, dbpath = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            code, _ = self._run_cli(tmp.name, dbpath)
            self.assertEqual(code, 2)
        finally:
            os.unlink(dbpath)
            os.unlink(tmp.name)

    def test_missing_schema_exit_code_2(self):
        fd, dbpath = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            code, _ = self._run_cli(WAL, dbpath,
                                    schema_path=os.path.join(ROOT, "no-such.sql"))
            self.assertEqual(code, 2)
        finally:
            os.unlink(dbpath)


if __name__ == "__main__":
    unittest.main()
