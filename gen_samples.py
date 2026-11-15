# -*- coding: utf-8 -*-
"""生成 CDC 样本：schema.sql 与 wal.jsonl。

wal 里混入了真实环境常见的情况：事务边界、同一事件被重放、乱序到达、
update/delete、以及带新增列的事件。
用法：python gen_samples.py
"""
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))

SCHEMA = """CREATE TABLE IF NOT EXISTS users (
  id   INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  city TEXT
);
"""


def ev(seq, op, tx, **kw):
    d = {"seq": seq, "op": op, "tx": tx}
    d.update(kw)
    return d


def events():
    return [
        ev(1, "begin", 1),
        ev(2, "insert", 1, table="users", data={"id": 1, "name": "张三", "city": "长春"}),
        ev(3, "insert", 1, table="users", data={"id": 2, "name": "李四", "city": "吉林"}),
        ev(4, "insert", 1, table="users", data={"id": 3, "name": "王五", "city": "四平"}),
        ev(5, "commit", 1),
        # 上游重放：与 seq=2 完全相同的 insert
        ev(2, "insert", 1, table="users", data={"id": 1, "name": "张三", "city": "长春"}),
        ev(7, "insert", 2, table="users", data={"id": 4, "name": "赵六", "city": "长春"}),
        # 乱序：seq=9 先于 seq=8 到达
        ev(9, "update", 2, table="users", key={"id": 1}, data={"city": "沈阳"}),
        ev(8, "delete", 2, table="users", key={"id": 2}),
        # schema 演进：多出一个 vip 列
        ev(10, "insert", 3, table="users", data={"id": 5, "name": "孙七", "city": "北京", "vip": 1}),
        ev(11, "begin", 4),
        ev(12, "insert", 4, table="users", data={"id": 6, "name": "周八", "city": "通化"}),
        ev(13, "commit", 4),
    ]


def main():
    with open(os.path.join(HERE, "schema.sql"), "w", encoding="utf-8") as f:
        f.write(SCHEMA)
    path = os.path.join(HERE, "wal.jsonl")
    with open(path, "w", encoding="utf-8") as f:
        for e in events():
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print("wal.jsonl %d 条事件" % len(events()))


if __name__ == "__main__":
    main()
