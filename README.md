# cdc-sync

变更日志（WAL）同步器。读取 `wal.jsonl`（13 条 JSONL 事件），按 seq 排序去重后
事务化应用到 `schema.sql` 定义的 SQLite 目标表。仅使用 Python 标准库。

## 运行

```powershell
python gen_samples.py          # 可选：重新生成 wal.jsonl / schema.sql
python -m cdc.cli wal.jsonl --db out.db
python -m pytest tests -q
```

## 设计要点

- **排序与去重**：`read_wal` 保持文件顺序（便于排查），`apply_all` 内按 `seq`
  升序重排；同 seq 完全一致的事件只应用一次，内容不一致时两条都不应用并计入
  `conflict`，错误信息里带两个版本的完整内容。
- **变更类型**：`insert` / `update`（按 `key` 定位，只改 `data` 给出的列）/
  `delete`（按 `key` 定位，未命中记 `skipped` 不报错）/ `upsert`
  （`ON CONFLICT(主键) DO UPDATE`，无主键时退化为 `DO NOTHING`）。
- **事务原子性**：`begin/commit` 之间的事件在同一个 SQLite 事务里应用，任何一条
  失败整段回滚，不留下半截数据，已回滚的 seq 不会写入幂等表；`begin` 后没有
  `commit` 的事务整体跳过并计入 `incomplete_tx`（边界结合事件的 `tx` 标识判断）；
  没有 `begin` 的孤立事件按单条事务处理。
- **幂等重放**：已应用的 seq 记录在 `cdc_applied_seq` 表，与业务 DML 同事务提交；
  整批重放时这些事件计入 `replayed` 并跳过，行数与内容不发生任何变化。
- **未知列策略（自动扩展）**：事件出现 `schema.sql` 里没有的列时，自动
  `ALTER TABLE ADD COLUMN`（按值推断类型：int→INTEGER、float→REAL、bytes→BLOB、
  其余→TEXT），并把变更写入 `cdc_schema_changes` 表（表名、列名、类型、来源 seq、
  时间戳），不静默丢列。目标表不存在则抛出带 seq 与表名的可定位错误并拒收该事务。
- **表名/列名转义**：全部走双引号标识符转义，值全部参数化。

## 统计与退出码

每次运行输出五项计数：`applied`（实际写入）、`skipped`（delete/update 未命中）、
`replayed`（命中幂等记录跳过）、`incomplete_tx`（跳过的不完整事务数）、
`conflict`（内容冲突的 seq 数）；详细原因写入 stderr。

- `0`：全部成功
- `1`：存在 seq 冲突，或有事件被拒收 / 事务回滚
- `2`：输入文件、JSON/事件格式或 schema.sql 错误

## 实测输出

首次运行（`python -m cdc.cli wal.jsonl --db out.db`）：

```
事件 13 条，applied=8 skipped=0 replayed=0 incomplete_tx=0 conflict=0
```

第二次整批重放（同一 db 文件，再跑一遍同样的命令）：

```
事件 13 条，applied=0 skipped=0 replayed=8 incomplete_tx=0 conflict=0
```

最终 `users` 表为 5 行（重复的 seq=2 只插入一次，乱序的 seq=8/9 按序处理，
id=2 被删除，id=1 的 city 改为沈阳，seq=10 自动扩展出 `vip` 列）：

| id | name | city |
|----|------|------|
| 1  | 张三 | 沈阳 |
| 3  | 王五 | 四平 |
| 4  | 赵六 | 长春 |
| 5  | 孙七 | 北京 |
| 6  | 周八 | 通化 |

`cdc_schema_changes` 中的结构变更记录：

```
('users', 'vip', 'INTEGER', 10)
```

## 测试

`python -m pytest tests -q` 共 29 个用例，覆盖：乱序应用、重复 seq 去重、
内容冲突拒收、update 局部列、delete 未命中 skipped、upsert、事务原子性回滚、
不完整事务跳过、孤立事件、重放幂等（同连接与新连接两种）、未知列自动扩展与
变更记录、表不存在拒收，以及 CLI 端到端的最终数据、重放和三档退出码。
