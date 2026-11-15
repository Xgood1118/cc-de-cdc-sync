# cdc-sync

变更日志（WAL）同步器骨架。`wal.jsonl` 是 13 条 JSONL 事件，`schema.sql` 是目标表结构。

## 已知缺口

- 只实现 insert，update/delete/begin/commit 全部静默返回 0
- 不排序、不去重，重复的 seq=2 会撞 `UNIQUE constraint failed: users.id`
- 事务没有原子性，不完整事务照样执行
- 不支持幂等重放
- 未知列（第 10 条的 vip）没有处理策略
- 没有统计，没有退出码区分

## 运行

```powershell
python gen_samples.py
python -m cdc.cli wal.jsonl --db out.db
python -m pytest tests -q
```

## 初始态实测

```
应用失败: UNIQUE constraint failed: users.id
```
