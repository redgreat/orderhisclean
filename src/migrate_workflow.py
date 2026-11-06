"""MigrateWorkflowHandler – 按工单主单创建年份迁移工作流运行时数据到完成时表

数据范围:
    tb_workorderinfo.CreatedAt 落在配置年份区间内
    -> workflowruntimeitems.TargetEntityId = tb_workorderinfo.Id
    -> 级联 steps / actors / comments / attachments / activities / relatedactors

每批在同一事务内完成: 删除 workflowcomplete* 中已存在的同工单项记录 -> 插入 workflowcomplete* -> 删除 workflowruntime* (以运行时数据为准移动)。
存在 workflowruntimereminderlogs 记录的工单项不迁移 (该表无完成时对应表, 且外键指向 items)。
"""
from __future__ import annotations

import datetime
import time
from concurrent.futures import ThreadPoolExecutor

import pymysql

from base_handler import BaseHandler
from dbutil import connect, delete_in, insert_into_from, ph, write_job_log

# (运行时表, 完成时表)
TABLE_PAIRS = [
    ("workflowruntimeitems", "workflowcompleteitems"),
    ("workflowruntimesteps", "workflowcompletesteps"),
    ("workflowruntimeactors", "workflowcompleteactors"),
    ("workflowruntimecomments", "workflowcompletecomments"),
    ("workflowruntimeattachments", "workflowcompleteattachments"),
    ("workflowruntimeactivities", "workflowcompleteactivities"),
    ("workflowruntimerelatedactors", "workflowcompleterelatedactors"),
]

SQL_SELECT_ITEMS = """
    SELECT i.Id, i.TargetEntityId
    FROM workflowruntimeitems i
    WHERE i.TargetEntityId IN (
            SELECT w.Id FROM tb_workorderinfo w
            WHERE w.CreatedAt >= %s AND w.CreatedAt < %s
              AND w.WorkStatus=9)
      AND NOT EXISTS (SELECT 1 FROM workflowruntimereminderlogs r WHERE r.RuntimeItemId = i.Id)
      AND MOD(CRC32(i.Id), %s) = %s
    ORDER BY i.Id
    LIMIT %s
"""

DEADLOCK_ERRORS = (1213, 1205)  # Deadlock found / Lock wait timeout
DEADLOCK_RETRIES = 3


class MigrateWorkflowHandler(BaseHandler):
    def __init__(
        self,
        connection_kwargs: dict,
        config: dict,
        cut_off_time: datetime.time | None = None,
        db_resolver=None,
        partition: int | None = None,
    ) -> None:
        if cut_off_time is None:
            cut_off_time = datetime.time(hour=23, minute=0, second=0)
        super().__init__(cut_off_time)

        config = config or {}
        self.conn_kwargs = connection_kwargs
        self.db_tag = config.get('_db_tag')
        self.years = config.get('years') or []
        self.batch_size = config.get('batch_size', 50)
        self.pause_seconds = config.get('pause_seconds', 30)
        self.workers = max(1, config.get('workers', 1))
        self.partition = partition  # None: 主实例；0..workers-1: 并发子实例负责的哈希分片
        self._schema_checked = False

    # --------------------------------------------------------
    # Implementation
    # --------------------------------------------------------
    def run(self) -> None:
        """workers>1 时按 Id 哈希分片, 每个分片一个线程并行迁移, 分片之间互不抢同一批数据"""
        if self.partition is not None or self.workers == 1:
            return super().run()
        self.log.info(f"启动 {self.workers} 个并发分片")
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(self._clone(k).run) for k in range(self.workers)]
            for future in futures:
                future.result()

    def _clone(self, partition: int) -> MigrateWorkflowHandler:
        return MigrateWorkflowHandler(
            connection_kwargs=self.conn_kwargs,
            config={
                'years': self.years,
                'batch_size': self.batch_size,
                'pause_seconds': self.pause_seconds,
                'workers': self.workers,
            },
            cut_off_time=self.cut_off_time,
            partition=partition,
        )

    def _process_once(self) -> bool:
        if not self.years:
            self.log.warning("未配置 years，跳过本次任务")
            return True

        with connect(self.conn_kwargs) as conn:
            if not self._schema_checked:
                self._check_schema(conn)
                self._schema_checked = True

            for year in self.years:
                if self._migrate_batch(conn, year):
                    if self.pause_seconds:
                        self.log.info(f"批处理完成，等待{self.pause_seconds}秒开始下一批...")
                        time.sleep(self.pause_seconds)
                    return False

        self.log.info(f"工作流运行时数据迁移完成，年份: {self.years}")
        return True

    def _check_schema(self, conn) -> None:
        """运行时表与完成时表字段必须一致，否则 INSERT ... SELECT * 会错位"""
        sql = """
            SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) AS cols
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
        """
        with conn.cursor() as cur:
            for runtime_table, complete_table in TABLE_PAIRS:
                cols = []
                for table in (runtime_table, complete_table):
                    cur.execute(sql, (table,))
                    cols.append(cur.fetchone()["cols"])
                if cols[0] != cols[1]:
                    raise RuntimeError(f"{runtime_table} 与 {complete_table} 字段不一致，已停止迁移")

    def _migrate_batch(self, conn, year: int) -> bool:
        """迁移某年份的一批工单项，返回 False 表示该年份已无数据"""
        start = datetime.datetime(year, 1, 1)
        end = datetime.datetime(year + 1, 1, 1)

        with conn.cursor() as cur:
            cur.execute(SQL_SELECT_ITEMS, (start, end, self.workers, self.partition or 0, self.batch_size))
            rows = cur.fetchall()

        if not rows:
            self.log.info(f"{year}年没有需要迁移的工作流运行时数据")
            return False

        item_ids = [row["Id"] for row in rows]
        workorder_ids = sorted({row["TargetEntityId"] for row in rows})
        self.log.info(f"{year}年本批迁移 {len(item_ids)} 个工单项，关联工单主单: {workorder_ids}")

        # 日志表检查要在写入之前做（其内部会 commit，不能放在迁移事务中间）
        job_log_ok = self._ensure_job_log(conn)
        for attempt in range(1, DEADLOCK_RETRIES + 1):
            try:
                with conn.cursor() as cur:
                    # 完成时表与运行时表存在 CurrentStepId <-> RuntimeItemId 循环外键，批内统一关闭外键检查
                    cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
                    counts = self._move_batch(cur, item_ids)
                    cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
                if job_log_ok:
                    write_job_log(conn, self.__class__.__name__, "migrate", self.db_tag,
                                  [(r["TargetEntityId"], r["Id"]) for r in rows])
                conn.commit()
                break
            except pymysql.err.OperationalError as e:
                conn.rollback()
                if e.args[0] in DEADLOCK_ERRORS and attempt < DEADLOCK_RETRIES:
                    self.log.warning(f"{year}年迁移遇到死锁/锁超时，第{attempt}次重试: {e}")
                    time.sleep(attempt)
                    continue
                self.log.error(f"{year}年工作流运行时数据迁移失败，已回滚本批: {e}")
                raise
            except Exception as e:
                conn.rollback()
                self.log.error(f"{year}年工作流运行时数据迁移失败，已回滚本批: {e}")
                raise

        self.log.info(f"{year}年迁移 {len(item_ids)} 个工单项完成: {counts}")
        return True

    def _move_batch(self, cur, item_ids: list) -> dict:
        # 运行时与完成时两侧的子记录 Id 取并集，确保完成时已有的残留子记录也一并清理
        step_ids = self._ids_under(cur, ["workflowruntimesteps", "workflowcompletesteps"], "RuntimeItemId", item_ids)
        actor_ids = self._ids_under(cur, ["workflowruntimeactors", "workflowcompleteactors"], "RuntimeStepId", step_ids)
        comment_ids = self._ids_under(cur, ["workflowruntimecomments", "workflowcompletecomments"], "RuntimeActorId", actor_ids)

        # (运行时表, 完成时表, 关联字段, 关联值): 父表在前插入, 子表在前删除
        plan = [
            ("workflowruntimeitems", "workflowcompleteitems", "Id", item_ids),
            ("workflowruntimesteps", "workflowcompletesteps", "RuntimeItemId", item_ids),
            ("workflowruntimeactors", "workflowcompleteactors", "RuntimeStepId", step_ids),
            ("workflowruntimecomments", "workflowcompletecomments", "RuntimeActorId", actor_ids),
            ("workflowruntimeattachments", "workflowcompleteattachments", "RuntimeCommentId", comment_ids),
            ("workflowruntimeactivities", "workflowcompleteactivities", "RuntimeItemId", item_ids),
            ("workflowruntimerelatedactors", "workflowcompleterelatedactors", "RuntimeStepId", step_ids),
        ]

        counts = {}
        for _, complete_table, key, ids in reversed(plan):
            deleted = delete_in(cur, complete_table, key, ids)
            if deleted:
                counts[f"del {complete_table}"] = deleted

        for runtime_table, complete_table, key, ids in plan:
            inserted = insert_into_from(cur, complete_table, runtime_table, key, ids)
            if inserted:
                counts[complete_table] = inserted

        for runtime_table, _, key, ids in reversed(plan):
            deleted = delete_in(cur, runtime_table, key, ids)
            if deleted:
                counts[runtime_table] = deleted
        return counts

    @staticmethod
    def _ids_under(cur, tables: list, key: str, parent_ids: list) -> list:
        if not parent_ids:
            return []
        ids = set()
        for table in tables:
            cur.execute(f"SELECT Id FROM {table} WHERE {key} IN ({ph(parent_ids)})", parent_ids)
            ids.update(row["Id"] for row in cur.fetchall())
        return list(ids)
