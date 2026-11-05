"""DeleteActorsHandler – 清理长期已完成工单中残留的 PROCESSING 状态 actors

数据范围:
    workflowruntimeitems.Status = 'ACCEPTED' 且创建超过90天
    且存在仍处于 PROCESSING 状态的 actors -> 删除这些残留 actors
"""
from __future__ import annotations

import datetime
import time

from base_handler import BaseHandler
from dbutil import connect, ph, write_job_log

SQL_SELECT_ITEMS = """
    SELECT i.Id, i.TargetEntityId
    FROM workflowruntimeitems i
    WHERE i.Status = 'ACCEPTED'
      AND i.CreatedAt < DATE_ADD(CURDATE(), INTERVAL -90 DAY)
      AND EXISTS(SELECT 1
        FROM workflowruntimesteps s
        JOIN workflowruntimeactors a
          ON a.RuntimeStepId=s.Id
          AND a.Status='PROCESSING'
          AND a.Active=1
          AND a.Deleted=0
        WHERE s.RuntimeItemId=i.Id
          AND s.Status='ACCEPTED'
          AND s.Deleted=0)
    ORDER BY i.Id
    LIMIT %s
"""

SQL_SELECT_STEPS = """
    SELECT Id
    FROM workflowruntimesteps
    WHERE Status = 'ACCEPTED'
      AND RuntimeItemId IN ({ids})
"""


class DeleteActorsHandler(BaseHandler):
    def __init__(
        self,
        connection_kwargs: dict,
        config: dict,
        cut_off_time: datetime.time | None = None,
        db_resolver=None,
    ) -> None:
        if cut_off_time is None:
            cut_off_time = datetime.time(hour=23, minute=0, second=0)
        super().__init__(cut_off_time)

        config = config or {}
        self.conn_kwargs = connection_kwargs
        self.db_tag = config.get('_db_tag')
        self.batch_size = config.get('batch_size', 100)
        self.pause_seconds = config.get('pause_seconds', 30)

    # --------------------------------------------------------
    # Implementation
    # --------------------------------------------------------
    def _process_once(self) -> bool:
        try:
            return self._clean_complete_actors()
        except Exception as e:
            self.log.error(f"处理过程中发生错误: {e}")
            return True

    def _clean_complete_actors(self) -> bool:
        processing_finished = True

        with connect(self.conn_kwargs) as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(SQL_SELECT_ITEMS, (self.batch_size,))
                    item_rows = cur.fetchall()

                if not item_rows:
                    self.log.info("未找到90天前已完成但仍残留PROCESSING actors的工单")
                    return True

                processing_finished = False
                item_ids = [row["Id"] for row in item_rows]
                workorder_ids = sorted({row["TargetEntityId"] for row in item_rows})
                self.log.info(f"找到{len(item_ids)}条残留PROCESSING actors的工单项，关联工单主单: {workorder_ids}")

                # 先查 steps，再按 step 删除残留的 actors
                with conn.cursor() as cur:
                    cur.execute(SQL_SELECT_STEPS.format(ids=ph(item_ids)), item_ids)
                    step_ids = [row["Id"] for row in cur.fetchall()]

                # 日志表检查要在写入之前做（其内部会 commit，不能放在删除事务中间）
                job_log_ok = self._ensure_job_log(conn)
                if step_ids:
                    with conn.cursor() as cur:
                        deleted = cur.execute(
                            f"DELETE FROM workflowruntimeactors WHERE RuntimeStepId IN ({ph(step_ids)}) AND Active = 1 AND Status='PROCESSING'",
                            step_ids
                        )
                    self.log.info(f"已删除{deleted}条残留的actors记录")
                else:
                    self.log.info("未找到需要清理的步骤记录")

                if job_log_ok:
                    write_job_log(conn, self.__class__.__name__, "delete", self.db_tag,
                                  [(r["TargetEntityId"], r["Id"]) for r in item_rows])
                conn.commit()

                if self.pause_seconds:
                    self.log.info(f"批处理完成，等待{self.pause_seconds}秒开始下一批...")
                    time.sleep(self.pause_seconds)
            except Exception as e:
                conn.rollback()
                self.log.error(f"清理残留actors数据时发生错误，已回滚本批: {e}")
                raise
        return processing_finished
