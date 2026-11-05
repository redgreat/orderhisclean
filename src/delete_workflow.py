"""DeleteWorkflowHandler – 清理已软删且超过30天的工作流运行时数据

数据范围:
    workflowruntimeitems.Deleted = 1 且 CreatedAt 超过30天
    -> 级联删除全部 workflowruntime* 子表数据（单连接单事务）
"""
from __future__ import annotations

import datetime
import time

from base_handler import BaseHandler
from dbutil import cascade_delete_workflow_items, connect, write_job_log

SQL_SELECT_ITEMS = """
    SELECT i.Id, i.TargetEntityId
    FROM workflowruntimeitems i
    WHERE i.Deleted = 1
      AND i.CreatedAt < DATE_ADD(CURDATE(), INTERVAL -30 DAY)
    ORDER BY i.Id
    LIMIT %s
"""


class DeleteWorkflowHandler(BaseHandler):
    def __init__(
        self,
        connection_kwargs: dict,
        config: dict,
        cut_off_time: datetime.time | None = None,
        db_resolver=None,
    ) -> None:
        if cut_off_time is None:
            # 默认截止时间：23:00
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
            return self._process_items()
        except Exception as e:
            self.log.error(f"处理过程中发生错误: {e}")
            return True

    def _process_items(self) -> bool:
        with connect(self.conn_kwargs) as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(SQL_SELECT_ITEMS, (self.batch_size,))
                    rows = cur.fetchall()

                if not rows:
                    self.log.info("没有更多已软删的workflowruntimeitems记录需要删除")
                    return True

                item_ids = [row["Id"] for row in rows]
                workorder_ids = sorted({row["TargetEntityId"] for row in rows})
                self.log.info(f"本批删除 {len(item_ids)} 个已软删工单项，关联工单主单: {workorder_ids}")

                # 日志表检查要在写入之前做（其内部会 commit，不能放在删除事务中间）
                job_log_ok = self._ensure_job_log(conn)
                with conn.cursor() as cur:
                    counts = cascade_delete_workflow_items(cur, item_ids)
                    if job_log_ok:
                        write_job_log(conn, self.__class__.__name__, "delete", self.db_tag,
                                      [(r["TargetEntityId"], r["Id"]) for r in rows])
                conn.commit()
                self.log.info(f"已删除 {len(item_ids)} 个工单项及其子表数据: {counts}")

                if self.pause_seconds:
                    self.log.info(f"批处理完成，等待{self.pause_seconds}秒开始下一批...")
                    time.sleep(self.pause_seconds)
                return False
            except Exception as e:
                conn.rollback()
                self.log.error(f"删除工作流数据时发生错误，已回滚本批: {e}")
                raise
