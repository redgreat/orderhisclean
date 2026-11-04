"""DeleteWorkflowHandler – 按工单主单创建时间区间删除工作流运行时数据

数据范围:
    tb_workorderinfo.CreatedAt 落在 [start_date, end_date) 区间内
    -> workflowruntimeitems.TargetEntityId = tb_workorderinfo.Id
    -> 级联删除 workflowruntimesteps / workflowruntimeactors
    仅处理 tb_workorderinfo.ServiceProviderCode <> '1002' 的工单
"""
from __future__ import annotations

import datetime
import time

import pymysql
from loguru import logger

from base_handler import BaseHandler


class DeleteWorkflowHandler(BaseHandler):
    def __init__(
        self,
        connection_kwargs: dict,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        batch_size: int = 100,
        cut_off_time: datetime.time | None = None,
    ) -> None:
        if cut_off_time is None:
            # 默认截止时间：23:00
            cut_off_time = datetime.time(hour=23, minute=0, second=0)
        super().__init__(cut_off_time)

        self.conn_kwargs = connection_kwargs
        self.start_date = start_date
        self.end_date = end_date
        self.batch_size = batch_size

    # --------------------------------------------------------
    # Implementation
    # --------------------------------------------------------
    def _get_connection(self):
        return pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **self.conn_kwargs)

    def _process_once(self) -> bool:
        if not self.start_date or not self.end_date:
            logger.warning("DeleteWorkflowHandler 未配置 start_date/end_date，跳过本次任务")
            return True

        try:
            return self._process_items()
        except Exception as e:
            logger.error(f"处理过程中发生错误: {e}")
            # 发生错误时返回True，表示结束本轮处理
            return True

    def _process_items(self) -> bool:
        # 查询区间内工单关联的workflowruntimeitems记录
        sql_select = """
            SELECT i.Id
            FROM workflowruntimeitems i
            JOIN tb_workorderinfo w ON i.TargetEntityId = w.Id
            WHERE w.ServiceProviderCode <> '1002'
              AND w.CreatedAt >= %s
              AND w.CreatedAt < %s
            ORDER BY i.Id
            LIMIT %s
        """
        processing_finished = True
        item_ids = []

        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql_select, (self.start_date, self.end_date, self.batch_size))
                    rows = cur.fetchall()
                    if not rows:
                        logger.info(f"区间[{self.start_date}, {self.end_date})内没有更多workflowruntimeitems记录需要删除")
                    else:
                        # 有记录需要处理，标记为未完成
                        processing_finished = False
                        # 收集所有item_id
                        item_ids = [row["Id"] for row in rows]

                        # 批量处理steps和actors
                        self._process_steps(item_ids)

                        # 删除workflowruntimeitems记录
                        placeholders_ids = ",".join(["%s"] * len(item_ids))
                        sql_delete_items = f"DELETE FROM workflowruntimeitems WHERE Id IN ({placeholders_ids});"
                        deleted_count_items = cur.execute(sql_delete_items, item_ids)
                        conn.commit()
                        logger.info(f"已从workflowruntimeitems删除{deleted_count_items}条记录")

                        # 每处理完一批后等待30秒，减轻数据库负载
                        logger.info("批处理完成，等待30秒开始下一批...")
                        time.sleep(30)
            except Exception as e:
                conn.rollback()
                logger.error(f"处理workflowruntimeitems表数据时发生错误: {e}, 处理数据：{item_ids}")
                raise
        return processing_finished

    def _process_steps(self, item_ids) -> None:
        # 如果没有item_ids，则直接返回
        if not item_ids:
            logger.info("没有workflowruntimeitems记录需要处理")
            return

        # 使用IN查询批量获取所有相关的steps
        placeholders_items = ",".join(["%s"] * len(item_ids))
        sql_select_steps = f"SELECT Id FROM workflowruntimesteps WHERE RuntimeItemId IN ({placeholders_items});"

        step_ids = []
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    # 查询所有相关的steps
                    cur.execute(sql_select_steps, item_ids)
                    rows_steps = cur.fetchall()

                    if not rows_steps:
                        logger.info("没有相关的workflowruntimesteps记录需要删除")
                    else:
                        # 收集所有step_id并批量处理actors
                        step_ids = [row["Id"] for row in rows_steps]
                        self._process_actors(step_ids)

                    # 批量删除steps
                    sql_delete_steps = f"DELETE FROM workflowruntimesteps WHERE RuntimeItemId IN ({placeholders_items});"
                    cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
                    deleted_count_steps = cur.execute(sql_delete_steps, item_ids)
                    cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
                    conn.commit()
                    logger.info(f"已从workflowruntimesteps删除{deleted_count_steps}条记录")
            except Exception as e:
                conn.rollback()
                logger.error(f"处理workflowruntimesteps表数据时发生错误: {e}, 处理数据：{step_ids}")
                raise

    def _process_actors(self, step_ids) -> None:
        # 如果没有step_ids，则直接返回
        if not step_ids:
            logger.info("没有workflowruntimesteps记录需要处理")
            return

        # 使用IN查询批量删除所有相关的actors
        placeholders_steps = ",".join(["%s"] * len(step_ids))
        delete_actors = f"DELETE FROM workflowruntimeactors WHERE RuntimeStepId IN ({placeholders_steps});"

        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    deleted_count = cur.execute(delete_actors, step_ids)
                    conn.commit()
                    logger.info(f"已从workflowruntimeactors删除{deleted_count}条记录")
            except Exception as e:
                conn.rollback()
                logger.error(f"处理workflowruntimeactors表数据时发生错误: {e}, 处理数据：{step_ids}")
                raise
