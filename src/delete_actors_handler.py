from __future__ import annotations

import datetime
import time
import pymysql
from loguru import logger

from base_handler import BaseHandler

class DeleteActorsHandler(BaseHandler):
    def __init__(
        self,
        connection_kwargs: dict,
        batch_size: int = 100,
        cut_off_time: datetime.time | None = None,
    ) -> None:
        if cut_off_time is None:
            cut_off_time = datetime.time(hour=23, minute=0, second=0)
        super().__init__(cut_off_time)

        self.conn_kwargs = connection_kwargs
        self.batch_size = batch_size

    # --------------------------------------------------------
    # Implementation
    # --------------------------------------------------------
    def _get_connection(self):
        return pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **self.conn_kwargs)

    def _process_once(self) -> bool:
        processing_finished = True

        try:
            processing_finished = self._clean_complete_actors()

            logger.info("本次批处理全部完成")
            return processing_finished
        except Exception as e:
            logger.error(f"处理过程中发生错误: {e}")
            return True

    def _clean_complete_actors(self) -> None:
        processing_finished = True
        item_ids = []

        with self._get_connection() as conn:
            try:
                sql_items = """
                SELECT i.Id
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

                with conn.cursor() as cur:
                    cur.execute(sql_items, (self.batch_size,))
                    item_rows = cur.fetchall()
                if not item_rows:
                    logger.info("未找到90天前已完成的workflowruntimeitems记录")
                else:
                    processing_finished = False
                    item_ids = [row["Id"] for row in item_rows]
                    logger.info(f"找到{len(item_ids)}条90天前已完成的workflowruntimeitems记录")

                    # 先查询workflowruntimesteps获取所有相关的stepId
                    placeholders_item_ids = ",".join(["%s"] * len(item_ids))
                    sql_steps = f"""
                        SELECT Id
                        FROM workflowruntimesteps
                        WHERE Status = 'ACCEPTED'
                          AND RuntimeItemId IN ({placeholders_item_ids})
                    """

                    with conn.cursor() as cur:
                        cur.execute(sql_steps, item_ids)
                        step_rows = cur.fetchall()
                        step_ids = [row["Id"] for row in step_rows]

                    if step_ids:
                        # 然后基于步骤Id删除actors记录
                        placeholders_step_ids = ",".join(["%s"] * len(step_ids))
                        sql_delete_actors = f"DELETE FROM workflowruntimeactors WHERE RuntimeStepId IN ({placeholders_step_ids}) AND Active = 1 AND Status='PROCESSING'"

                        with conn.cursor() as cur:
                            deleted_actors_count = cur.execute(sql_delete_actors, step_ids)

                        logger.info(f"已删除{deleted_actors_count}条90天前已完成工作流相关的actors记录")
                    else:
                        logger.info("未找到需要删除的90天前未完成工作流相关的步骤记录")

                # 提交事务
                conn.commit()
                logger.info("清理工作流运行时数据完成")

                # 每处理完一批后等待30秒，减轻数据库负载
                if not processing_finished:
                    logger.info("长期未完成数据批处理完成，等待30秒开始下一批...")
                    time.sleep(30)

            except Exception as e:
                conn.rollback()
                logger.error(f"清理工作流运行时数据时发生错误: {e}")
                raise
        return processing_finished