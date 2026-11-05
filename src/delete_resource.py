"""DeleteResourceHandler – 删除30天前已软删的资源关联记录

数据范围:
    tb_workresourceinfo.Deleted=1 且创建超过30天
    -> 删除 tb_workresourceinfo 及关联的 basic_resourceitem
"""
from __future__ import annotations

import datetime

from base_handler import BaseHandler
from dbutil import connect, ph, write_job_log


class DeleteResourceHandler(BaseHandler):
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

    # --------------------------------------------------------
    # Implementation
    # --------------------------------------------------------
    def _process_once(self) -> bool:
        sql_select = f"SELECT Id, ResourceId, WorkOrderId FROM tb_workresourceinfo WHERE Deleted=1 AND CreatedAt<DATE_ADD(CURDATE(), INTERVAL -30 DAY) ORDER BY Id LIMIT {self.batch_size} FOR UPDATE;"
        with connect(self.conn_kwargs) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_select)
                rows = cur.fetchall()
                ids = [row["Id"] for row in rows]
                resource_ids = [row["ResourceId"] for row in rows]
            if not ids:
                self.log.info("没有更多需要删除的资源记录")
                return True  # finished

            self.log.info(f"本批删除 {len(ids)} 条资源关联记录，单号: {sorted(set(ids))}")

            resource_ids = [rid for rid in resource_ids if rid]
            workorder_ids = [row["WorkOrderId"] for row in rows]

            # 日志表检查要在写入之前做（其内部会 commit，不能放在删除事务中间）
            job_log_ok = self._ensure_job_log(conn)
            with conn.cursor() as cur:
                deleted_count_workinfo = cur.execute(f"DELETE FROM tb_workresourceinfo WHERE Id IN ({ph(ids)});", ids)
                deleted_count_resource = 0
                if resource_ids:
                    deleted_count_resource = cur.execute(f"DELETE FROM basic_resourceitem WHERE Id IN ({ph(resource_ids)});", resource_ids)
                if job_log_ok:
                    write_job_log(conn, self.__class__.__name__, "delete", self.db_tag,
                                  [(w, i) for w, i in zip(workorder_ids, ids)])

            conn.commit()
            self.log.info(f"已从tb_workresourceinfo删除{deleted_count_workinfo}条记录")
            if resource_ids:
                self.log.info(f"已从basic_resourceitem删除{deleted_count_resource}条记录")
        return False
