"""SplitWorkflowHandler – 双库拆分清理工作流数据，使两套库数据独立

规则:
    afc 库（database 标签 -> servicecenter_afc）:
        删除 ServiceProviderCode <> '1002' 或为 NULL 的工单关联的全部工作流数据
        （workflowruntime* 和 workflowcomplete* 两套表都删）
    主库（database_main 标签 -> serviceordercenter）:
        删除 ServiceProviderCode = '1002' 的工单关联的全部工作流数据

每轮两个库各处理一批工单，各自单事务级联删除并写单号日志。
"""
from __future__ import annotations

import datetime
import time

from base_handler import BaseHandler
from dbutil import (
    cascade_delete_workflow_items,
    connect,
    parse_date,
    select_item_rows,
    write_job_log,
)

# afc 库：删掉除服务商编码 1002 以外的工单工作流数据（含 NULL）
COND_AFC = "(w.ServiceProviderCode <> '1002' OR w.ServiceProviderCode IS NULL)"
# 主库：删掉服务商编码 1002 的工单工作流数据
COND_MAIN = "w.ServiceProviderCode = '1002'"

# 只挑在运行时表或完成时表里有数据的工单，避免没有数据的工单卡住批次进度
SQL_SELECT_WO = """
    SELECT w.Id
    FROM tb_workorderinfo w
    WHERE {cond}{date_clause}
      AND (EXISTS (SELECT 1 FROM workflowruntimeitems i WHERE i.TargetEntityId = w.Id)
        OR EXISTS (SELECT 1 FROM workflowcompleteitems c WHERE c.TargetEntityId = w.Id))
    ORDER BY w.Id
    LIMIT %s
"""


class SplitWorkflowHandler(BaseHandler):
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
        # database 标签指向 afc 库（servicecenter_afc）
        self.conn_kwargs = connection_kwargs
        self.db_tag = config.get('_db_tag')

        # database_main 标签指向主库（serviceordercenter）
        main_tag = config.get('database_main')
        if not db_resolver or not main_tag:
            raise ValueError("SplitWorkflowHandler 需要配置 database_main 标签（指向主库）")
        self.main_kwargs = db_resolver(main_tag)
        self.main_tag = main_tag
        if not self.main_kwargs.get('database') or not self.main_kwargs.get('host'):
            raise ValueError(f"database_main 标签 '{main_tag}' 未在 database 节点中定义")
        if (self.main_kwargs.get('host'), self.main_kwargs.get('database')) == \
                (self.conn_kwargs.get('host'), self.conn_kwargs.get('database')):
            raise ValueError("database 与 database_main 指向同一库，拒绝执行拆分删除")

        self.start_date = parse_date(config.get('start_date'))
        self.end_date = parse_date(config.get('end_date'))
        self.batch_size = config.get('batch_size', 100)
        self.pause_seconds = config.get('pause_seconds', 30)

        # 可选的工单创建时间区间，两库共用；不配置则全量
        self.date_clause = ""
        if self.start_date:
            self.date_clause += " AND w.CreatedAt >= %s"
        if self.end_date:
            self.date_clause += " AND w.CreatedAt < %s"

    # --------------------------------------------------------
    # Implementation
    # --------------------------------------------------------
    def _process_once(self) -> bool:
        try:
            done_afc = self._process_db(self.conn_kwargs, self.db_tag, COND_AFC, "afc")
            done_main = self._process_db(self.main_kwargs, self.main_tag, COND_MAIN, "main")
            return done_afc and done_main
        except Exception as e:
            self.log.error(f"处理过程中发生错误: {e}")
            return True

    def _process_db(self, conn_kwargs: dict, db_tag: str, cond: str, label: str) -> bool:
        """处理一个库的一批工单；返回 True 表示该库已删完"""
        with connect(conn_kwargs) as conn:
            try:
                params = []
                if self.start_date:
                    params.append(self.start_date)
                if self.end_date:
                    params.append(self.end_date)
                params.append(self.batch_size)

                with conn.cursor() as cur:
                    cur.execute(SQL_SELECT_WO.format(cond=cond, date_clause=self.date_clause), params)
                    wo_ids = [row["Id"] for row in cur.fetchall()]

                if not wo_ids:
                    self.log.info(f"[{label}]没有更多需要删除的工单")
                    return True

                self.log.info(f"[{label}]本批删除 {len(wo_ids)} 个工单的工作流数据，工单主单: {sorted(wo_ids)}")

                # 日志表检查要在写入之前做（其内部会 commit，不能放在删除事务中间）
                job_log_ok = self._ensure_job_log(conn, label)
                with conn.cursor() as cur:
                    # 运行时侧
                    runtime_rows = select_item_rows(cur, "workflowruntimeitems", wo_ids)
                    counts = cascade_delete_workflow_items(
                        cur, [r["Id"] for r in runtime_rows], prefix="workflowruntime")
                    # 完成时侧
                    complete_rows = select_item_rows(cur, "workflowcompleteitems", wo_ids)
                    complete_counts = cascade_delete_workflow_items(
                        cur, [r["Id"] for r in complete_rows], prefix="workflowcomplete")
                    counts.update(complete_counts)

                    if job_log_ok:
                        log_rows = [(r["TargetEntityId"], r["Id"]) for r in runtime_rows]
                        log_rows += [(r["TargetEntityId"], r["Id"]) for r in complete_rows]
                        write_job_log(conn, self.__class__.__name__, "split_delete", db_tag, log_rows)
                conn.commit()
                self.log.info(f"[{label}]已删除 {len(wo_ids)} 个工单的工作流数据: {counts}")

                if self.pause_seconds:
                    time.sleep(self.pause_seconds)
                return False
            except Exception as e:
                conn.rollback()
                self.log.error(f"[{label}]删除工作流数据时发生错误，已回滚本批: {e}")
                raise
