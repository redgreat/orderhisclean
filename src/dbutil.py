"""dbutil – 处理器共用的数据库工具函数"""
from __future__ import annotations

import datetime
import pymysql


def connect(conn_kwargs: dict):
    """按配置创建 pymysql 连接（DictCursor）"""
    return pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **conn_kwargs)


def ph(ids: list) -> str:
    """生成 IN (...) 占位符"""
    return ",".join(["%s"] * len(ids))


def select_ids(cur, table: str, column: str, parent_ids: list) -> list:
    """按外键列查询 Id 列表，空输入直接返回 []"""
    if not parent_ids:
        return []
    cur.execute(f"SELECT Id FROM {table} WHERE {column} IN ({ph(parent_ids)})", parent_ids)
    return [row["Id"] for row in cur.fetchall()]


def delete_in(cur, table: str, column: str, ids: list) -> int:
    """DELETE FROM table WHERE column IN (...)，返回删除行数"""
    if not ids:
        return 0
    cur.execute(f"DELETE FROM {table} WHERE {column} IN ({ph(ids)})", ids)
    return cur.rowcount


def insert_into_from(cur, target: str, source: str, column: str, ids: list) -> int:
    """INSERT INTO target SELECT * FROM source WHERE column IN (...)，返回插入行数"""
    if not ids:
        return 0
    cur.execute(f"INSERT INTO {target} SELECT * FROM {source} WHERE {column} IN ({ph(ids)})", ids)
    return cur.rowcount


def parse_date(value) -> datetime.datetime | None:
    """把配置中的日期统一转成 datetime；支持 str/date/datetime，空值返回 None"""
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime(value.year, value.month, value.day)
    return datetime.datetime.strptime(str(value)[:10], "%Y-%m-%d")


# --------------------------------------------------------
# 工作流数据级联删除（子表在前，父表在后）
# prefix='workflowruntime' 删运行时表；prefix='workflowcomplete' 删完成时表
# --------------------------------------------------------

def cascade_delete_workflow_items(cur, item_ids: list, prefix: str = "workflowruntime") -> dict:
    """级联删除一批工单项的全部工作流数据

    Args:
        cur: 调用方连接上的游标（事务由调用方管理）
        item_ids: {prefix}items.Id 列表
        prefix: 'workflowruntime' 或 'workflowcomplete'

    Returns:
        dict: {表名: 删除行数}
    """
    if not item_ids:
        return {}
    # items.CurrentStepId 与 steps 互相引用，批内统一关闭外键检查
    cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
    step_ids = select_ids(cur, f"{prefix}steps", "RuntimeItemId", item_ids)
    actor_ids = select_ids(cur, f"{prefix}actors", "RuntimeStepId", step_ids)
    comment_ids = select_ids(cur, f"{prefix}comments", "RuntimeActorId", actor_ids)
    id_map = {
        "item_ids": item_ids,
        "step_ids": step_ids,
        "actor_ids": actor_ids,
        "comment_ids": comment_ids,
    }
    order = [
        (f"{prefix}attachments", "RuntimeCommentId", "comment_ids"),
        (f"{prefix}comments", "RuntimeActorId", "actor_ids"),
        (f"{prefix}actors", "RuntimeStepId", "step_ids"),
        (f"{prefix}relatedactors", "RuntimeStepId", "step_ids"),
        (f"{prefix}activities", "RuntimeItemId", "item_ids"),
    ]
    # reminderlogs/runtimestatus 只在运行时侧存在
    if prefix == "workflowruntime":
        order += [
            ("workflowruntimereminderlogs", "RuntimeItemId", "item_ids"),
            ("workflowruntimestatus", "RuntimeItemId", "item_ids"),
        ]
    order += [
        (f"{prefix}steps", "RuntimeItemId", "item_ids"),
        (f"{prefix}items", "Id", "item_ids"),
    ]
    counts = {}
    for table, column, key in order:
        deleted = delete_in(cur, table, column, id_map[key])
        if deleted:
            counts[table] = deleted
    cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
    return counts


def select_item_rows(cur, table: str, wo_ids: list) -> list:
    """按工单主单查询工单项 Id + TargetEntityId（用于拆分清理和单号日志）"""
    if not wo_ids:
        return []
    cur.execute(
        f"SELECT Id, TargetEntityId FROM {table} WHERE TargetEntityId IN ({ph(wo_ids)})",
        wo_ids,
    )
    return cur.fetchall()


# --------------------------------------------------------
# 数据库日志表：记录每批处理的单号，便于追溯
# --------------------------------------------------------
JOB_LOG_TABLE = "job_clean_logs"

JOB_LOG_DDL = f"""
CREATE TABLE IF NOT EXISTS {JOB_LOG_TABLE} (
  Id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  Handler VARCHAR(64) NOT NULL COMMENT '处理器类名',
  Action VARCHAR(32) NOT NULL COMMENT '动作: delete/migrate',
  DbTag VARCHAR(64) NULL COMMENT '数据库标签',
  WorkOrderId VARCHAR(64) NULL COMMENT '工单主单Id',
  ItemId VARCHAR(64) NULL COMMENT '业务行Id',
  Detail VARCHAR(512) NULL,
  CreatedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_workorder (WorkOrderId),
  KEY idx_handler (Handler, CreatedAt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def ensure_job_log_table(conn) -> bool:
    """确保日志表存在；失败返回 False（不阻断业务）"""
    try:
        with conn.cursor() as cur:
            cur.execute(JOB_LOG_DDL)
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False


def write_job_log(conn, handler: str, action: str, db_tag, rows: list, detail: str | None = None) -> None:
    """批量写入日志表；rows 为 (WorkOrderId, ItemId) 列表，事务由调用方管理"""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {JOB_LOG_TABLE} (Handler, Action, DbTag, WorkOrderId, ItemId, Detail) "
            f"VALUES (%s, %s, %s, %s, %s, %s)",
            [(handler, action, db_tag, w, i, detail) for w, i in rows],
        )
