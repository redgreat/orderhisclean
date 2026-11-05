"""MigrationHandler – 通用数据迁移处理器示例

从源表查询一批数据写入目标表（同一数据库连接内）。
可以根据实际需求修改查询条件和迁移逻辑。
"""
from __future__ import annotations

import datetime

from base_handler import BaseHandler
from dbutil import connect


class MigrationHandler(BaseHandler):
    def __init__(
        self,
        connection_kwargs: dict,
        config: dict,
        cut_off_time: datetime.time | None = None,
        db_resolver=None,
    ) -> None:
        if cut_off_time is None:
            # 默认截止时间：22:30
            cut_off_time = datetime.time(hour=22, minute=30, second=0)
        super().__init__(cut_off_time)

        config = config or {}
        self.conn_kwargs = connection_kwargs
        self.source_table = config.get('source_table', 'source_table')
        self.target_table = config.get('target_table', 'target_table')
        self.where_clause = config.get('where_clause', "status = 'COMPLETED'")
        self.batch_size = config.get('batch_size', 3000)

    # --------------------------------------------------------
    # 实现批处理逻辑
    # --------------------------------------------------------
    def _process_once(self) -> bool:
        """执行一批数据迁移"""
        records = self._fetch_batch_from_source()

        if not records:
            self.log.info(f"没有更多数据需要从 {self.source_table} 迁移到 {self.target_table}")
            return True

        migrated_count = self._migrate_to_target(records)
        ids = sorted(str(r.get('Id')) for r in records if r.get('Id'))
        self.log.info(f"已迁移 {migrated_count} 条记录从 {self.source_table} 到 {self.target_table}，单号: {ids}")
        return False

    def _fetch_batch_from_source(self) -> list[dict]:
        sql = f"""
            SELECT * FROM {self.source_table}
            WHERE {self.where_clause}
            LIMIT {self.batch_size}
        """
        with connect(self.conn_kwargs) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()

    def _migrate_to_target(self, records: list[dict]) -> int:
        if not records:
            return 0

        fields = list(records[0].keys())
        placeholders = ", ".join(["%s"] * len(fields))
        columns = ", ".join([f"`{field}`" for field in fields])

        sql = f"""
            INSERT INTO {self.target_table} ({columns})
            VALUES ({placeholders})
        """

        values = [[record[field] for field in fields] for record in records]

        with connect(self.conn_kwargs) as conn:
            with conn.cursor() as cursor:
                inserted = 0
                for row in values:
                    try:
                        cursor.execute(sql, row)
                        inserted += 1
                    except Exception as e:
                        self.log.error(f"插入记录失败: {e}")
            conn.commit()

        return inserted
