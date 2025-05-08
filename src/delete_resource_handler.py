"""DeleteResourceHandler – sample concrete handler performing DELETE from MySQL.

This is an **example** implementation. Modify the SQL and connection details
as needed.
"""
from __future__ import annotations

import datetime
import pymysql
from loguru import logger

from base_handler import BaseHandler

class DeleteResourceHandler(BaseHandler):
    """Periodically delete rows matching *where_clause* from *table*."""

    def __init__(
        self,
        connection_kwargs: dict,
        batch_size: int = 100,
        cut_off_time: datetime.time | None = None,
    ) -> None:
        if cut_off_time is None:
            # Default: stop at 23:00
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
        sql_select = f"SELECT Id, ResourceId FROM tb_workresourceinfo WHERE Deleted=1 AND CreatedAt<DATE_ADD(CURDATE(), INTERVAL -30 DAY) ORDER BY Id LIMIT {self.batch_size} FOR UPDATE;"
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_select)
                rows = cur.fetchall()
                ids = [row["Id"] for row in rows]
                resource_ids = [row["ResourceId"] for row in rows]
            if not ids:
                logger.info("No more rows to delete today.")
                return True  # finished
            placeholders_ids = ",".join(["%s"] * len(ids))
            sql_delete_workinfo = f"DELETE FROM tb_workresourceinfo WHERE Id IN ({placeholders_ids});"
            
            resource_ids = [rid for rid in resource_ids if rid]
            if resource_ids:
                placeholders_resource = ",".join(["%s"] * len(resource_ids))
                sql_delete_resource = f"DELETE FROM basic_resourceitem WHERE Id IN ({placeholders_resource});"
            
            with conn.cursor() as cur:
                deleted_count_workinfo = cur.execute(sql_delete_workinfo, ids)
                deleted_count_resource = 0
                if resource_ids:
                    deleted_count_resource = cur.execute(sql_delete_resource, resource_ids)
            
            conn.commit()
            logger.info(f"Deleted {deleted_count_workinfo} rows from tb_workresourceinfo")
            if resource_ids:
                logger.info(f"Deleted {deleted_count_resource} rows from basic_resourceitem")
        return False
