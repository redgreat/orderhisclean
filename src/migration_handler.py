"""MigrationHandler – 数据迁移处理器示例

这个处理器实现了从源数据库到目标数据库的数据迁移功能。
可以根据实际需求修改查询条件和迁移逻辑。
"""
from __future__ import annotations

import datetime
import pymysql
from loguru import logger

from base_handler import BaseHandler
from config import SOURCE_MYSQL_CONF, TARGET_MYSQL_CONF

class MigrationHandler(BaseHandler):
    """从源数据库迁移数据到目标数据库的处理器"""

    def __init__(
        self,
        source_conn_kwargs: dict,
        target_conn_kwargs: dict,
        source_table: str,
        target_table: str,
        where_clause: str,
        batch_size: int = 5000,
        cut_off_time: datetime.time | None = None,
    ) -> None:
        if cut_off_time is None:
            # 默认截止时间：22:30
            cut_off_time = datetime.time(hour=22, minute=30, second=0)
        super().__init__(cut_off_time)

        self.source_conn_kwargs = source_conn_kwargs
        self.target_conn_kwargs = target_conn_kwargs
        self.source_table = source_table
        self.target_table = target_table
        self.where_clause = where_clause
        self.batch_size = batch_size

    # --------------------------------------------------------
    # 数据库连接
    # --------------------------------------------------------
    def _get_source_connection(self):
        """获取源数据库连接"""
        return pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **self.source_conn_kwargs)

    def _get_target_connection(self):
        """获取目标数据库连接"""
        return pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **self.target_conn_kwargs)

    # --------------------------------------------------------
    # 实现批处理逻辑
    # --------------------------------------------------------
    def _process_once(self) -> bool:
        """执行一批数据迁移
        
        Returns:
            bool: True表示今天的任务已完成，False表示还有数据需要处理
        """
        # 1. 从源数据库查询一批数据
        records = self._fetch_batch_from_source()
        
        if not records:
            logger.info(f"没有更多数据需要从 {self.source_table} 迁移到 {self.target_table}")
            return True  # 任务完成
        
        # 2. 将数据写入目标数据库
        migrated_count = self._migrate_to_target(records)
        
        # 3. 可选：在源数据库中标记已迁移的数据
        # self._mark_as_migrated(records)
        
        logger.info(f"已迁移 {migrated_count} 条记录从 {self.source_table} 到 {self.target_table}")
        
        # 返回False表示继续处理下一批
        return False
    
    def _fetch_batch_from_source(self) -> list[dict]:
        """从源数据库获取一批数据
        
        Returns:
            list[dict]: 查询结果记录列表
        """
        sql = f"""
            SELECT * FROM {self.source_table} 
            WHERE {self.where_clause}
            LIMIT {self.batch_size}
            FOR UPDATE
        """
        
        with self._get_source_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()
    
    def _migrate_to_target(self, records: list[dict]) -> int:
        """将数据写入目标数据库
        
        Args:
            records: 要迁移的记录列表
            
        Returns:
            int: 成功迁移的记录数
        """
        if not records:
            return 0
            
        # 获取字段名列表
        fields = list(records[0].keys())
        
        # 构建INSERT语句
        placeholders = ", ".join(["%s"] * len(fields))
        columns = ", ".join([f"`{field}`" for field in fields])
        
        sql = f"""
            INSERT INTO {self.target_table} ({columns})
            VALUES ({placeholders})
        """
        
        # 准备数据
        values = []
        for record in records:
            row = [record[field] for field in fields]
            values.append(row)
        
        # 执行批量插入
        with self._get_target_connection() as conn:
            with conn.cursor() as cursor:
                inserted = 0
                for row in values:
                    try:
                        cursor.execute(sql, row)
                        inserted += 1
                    except Exception as e:
                        logger.error(f"插入记录失败: {e}")
            conn.commit()
        
        return inserted
    
    def _mark_as_migrated(self, records: list[dict]) -> None:
        """在源数据库中标记已迁移的记录
        
        这个方法可以根据需要实现，例如：
        - 更新状态字段
        - 移动到历史表
        - 删除已迁移的记录
        
        Args:
            records: 已迁移的记录列表
        """
        if not records:
            return
            
        # 示例：更新状态字段
        # ids = [str(record['id']) for record in records]
        # id_list = ", ".join(ids)
        # 
        # sql = f"""
        #     UPDATE {self.source_table}
        #     SET migrated = 1, migrated_at = NOW()
        #     WHERE id IN ({id_list})
        # """
        # 
        # with self._get_source_connection() as conn:
        #     with conn.cursor() as cursor:
        #         cursor.execute(sql)
        #     conn.commit()
