#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# @generate at 2025/3/24 09:33
# comment: supplier_cust表数据同步处理

import pymysql
from loguru import logger
from typing import Dict


class CustProcessor:
    def __init__(self, db_config: Dict):
        self.conn = pymysql.connect(**db_config)
        self.table_name = "tb_suppliersettle"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def _execute_sql(self, sql: str, params: tuple):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, params)
            self.conn.commit()
            logger.success(f"SQL执行成功: {sql} | 参数: {params}")
            return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"数据库操作失败: {str(e)}")
            return False

    def handle_event(self, action: str, data: Dict):
        """统一事件处理入口"""
        handler = getattr(self, action, None)
        if handler:
            return handler(data)
        logger.warning(f"未定义的操作类型: {action}")
        return False

    def replace(self, data: Dict) -> bool:
        """处理供应商结算单位信息"""
        columns = ['Id', 'supplierId', 'CustSettleId', 'CustSettleName', 'CreatedById', 'CreatedAt', 'UpdatedById',
                   'UpdatedAt', 'DeletedById', 'DeletedAt', 'Deleted']
        placeholders = ['%s'] * len(columns)
        sql = f"REPLACE INTO {self.table_name} ({','.join(columns)}) VALUES ({','.join(placeholders)})"
        params = (
            data.get('id'),
            data.get('supplier_id'),
            data.get('custsettle_id'),
            data.get('custsettle_name'),
            data.get('created_by_id'),
            data.get('created_at'),
            data.get('updated_by_id'),
            data.get('updated_at'),
            data.get('deleted_by_id'),
            data.get('deleted_at'),
            data.get('deleted'),
        )
        return self._execute_sql(sql, params)

    def delete(self, data: Dict) -> bool:
        """处理供应商删除"""
        sql = f"DELETE FROM {self.table_name} WHERE id=%s"
        return self._execute_sql(sql, (data.get('id'),))
