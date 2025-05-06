#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# @generate at 2025/3/24 09:33
# comment: supplier_info表数据同步处理

import pymysql
from loguru import logger
from typing import Dict


def convert_mainpart_id(mainpart_id: str):
    """转换货主Id
    """
    type_mapping = {
        # 正式环境
        'DD9999999970': 'DT0000000001',
        'DD9999999971': 'DT0000000002',
        # 测试环境
        'DD9999999936': 'DT0000000001',
        'DD9999999937': 'DT0000000002',
    }

    if isinstance(mainpart_id, str) and mainpart_id.startswith('DD'):
        return type_mapping.get(mainpart_id, mainpart_id)
    return mainpart_id


def convert_supplier_type(supplier_type: str):
    """转换供应商类型编码
    Args:
        supplier_type (str): 原始供应商类型编码
    Returns:
        int/str: 转换后的数值或原始值
    正式对应关系：
    type_mapping = {
        'DD9999999997': 0,
        'DD9999999996': 1,
        'DD9999999995': 2
    }
    """
    type_mapping = {
        'DD9999999980': 0,
        'DD9999999979': 1,
        'DD9999999978': 2
    }

    if isinstance(supplier_type, str) and supplier_type.startswith('DD'):
        return type_mapping.get(supplier_type, supplier_type)
    return supplier_type


class SupplierProcessor:
    def __init__(self, db_config: Dict):
        self.conn = pymysql.connect(**db_config)
        self.table_name = "tb_supplier"

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

    def insert(self, data: Dict) -> bool:
        """处理新增供应商"""
        columns = ['Id', 'MainPartId', 'OwnerId', 'Name', 'ShortName', 'SupplierType', 'GradeId', 'BalanceCycle',
                   'CustSettleId', 'CustSettleName', 'ProCode', 'CityCode', 'Address', 'LinkMan', 'LinkTel',
                   'IsCharge', 'CmsSupplierType', 'SupplierStatus', 'CreatedById', 'CreatedAt', 'UpdatedById',
                   'UpdatedAt', 'DeletedById', 'DeletedAt', 'Deleted']
        placeholders = ['%s'] * len(columns)
        sql = f"INSERT INTO {self.table_name} ({','.join(columns)}) VALUES ({','.join(placeholders)})"
        params = (
            data.get('id'),
            convert_mainpart_id(data.get('mainpart_id')),
            data.get('owner_id'),
            data.get('supplier_name'),
            data.get('supplier_name'),
            convert_supplier_type(data.get('supplier_nature_id')),
            None,
            data.get('balance_cycle'),
            None,
            None,
            data.get('pro_code'),
            data.get('city_code'),
            data.get('address'),
            data.get('link_man'),
            data.get('link_tel'),
            data.get('is_charge'),
            data.get('supplier_nature_id'),
            data.get('audit_status'),
            data.get('created_by_id'),
            data.get('created_at'),
            data.get('updated_by_id'),
            data.get('updated_at'),
            data.get('deleted_by_id'),
            data.get('deleted_at'),
            data.get('deleted'),
        )
        return self._execute_sql(sql, params)

    def replace(self, data: Dict) -> bool:
        """处理供应商更新_REPLACE"""
        columns = ['Id', 'MainPartId', 'OwnerId', 'Name', 'ShortName', 'SupplierType', 'GradeId', 'BalanceCycle',
                   'CustSettleId', 'CustSettleName', 'ProCode', 'CityCode', 'Address', 'LinkMan', 'LinkTel',
                   'IsCharge', 'CmsSupplierType', 'SupplierStatus', 'CreatedById', 'CreatedAt', 'UpdatedById',
                   'UpdatedAt', 'DeletedById', 'DeletedAt', 'Deleted']
        placeholders = ['%s'] * len(columns)
        sql = f"REPLACE INTO {self.table_name} ({','.join(columns)}) VALUES ({','.join(placeholders)})"
        params = (
            data.get('id'),
            convert_mainpart_id(data.get('mainpart_id')),
            data.get('owner_id'),
            data.get('supplier_name'),
            data.get('supplier_name'),
            convert_supplier_type(data.get('supplier_nature_id')),
            None,
            data.get('balance_cycle'),
            None,
            None,
            data.get('pro_code'),
            data.get('city_code'),
            data.get('address'),
            data.get('link_man'),
            data.get('link_tel'),
            data.get('is_charge'),
            data.get('supplier_nature_id'),
            data.get('audit_status'),
            data.get('created_by_id'),
            data.get('created_at'),
            data.get('updated_by_id'),
            data.get('updated_at'),
            data.get('deleted_by_id'),
            data.get('deleted_at'),
            data.get('deleted'),
        )
        return self._execute_sql(sql, params)

    def update(self, data: Dict) -> bool:
        """处理供应商更新"""
        set_clause = ("MainPartId=%s, OwnerId=%s, Name=%s, ShortName=%s, SupplierType=%s, GradeId=%s, "
                      "BalanceCycle=%s, CustSettleId=%s, CustSettleName=%s, ProCode=%s, CityCode=%s, Address=%s, "
                      "LinkMan=%s, LinkTel=%s, IsCharge=%s, CmsSupplierType=%s, SupplierStatus=%s, CreatedById=%s, "
                      "CreatedAt=%s, UpdatedById=%s, UpdatedAt=%s, DeletedById=%s, DeletedAt=%s, Deleted=%s")
        sql = f"UPDATE {self.table_name} SET {set_clause} WHERE id=%s"
        params = (
            convert_mainpart_id(data.get('mainpart_id')),
            data.get('owner_id'),
            data.get('supplier_name'),
            data.get('supplier_name'),
            convert_supplier_type(data.get('supplier_nature_id')),
            None,
            data.get('balance_cycle'),
            None,
            None,
            data.get('pro_code'),
            data.get('city_code'),
            data.get('address'),
            data.get('link_man'),
            data.get('link_tel'),
            data.get('is_charge'),
            data.get('supplier_nature_id'),
            data.get('audit_status'),
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
        sql = f"DELETE FROM {self.table_name} WHERE supplier_id=%s"
        return self._execute_sql(sql, (data.get('id'),))
