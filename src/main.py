#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# @generate at 2025/3/21 15:26
# comment: 监听mysql binglog，监听变化量后写入宽表

import datetime
import decimal
import sys
import os
import configparser
from loguru import logger
import json

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

from supplier_info import SupplierProcessor
from supplier_cust import CustProcessor

# 数据库连接定义
config = configparser.ConfigParser()
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
config_path = os.path.join(project_root, "conf", "db.cnf")
config.read(config_path)

src_host = config.get("source", "host")
src_database = config.get("source", "database").split(',')
src_tables = config.get("source", "tables").split(',')
src_user = config.get("source", "user")
src_password = config.get("source", "password")
src_port = int(config.get("source", "port"))
src_charset = config.get("source", "charset")

tar_host = config.get("target", "host")
tar_port = int(config.get("target", "port"))
tar_database = config.get("target", "database")
tar_user = config.get("target", "user")
tar_password = config.get("target", "password")
tar_charset = config.get("target", "charset")

# 日志配置
logDir = os.path.expanduser("../log/")
if not os.path.exists(logDir):
    os.mkdir(logDir)
logFile = os.path.join(logDir, "repl.log")
# logger.remove(handler_id=None)

logger.add(
    logFile,
    colorize=True,
    rotation="1 days",
    retention="3 days",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
    backtrace=True,
    diagnose=True,
    level="INFO",
)

SRC_MYSQL_SETTINGS = {
    "host": src_host,
    "port": src_port,
    "user": src_user,
    "passwd": src_password,
    "charset": src_charset,
}

TAR_MYSQL_SETTINGS = {
    "host": tar_host,
    "port": tar_port,
    "database": tar_database,
    "user": tar_user,
    "passwd": tar_password,
    "charset": tar_charset,
}


def dict_to_str(value):
    if isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(value, decimal.Decimal):
        return str(value)
    elif isinstance(value, bytes):
        return value.hex()
    elif isinstance(value, str):
        return value.strip("'")
    elif isinstance(value, int):
        return value
    elif value is None:
        # return 'NULL'
        return None
    else:
        return f"'{value}'"


def dict_to_json(res_value):
    json_record = {}
    for key, value in res_value.items():
        if key not in ['schema', 'table', 'action']:
            json_record[key] = dict_to_str(value)
    return json.dumps(json_record, ensure_ascii=False, indent=4)


def main():
    stream = BinLogStreamReader(
        connection_settings=SRC_MYSQL_SETTINGS,
        server_id=3,
        blocking=True,  # 持续监听
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],  # 指定只监听某些事件
        only_schemas=src_database,  # 指定只监听某些库（但binlog还是要读取全部滴）
        only_tables=src_tables,  # 指定监听某些表
    )

    with SupplierProcessor(TAR_MYSQL_SETTINGS) as supplier_handler, CustProcessor(TAR_MYSQL_SETTINGS) as cust_handler:
        for binlog_event in stream:
            for row in binlog_event.rows:
                event = {"schema": binlog_event.schema, "table": binlog_event.table}

                if isinstance(binlog_event, WriteRowsEvent):
                    event["action"] = "replace"
                    event.update(row["values"])
                elif isinstance(binlog_event, UpdateRowsEvent):
                    event["action"] = "replace"
                    event.update(row["after_values"])
                elif isinstance(binlog_event, DeleteRowsEvent):
                    event["action"] = "delete"
                    event.update(row["values"])

                json_data = json.loads(dict_to_json(event))
                if binlog_event.table == "supplier_info":
                    supplier_handler.handle_event(
                        action=event["action"],
                        data=json_data
                    )
                elif binlog_event.table == "supplier_cust":
                    cust_handler.handle_event(
                        action=event["action"],
                        data=json_data
                    )

    sys.stdout.flush()

    stream.close()


if __name__ == "__main__":
    main()
