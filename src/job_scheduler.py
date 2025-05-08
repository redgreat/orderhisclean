"""Daily job scheduler using *schedule* package.

This script discovers handler classes and schedules them once per day at the
configured *start time*. Each handler runs until completed or until its own
cut-off time (defined inside handler).
"""
from __future__ import annotations

import importlib
import inspect
import time

import schedule
from loguru import logger

# 导入配置和数据库配置（包含日志配置，会自动初始化日志）
from config import config, SOURCE_MYSQL_CONF, TARGET_MYSQL_CONF
# 导入基础处理器
from base_handler import BaseHandler


def _discover_handlers() -> list[type[BaseHandler]]:
    """Import modules and collect subclasses of *BaseHandler*."""
    handlers: list[type[BaseHandler]] = []
    # 从配置文件获取处理器模块列表
    handler_modules = config.get_handler_modules()
    
    for module_name in handler_modules:
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            logger.error(f"Cannot import handler module {module_name}: {exc}")
            continue
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, BaseHandler) and obj is not BaseHandler:
                handlers.append(obj)
    return handlers


def _run_handlers() -> None:
    """Instantiate and run all handlers in order."""
    logger.info("Daily job started… discovering handlers")
    for handler_cls in _discover_handlers():
        handler_name = handler_cls.__name__.lower()
        logger.info(f"Running handler {handler_cls.__name__}")
        try:
            # 获取处理器特定配置
            handler_config = config.get_handler_config(handler_name)
            
            # 获取截止时间（所有处理器通用）
            cut_off_time_str = handler_config.get('cut_off_time', '23:00:00')
            cut_off_time = config.parse_time(cut_off_time_str)
            
            if handler_cls.__name__ == "DeleteResourceHandler" or handler_cls.__name__ == "DeleteWorkflowHandler":
                handler = handler_cls(
                    connection_kwargs=SOURCE_MYSQL_CONF,
                    batch_size=handler_config.get('batch_size', 100),
                    cut_off_time=cut_off_time
                )
            # elif handler_cls.__name__ == "MigrationHandler":
            #     handler = handler_cls(
            #         source_conn_kwargs=SOURCE_MYSQL_CONF,
            #         target_conn_kwargs=TARGET_MYSQL_CONF,
            #         source_table=handler_config.get('source_table', 'source_table'),
            #         target_table=handler_config.get('target_table', 'target_table'),
            #         where_clause=handler_config.get('where_clause', "status = 'COMPLETED'"),
            #         batch_size=handler_config.get('batch_size', 3000),
            #         cut_off_time=cut_off_time
            #     )
            # else:
            #     # 对于其他处理器类型，可以根据需要添加特定的实例化逻辑
            #     handler = handler_cls()
                
            handler.run()
        except Exception:
            logger.exception(f"Handler {handler_cls.__name__} failed")


def main() -> None:
    logger.info("Scheduler booting…")
    # 从配置文件获取开始时间
    start_time = config.get_start_time()
    logger.info(f"调度器将在每天 {start_time} 开始运行任务")
    
    schedule.every().day.at(start_time).do(_run_handlers)

    while True:
        schedule.run_pending()
        time.sleep(30)  # seconds


if __name__ == "__main__":
    main()
