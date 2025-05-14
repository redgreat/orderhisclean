"""Daily job scheduler using *schedule* package.

This script discovers handler classes and schedules them once per day at the
configured *start time*. Each handler runs until completed or until its own
cut-off time (defined inside handler).

可以通过 --run-now 参数立即执行一次任务
"""
from __future__ import annotations

import importlib
import inspect
import time
import os
import sys
import argparse

import schedule
from loguru import logger

# 添加src目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

parent_dir = os.path.dirname(current_dir)
for path in [current_dir, parent_dir]:
    if path not in sys.path:
        sys.path.append(path)

# 导入配置和数据库配置（包含日志配置，会自动初始化日志）
from config import config, SOURCE_MYSQL_CONF, TARGET_MYSQL_CONF
# 导入基础处理器
from base_handler import BaseHandler


def _discover_handlers() -> list[type[BaseHandler]]:
    """Import modules and collect subclasses of *BaseHandler*."""
    handlers: list[type[BaseHandler]] = []
    handler_modules = config.get_handler_modules()
    
    for module_name in handler_modules:
        try:
            try:
                module = importlib.import_module(module_name)
                logger.info(f"成功导入模块: {module_name}")
            except ModuleNotFoundError:
                try:
                    module_path = f"src.{module_name}"
                    module = importlib.import_module(module_path)
                    logger.info(f"成功导入模块: {module_path}")
                except ModuleNotFoundError as exc:
                    logger.error(f"无法导入模块: {module_name} 或 src.{module_name}: {exc}")
                    continue
        except Exception as exc:
            logger.error(f"导入模块时出错 {module_name}: {exc}")
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
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='任务调度器')
    parser.add_argument('--run-now', action='store_true', help='立即执行一次任务，不进入调度模式')
    
    # 检查环境变量和命令行参数
    args = parser.parse_args()
    run_now = args.run_now or os.environ.get('RUN_NOW', '').lower() in ('true', '1', 'yes')
    
    if run_now:
        logger.info("手动触发模式：立即执行任务")
        _run_handlers()
        return

    logger.info("调度器启动中...")
    # 从配置文件获取开始时间
    start_time = config.get_start_time()
    logger.info(f"调度器将在每天 {start_time} 开始运行任务")
    
    schedule.every().day.at(start_time).do(_run_handlers)

    while True:
        schedule.run_pending()
        time.sleep(30)  # seconds


if __name__ == "__main__":
    main()
