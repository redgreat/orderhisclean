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
from concurrent.futures import ThreadPoolExecutor

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
from config import config, LOG_DIR
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


_HANDLER_SINKS: set = set()


def _add_handler_log_sink(cls_name: str, key: str) -> None:
    """为处理器添加独立日志文件（幂等），通过 extra.handler 路由记录"""
    if cls_name in _HANDLER_SINKS:
        return
    _HANDLER_SINKS.add(cls_name)
    log_config = config.get_logging_config()
    logger.add(
        os.path.join(LOG_DIR, f"{key}.log"),
        level=log_config.get('level', 'INFO'),
        format=log_config.get('format', "{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}"),
        rotation=log_config.get('rotation', "1 days"),
        retention=log_config.get('retention', "7 days"),
        compression="zip",
        enqueue=True,
        filter=lambda record, name=cls_name: record["extra"].get("handler") == name,
    )


def _build_handler(handler_cls: type[BaseHandler]) -> BaseHandler | None:
    """校验配置并实例化处理器；不满足运行条件返回 None，绝不强行执行"""
    key = handler_cls.__name__.lower()
    handler_config = config.get_handler_config(key)

    # enabled 必须显式配置为 true 才执行，默认禁用
    if not handler_config.get('enabled', False):
        logger.info(f"{handler_cls.__name__} 未配置 enabled: true，跳过（默认禁用）")
        return None

    # 每个处理器必须显式声明数据库标签（his库/正式库/_afc库），避免误连错误库
    db_name = handler_config.get('database')
    if not db_name:
        logger.error(f"{handler_cls.__name__} 未配置 database 数据库标签，已跳过")
        return None

    db_conf = config.get_mysql_conf(db_name)
    if not db_conf.get('database') or not db_conf.get('host'):
        logger.error(f"{handler_cls.__name__} 的数据库标签 '{db_name}' 未在 database 节点中定义（缺 host/database），已跳过")
        return None

    _add_handler_log_sink(handler_cls.__name__, key)
    logger.info(f"{handler_cls.__name__} 使用数据库标签 '{db_name}' -> {db_conf['database']}@{db_conf.get('host')}")

    cut_off_time = config.parse_time(handler_config.get('cut_off_time', '23:00:00'))
    # 复制配置并注入数据库标签名，供处理器写日志表时标注来源库
    cfg = dict(handler_config)
    cfg['_db_tag'] = db_name
    return handler_cls(
        connection_kwargs=db_conf,
        config=cfg,
        cut_off_time=cut_off_time,
        db_resolver=config.get_mysql_conf,
    )


def _run_handlers() -> None:
    """实例化并并发执行所有已启用的处理器。"""
    logger.info("Daily job started… discovering handlers")
    handlers: list = []
    for handler_cls in _discover_handlers():
        try:
            handler = _build_handler(handler_cls)
            if handler is not None:
                handlers.append(handler)
        except Exception:
            logger.exception(f"Handler {handler_cls.__name__} 初始化失败")

    if not handlers:
        logger.info("没有启用的处理器，本次任务结束")
        return

    # 各处理器并发执行，互不影响
    with ThreadPoolExecutor(max_workers=len(handlers)) as pool:
        futures = {pool.submit(h.run): h.__class__.__name__ for h in handlers}
        for future in futures:
            name = futures[future]
            try:
                future.result()
            except Exception:
                logger.exception(f"Handler {name} failed")


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
