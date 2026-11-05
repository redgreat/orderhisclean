"""BaseHandler: abstract base class for daily batch job handlers.

Each Handler should inherit from ``BaseHandler`` and implement the
``_process_once`` method, which performs one batch of work.

The ``run`` method automatically loops ``_process_once`` until either:
  1. ``_process_once`` returns ``True`` (indicating today's work is done), or
  2. The current time has passed the configured *cut-off* time.

This keeps the business logic inside handlers short while ensuring
uniform time-limit control.
"""
from __future__ import annotations

import datetime
from abc import ABC, abstractmethod
from loguru import logger

from dbutil import ensure_job_log_table

class BaseHandler(ABC):
    """Abstract base class for all concrete handlers."""

    def __init__(self, cut_off_time: datetime.time, **kwargs):
        if not isinstance(cut_off_time, datetime.time):
            raise TypeError("cut_off_time must be datetime.time instance")
        self.cut_off_time: datetime.time = cut_off_time
        # 每个处理器独立的日志通道，写入各自的日志文件
        self.log = logger.bind(handler=self.__class__.__name__)
        # 数据库日志表是否可用（按 key 缓存，多库处理器各库独立判断）
        self._job_log_ready: dict = {}
        # Keep the original kwargs for debugging / child use
        self.kwargs = kwargs

    def _ensure_job_log(self, conn, key: str | None = None) -> bool:
        """确保数据库日志表 job_clean_logs 存在；不可用时返回 False（每个 key 只警告一次）"""
        key = key or "default"
        if key not in self._job_log_ready:
            ok = ensure_job_log_table(conn)
            self._job_log_ready[key] = ok
            if not ok:
                self.log.warning(f"[{key}]数据库日志表 job_clean_logs 创建失败，将跳过数据库日志记录")
        return self._job_log_ready[key]

    # --------------------------------------------------------
    # Helper
    # --------------------------------------------------------
    @property
    def _time_exceeded(self) -> bool:
        """Return True if *now* is **later or equal** than cut-off time."""
        now = datetime.datetime.now().time()
        return now >= self.cut_off_time

    # --------------------------------------------------------
    # Life-cycle
    # --------------------------------------------------------
    def run(self) -> None:
        """Run one day's work.

        Continually invokes ``_process_once`` until finished or the time limit
        is reached.
        """
        self.log.info(f"{self.__class__.__name__} started today with cut_off_time={self.cut_off_time}")
        try:
            while not self._time_exceeded:
                finished = self._process_once()
                if finished:
                    self.log.info(f"{self.__class__.__name__}: all tasks completed for today.")
                    break
            else:
                # loop never entered or broke because cut-off reached
                self.log.warning(
                    f"{self.__class__.__name__}: reached cut-off time (now>{self.cut_off_time}). Will continue tomorrow."
                )
        except Exception:
            self.log.exception(f"{self.__class__.__name__}: unhandled exception during run")
            raise
        finally:
            self.log.info(f"{self.__class__.__name__} finished today's run")

    # --------------------------------------------------------
    # To be implemented by subclasses
    # --------------------------------------------------------
    @abstractmethod
    def _process_once(self) -> bool:
        """Execute **one** batch of work.

        Returns
        -------
        bool
            *True*  – no more work is left **today**; stop the loop.
            *False* – continue looping until finished or time limit.
        """
