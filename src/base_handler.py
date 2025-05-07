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

class BaseHandler(ABC):
    """Abstract base class for all concrete handlers."""

    def __init__(self, cut_off_time: datetime.time, **kwargs):
        if not isinstance(cut_off_time, datetime.time):
            raise TypeError("cut_off_time must be datetime.time instance")
        self.cut_off_time: datetime.time = cut_off_time
        # Keep the original kwargs for debugging / child use
        self.kwargs = kwargs

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
        logger.info(f"{self.__class__.__name__} started today with cut_off_time={self.cut_off_time}")
        try:
            while not self._time_exceeded:
                finished = self._process_once()
                if finished:
                    logger.info(f"{self.__class__.__name__}: all tasks completed for today.")
                    break
            else:
                # loop never entered or broke because cut-off reached
                logger.warning(
                    f"{self.__class__.__name__}: reached cut-off time (now>{self.cut_off_time}). Will continue tomorrow."
                )
        except Exception:
            logger.exception(f"{self.__class__.__name__}: unhandled exception during run")
            raise
        finally:
            logger.info(f"{self.__class__.__name__} finished today's run")

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
