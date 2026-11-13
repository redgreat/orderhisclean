"""SplitResourceHandler – 双库拆分清理资源关联数据，使两套库数据独立

规则:
    afc 库（database 标签 -> servicecenter_afc）:
        删除 ServiceProviderCode <> '1002' 或为 NULL 的工单关联的 tb_workresourceinfo / basic_resourceitem
    主库（database_main 标签 -> serviceordercenter）:
        删除 ServiceProviderCode = '1002' 的工单关联的 tb_workresourceinfo / basic_resourceitem

每轮两个库各处理一批工单，各自单事务删除并写单号日志。
workers>1 时按工单 Id 哈希分片并行，分片之间互不抢同一批数据。
每个库维护 last_id 水位线（按工单 Id 顺序推进），避免每批重扫已处理区间。
"""
from __future__ import annotations

import datetime
import time
from concurrent.futures import ThreadPoolExecutor

import pymysql

from base_handler import BaseHandler
from dbutil import connect, delete_in, parse_date, ph, write_job_log

# afc 库：删掉除服务商编码 1002 以外的工单资源数据（含 NULL）
COND_AFC = "(w.ServiceProviderCode <> '1002' OR w.ServiceProviderCode IS NULL)"
# 主库：删掉服务商编码 1002 的工单资源数据
COND_MAIN = "w.ServiceProviderCode = '1002'"

# 只挑在资源关联表里有数据的工单，避免没有数据的工单卡住批次进度
SQL_SELECT_WO = """
    SELECT w.Id
    FROM tb_workorderinfo w
    WHERE {cond}{date_clause}{id_clause}
      AND EXISTS (SELECT 1 FROM tb_workresourceinfo r WHERE r.WorkOrderId = w.Id)
      AND MOD(CRC32(w.Id), %s) = %s
    ORDER BY w.Id
    LIMIT %s
"""

SQL_SELECT_ROWS = """
    SELECT r.Id, r.WorkOrderId, r.ResourceId
    FROM tb_workresourceinfo r
    WHERE r.WorkOrderId IN ({ids})
"""

DEADLOCK_ERRORS = (1213, 1205)              # Deadlock found / Lock wait timeout
CONNECTION_ERRORS = (2002, 2003, 2006, 2013)  # 连接失败/连接丢失，重建连接后重试
MAX_RETRIES = 3


class SplitResourceHandler(BaseHandler):
    def __init__(
        self,
        connection_kwargs: dict,
        config: dict,
        cut_off_time: datetime.time | None = None,
        db_resolver=None,
        partition: int | None = None,
    ) -> None:
        if cut_off_time is None:
            cut_off_time = datetime.time(hour=23, minute=0, second=0)
        super().__init__(cut_off_time)

        config = config or {}
        self._config = config
        self._db_resolver = db_resolver
        self.conn_kwargs = connection_kwargs
        self.db_tag = config.get('_db_tag')

        main_tag = config.get('database_main')
        if not db_resolver or not main_tag:
            raise ValueError("SplitResourceHandler 需要配置 database_main 标签（指向主库）")
        self.main_kwargs = db_resolver(main_tag)
        self.main_tag = main_tag
        if not self.main_kwargs.get('database') or not self.main_kwargs.get('host'):
            raise ValueError(f"database_main 标签 '{main_tag}' 未在 database 节点中定义")
        if (self.main_kwargs.get('host'), self.main_kwargs.get('database')) == \
                (self.conn_kwargs.get('host'), self.conn_kwargs.get('database')):
            raise ValueError("database 与 database_main 指向同一库，拒绝执行拆分删除")

        self.start_date = parse_date(config.get('start_date'))
        self.end_date = parse_date(config.get('end_date'))
        self.batch_size = config.get('batch_size', 100)
        self.pause_seconds = config.get('pause_seconds', 30)
        self.workers = max(1, config.get('workers', 1))
        self.partition = partition  # None: 主实例；0..workers-1: 并发子实例负责的哈希分片
        # 默认只按工单主单写日志行（Detail 记删除行数）；log_rows: true 恢复按资源行写
        self.log_rows = config.get('log_rows', False)

        self.date_clause = ""
        if self.start_date:
            self.date_clause += " AND w.CreatedAt >= %s"
        if self.end_date:
            self.date_clause += " AND w.CreatedAt < %s"

        self._conns = {}     # label -> 长连接，避免每批重新建连
        self._last_id = {}   # label -> 已处理的最大工单 Id（水位线）
        self._done = set()   # 已处理完毕的 label

    # --------------------------------------------------------
    # Implementation
    # --------------------------------------------------------
    def run(self) -> None:
        """workers>1 时按工单 Id 哈希分片并行处理, 分片之间互不抢同一批数据"""
        try:
            if self.partition is not None or self.workers == 1:
                return super().run()
            self.log.info(f"启动 {self.workers} 个并发分片")
            with ThreadPoolExecutor(max_workers=self.workers) as pool:
                futures = [pool.submit(self._clone(k).run) for k in range(self.workers)]
                for future in futures:
                    future.result()
        finally:
            self._close_conns()

    def _clone(self, partition: int) -> SplitResourceHandler:
        return self.__class__(
            connection_kwargs=self.conn_kwargs,
            config=self._config,
            cut_off_time=self.cut_off_time,
            db_resolver=self._db_resolver,
            partition=partition,
        )

    def _get_conn(self, label: str, conn_kwargs: dict):
        """每个库复用一条长连接；断开时自动重连"""
        conn = self._conns.get(label)
        if conn is None:
            conn = connect(conn_kwargs)
            with conn.cursor() as cur:
                cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
            self._conns[label] = conn
        else:
            conn.ping(reconnect=True)
        return conn

    def _drop_conn(self, label: str) -> None:
        conn = self._conns.pop(label, None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def _close_conns(self) -> None:
        for label in list(self._conns):
            self._drop_conn(label)

    def _process_once(self) -> bool:
        try:
            if "afc" not in self._done and self._process_with_retry(
                    self.conn_kwargs, self.db_tag, COND_AFC, "afc"):
                self._done.add("afc")
            if "main" not in self._done and self._process_with_retry(
                    self.main_kwargs, self.main_tag, COND_MAIN, "main"):
                self._done.add("main")
            return len(self._done) == 2
        except Exception as e:
            self.log.error(f"处理过程中发生错误: {e}")
            return True

    def _process_with_retry(self, conn_kwargs: dict, db_tag: str, cond: str, label: str) -> bool:
        """死锁/锁超时/连接丢失时自动重试；连接类错误先丢弃旧连接再重建"""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return self._process_db(conn_kwargs, db_tag, cond, label)
            except pymysql.err.OperationalError as e:
                if e.args[0] in CONNECTION_ERRORS:
                    self._drop_conn(label)
                if e.args[0] in DEADLOCK_ERRORS + CONNECTION_ERRORS and attempt < MAX_RETRIES:
                    self.log.warning(f"[{label}]死锁/锁超时/连接异常，第{attempt}次重试: {e}")
                    time.sleep(attempt)
                    continue
                raise

    def _process_db(self, conn_kwargs: dict, db_tag: str, cond: str, label: str) -> bool:
        """处理一个库的一批工单；返回 True 表示该库已删完"""
        conn = self._get_conn(label, conn_kwargs)
        try:
            # 水位线：只扫描还没处理过的工单区间，避免每批重扫已删前缀
            last_id = self._last_id.get(label)
            params = []
            if self.start_date:
                params.append(self.start_date)
            if self.end_date:
                params.append(self.end_date)
            if last_id:
                params.append(last_id)
            params += [self.workers, self.partition or 0, self.batch_size]

            with conn.cursor() as cur:
                cur.execute(
                    SQL_SELECT_WO.format(
                        cond=cond,
                        date_clause=self.date_clause,
                        id_clause=" AND w.Id > %s" if last_id else "",
                    ),
                    params,
                )
                wo_ids = [row["Id"] for row in cur.fetchall()]

            if not wo_ids:
                self.log.info(f"[{label}]没有更多需要删除的工单")
                return True

            with conn.cursor() as cur:
                cur.execute(SQL_SELECT_ROWS.format(ids=ph(wo_ids)), wo_ids)
                rows = cur.fetchall()

            ids = [row["Id"] for row in rows]
            resource_ids = [row["ResourceId"] for row in rows if row["ResourceId"]]
            self.log.info(f"[{label}]本批删除 {len(ids)} 条资源关联记录，关联工单主单: {sorted(wo_ids)}")

            job_log_ok = self._ensure_job_log(conn, label)
            with conn.cursor() as cur:
                deleted_workinfo = delete_in(cur, "tb_workresourceinfo", "Id", ids)
                deleted_resource = delete_in(cur, "basic_resourceitem", "Id", resource_ids)
                if job_log_ok:
                    if self.log_rows:
                        log_rows = [(row["WorkOrderId"], row["Id"]) for row in rows]
                        write_job_log(conn, self.__class__.__name__, "split_delete",
                                      db_tag, log_rows)
                    else:
                        write_job_log(conn, self.__class__.__name__, "split_delete",
                                      db_tag, [(w, None) for w in wo_ids],
                                      detail=f"resource_rows={len(ids)}")
            conn.commit()
            self._last_id[label] = wo_ids[-1]
            self.log.info(f"[{label}]已删除 tb_workresourceinfo {deleted_workinfo} 条、basic_resourceitem {deleted_resource} 条")

            if self.pause_seconds:
                time.sleep(self.pause_seconds)
            return False
        except Exception as e:
            conn.rollback()
            self.log.error(f"[{label}]删除资源数据时发生错误，已回滚本批: {e}")
            raise
