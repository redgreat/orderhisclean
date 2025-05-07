"""配置管理模块 - 从YAML文件加载配置并提供数据库连接参数和日志配置"""
import os
import sys
import datetime
from typing import Any, Dict, List, Optional

import yaml
from loguru import logger

class ConfigLoader:
    """配置加载器 - 负责从YAML文件加载配置并提供访问方法"""
    
    def __init__(self, config_path: Optional[str] = None):
        """初始化配置加载器
        
        Args:
            config_path: 配置文件路径，如果为None则使用默认路径
        """
        if config_path is None:
            # 默认配置文件路径
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(current_dir)
            config_path = os.path.join(project_root, "conf", "config.yml")
        
        self.config_path = config_path
        self.config = self._load_config()
        
        # 初始化数据库连接配置
        self.source_db_conf = self.get_db_config('source')
        self.target_db_conf = self.get_db_config('target')
        
        # 创建数据库连接参数
        self.source_mysql_conf = self._get_mysql_conn_kwargs(self.source_db_conf)
        self.target_mysql_conf = self._get_mysql_conn_kwargs(self.target_db_conf)
        
    def _load_config(self) -> Dict[str, Any]:
        """加载YAML配置文件
        
        Returns:
            Dict[str, Any]: 配置字典
        """
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            raise
    
    def get_scheduler_config(self) -> Dict[str, Any]:
        """获取调度器配置
        
        Returns:
            Dict[str, Any]: 调度器配置
        """
        return self.config.get('scheduler', {})
    
    def get_db_config(self, db_type: str) -> Dict[str, Any]:
        """获取数据库配置
        
        Args:
            db_type: 数据库类型，默认为'main'
            
        Returns:
            Dict[str, Any]: 数据库配置
        """
        return self.config.get('database', {}).get(db_type, {})
    
    def get_handler_config(self, handler_name: str) -> Dict[str, Any]:
        """获取处理器配置
        
        Args:
            handler_name: 处理器名称
            
        Returns:
            Dict[str, Any]: 处理器配置
        """
        return self.config.get('handlers', {}).get(handler_name, {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """获取日志配置
        
        Returns:
            Dict[str, Any]: 日志配置
        """
        return self.config.get('logging', {})
    
    def get_handler_modules(self) -> List[str]:
        """获取处理器模块列表
        
        Returns:
            List[str]: 处理器模块列表
        """
        return self.config.get('scheduler', {}).get('handler_modules', [])
    
    def get_start_time(self) -> str:
        """获取调度开始时间
        
        Returns:
            str: 调度开始时间，格式为 "HH:MM"
        """
        return self.config.get('scheduler', {}).get('start_time', "02:00")
    
    def parse_time(self, time_str: str) -> datetime.time:
        """解析时间字符串为datetime.time对象
        
        Args:
            time_str: 时间字符串，格式为 "HH:MM:SS" 或 "HH:MM"
            
        Returns:
            datetime.time: 解析后的时间对象
        """
        if ":" not in time_str:
            raise ValueError(f"无效的时间格式: {time_str}")
        
        parts = time_str.split(":")
        if len(parts) == 2:
            hour, minute = map(int, parts)
            return datetime.time(hour=hour, minute=minute)
        elif len(parts) == 3:
            hour, minute, second = map(int, parts)
            return datetime.time(hour=hour, minute=minute, second=second)
        else:
            raise ValueError(f"无效的时间格式: {time_str}")
    
    def _get_mysql_conn_kwargs(self, db_conf: Dict[str, Any]) -> Dict[str, Any]:
        """转换配置为pymysql连接参数格式
        
        Args:
            db_conf: 数据库配置字典
            
        Returns:
            dict: pymysql连接参数
        """
        # 创建一个新的字典，避免修改原始配置
        conn_kwargs = {}
        
        # 复制基本连接参数
        for key in ['host', 'port', 'user', 'charset']:
            if key in db_conf:
                conn_kwargs[key] = db_conf[key]
        
        # 密码键名转换: password -> passwd
        if 'password' in db_conf:
            conn_kwargs['passwd'] = db_conf['password']
        
        # 数据库名称，如果是逗号分隔的列表，只取第一个
        if 'database' in db_conf:
            db_name = db_conf['database']
            if isinstance(db_name, str) and ',' in db_name:
                db_name = db_name.split(',')[0]
            conn_kwargs['database'] = db_name
        
        return conn_kwargs

def setup_logger(config_instance):
    """根据YAML配置设置loguru日志
    
    Args:
        config_instance: 配置加载器实例
    """
    # 获取日志配置
    log_config = config_instance.get_logging_config()
    
    # 获取日志目录和文件名
    log_dir = log_config.get('dir', 'log')
    log_file = log_config.get('filename', 'batch_jobs.log')
    
    # 确保日志目录存在
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir_path = os.path.join(project_root, log_dir)
    if not os.path.exists(log_dir_path):
        os.makedirs(log_dir_path)
    
    # 完整日志文件路径
    log_file_path = os.path.join(log_dir_path, log_file)
    
    # 移除默认处理器
    logger.remove()
    
    # 添加控制台处理器
    logger.add(
        sys.stderr,
        level=log_config.get('level', 'INFO'),
        format=log_config.get('format', "{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}"),
        colorize=log_config.get('colorize', True)
    )
    
    # 添加文件处理器
    logger.add(
        log_file_path,
        level=log_config.get('level', 'INFO'),
        format=log_config.get('format', "{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}"),
        rotation=log_config.get('rotation', "1 days"),
        retention=log_config.get('retention', "7 days"),
        compression="zip",
        backtrace=log_config.get('backtrace', True),
        diagnose=log_config.get('diagnose', True),
        colorize=False,
        enqueue=True
    )
    
    logger.info(f"日志配置已初始化，日志文件: {log_file_path}")

# 创建全局配置实例
config = ConfigLoader()

# 导出数据库连接配置，便于其他模块直接使用
SOURCE_MYSQL_CONF = config.source_mysql_conf
TARGET_MYSQL_CONF = config.target_mysql_conf

# 初始化日志配置
setup_logger(config)
