"""
数据采集器基类
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
import logging
import os
import sys
import json

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from storage.storage import Storage as JSONStorage


def get_tushare_token() -> str:
    """获取Tushare Token - 优先从本地配置文件读取，其次从环境变量"""
    # 1. 尝试从本地配置文件读取
    config_path = os.path.join(project_root, 'config', 'local_config.json')
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                if config.get('tushare_token'):
                    return config['tushare_token']
        except Exception:
            pass
    
    # 2. 备选：从环境变量读取
    return os.environ.get('TUSHARE_TOKEN', '')


class BaseCollector(ABC):
    """数据采集基类"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        if config is None:
            config = {}
        
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 数据存储目录
        data_dir = config.get('data_dir', 'data')
        # 相对main.py的路径
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.data_dir = os.path.join(base_dir, data_dir)
        
        self.storage = JSONStorage(self.data_dir)
        
        # 确保数据目录存在
        os.makedirs(self.data_dir, exist_ok=True)
    
    @abstractmethod
    def fetch(self) -> Any:
        """获取原始数据"""
        pass
    
    @abstractmethod
    def transform(self, raw_data: Any) -> Dict[str, Any]:
        """转换数据格式"""
        pass
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """验证数据"""
        # 默认验证通过
        return True
    
    def save(self, data: Dict[str, Any], prefix: str, date: str = None) -> str:
        """保存到本地"""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        return self.storage.save(prefix, data, date)
    
    def run(self, date: str = None, save: bool = False, start_date: str = None, end_date: str = None) -> Optional[Dict[str, Any]]:
        """执行采集流程
        
        Args:
            date: 日期字符串 (单个日期)
            save: 是否保存到文件，默认False（数据直接存数据库）
            start_date: 起始日期 (格式：YYYYMMDD)
            end_date: 结束日期 (格式：YYYYMMDD)
        """
        try:
            self.logger.info(f"Starting collection for {self.__class__.__name__}")
            
            # 1. 获取原始数据
            raw = self.fetch(start_date=start_date, end_date=end_date)
            if raw is None or (isinstance(raw, (list, dict)) and len(raw) == 0):
                self.logger.warning(f"No raw data fetched from {self.__class__.__name__}")
                return None
            
            # 2. 转换格式
            data = self.transform(raw)
            
            # 3. 验证数据
            if not self.validate(data):
                self.logger.error(f"Data validation failed for {self.__class__.__name__}")
                return None
            
            # 4. 保存数据（可选）
            if save:
                prefix = self.get_data_prefix()
                filepath = self.save(data, prefix, date)
                self.logger.info(f"Data saved to {filepath}")
            
            return data
            
        except Exception as e:
            self.logger.error(f"Collection failed: {e}")
            raise
    
    @abstractmethod
    def get_data_prefix(self) -> str:
        """获取数据前缀"""
        pass
