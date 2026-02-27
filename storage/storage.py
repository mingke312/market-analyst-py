#!/usr/bin/env python3
"""
数据存储模块
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path


class Storage:
    """JSON文件存储"""
    
    def __init__(self, data_dir: str = None):
        if data_dir is None:
            # 默认数据目录
            base_dir = Path(__file__).parent
            data_dir = base_dir / "data"
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_date(self, date: str = None) -> str:
        """获取日期字符串"""
        if date is None:
            return datetime.now().strftime("%Y-%m-%d")
        return date
    
    def _get_filepath(self, prefix: str, date: str = None) -> Path:
        """获取文件路径"""
        date_str = self._get_date(date)
        return self.data_dir / f"{prefix}_{date_str}.json"
    
    def save(self, prefix: str, data: Dict, date: str = None) -> str:
        """
        保存数据到JSON文件
        
        Args:
            prefix: 文件名前缀 (如 market, futures, news)
            data: 要保存的数据
            date: 日期，默认今天
        
        Returns:
            保存的文件路径
        """
        filepath = self._get_filepath(prefix, date)
        
        # 添加元数据
        full_data = {
            'date': self._get_date(date),
            'type': prefix,
            'timestamp': datetime.now().isoformat(),
            'data': data
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(full_data, f, ensure_ascii=False, indent=2)
        
        return str(filepath)
    
    def load(self, prefix: str, date: str = None) -> Optional[Dict]:
        """
        加载数据
        
        Args:
            prefix: 文件名前缀
            date: 日期，默认今天
        
        Returns:
            数据字典，不存在返回None
        """
        filepath = self._get_filepath(prefix, date)
        
        if not filepath.exists():
            return None
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def exists(self, prefix: str, date: str = None) -> bool:
        """检查数据文件是否存在"""
        return self._get_filepath(prefix, date).exists()
    
    def list_dates(self, prefix: str) -> List[str]:
        """列出所有日期"""
        pattern = f"{prefix}_*.json"
        files = self.data_dir.glob(pattern)
        dates = []
        for f in files:
            # 提取日期
            name = f.stem
            date_str = name.replace(f"{prefix}_", "")
            dates.append(date_str)
        return sorted(dates)
    
    def load_range(self, prefix: str, start_date: str, end_date: str) -> List[Dict]:
        """
        加载日期范围内的数据
        
        Args:
            prefix: 文件名前缀
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
        
        Returns:
            数据列表
        """
        results = []
        for date_str in self.list_dates(prefix):
            if start_date <= date_str <= end_date:
                data = self.load(prefix, date_str)
                if data:
                    results.append(data)
        return results
    
    def delete(self, prefix: str, date: str = None) -> bool:
        """删除数据文件"""
        filepath = self._get_filepath(prefix, date)
        if filepath.exists():
            filepath.unlink()
            return True
        return False
    
    # 便捷方法
    def save_market(self, data: Dict, date: str = None) -> str:
        """保存行情数据"""
        return self.save('market', data, date)
    
    def load_market(self, date: str = None) -> Optional[Dict]:
        """加载行情数据"""
        return self.load('market', date)
    
    def save_futures(self, data: Dict, date: str = None) -> str:
        """保存期货数据"""
        return self.save('futures', data, date)
    
    def load_futures(self, date: str = None) -> Optional[Dict]:
        """加载期货数据"""
        return self.load('futures', date)
    
    def save_news(self, data: Dict, date: str = None) -> str:
        """保存新闻数据"""
        return self.save('news', data, date)
    
    def load_news(self, date: str = None) -> Optional[Dict]:
        """加载新闻数据"""
        return self.load('news', date)
    
    def save_basis(self, data: Dict, date: str = None) -> str:
        """保存基差数据"""
        return self.save('basis', data, date)
    
    def load_basis(self, date: str = None) -> Optional[Dict]:
        """加载基差数据"""
        return self.load('basis', date)
    
    def save_analysis(self, data: Dict, date: str = None) -> str:
        """保存分析结果"""
        return self.save('analysis', data, date)
    
    def load_analysis(self, date: str = None) -> Optional[Dict]:
        """加载分析结果"""
        return self.load('analysis', date)


# 全局存储实例
_default_storage = None

def get_storage() -> Storage:
    """获取默认存储实例"""
    global _default_storage
    if _default_storage is None:
        _default_storage = Storage()
    return _default_storage


if __name__ == "__main__":
    # 测试
    storage = Storage()
    
    # 保存测试数据
    test_data = [
        {'code': 'sh000001', 'name': '上证指数', 'price': 4146.63, 'change_percent': -0.01}
    ]
    
    filepath = storage.save_market(test_data)
    print(f"保存到: {filepath}")
    
    # 加载测试
    data = storage.load_market()
    print(f"加载数据: {data}")
