"""
A股行情数据采集器 - TuShare版本
"""
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tushare as ts
from collectors.base import BaseCollector, get_tushare_token


class StockCollector(BaseCollector):
    """A股行情数据采集器 - TuShare"""
    
    # 指数代码映射 (TuShare格式)
    INDEX_CODES = {
        '000001.SH': '上证指数',
        '399001.SZ': '深证成指',
        '399006.SZ': '创业板指',
        '000300.SH': '沪深300',
        '000905.SH': '中证500',
        '000688.SH': '科创50',
        '000852.SH': '中证1000',
    }
    
    def __init__(self, config: Dict[str, Any] = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        token = self.config.get('tushare_token', os.getenv('TUSHARE_TOKEN', ''))
        if token:
            self.pro = ts.pro_api(token)
            self.logger.info("TuShare API initialized")
        else:
            self.pro = None
    
    def fetch(self, start_date: str = None, end_date: str = None) -> List[Dict]:
        """从TuShare获取指数行情
        
        Args:
            start_date: 起始日期 (格式：YYYYMMDD)
            end_date: 结束日期 (格式：YYYYMMDD)
        """
        if not self.pro:
            raise ValueError("TuShare token not configured")
        
        indices = []
        
        # 如果没有提供日期，默认获取今天的数据
        if not start_date:
            start_date = datetime.now().strftime('%Y%m%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        
        for ts_code, name in self.INDEX_CODES.items():
            try:
                df = self.pro.index_daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )
                
                if df is not None and len(df) > 0:
                    # 获取最新数据
                    latest = df.iloc[-1]
                    
                    # 转换为dict，处理datetime类型
                    latest_dict = {}
                    for k, v in latest.items():
                        if hasattr(v, 'strftime'):  # datetime类型
                            latest_dict[k] = v.strftime('%Y-%m-%d')
                        else:
                            latest_dict[k] = v
                    
                    latest = latest_dict
                    index_data = {
                        'ts_code': ts_code,
                        'name': name,
                        'date': latest.get('trade_date', ''),
                        'open': latest.get('open', 0),
                        'high': latest.get('high', 0),
                        'low': latest.get('low', 0),
                        'close': latest.get('close', 0),
                        'pre_close': latest.get('pre_close', 0),
                        'change': latest.get('pct_chg', 0),
                        'vol': latest.get('vol', 0),
                        'amount': latest.get('amount', 0),  # TuShare返回的就是千元
                    }
                    indices.append(index_data)
                    self.logger.info(f"Fetched {name}: {latest.get('close')} ({latest.get('pct_chg')}%)")
                    
            except Exception as e:
                self.logger.warning(f"Failed to fetch {ts_code}: {e}")
        
        return indices
    
    def transform(self, raw_data: List[Dict]) -> Dict[str, Any]:
        """转换为标准格式"""
        date_str = datetime.now().strftime('%Y-%m-%d')
        
        indices = []
        for item in raw_data:
            try:
                index = {
                    'date': date_str,
                    'code': item.get('ts_code', '').replace('.SH', '').replace('.SZ', ''),
                    'name': item.get('name', ''),
                    'open': item.get('open', 0),
                    'high': item.get('high', 0),
                    'low': item.get('low', 0),
                    'close': item.get('close', 0),
                    'pct_chg': item.get('change', 0),
                    'volume': item.get('vol', 0),
                    'amount': item.get('amount', 0),  # 千元
                }
                indices.append(index)
            except Exception as e:
                self.logger.warning(f"Failed to transform: {e}")
        
        return {
            'date': date_str,
            'timestamp': datetime.now().isoformat(),
            'source': 'TuShare',
            'type': 'stock',
            'indices': indices
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        if not data.get('indices'):
            self.logger.error("No indices data")
            return False
        
        if len(data.get('indices', [])) < 6:
            self.logger.warning(f"Only {len(data.get('indices', []))} indices collected")
        
        return True
    
    def get_data_prefix(self) -> str:
        return 'stock'


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # 测试
    config = {
        'tushare_token': get_tushare_token()
    }
    collector = StockCollector(config)
    result = collector.run()
    print(f"Collected {len(result.get('indices', []))} indices" if result else "No data")
