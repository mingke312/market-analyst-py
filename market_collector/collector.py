#!/usr/bin/env python3
"""
行情数据收集模块
从腾讯财经获取A股指数数据
"""

import re
import urllib.request
import urllib.error
from typing import Dict, List, Optional
from datetime import datetime
import json


# 指数代码映射
INDICES = [
    {'code': 'sh000001', 'name': '上证指数'},
    {'code': 'sz399001', 'name': '深证成指'},
    {'code': 'sh000300', 'name': '沪深300'},
    {'code': 'sh000905', 'name': '中证500'},
    {'code': 'sh000852', 'name': '中证1000'},
    {'code': 'sh000016', 'name': '上证50'},
    {'code': 'sh000688', 'name': '科创50'},
    {'code': 'sz399006', 'name': '创业板指'},
]


class MarketCollector:
    """行情数据收集器"""
    
    BASE_URL = "https://qt.gtimg.cn/q="
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def fetch(self, codes: List[str] = None) -> List[Dict]:
        """
        获取行情数据
        
        Args:
            codes: 指数代码列表，默认获取所有
        
        Returns:
            行情数据列表
        """
        if codes is None:
            codes = [idx['code'] for idx in INDICES]
        
        # 构建URL
        codes_str = ','.join(codes)
        url = f"{self.BASE_URL}{codes_str}"
        
        # 请求
        req = urllib.request.Request(url, headers=self.headers)
        
        try:
            with urllib.request.urlopen(req, timeout=15) as response:
                content = response.read()
                # GB18030转UTF-8
                content = content.decode('GB18030', errors='ignore')
                return self._parse(content)
        except urllib.error.URLError as e:
            print(f"获取行情数据失败: {e}")
            return []
    
    def _parse(self, content: str) -> List[Dict]:
        """解析返回数据"""
        results = []
        
        # 格式: v_sh000001="1~上证指数~000001~4146.63~4147.23~4151.07~651702826~0~0~...~-0.60~-0.01~4152.19~4127.15~..."
        # 字段位置:
        # parts[3] = 当前价, parts[4] = 昨收, parts[5] = 今开
        # parts[6] = 成交量
        # parts[31] = 涨跌额, parts[32] = 涨跌幅
        # parts[33] = 最高, parts[34] = 最低
        # parts[37] = 成交额(元)
        pattern = r'v_(sh|sz)(\w+)="([^"]+)"'
        matches = re.findall(pattern, content)
        
        for prefix, code, data in matches:
            full_code = f"{prefix}{code}"
            parts = data.split('~')
            
            if len(parts) < 38:
                continue
            
            try:
                result = {
                    'code': full_code,
                    'name': parts[1],
                    'price': self._safe_float(parts[3]),
                    'prev_close': self._safe_float(parts[4]),
                    'open': self._safe_float(parts[5]),
                    'high': self._safe_float(parts[33]),
                    'low': self._safe_float(parts[34]),
                    'volume': self._safe_int(parts[6]),
                    'amount': self._safe_float(parts[37]),
                    'change': self._safe_float(parts[31]),
                    'change_percent': self._safe_float(parts[32]),
                }
                
                results.append(result)
                
            except (ValueError, IndexError) as e:
                print(f"解析数据失败 {full_code}: {e}")
                continue
        
        return results
    
    def _safe_float(self, s: str) -> float:
        """安全转换为浮点数"""
        try:
            return float(s) if s else 0
        except:
            return 0
    
    def _safe_int(self, s: str) -> int:
        """安全转换为整数"""
        try:
            return int(s) if s else 0
        except:
            return 0
    
    def collect_all(self) -> Dict:
        """
        收集所有指数数据
        
        Returns:
            完整数据字典
        """
        data = self.fetch()
        
        return {
            'date': datetime.now().strftime("%Y-%m-%d"),
            'type': 'indices',
            'count': len(data),
            'data': data,
            'timestamp': datetime.now().isoformat()
        }


def main():
    """命令行入口"""
    import sys
    
    collector = MarketCollector()
    result = collector.collect_all()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--json':
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"📊 行情数据 ({result['count']}个指数)")
        print("-" * 50)
        for item in result['data']:
            change = f"{item['change_percent']:+.2f}%"
            print(f"{item['name']:8s}: {item['price']:>10.2f}  {change}")


if __name__ == "__main__":
    main()
