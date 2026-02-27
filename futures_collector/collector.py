#!/usr/bin/env python3
"""
期货数据收集模块
从东方财富获取股指期货数据
"""

import re
import json
from typing import Dict, Optional
from datetime import datetime
import urllib.request
import urllib.error


# 期货代码映射
FUTURES_CODES = {
    'IF': {'name': '沪深300', 'exchange': '中金所'},
    'IC': {'name': '中证500', 'exchange': '中金所'},
    'IM': {'name': '中证1000', 'exchange': '中金所'},
    'IH': {'name': '上证50', 'exchange': '中金所'},
}

CONTRACT_TYPES = ['当月', '下季', '隔季']


class FuturesCollector:
    """期货数据收集器"""
    
    # 东方财富期货数据API
    BASE_URL = "https://push2.eastmoney.com/api/qt/ulist.np/get"
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def _get_contract_code(self, futures_code: str, contract_type: str) -> str:
        """获取合约代码"""
        # 东方财富合约代码规则
        # IF当月 = IF2206, IF下季 = IF2209, IF隔季 = IF2212
        
        # 获取当前年月
        now = datetime.now()
        year = now.year
        month = now.month
        
        # 合约月份映射
        month_map = {
            '当月': month,
            '下季': month + 1 if month < 12 else 1,
            '隔季': month + 2 if month < 11 else (month + 2 - 12),
        }
        
        target_month = month_map[contract_type]
        target_year = year if target_month > month else year + 1
        
        # 简写年份（后两位）
        year_short = target_year % 100
        
        return f"{futures_code}{year_short:02d}{target_month:02d}"
    
    def fetch(self, futures_code: str, contract_type: str) -> Optional[Dict]:
        """
        获取单个期货合约数据
        
        Args:
            futures_code: 期货代码 (IF/IC/IM/IH)
            contract_type: 合约类型 (当月/下季/隔季)
        
        Returns:
            合约数据
        """
        contract_code = self._get_contract_code(futures_code, contract_type)
        
        # 东方财富行情API
        url = f"https://push2.eastmoney.com/api/qt/stock/get?secid=90.{contract_code}&fields=f43,f44,f45,f46,f47,f48,f50,f51,f52,f57,f58,f59,f60,f169,f170,f171"
        
        req = urllib.request.Request(url, headers=self.headers)
        
        try:
            with urllib.request.urlopen(req, timeout=15) as response:
                content = response.read().decode('utf-8')
                data = json.loads(content)
                
                if data.get('data') is None:
                    return None
                
                stock_data = data['data']
                return {
                    'code': contract_code,
                    'price': float(stock_data.get('f43', 0)) / 1000 if stock_data.get('f43') else 0,
                    'open': float(stock_data.get('f44', 0)) / 1000 if stock_data.get('f44') else 0,
                    'high': float(stock_data.get('f45', 0)) / 1000 if stock_data.get('f45') else 0,
                    'low': float(stock_data.get('f46', 0)) / 1000 if stock_data.get('f46') else 0,
                    'volume': stock_data.get('f47', 0),
                    'amount': float(stock_data.get('f48', 0)) / 100000000 if stock_data.get('f48') else 0,
                    'change': float(stock_data.get('f169', 0)) / 1000 if stock_data.get('f169') else 0,
                    'change_percent': float(stock_data.get('f170', 0)) / 100 if stock_data.get('f170') else 0,
                    'settlement': float(stock_data.get('f171', 0)) / 1000 if stock_data.get('f171') else 0,
                }
                
        except (urllib.error.URLError, json.JSONDecodeError, KeyError) as e:
            print(f"获取{futures_code}{contract_type}失败: {e}")
            return None
    
    def collect_all(self) -> Dict:
        """
        收集所有期货数据
        
        Returns:
            完整数据字典
        """
        results = {}
        
        for code in FUTURES_CODES:
            results[code] = {}
            
            for contract_type in CONTRACT_TYPES:
                if code == 'IH' and contract_type == '隔季':
                    # IH没有隔季合约
                    continue
                
                try:
                    data = self.fetch(code, contract_type)
                    if data:
                        results[code][contract_type] = data
                    else:
                        results[code][contract_type] = None
                except Exception as e:
                    print(f"获取{code}{contract_type}失败: {e}")
                    results[code][contract_type] = None
        
        return {
            'date': datetime.now().strftime("%Y-%m-%d"),
            'type': 'futures',
            'data': results,
            'timestamp': datetime.now().isoformat()
        }


def main():
    """命令行入口"""
    import sys
    
    collector = FuturesCollector()
    result = collector.collect_all()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--json':
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print("📈 期货数据")
        print("-" * 50)
        
        for code, contracts in result['data'].items():
            print(f"\n{FUTURES_CODES[code]['name']} ({code}):")
            for ct, data in contracts.items():
                if data:
                    change = f"{data['change_percent']:+.2f}%"
                    print(f"  {ct:4s}: {data['price']:>8.2f}  {change}")
                else:
                    print(f"  {ct:4s}: --")


if __name__ == "__main__":
    main()
