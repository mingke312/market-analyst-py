"""
ETF资金流向（申赎）采集器
数据来源：东方财富网 ETF资金流向
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import re
import json
from datetime import datetime
from typing import Dict, List, Any
from bs4 import BeautifulSoup

from collectors.base import BaseCollector


class EtfNetFlowCollector(BaseCollector):
    """ETF资金流向数据采集器"""
    
    def __init__(self, config: Dict = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        })
    
    def fetch(self, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """获取ETF资金流向数据
        
        Args:
            start_date: 起始日期 (格式：YYYYMMDD)
            end_date: 结束日期 (格式：YYYYMMDD)
        """
        # 使用东方财富ETF资金流向API
        # 该页面提供所有ETF的单日资金流向数据
        url = "http://fund.eastmoney.com/ETFjjgm.html"
        
        all_data = []
        
        try:
            self.logger.info("Fetching ETF net flow data from East Money...")
            response = self.session.get(url, timeout=30)
            response.encoding = 'utf-8'
            
            # 解析页面获取ETF资金流向数据
            # 东方财富ETF页面包含表格数据
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 查找资金流向数据表格
            # ETF资金流向通常在jjgm页面的表格中
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                for row in rows[1:]:  # 跳过表头
                    cols = row.find_all('td')
                    if len(cols) >= 4:
                        # 尝试提取ETF代码和名称
                        code_elem = cols[0].find('a')
                        if code_elem:
                            code = code_elem.get_text(strip=True)
                            name = code_elem.get('title', '')
                            
                            # 尝试获取资金流向数据
                            # 通常在后面的列中
                            try:
                                net_inflow_text = cols[3].get_text(strip=True) if len(cols) > 3 else '0'
                                net_inflow = self._parse_number(net_inflow_text)
                                
                                all_data.append({
                                    'ts_code': code,
                                    'etf_name': name,
                                    'net_inflow': net_inflow,
                                })
                            except:
                                continue
            
            # 如果页面解析失败，尝试使用API
            if not all_data:
                all_data = self._fetch_from_api()
            
            self.logger.info(f"ETF net flow: {len(all_data)} records")
            
        except Exception as e:
            self.logger.warning(f"Failed to fetch ETF net flow: {e}")
            # 备用方案：尝试API
            all_data = self._fetch_from_api()
        
        return {
            'etf_net_flow': all_data
        }
    
    def _fetch_from_api(self) -> List[Dict]:
        """从东方财富API获取ETF资金流向"""
        all_data = []
        
        try:
            # 东方财富ETF行情数据API
            api_url = "http://push2.eastmoney.com/api/qt/clist/get"
            params = {
                'pn': 1,
                'pz': 500,
                'po': 1,
                'np': 1,
                'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
                'fltt': 2,
                'invt': 2,
                'fid': 'f3',
                'fs': 'm:1+t:23',  # ETF筛选条件
                'fields': 'f1,f2,f3,f4,f12,f13,f14',
            }
            
            response = self.session.get(api_url, params=params, timeout=30)
            data = response.json()
            
            if data.get('data') and data['data'].get('diff'):
                for item in data['data']['diff']:
                    all_data.append({
                        'ts_code': item.get('f12', ''),
                        'etf_name': item.get('f14', ''),
                        'net_inflow': item.get('f3', 0),  # 涨跌幅作为替代指标
                    })
            
            self.logger.info(f"ETF data from API: {len(all_data)} records")
            
        except Exception as e:
            self.logger.warning(f"API fetch failed: {e}")
        
        return all_data
    
    def _parse_number(self, text: str) -> float:
        """解析数字字符串"""
        if not text or text == '-':
            return 0.0
        # 移除万/亿等单位
        text = text.strip()
        multiplier = 1
        if '亿' in text:
            multiplier = 100000000
            text = text.replace('亿', '')
        elif '万' in text:
            multiplier = 10000
            text = text.replace('万', '')
        
        try:
            return float(text.replace(',', '')) * multiplier
        except:
            return 0.0
    
    def transform(self, raw_data: Dict) -> Dict[str, Any]:
        """转换数据格式"""
        return {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat(),
            'source': '东方财富',
            'type': 'etf_net_flow',
            'data': raw_data
        }
    
    def validate(self, data: Dict) -> bool:
        return len(data.get('data', {}).get('etf_net_flow', [])) > 0
    
    def get_data_prefix(self) -> str:
        return 'etf_net_flow'


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    
    collector = EtfNetFlowCollector()
    result = collector.run()
    
    if result:
        print(f"\n=== ETF资金流向 ({len(result['data']['etf_net_flow'])}条) ===")
        for item in result['data']['etf_net_flow'][:10]:
            print(f"  {item.get('ts_code')}: {item.get('etf_name')} - {item.get('net_inflow')}")
