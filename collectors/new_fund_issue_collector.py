"""
新基金发行数据采集器
数据来源：东方财富网 新基金发行
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import re
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any
from bs4 import BeautifulSoup

from collectors.base import BaseCollector


class NewFundIssueCollector(BaseCollector):
    """新基金发行数据采集器"""
    
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
        """获取新基金发行数据
        
        Args:
            start_date: 起始日期 (格式：YYYYMMDD)
            end_date: 结束日期 (格式：YYYYMMDD)
        """
        # 转换日期格式
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')
        
        all_data = []
        
        # 方法1: 从东方财富新基金页面获取
        all_data = self._fetch_from_eastmoney(start_date, end_date)
        
        # 方法2: 如果方法1失败，使用API
        if not all_data:
            all_data = self._fetch_from_api(start_date, end_date)
        
        self.logger.info(f"New fund issue: {len(all_data)} records")
        
        return {
            'new_fund_issue': all_data
        }
    
    def _fetch_from_eastmoney(self, start_date: str, end_date: str) -> List[Dict]:
        """从东方财富网页获取新基金发行数据"""
        data = []
        
        try:
            # 东方财富新基金发行页面
            url = "http://fund.eastmoney.com/data/xg_xjjj.html"
            
            self.logger.info("Fetching new fund issue data from East Money...")
            response = self.session.get(url, timeout=30)
            response.encoding = 'utf-8'
            
            # 解析页面中的JavaScript数据
            # 东方财富页面数据通常在var allData变量中
            script_match = re.search(r'var\s+allData\s*=\s*(\[[^\]]+\]);', response.text)
            
            if script_match:
                try:
                    # 解析数据
                    json_str = script_match.group(1)
                    # 处理JavaScript数组格式
                    items = re.findall(r'\{[^\}]+\}', json_str)
                    
                    for item in items[:100]:  # 取最近100条
                        try:
                            # 提取字段
                            code_match = re.search(r'"[^"]*":\s*"([^"]*)"', item)
                            if code_match:
                                fund_info = self._parse_fund_item(item)
                                if fund_info:
                                    data.append(fund_info)
                        except:
                            continue
                except:
                    pass
            
            # 如果解析失败，尝试解析表格
            if not data:
                soup = BeautifulSoup(response.text, 'html.parser')
                tables = soup.find_all('table')
                
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows[1:]:
                        cols = row.find_all('td')
                        if len(cols) >= 5:
                            try:
                                fund_info = {
                                    'fund_name': cols[0].get_text(strip=True),
                                    'fund_type': cols[1].get_text(strip=True),
                                    'issue_scale': self._parse_number(cols[2].get_text(strip=True)),
                                    'issue_date': cols[3].get_text(strip=True),
                                    'issue_status': cols[4].get_text(strip=True),
                                }
                                if fund_info['fund_name']:
                                    data.append(fund_info)
                            except:
                                continue
            
            self.logger.info(f"Parsed {len(data)} new fund records from HTML")
            
        except Exception as e:
            self.logger.warning(f"East Money fetch failed: {e}")
        
        return data
    
    def _fetch_from_api(self, start_date: str, end_date: str) -> List[Dict]:
        """从东方财富API获取新基金发行数据"""
        data = []
        
        try:
            # 新基金发行API
            api_url = "http://fund.eastmoney.com/data/xg_xjjj_data.aspx"
            params = {
                'rnd': int(datetime.now().timestamp() * 1000),
                'page': 1,
                'rows': 100,
                'startdate': f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}",
                'enddate': f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}",
            }
            
            response = self.session.get(api_url, params=params, timeout=30)
            response.encoding = 'utf-8'
            
            # 解析返回的JavaScript数据
            match = re.search(r'data\s*=\s*(\{.*\});', response.text)
            if match:
                json_str = match.group(1)
                result = json.loads(json_str)
                
                if result.get('data'):
                    for item in result['data']:
                        data.append({
                            'fund_name': item.get('NAME', ''),
                            'fund_type': item.get('TYPE', ''),
                            'issue_scale': float(item.get('SCALE', 0) or 0),
                            'issue_date': item.get('SDATE', ''),
                            'issue_status': item.get('STATUS', ''),
                        })
            
            self.logger.info(f"API returned {len(data)} new fund records")
            
        except Exception as e:
            self.logger.warning(f"API fetch failed: {e}")
        
        return data
    
    def _parse_fund_item(self, item: str) -> Dict:
        """解析基金条目"""
        try:
            # 提取字段值
            name_match = re.search(r'NAME\s*:\s*"([^"]*)"', item)
            type_match = re.search(r'TYPE\s*:\s*"([^"]*)"', item)
            scale_match = re.search(r'SCALE\s*:\s*"([^"]*)"', item)
            date_match = re.search(r'SDATE\s*:\s*"([^"]*)"', item)
            status_match = re.search(r'STATUS\s*:\s*"([^"]*)"', item)
            
            if name_match:
                return {
                    'fund_name': name_match.group(1),
                    'fund_type': type_match.group(1) if type_match else '',
                    'issue_scale': self._parse_number(scale_match.group(1) if scale_match else '0'),
                    'issue_date': date_match.group(1) if date_match else '',
                    'issue_status': status_match.group(1) if status_match else '',
                }
        except:
            pass
        return None
    
    def _parse_number(self, text: str) -> float:
        """解析数字字符串"""
        if not text or text == '-':
            return 0.0
        text = text.strip()
        try:
            return float(text.replace(',', ''))
        except:
            return 0.0
    
    def transform(self, raw_data: Dict) -> Dict[str, Any]:
        """转换数据格式"""
        return {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat(),
            'source': '东方财富',
            'type': 'new_fund_issue',
            'data': raw_data
        }
    
    def validate(self, data: Dict) -> bool:
        return len(data.get('data', {}).get('new_fund_issue', [])) > 0
    
    def get_data_prefix(self) -> str:
        return 'new_fund_issue'


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    
    collector = NewFundIssueCollector()
    result = collector.run()
    
    if result:
        print(f"\n=== 新基金发行 ({len(result['data']['new_fund_issue'])}条) ===")
        for item in result['data']['new_fund_issue'][:10]:
            print(f"  {item.get('fund_name')[:30]} | {item.get('fund_type')} | {item.get('issue_scale')}亿")
