"""
央行金融数据自动采集器
从中国人民银行官网自动获取每月金融统计数据报告
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
from typing import Dict, Any, Optional

from collectors.base import BaseCollector


class PBOCMonthlyCollector(BaseCollector):
    """央行月度金融数据采集器"""
    
    # 央行调查统计司页面
    BASE_URL = "https://www.pbc.gov.cn/diaochatongjisi/116219/116225/"
    
    def __init__(self, config: Dict[str, Any] = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def fetch(self) -> Dict[str, Any]:
        """获取最新金融数据"""
        data = {
            'report_date': datetime.now().strftime('%Y-%m-%d'),
            'data': {}
        }
        
        try:
            # 获取最新报告页面
            response = self.session.get(self.BASE_URL, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 查找最新报告链接
            links = soup.find_all('a', href=True)
            latest_report = None
            
            for link in links:
                href = link.get('href', '')
                text = link.get_text()
                if '金融统计数据报告' in text and '2026' in href:
                    latest_report = {
                        'url': 'https://www.pbc.gov.cn' + href if href.startswith('/') else href,
                        'title': text.strip()
                    }
                    break
            
            if latest_report:
                self.logger.info(f"找到最新报告: {latest_report['title']}")
                data['latest_report'] = latest_report
                
                # 解析报告内容
                report_data = self.parse_report(latest_report['url'])
                if report_data:
                    data['data'] = report_data
                    
        except Exception as e:
            self.logger.error(f"获取央行数据失败: {e}")
        
        return data
    
    def parse_report(self, url: str) -> Optional[Dict]:
        """解析报告页面"""
        try:
            response = self.session.get(url, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.get_text()
            
            data = {}
            
            # 提取货币供应量
            if '广义货币(M2)' in text:
                # M2
                m2_match = re.search(r'广义货币\(M2\)余额(\d+\.?\d*)万亿元，同比增长(\d+\.?\d*)%', text)
                if m2_match:
                    data['m2'] = {
                        'value': float(m2_match.group(1)),
                        'unit': '万亿元',
                        'yoy': float(m2_match.group(2))
                    }
                
                # M1
                m1_match = re.search(r'狭义货币\(M1\)余额(\d+\.?\d*)万亿元，同比增长(\d+\.?\d*)%', text)
                if m1_match:
                    data['m1'] = {
                        'value': float(m1_match.group(1)),
                        'unit': '万亿元',
                        'yoy': float(m1_match.group(2))
                    }
                
                # M0
                m0_match = re.search(r'流通中货币\(M0\)余额(\d+\.?\d*)万亿元，同比增长(\d+\.?\d*)%', text)
                if m0_match:
                    data['m0'] = {
                        'value': float(m0_match.group(1)),
                        'unit': '万亿元',
                        'yoy': float(m0_match.group(2))
                    }
            
            # 社融存量
            sf_match = re.search(r'社会融资规模存量为(\d+\.?\d*)万亿元，同比增长(\d+\.?\d*)%', text)
            if sf_match:
                data['social_financing_stock'] = {
                    'value': float(sf_match.group(1)),
                    'unit': '万亿元',
                    'yoy': float(sf_match.group(2))
                }
            
            # 社融增量
            sf_inc_match = re.search(r'社会融资规模增量累计为(\d+\.?\d*)万亿元', text)
            if sf_inc_match:
                data['social_financing_increment'] = {
                    'value': float(sf_inc_match.group(1)),
                    'unit': '万亿元'
                }
            
            # 人民币贷款
            loan_match = re.search(r'人民币贷款余额(\d+\.?\d*)万亿元，同比增长(\d+\.?\d*)%', text)
            if loan_match:
                data['rmb_loan'] = {
                    'value': float(loan_match.group(1)),
                    'unit': '万亿元',
                    'yoy': float(loan_match.group(2))
                }
            
            # 人民币存款
            deposit_match = re.search(r'人民币存款余额(\d+\.?\d*)万亿元，同比增长(\d+\.?\d*)%', text)
            if deposit_match:
                data['rmb_deposit'] = {
                    'value': float(deposit_match.group(1)),
                    'unit': '万亿元',
                    'yoy': float(deposit_match.group(2))
                }
            
            return data
            
        except Exception as e:
            self.logger.error(f"解析报告失败: {e}")
            return None
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """转换数据格式"""
        return {
            'date': datetime.now().strftime('%Y-%m'),
            'timestamp': datetime.now().isoformat(),
            'source': '中国人民银行',
            'type': 'pboc_monthly',
            'data': raw_data
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        return len(data.get('data', {})) > 0
    
    def get_data_prefix(self) -> str:
        return 'pboc_monthly'


def main():
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    print("=" * 60)
    print("央行月度金融数据采集")
    print("=" * 60)
    
    collector = PBOCMonthlyCollector()
    result = collector.run()
    
    if result:
        print("\n✅ 采集成功!")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print("\n❌ 采集失败")


if __name__ == '__main__':
    main()
