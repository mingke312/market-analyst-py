"""
房地产数据采集器 - 从国家统计局获取月度数据
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import re
from html import unescape
from typing import Dict, Any, List
from datetime import datetime

from collectors.base import BaseCollector, get_tushare_token


class RealEstateCollector(BaseCollector):
    """房地产数据采集器 - 国家统计局月度数据"""
    
    # 月度房地产数据页面URLs (2023-2025)
    MONTHLY_URLS = [
        # 2025
        ("2025-11", "https://www.stats.gov.cn/sj/zxfb/202512/t20251215_1962072.html"),
        ("2025-10", "https://www.stats.gov.cn/sj/zxfb/202511/t20251114_1961853.html"),
        ("2025-09", "https://www.stats.gov.cn/sj/zxfb/202510/t20251020_1961609.html"),
        ("2025-08", "https://www.stats.gov.cn/sj/zxfb/202509/t20250915_1961183.html"),
        ("2025-07", "https://www.stats.gov.cn/sj/zxfb/202508/t20250815_1960787.html"),
        ("2025-06", "https://www.stats.gov.cn/sj/zxfb/202507/t20250715_1960410.html"),
        ("2025-05", "https://www.stats.gov.cn/sj/zxfb/202506/t20250616_1960170.html"),
        ("2025-04", "https://www.stats.gov.cn/sj/zxfb/202505/t20250519_1959865.html"),
        ("2025-03", "https://www.stats.gov.cn/sj/zxfb/202504/t20250416_1959323.html"),
        ("2025-02", "https://www.stats.gov.cn/sj/zxfb/202503/t20250317_1959016.html"),
        # 2024
        ("2024-11", "https://www.stats.gov.cn/sj/zxfb/202412/t20241216_1957761.html"),
        ("2024-10", "https://www.stats.gov.cn/sj/zxfb/202411/t20241115_1957428.html"),
        ("2024-09", "https://www.stats.gov.cn/sj/zxfb/202410/t20241018_1957040.html"),
        ("2024-08", "https://www.stats.gov.cn/sj/zxfb/202409/t20240914_1956482.html"),
        ("2024-07", "https://www.stats.gov.cn/sj/zxfb/202408/t20240815_1955982.html"),
        ("2024-06", "https://www.stats.gov.cn/sj/zxfb/202407/t20240715_1955605.html"),
        ("2024-05", "https://www.stats.gov.cn/sj/zxfb/202406/t20240617_1954711.html"),
        ("2024-04", "https://www.stats.gov.cn/sj/zxfb/202405/t20240520_1950423.html"),
        ("2024-03", "https://www.stats.gov.cn/sj/zxfb/202404/t20240416_1948571.html"),
        ("2024-02", "https://www.stats.gov.cn/sj/zxfb/202403/t20240322_1948133.html"),
        # 2023
        ("2023-11", "https://www.stats.gov.cn/sj/zxfb/202312/t20231215_1945574.html"),
        ("2023-10", "https://www.stats.gov.cn/sj/zxfb/202311/t20231115_1944529.html"),
        ("2023-09", "https://www.stats.gov.cn/sj/zxfb/202310/t20231018_1943656.html"),
        ("2023-08", "https://www.stats.gov.cn/sj/zxfb/202309/t20230915_1942847.html"),
        ("2023-07", "https://www.stats.gov.cn/sj/zxfb/202308/t20230815_1941961.html"),
        ("2023-06", "https://www.stats.gov.cn/sj/zxfb/202307/t20230715_1941273.html"),
        ("2023-05", "https://www.stats.gov.cn/sj/zxfb/202306/t20230615_1940629.html"),
        ("2023-04", "https://www.stats.gov.cn/sj/zxfb/202305/t20230516_1939489.html"),
        ("2023-03", "https://www.stats.gov.cn/sj/zxfb/202304/t20230418_1938709.html"),
    ]
    
    # 年度房地产数据URLs
    ANNUAL_URLS = [
        ("2025", "https://www.stats.gov.cn/sj/zxfb/202601/t20260119_1962324.html"),
        ("2024", "https://www.stats.gov.cn/sj/zxfb/202501/t20250117_1958328.html"),
        ("2023", "https://www.stats.gov.cn/sj/zxfb/202401/t20240116_1946623.html"),
    ]
    
    def __init__(self, config: Dict[str, Any] = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def _extract_data(self, html: str) -> Dict[str, Any]:
        """从HTML中提取房地产数据"""
        # Remove HTML tags but keep text
        text = re.sub(r'<span[^>]*>([^<]*)</span>', r'\1', html)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = unescape(text)
        text = re.sub(r'\s+', ' ', text)
        
        data = {}
        
        # Extract patterns (value only, yoy calculated separately)
        patterns = [
            ('total_investment', r'房地产开发投资(\d+)亿元'),
            ('residential_investment', r'住宅投资(\d+)亿元'),
            ('construction_area', r'房屋施工面积(\d+)万平方米'),
            ('new_start_area', r'房屋新开工面积(\d+)万平方米'),
            ('completion_area', r'房屋竣工面积(\d+)万平方米'),
            ('sales_area', r'新建商品房销售面积(\d+)万平方米'),
            ('sales_amount', r'新建商品房销售额(\d+)亿元'),
            ('inventory', r'商品房待售面积(\d+)万平方米'),
            ('funds', r'房地产开发企业到位资金(\d+)亿元'),
        ]
        
        for key, pattern in patterns:
            match = re.search(pattern, text)
            if match:
                data[key] = int(match.group(1))
        
        # Extract yoy changes
        yoy_patterns = [
            ('total_investment_yoy', r'房地产开发投资.*?下降(\d+\.\d+)%'),
            ('sales_area_yoy', r'销售面积下降(\d+\.\d+)%'),
            ('sales_amount_yoy', r'销售额下降(\d+\.\d+)%'),
            ('inventory_yoy', r'比上年末增长(\d+\.\d+)%'),
            ('funds_yoy', r'到位资金.*?下降(\d+\.\d+)%'),
        ]
        
        for key, pattern in yoy_patterns:
            match = re.search(pattern, text)
            if match:
                data[key] = -float(match.group(1))  # 下降转为负数
        
        return data
    
    def fetch(self) -> Dict[str, Any]:
        """从统计局官网获取房地产月度数据"""
        all_data = {}
        
        # 获取月度累计数据
        for month, url in self.MONTHLY_URLS:
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                html = response.content.decode('utf-8', errors='ignore')
                
                data = self._extract_data(html)
                if data:
                    all_data[month] = data
                    self.logger.info(f"Fetched {month}: {list(data.keys())}")
            except Exception as e:
                self.logger.warning(f"Failed to fetch {month}: {e}")
        
        # 获取年度数据
        for year, url in self.ANNUAL_URLS:
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                html = response.content.decode('utf-8', errors='ignore')
                
                annual_data = self._extract_data(html)
                if annual_data:
                    all_data[year] = annual_data
                    self.logger.info(f"Fetched annual {year}: {list(annual_data.keys())}")
            except Exception as e:
                self.logger.warning(f"Failed to fetch annual {year}: {e}")
        
        return {'real_estate': all_data}
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'date': datetime.now().strftime('%Y-%m'),
            'timestamp': datetime.now().isoformat(),
            'source': '国家统计局',
            'type': 'real_estate',
            'data': raw_data
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        return len(data.get('data', {}).get('real_estate', {})) > 0
    
    def get_data_prefix(self) -> str:
        return 'real_estate'


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    
    collector = RealEstateCollector()
    result = collector.run()
    
    if result:
        print('\n=== 房地产数据 ===')
        re_data = result.get('data', {}).get('real_estate', {})
        for period, data in sorted(re_data.items()):
            print(f"\n{period}:")
            for k, v in data.items():
                print(f"  {k}: {v}")
