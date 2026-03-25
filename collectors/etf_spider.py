#!/usr/bin/env python3
"""
ETF数据采集器 - 从天天基金网获取数据
"""
import os
import sys
import json
import logging
from datetime import datetime
import requests
import pandas as pd
import re

# 添加项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from collectors.base import get_tushare_token


class ETFSpider:
    """ETF数据爬虫 - 从天天基金网获取"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://fund.eastmoney.com/'
        }
        self.logger = logging.getLogger(__name__)
    
    def get_etf_list(self):
        """获取ETF列表"""
        # 使用东方财富API获取ETF列表
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            'pn': 1,
            'pz': 500,
            'po': 1,
            'np': 1,
            'fltt': 2,
            'invt': 2,
            'fid': 'f3',
            'fs': 'm:1+t:80',  # ETF筛选条件
            'fields': 'f1,f2,f3,f4,f12,f13,f14'
        }
        
        try:
            resp = requests.get(url, params=params, headers=self.headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                diff = data.get('data', {}).get('diff', [])
                return diff
        except Exception as e:
            self.logger.warning(f"获取ETF列表失败: {e}")
        
        return []
    
    def get_etf_scale(self, etf_code):
        """获取单个ETF规模数据"""
        # 尝试从基金详情页获取规模数据
        url = f"https://fund.eastmoney.com/pingzhongdata/{etf_code}.js"
        
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            if resp.status_code == 200:
                content = resp.text
                
                # 提取数据
                data = {
                    'code': etf_code,
                    'data': content[:500] if content else ''
                }
                
                # 尝试提取净资产规模
                # 格式: fund_zsze = "123.45亿"
                match = re.search(r'fund_zsze\s*=\s*["\']([^"\']+)', content)
                if match:
                    data['scale'] = match.group(1)
                
                return data
        except Exception as e:
            self.logger.warning(f"获取ETF规模失败 {etf_code}: {e}")
        
        return None
    
    def fetch_all(self):
        """获取所有ETF数据"""
        data = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'etf_list': [],
            'etf_scale': []
        }
        
        # 获取ETF列表
        etf_list = self.get_etf_list()
        data['etf_list'] = etf_list
        self.logger.info(f"获取到 {len(etf_list)} 个ETF")
        
        # 获取主要ETF的规模数据（前20个）
        for etf in etf_list[:20]:
            code = etf.get('f12')
            if code:
                scale_data = self.get_etf_scale(code)
                if scale_data:
                    data['etf_scale'].append(scale_data)
        
        return data


def main():
    """测试采集器"""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    spider = ETFSpider()
    result = spider.fetch_all()
    
    print(f"获取到 {len(result.get('etf_list', []))} 个ETF")
    print(f"规模数据: {len(result.get('etf_scale', []))} 条")
    
    # 保存数据
    today = datetime.now().strftime('%Y-%m-%d')
    output_file = f'data/etf_{today}.json'
    os.makedirs('data', exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print(f"数据已保存到: {output_file}")


if __name__ == '__main__':
    main()
