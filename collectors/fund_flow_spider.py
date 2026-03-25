#!/usr/bin/env python3
"""
基金数据采集器 - 从天天基金网获取数据
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta
import requests
import pandas as pd
from bs4 import BeautifulSoup

# 添加项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from collectors.base import BaseCollector, get_tushare_token


class FundFlowSpider(BaseCollector):
    """从天天基金网爬取资金流数据"""
    
    def __init__(self, config):
        super().__init__(config)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://fund.eastmoney.com/'
        }
    
    def fetch_etf_flow(self):
        """获取ETF资金流数据"""
        # 获取主要ETF列表
        etf_codes = ['510300', '510500', '159919', '159915', '159810', '159992']
        # 沪深300, 500, 创业板, 中证500, 1000, 2000
        
        # 从天天基金获取ETF规模数据
        url = "https://fund.eastmoney.com/data/fundranking.html"
        
        results = []
        for code in etf_codes:
            try:
                # 获取单个ETF的详情页面
                detail_url = f"https://fund.eastmoney.com/{code}.html"
                resp = requests.get(detail_url, headers=self.headers, timeout=10)
                if resp.status_code == 200:
                    results.append({
                        'code': code,
                        'status': 'success'
                    })
            except Exception as e:
                results.append({
                    'code': code,
                    'status': 'failed',
                    'error': str(e)
                })
        
        return results
    
    def fetch_fund_gy(self):
        """获取基金规模数据"""
        # 尝试获取基金规模页面
        url = "https://fund.eastmoney.com/data/fundgm.html"
        
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            if resp.status_code == 200:
                return {'status': 'success', 'content': resp.text[:1000]}
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
        
        return {'status': 'no_data'}
    
    def fetch_all(self):
        """获取所有基金相关数据"""
        data = {}
        
        # 尝试获取各种数据
        gy_result = self.fetch_fund_gy()
        data['fund_gy'] = gy_result
        
        etf_result = self.fetch_etf_flow()
        data['etf_flow'] = etf_result
        
        return data


def main():
    """测试采集器"""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    config = {'tushare_token': get_tushare_token()}
    spider = FundFlowSpider(config)
    
    logger.info("开始采集基金数据...")
    
    # 测试获取基金规模数据
    result = spider.fetch_fund_gy()
    logger.info(f"基金规模页面: {result.get('status')}")
    
    # 测试获取ETF数据
    etf_result = spider.fetch_etf_flow()
    logger.info(f"ETF数据: {etf_result}")


if __name__ == '__main__':
    main()
