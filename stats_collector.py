#!/usr/bin/env python3
"""
统计局数据采集器
使用Playwright从国家统计局网站获取宏观经济数据
"""

import re
import json
import os
from datetime import datetime
from typing import Dict, Optional
from playwright.sync_api import sync_playwright


class StatsCollector:
    """统计局数据采集器"""
    
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.browser = None
        self.page = None
    
    def __enter__(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless)
        self.page = self.browser.new_page()
        return self
    
    def __exit__(self, *args):
        if self.browser:
            self.browser.close()
        if hasattr(self, 'playwright'):
            self.playwright.stop()
    
    def get_gdp(self) -> Dict:
        """获取GDP数据"""
        self.page.goto('https://data.stats.gov.cn/easyquery.htm?cn=C01')
        self.page.wait_for_timeout(5000)
        
        text = self.page.locator('body').inner_text()
        
        data = {
            'name': '国内生产总值',
            'unit': '亿元',
            'annual': {},
            'quarterly': {}
        }
        
        # Parse GDP data from text
        lines = text.split('\n')
        for line in lines:
            if '国内生产总值(亿元)' in line and '全国' in line:
                if '2025年' in line:
                    match = re.search(r'2025年\s+([\d.]+)', line)
                    if match:
                        data['annual']['2025'] = float(match.group(1))
                if '2024年' in line:
                    match = re.search(r'2024年\s+([\d.]+)', line)
                    if match:
                        data['annual']['2024'] = float(match.group(1))
            
            # Growth rate
            if '国内生产总值增长' in line and '百分点' in line:
                if '2025年' in line:
                    match = re.search(r'2025年\s+([-\d.]+)', line)
                    if match:
                        data['growth_2025'] = float(match.group(1))
                if '2024年' in line:
                    match = re.search(r'2024年\s+([-\d.]+)', line)
                    if match:
                        data['growth_2024'] = float(match.group(1))
        
        return data
    
    def get_cpi_ppi(self) -> Dict:
        """获取CPI和PPI数据"""
        self.page.goto('https://data.stats.gov.cn/easyquery.htm?cn=C01')
        self.page.wait_for_timeout(6000)
        
        # Try different navigation approaches
        try:
            # Click on price index section
            self.page.click('text=价格指数', timeout=3000)
            self.page.wait_for_timeout(3000)
        except:
            pass
        
        text = self.page.locator('body').inner_text()
        
        data = {
            'cpi': {'name': '居民消费价格指数'},
            'ppi': {'name': '工业生产者出厂价格指数'}
        }
        
        lines = text.split('\n')
        for line in lines:
            # CPI
            if '居民消费价格指数' in line and '%' in line:
                if '2025年' in line:
                    match = re.search(r'2025年\s+([-\d.]+)', line)
                    if match:
                        data['cpi']['2025'] = float(match.group(1))
                if '2024年' in line:
                    match = re.search(r'2024年\s+([-\d.]+)', line)
                    if match:
                        data['cpi']['2024'] = float(match.group(1))
            
            # PPI
            if '工业生产者出厂价格指数' in line and '%' in line:
                if '2025年' in line:
                    match = re.search(r'2025年\s+([-\d.]+)', line)
                    if match:
                        data['ppi']['2025'] = float(match.group(1))
                if '2024年' in line:
                    match = re.search(r'2024年\s+([-\d.]+)', line)
                    if match:
                        data['ppi']['2024'] = float(match.group(1))
        
        return data
    
    def get_all(self) -> Dict:
        """获取所有可用的统计数据"""
        result = {
            'source': '国家统计局',
            'url': 'https://data.stats.gov.cn/',
            'timestamp': datetime.now().isoformat(),
            'data': {}
        }
        
        # GDP
        try:
            result['data']['gdp'] = self.get_gdp()
            print(f"✓ GDP: {result['data'].get('gdp', {}).get('annual', {})}")
        except Exception as e:
            print(f"✗ GDP error: {e}")
        
        # CPI/PPI
        try:
            result['data']['cpi_ppi'] = self.get_cpi_ppi()
            print(f"✓ CPI/PPI: {result['data'].get('cpi_ppi', {})}")
        except Exception as e:
            print(f"✗ CPI/PPI error: {e}")
        
        return result


def collect_stats_data() -> Dict:
    """采集统计局数据"""
    with StatsCollector(headless=True) as collector:
        return collector.get_all()


if __name__ == '__main__':
    print("正在从国家统计局获取数据...")
    data = collect_stats_data()
    print("\n获取的数据:")
    print(json.dumps(data, ensure_ascii=False, indent=2))
