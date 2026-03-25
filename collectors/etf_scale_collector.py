#!/usr/bin/env python3
"""
ETF规模数据采集器 - 从天天基金网获取
"""
import os
import sys
import json
import logging
from datetime import datetime
import requests
import re

# 添加项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


class ETFScaleCollector:
    """ETF规模数据采集器"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://fund.eastmoney.com/'
        }
        self.logger = logging.getLogger(__name__)
        
        # 主要ETF列表
        self.etf_codes = [
            '510300',  # 沪深300
            '510500',  # 中证500
            '159919',  # 嘉实沪深300
            '159915',  # 创业板
            '159810',  # 创业板
            '159992',  # 创新药
            '512880',  # 证券公司
            '513050',  # 中概互联网
            '513100',  # 纳指100
            '513500',  # 标普500
        ]
    
    def fetch_one(self, code: str) -> dict:
        """获取单个ETF的规模数据"""
        url = f"https://fund.eastmoney.com/pingzhongdata/{code}.js"
        
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            if resp.status_code != 200:
                return None
            
            content = resp.text
            
            # 提取基金名称
            name_match = re.search(r'var\s+fS_name\s*=\s*["\']([^"\']+)', content)
            name = name_match.group(1) if name_match else code
            
            # 提取规模
            yi_match = re.search(r'([\d.]+)\s*亿', content)
            scale = float(yi_match.group(1)) if yi_match else None
            
            # 提取最新净值
            nav_match = re.search(r'var\s+fund_nav\s*=\s*([\d.]+)', content)
            nav = float(nav_match.group(1)) if nav_match else None
            
            # 提取最新涨跌幅
            change_match = re.search(r'var\s+fund_sy\s*=\s*["\']([+-]?[\d.]+)', content)
            change = float(change_match.group(1)) if change_match else None
            
            return {
                'code': code,
                'name': name,
                'scale_billion': scale,  # 亿元
                'nav': nav,
                'change_pct': change
            }
        except Exception as e:
            self.logger.warning(f"获取 {code} 失败: {e}")
            return None
    
    def fetch_all(self) -> dict:
        """获取所有ETF的规模数据"""
        results = []
        
        for code in self.etf_codes:
            data = self.fetch_one(code)
            if data:
                results.append(data)
                self.logger.info(f"✅ {code}: {data['name']}, 规模: {data['scale_billion']}亿")
        
        return {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'etf_data': results
        }


def main():
    """测试采集器"""
    logging.basicConfig(level=logging.INFO)
    
    collector = ETFScaleCollector()
    result = collector.fetch_all()
    
    # 保存数据
    today = datetime.now().strftime('%Y-%m-%d')
    output_file = f'data/etf_scale_{today}.json'
    os.makedirs('data', exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print(f"\n数据已保存到: {output_file}")
    print(f"共获取 {len(result['etf_data'])} 个ETF数据")


if __name__ == '__main__':
    main()
