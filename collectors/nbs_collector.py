"""
房地产和PMI数据采集器 - 从国家统计局获取
"""
import os
import sys

# 添加父目录到路径
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

import requests
from typing import Dict, Any
from datetime import datetime

from collectors.base import BaseCollector


class RealEstateCollector(BaseCollector):
    """房地产数据采集器 - 国家统计局"""
    
    REAL_ESTATE_URL = "https://www.stats.gov.cn/sj/zxfb/202601/t20260119_1962324.html"
    
    def __init__(self, config: Dict[str, Any] = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0'
        })
    
    def fetch(self) -> Dict[str, Any]:
        data = {}
        try:
            response = self.session.get(self.REAL_ESTATE_URL, timeout=30)
            response.raise_for_status()
            html = response.text
            
            import re
            
            # 房地产开发投资
            match = re.search(r'全国房地产开发投资(\d+)亿元', html)
            if match:
                data['total_investment'] = int(match.group(1))
            
            match = re.search(r'比上年下降(\d+\.\d+)%', html)
            if match:
                data['total_investment_yoy'] = -float(match.group(1))
            
            match = re.search(r'其中，住宅投资(\d+)亿元', html)
            if match:
                data['residential_investment'] = int(match.group(1))
            
            match = re.search(r'房屋施工面积(\d+)万平方米', html)
            if match:
                data['construction_area'] = int(match.group(1))
            
            match = re.search(r'房屋新开工面积(\d+)万平方米', html)
            if match:
                data['new_start_area'] = int(match.group(1))
            
            match = re.search(r'房屋竣工面积(\d+)万平方米', html)
            if match:
                data['completion_area'] = int(match.group(1))
            
            match = re.search(r'新建商品房销售面积(\d+)万平方米', html)
            if match:
                data['sales_area'] = int(match.group(1))
            
            match = re.search(r'新建商品房销售额(\d+)亿元', html)
            if match:
                data['sales_amount'] = int(match.group(1))
            
            match = re.search(r'商品房待售面积(\d+)万平方米', html)
            if match:
                data['inventory'] = int(match.group(1))
            
            match = re.search(r'房地产开发企业到位资金(\d+)亿元', html)
            if match:
                data['funds'] = int(match.group(1))
            
            match = re.search(r'房地产开发景气指数.*?为(\d+\.\d+)', html)
            if match:
                data['prosperity_index'] = float(match.group(1))
            
            self.logger.info(f"Real estate: {list(data.keys())}")
        except Exception as e:
            self.logger.warning(f"Failed: {e}")
        
        return {'real_estate': data}
    
    def transform(self, raw_data: Dict) -> Dict[str, Any]:
        return {
            'date': '2025', 'timestamp': datetime.now().isoformat(),
            'source': '国家统计局', 'type': 'real_estate', 'data': raw_data
        }
    
    def validate(self, data: Dict) -> bool:
        return len(data.get('data', {}).get('real_estate', {})) > 0
    
    def get_data_prefix(self) -> str:
        return 'real_estate'


class PMICollector(BaseCollector):
    """PMI采集器"""
    
    PMI_URL = "https://www.stats.gov.cn/sj/zxfb/202601/t20260131_1962416.html"
    
    def __init__(self, config: Dict = None):
        if config is None:
            config = {}
        super().__init__(config)
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
    
    def fetch(self) -> Dict:
        data = {}
        try:
            response = self.session.get(self.PMI_URL, timeout=30)
            response.raise_for_status()
            html = response.text
            import re
            
            # 制造业PMI
            match = re.search(r'制造业采购经理指数.*?为(\d+\.\d+)%', html)
            if match:
                data['manufacturing_pmi'] = float(match.group(1))
            
            # 制造业PMI环比
            match = re.search(r'制造业采购经理指数.*?比上月下降(\d+\.\d+)个百分点', html)
            if match:
                data['manufacturing_pmi_mom'] = -float(match.group(1))
            else:
                match = re.search(r'制造业采购经理指数.*?比上月上升(\d+\.\d+)个百分点', html)
                if match:
                    data['manufacturing_pmi_mom'] = float(match.group(1))
            
            # 大型企业PMI
            match = re.search(r'大型企业PMI.*?为(\d+\.\d+)%', html)
            if match:
                data['large_pmi'] = float(match.group(1))
            
            # 非制造业
            match = re.search(r'非制造业商务活动指数.*?为(\d+\.\d+)%', html)
            if match:
                data['non_manufacturing_pmi'] = float(match.group(1))
            
            # 综合PMI
            match = re.search(r'综合PMI产出指数.*?为(\d+\.\d+)%', html)
            if match:
                data['composite_pmi'] = float(match.group(1))
            
            self.logger.info(f"PMI: {list(data.keys())}")
        except Exception as e:
            self.logger.warning(f"Failed: {e}")
        
        return {'pmi': data}
    
    def transform(self, raw_data: Dict) -> Dict[str, Any]:
        return {
            'date': '2026-01', 'timestamp': datetime.now().isoformat(),
            'source': '国家统计局', 'type': 'pmi', 'data': raw_data
        }
    
    def validate(self, data: Dict) -> bool:
        return len(data.get('data', {}).get('pmi', {})) > 0
    
    def get_data_prefix(self) -> str:
        return 'pmi'


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    
    print("=== 房地产 ===")
    re_collector = RealEstateCollector()
    re_result = re_collector.run()
    if re_result:
        for k, v in re_result.get('data', {}).get('real_estate', {}).items():
            print(f"  {k}: {v}")
    
    print("\n=== PMI ===")
    pmi_collector = PMICollector()
    pmi_result = pmi_collector.run()
    if pmi_result:
        for k, v in pmi_result.get('data', {}).get('pmi', {}).items():
            print(f"  {k}: {v}")
