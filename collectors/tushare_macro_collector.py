"""
货币供应量和社会融资数据采集器 - 基于 TuShare Pro
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tushare
from typing import Dict, Any, List
from datetime import datetime

from collectors.base import BaseCollector, get_tushare_token


class MoneySupplyCollector(BaseCollector):
    """货币供应量数据采集器 - TuShare Pro"""
    
    def __init__(self, config: Dict[str, Any] = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        # 获取 token
        token = get_tushare_token()
        if not token:
            raise ValueError("TuShare token 未配置")
        
        self.pro = tushare.pro_api(token)
    
    def fetch(self, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """获取货币供应量数据
        
        Args:
            start_date: 起始月份 (格式：YYYYMM)
            end_date: 结束月份 (格式：YYYYMM)
        """
        try:
            from datetime import datetime
            now = datetime.now()
            
            # 如果没有提供日期，默认获取最近24个月
            if not end_date:
                end_date = now.strftime('%Y%m')
            if not start_date:
                # 24个月前
                year = now.year
                month = now.month - 24
                while month <= 0:
                    month += 12
                    year -= 1
                start_date = f"{year}{month:02d}"
            
            self.logger.info(f"Money supply query range: {start_date} to {end_date}")
            
            df = self.pro.cn_m(start_m=start_date, end_m=end_date)
            
            records = []
            for _, row in df.iterrows():
                record = {
                    'month': str(row['month']),
                    'm0': row['m0'],
                    'm0_yoy': row['m0_yoy'],
                    'm0_mom': row['m0_mom'],
                    'm1': row['m1'],
                    'm1_yoy': row['m1_yoy'],
                    'm1_mom': row['m1_mom'],
                    'm2': row['m2'],
                    'm2_yoy': row['m2_yoy'],
                    'm2_mom': row['m2_mom'],
                }
                records.append(record)
            
            return {'money_supply': records}
            
        except Exception as e:
            self.logger.error(f"获取货币供应量数据失败: {e}")
            raise
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """转换数据格式"""
        return {
            'date': datetime.now().strftime('%Y-%m'),
            'timestamp': datetime.now().isoformat(),
            'source': 'TuShare Pro',
            'type': 'money_supply',
            'data': raw_data
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        # 货币供应量是月度数据，可能数据还没发布，给出友好提示
        records = data.get('data', {}).get('money_supply', [])
        if len(records) == 0:
            self.logger.warning("货币供应量数据未发布（3月数据通常4月中旬发布）")
            return True  # 不阻止流程，只是没有新数据
        return True
    
    def get_data_prefix(self) -> str:
        return 'money_supply'


class SocialFinancingProCollector(BaseCollector):
    """社会融资规模数据采集器 - TuShare Pro"""
    
    def __init__(self, config: Dict[str, Any] = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        # 获取 token
        token = get_tushare_token()
        if not token:
            raise ValueError("TuShare token 未配置")
        
        self.pro = tushare.pro_api(token)
    
    def fetch(self, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """获取社会融资数据
        
        Args:
            start_date: 起始月份 (格式：YYYYMM)
            end_date: 结束月份 (格式：YYYYMM)
        """
        from datetime import datetime
        try:
            now = datetime.now()
            
            # 如果没有提供日期，默认获取最近24个月
            if not end_date:
                end_date = now.strftime('%Y%m')
            if not start_date:
                start_date = '202401'
            
            self.logger.info(f"Social financing query range: {start_date} to {end_date}")
            
            # 获取最近24个月的数据
            df = self.pro.sf_month(start_m=start_date, end_m=end_date)
            
            records = []
            for _, row in df.iterrows():
                record = {
                    'month': str(row['month']),
                    'inc_month': row['inc_month'],      # 社融增量当月值（亿元）
                    'inc_cumval': row['inc_cumval'],    # 社融增量累计值（亿元）
                    'stk_endval': row['stk_endval'],    # 社融存量期末值（万亿元）
                }
                records.append(record)
            
            return {'social_financing': records}
            
        except Exception as e:
            self.logger.error(f"获取社会融资数据失败: {e}")
            raise
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """转换数据格式"""
        return {
            'date': datetime.now().strftime('%Y-%m'),
            'timestamp': datetime.now().isoformat(),
            'source': 'TuShare Pro',
            'type': 'social_financing',
            'data': raw_data
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        return len(data.get('data', {}).get('social_financing', [])) > 0
    
    def get_data_prefix(self) -> str:
        return 'social_financing_pro'


def main():
    import logging
    import json
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    print("=" * 60)
    print("📊 货币供应量和社会融资数据 (TuShare Pro)")
    print("=" * 60)
    
    # 货币供应量
    print("\n【货币供应量 M0/M1/M2】")
    try:
        collector = MoneySupplyCollector()
        result = collector.run()
        if result:
            data = result.get('data', {}).get('money_supply', [])
            for item in data[-6:]:
                print(f"  {item['month']}: M0={item['m0']:,.0f}亿(同比{item['m0_yoy']:+.1f}%) "
                      f"M1={item['m1']:,.0f}亿(同比{item['m1_yoy']:+.1f}%) "
                      f"M2={item['m2']:,.0f}亿(同比{item['m2_yoy']:+.1f}%)")
    except Exception as e:
        print(f"  获取失败: {e}")
    
    # 社会融资
    print("\n【社会融资规模】")
    try:
        collector = SocialFinancingProCollector()
        result = collector.run()
        if result:
            data = result.get('data', {}).get('social_financing', [])
            for item in data[-6:]:
                print(f"  {item['month']}: 增量={item['inc_month']:,.0f}亿 存量={item['stk_endval']:,.2f}万亿")
    except Exception as e:
        print(f"  获取失败: {e}")
    
    print("\n✅ 数据获取完成")


if __name__ == '__main__':
    main()
