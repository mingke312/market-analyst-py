"""
资金流数据采集器 - A股资金流
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tushare as ts
from typing import Dict, Any
from datetime import datetime

from collectors.base import BaseCollector, get_tushare_token
from datetime import datetime, timedelta


class FundFlowCollector(BaseCollector):
    """A股资金流数据采集器"""
    
    def __init__(self, config: Dict[str, Any] = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        token = self.config.get('tushare_token', os.getenv('TUSHARE_TOKEN', ''))
        if token:
            self.pro = ts.pro_api(token)
            self.logger.info("TuShare API initialized")
        else:
            self.pro = None
    
    def fetch(self, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """获取资金流数据
        
        Args:
            start_date: 起始日期 (格式：YYYYMMDD)
            end_date: 结束日期 (格式：YYYYMMDD)
        """
        if not self.pro:
            raise ValueError("TuShare token not configured")
        
        data = {}
        
        # 如果没有提供日期，使用默认今天
        if not start_date:
            start_date = datetime.now().strftime('%Y%m%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        
        today = end_date  # 使用end_date作为"今日"
        
        # 1. 融资融券汇总 - 获取所有历史
        try:
            df = self.pro.margin()
            if df is not None and len(df) > 0:
                data['margin_summary'] = df.to_dict('records')
                self.logger.info(f"Margin summary: {len(data['margin_summary'])} records")
        except Exception as e:
            self.logger.warning(f"Margin fetch failed: {e}")
        
        # 2. 融资融券明细 (当日)
        try:
            df = self.pro.margin_detail(trade_date=today)
            if df is not None and len(df) > 0:
                data['margin_detail'] = df.to_dict('records')
                self.logger.info(f"Margin detail: {len(data['margin_detail'])} records")
        except Exception as e:
            self.logger.warning(f"Margin detail fetch failed: {e}")
        
        # 3. 北向资金 - 获取3年历史
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=3*365)).strftime('%Y%m%d')
            df = self.pro.moneyflow_hsgt(start_date=start_date, end_date=end_date)
            if df is not None and len(df) > 0:
                data['north_south'] = df.to_dict('records')
                self.logger.info(f"North-south data: {len(data['north_south'])} records")
        except Exception as e:
            self.logger.warning(f"North-south fetch failed: {e}")
        
        # 4. 龙虎榜机构买卖 (当日)
        try:
            df = self.pro.top_inst(trade_date=today)
            if df is not None and len(df) > 0:
                data['top_institution'] = df.to_dict('records')
                self.logger.info(f"Top institution: {len(data['top_institution'])} records")
        except Exception as e:
            self.logger.warning(f"Top institution fetch failed: {e}")
        
        # 5. 新增投资者数量
        try:
            df = self.pro.stk_holdernumber()
            if df is not None and len(df) > 0:
                data['new_shareholders'] = df.to_dict('records')
                self.logger.info(f"New shareholders: {len(data['new_shareholders'])} records")
        except Exception as e:
            self.logger.warning(f"New shareholders fetch failed: {e}")
        
        # 6. IPO数据 - 获取近2年数据
        try:
            start_date = (datetime.now() - timedelta(days=2*365)).strftime('%Y%m%d')
            df = self.pro.new_share(start_date=start_date, end_date=today)
            if df is not None and len(df) > 0:
                data['ipo'] = df.to_dict('records')
                self.logger.info(f"IPO data: {len(data['ipo'])} records")
        except Exception as e:
            self.logger.warning(f"IPO fetch failed: {e}")
        
        # 7. 股东增减持数据 - 分段获取3年历史数据
        try:
            all_records = []
            # 分4个时间段获取，每段最多3000条
            periods = [
                (datetime.now() - timedelta(days=365*3)).strftime('%Y%m%d'),
                (datetime.now() - timedelta(days=365*2)).strftime('%Y%m%d'),
                (datetime.now() - timedelta(days=365)).strftime('%Y%m%d'),
                datetime.now().strftime('%Y%m%d')
            ]
            
            for i in range(len(periods) - 1):
                start = periods[i]
                end = periods[i + 1]
                df = self.pro.stk_holdertrade(start_date=start, end_date=end)
                if df is not None and len(df) > 0:
                    all_records.extend(df.to_dict('records'))
                    self.logger.info(f"Holder trade {start}~{end}: {len(df)} records")
            
            if all_records:
                data['holder_trade'] = all_records
                self.logger.info(f"Holder trade total: {len(data['holder_trade'])} records")
        except Exception as e:
            self.logger.warning(f"Holder trade fetch failed: {e}")
        
        return data
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """转换数据格式"""
        return {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat(),
            'source': 'TuShare',
            'type': 'fund_flow',
            'data': raw_data
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """验证数据"""
        if not data.get('data'):
            self.logger.warning("No fund flow data collected")
            return False
        return True
    
    def get_data_prefix(self) -> str:
        return 'fund_flow'


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    
    collector = FundFlowCollector({'tushare_token': get_tushare_token()})
    try:
        result = collector.run()
        print(f"Collected: {list(result.get('data', {}).keys())}" if result else "No data")
    except Exception as e:
        print(f"Error: {e}")
