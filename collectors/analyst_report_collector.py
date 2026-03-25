"""
分析师研究报告采集器
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tushare as ts
from typing import Dict, Any, List
from datetime import datetime

from collectors.base import BaseCollector, get_tushare_token


class AnalystReportCollector(BaseCollector):
    """分析师研究报告采集器"""
    
    def __init__(self, config: Dict[str, Any] = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        token = self.config.get('tushare_token', get_tushare_token())
        if token:
            self.pro = ts.pro_api(token)
            self.logger.info("TuShare API initialized")
        else:
            self.pro = None
    
    def fetch(self) -> List[Dict[str, Any]]:
        """获取研究报告"""
        if not self.pro:
            raise ValueError("TuShare token not configured")
        
        reports = []
        
        # 获取最新研报
        try:
            # 获取今日研报
            df = self.pro.ths_day()
            if df is not None and len(df) > 0:
                reports.extend(df.to_dict('records'))
                self.logger.info(f"Analyst reports: {len(reports)} records")
        except Exception as e:
            self.logger.warning(f"Analyst report fetch failed: {e}")
        
        return reports
    
    def transform(self, raw_data: List[Dict]) -> Dict[str, Any]:
        """转换为标准格式"""
        reports = []
        for item in raw_data:
            try:
                report = {
                    'date': item.get('publish_date', datetime.now().strftime('%Y-%m-%d')),
                    'title': item.get('title', ''),
                    'analyst_name': item.get('analyst_name', item.get('member_name', '')),
                    'analyst_org': item.get('org_name', ''),
                    'rating': item.get('rating', ''),
                    'target_price': item.get('target_price'),
                    'industry': item.get('industry', ''),
                    'summary': item.get('summary', item.get('abstract', '')),
                    'url': item.get('url', ''),
                }
                reports.append(report)
            except Exception as e:
                self.logger.warning(f"Failed to transform report: {e}")
        
        return {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat(),
            'source': 'TuShare/同花顺',
            'type': 'analyst_report',
            'reports': reports
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """验证数据"""
        if not data.get('reports'):
            self.logger.warning("No analyst reports collected")
            return False
        return True
    
    def get_data_prefix(self) -> str:
        return 'analyst_report'


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    
    collector = AnalystReportCollector({'tushare_token': get_tushare_token()})
    try:
        result = collector.run()
        print(f"Collected: {len(result.get('reports', []))} reports" if result else "No data")
    except Exception as e:
        print(f"Error: {e}")
