"""
社会融资数据采集器 - 从央行官网获取
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from typing import Dict, Any
from datetime import datetime
import pandas as pd

from collectors.base import BaseCollector, get_tushare_token


class SocialFinancingCollector(BaseCollector):
    """社会融资规模数据采集器 - 央行官网"""
    
    # 社融数据URL - 2025年
    DATA_URL = "https://www.pbc.gov.cn/diaochatongjisi/attachDir/2026/02/2026021418235592487.xlsx"
    
    # 备用URL - 2026年
    DATA_URL_2026 = "https://www.pbc.gov.cn/diaochatongjisi/attachDir/2026/02/2026021616264071771.xlsx"
    
    def __init__(self, config: Dict[str, Any] = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def fetch(self) -> Dict[str, Any]:
        """从央行官网获取社融数据"""
        data = {}
        
        # 尝试获取2025年数据
        for url in [self.DATA_URL, self.DATA_URL_2026]:
            try:
                self.logger.info(f"Fetching: {url}")
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                # 保存到临时文件
                temp_file = '/tmp/srzs.xlsx'
                with open(temp_file, 'wb') as f:
                    f.write(response.content)
                
                # 解析Excel
                df = pd.read_excel(temp_file, header=None)
                
                # 找到数据行（从第11行开始，即index 11）
                records = []
                data_index = 0  # 数据行索引
                for i in range(11, 25):  # 扩展到2026年
                    if i < len(df):
                        row = df.iloc[i]
                        month_raw = str(row[0]).strip() if pd.notna(row[0]) else None
                        if month_raw and month_raw.startswith('202'):
                            # 转换月份格式：从 2025.01 转换为 202501
                            month = self._format_month(month_raw, data_index)
                            data_index += 1
                            if month:
                                record = {
                                    'month': month,
                                'total': self._parse_float(row[1]),          # 社会融资规模增量
                                'rmb_loan': self._parse_float(row[2]),      # 人民币贷款
                                'foreign_loan': self._parse_float(row[3]),  # 外币贷款
                                'entrusted_loan': self._parse_float(row[4]), # 委托贷款
                                'trust_loan': self._parse_float(row[5]),     # 信托贷款
                                'ba_acceptance': self._parse_float(row[6]),  # 未贴现银行承兑汇票
                                'corporate_bond': self._parse_float(row[7]), # 企业债券
                                'gov_bond': self._parse_float(row[8]),       # 政府债券
                                'stock_financing': self._parse_float(row[9]), # 股票融资
                                'abs': self._parse_float(row[10]),          # 资产支持证券
                                'loan_writeoff': self._parse_float(row[11]), # 贷款核销
                            }
                            records.append(record)
                
                if records:
                    data['social_financing'] = records
                    self.logger.info(f"Social financing data: {len(records)} records")
                    break
                    
            except Exception as e:
                self.logger.warning(f"Failed to fetch {url}: {e}")
        
        return data
    
    def _parse_float(self, value) -> float:
        """安全转换为浮点数"""
        if pd.isna(value):
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def _format_month(self, month_raw: str, row_index: int = 0) -> str:
        """将月份格式转换为 YYYYMM 格式
        
        处理Excel中的日期格式问题：
        - 2025.01 -> 202501 (1月)
        - 2025.1  -> 202510 (10月，Excel会把10月存为1位小数)
        
        参数:
            month_raw: 原始月份字符串
            row_index: 行索引，用于判断是否是10月
        """
        try:
            if '.' in month_raw:
                parts = month_raw.split('.')
                year = parts[0]
                month_str = parts[1]
                
                # 根据小数位数和行索引判断：
                # 2位小数如 "01", "02" -> 就是01月、02月
                # 1位小数如 "1" -> 可能是1月，也可能是10月
                # 行索引 9 (第10行数据) 对应10月
                if len(month_str) == 1:
                    if row_index == 9:  # 第10条记录是10月
                        month = '10'
                    else:
                        month = month_str.zfill(2)
                else:
                    month = month_str.zfill(2)
                
                return f"{year}{month}"
            # 处理 202501 格式
            elif len(month_raw) == 6:
                return month_raw
            return None
        except Exception:
            return None
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """转换数据格式"""
        return {
            'date': datetime.now().strftime('%Y-%m'),
            'timestamp': datetime.now().isoformat(),
            'source': '中国人民银行',
            'type': 'social_financing',
            'data': raw_data
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        return len(data.get('data', {}).get('social_financing', [])) > 0
    
    def get_data_prefix(self) -> str:
        return 'social_financing'


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    
    collector = SocialFinancingCollector()
    result = collector.run()
    if result:
        print(f"\n=== 社融数据 ===")
        for item in result.get('data', {}).get('social_financing', []):
            print(f"{item.get('month')}: 社融增量={item.get('total')}亿")
