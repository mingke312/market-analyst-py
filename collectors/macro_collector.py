"""
宏观经济数据采集器 - 使用TuShare
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tushare as ts
from typing import Dict, Any, Tuple
from datetime import datetime, timedelta
import pandas as pd
import json

from collectors.base import BaseCollector, get_tushare_token


class MacroCollector(BaseCollector):
    """宏观经济数据采集器 - TuShare接口"""
    
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
        
        # 数据存储目录
        self.data_dir = self.config.get('data_dir', os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data'))
    
    def _get_latest_stored_date(self, data_type: str) -> str:
        """获取本地存储的最新数据日期"""
        import glob
        
        pattern = os.path.join(self.data_dir, f'macro_????-??-??.json')
        files = glob.glob(pattern)
        
        if not files:
            # 如果没有历史数据，从1年前开始
            return (datetime.now() - timedelta(days=365)).strftime('%Y%m')
        
        # 读取最新的macro文件
        latest_file = sorted(files)[-1]
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                content = json.load(f)
                # 数据是嵌套在 data.data 中的
                outer_data = content.get('data', {})
                data = outer_data.get('data', {})
                
                if data_type == 'cpi' and 'cpi' in data:
                    records = data['cpi']
                    if records:
                        return records[0].get('month', '')
                elif data_type == 'ppi' and 'ppi' in data:
                    records = data['ppi']
                    if records:
                        return records[0].get('month', '')
                elif data_type == 'm2' and 'm2' in data:
                    records = data['m2']
                    if records:
                        return records[0].get('month', '')
                elif data_type == 'pmi' and 'pmi' in data:
                    records = data['pmi']
                    if records:
                        return records[0].get('MONTH', '')
                elif data_type == 'gdp' and 'gdp' in data:
                    records = data['gdp']
                    if records:
                        return records[0].get('quarter', '')
        except Exception as e:
            self.logger.warning(f"Failed to read latest date from {latest_file}: {e}")
        
        return (datetime.now() - timedelta(days=365)).strftime('%Y%m')
    
    def _calculate_date_range(self, data_type: str) -> Tuple[str, str]:
        """计算查询日期范围，从最新数据之后开始查询"""
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        
        # 获取本地存储的最新日期
        latest_date = self._get_latest_stored_date(data_type)
        
        if data_type == 'gdp':
            # GDP是季度数据
            if latest_date:
                latest_year = int(latest_date[:4])
                latest_qtr = int(latest_date[5]) if 'Q' in latest_date else 1
                # 从下一个季度开始查询
                if latest_qtr == 4:
                    start_year = latest_year + 1
                    start_qtr = 1
                else:
                    start_year = latest_year
                    start_qtr = latest_qtr + 1
            else:
                start_year = current_year - 1
                start_qtr = 1
            
            # 结束日期为上一个完整季度（当前季度数据通常还没发布）
            # Q1 -> 去年Q4, Q2 -> 今年Q1, Q3 -> 今年Q2, Q4 -> 今年Q3
            if current_month <= 3:
                # Q1: 上一年Q4
                end_year = current_year - 1
                end_qtr = 4
            elif current_month <= 6:
                # Q2: 今年Q1
                end_year = current_year
                end_qtr = 1
            elif current_month <= 9:
                # Q3: 今年Q2
                end_year = current_year
                end_qtr = 2
            else:
                # Q4: 今年Q3
                end_year = current_year
                end_qtr = 3
            
            start_quarter = f"{start_year}Q{start_qtr}"
            end_quarter = f"{end_year}Q{end_qtr}"
            
            return start_quarter, end_quarter
        else:
            # 月度数据
            if latest_date:
                latest_year = int(latest_date[:4])
                latest_month = int(latest_date[4:])
                
                # 从下一个月开始
                if latest_month == 12:
                    start_year = latest_year + 1
                    start_month = 1
                else:
                    start_year = latest_year
                    start_month = latest_month + 1
            else:
                start_year = current_year - 1
                start_month = 1
            
            # 结束日期为当前月份
            end_year = current_year
            end_month = current_month
            
            start_str = f"{start_year}{start_month:02d}"
            end_str = f"{end_year}{end_month:02d}"
            
            return start_str, end_str
    
    def _load_historical_data(self) -> Dict[str, Any]:
        """加载历史数据"""
        import glob
        
        pattern = os.path.join(self.data_dir, f'macro_????-??-??.json')
        files = glob.glob(pattern)
        
        if not files:
            return {}
        
        # 读取所有历史文件，收集各类数据
        all_data = {
            'gdp': [],
            'cpi': [],
            'ppi': [],
            'm2': [],
            'pmi': [],
            'social_financing': []
        }
        
        for f in sorted(files):
            try:
                with open(f, 'r', encoding='utf-8') as fp:
                    content = json.load(fp)
                    outer_data = content.get('data', {})
                    data = outer_data.get('data', {})
                    
                    for key in all_data.keys():
                        if key in data:
                            all_data[key].extend(data[key])
            except Exception as e:
                self.logger.warning(f"Failed to load historical data from {f}: {e}")
        
        # 去重并排序
        result = {}
        for key, records in all_data.items():
            if key == 'gdp':
                result[key] = self._merge_data([], records, 'quarter')
            else:
                result[key] = self._merge_data([], records, 'month' if key != 'pmi' else 'MONTH')
        
        return result
    
    def _merge_data(self, existing: list, new: list, date_key: str) -> list:
        """合并历史数据和新数据，去除重复"""
        # 合并所有数据
        merged = {}
        
        # 添加现有数据
        for item in existing:
            if item and date_key in item:
                merged[item[date_key]] = item
        
        # 添加新数据
        for item in new:
            if item and date_key in item:
                merged[item[date_key]] = item
        
        # 转换为列表
        result = list(merged.values())
        
        if not result:
            return []
        
        # 根据日期键排序
        if 'Q' in str(result[0].get(date_key, '')):
            # 季度排序
            result.sort(key=lambda x: (x[date_key][:4], int(x[date_key][5])), reverse=True)
        else:
            # 月份排序
            result.sort(key=lambda x: x[date_key], reverse=True)
        
        return result
    
    def fetch(self, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """从TuShare获取宏观数据"""
        if not self.pro:
            raise ValueError("TuShare token not configured")
        
        data = {}
        
        # 获取历史数据进行合并
        historical_data = self._load_historical_data()
        
        # 1. GDP数据 - 季度
        try:
            start_qtr, end_qtr = self._calculate_date_range('gdp')
            self.logger.info(f"GDP query range: {start_qtr} to {end_qtr}")
            # GDP季度比较需要特殊处理
            def qtr_cmp(a, b):
                """比较季度: 2025Q4 > 2025Q3"""
                a_year, a_q = int(a[:4]), int(a[5])
                b_year, b_q = int(b[:4]), int(b[5])
                if a_year != b_year:
                    return a_year - b_year
                return a_q - b_q
            
            if qtr_cmp(start_qtr, end_qtr) <= 0:
                df = self.pro.cn_gdp(quarter=start_qtr)
                if df is not None and len(df) > 0:
                    new_gdp = df.to_dict('records')
                    # 合并历史和新数据，去重
                    existing = historical_data.get('gdp', [])
                    data['gdp'] = self._merge_data(existing, new_gdp, 'quarter')
                    self.logger.info(f"GDP data: {len(data['gdp'])} records (merged)")
            else:
                self.logger.info("GDP already up to date")
                data['gdp'] = historical_data.get('gdp', [])
        except Exception as e:
            self.logger.warning(f"GDP fetch failed: {e}")
        
        # 2. CPI数据 - 月度
        try:
            start_m, end_m = self._calculate_date_range('cpi')
            self.logger.info(f"CPI query range: {start_m} to {end_m}")
            if start_m <= end_m:
                df = self.pro.cn_cpi(start_m=start_m, end_m=end_m)
                if df is not None and len(df) > 0:
                    new_cpi = df.to_dict('records')
                    # 合并历史和新数据，去重
                    existing = historical_data.get('cpi', [])
                    data['cpi'] = self._merge_data(existing, new_cpi, 'month')
                    self.logger.info(f"CPI data: {len(data['cpi'])} records (merged)")
            else:
                self.logger.info("CPI already up to date")
                data['cpi'] = historical_data.get('cpi', [])
        except Exception as e:
            self.logger.warning(f"CPI fetch failed: {e}")
        
        # 3. PPI数据 - 月度
        try:
            start_m, end_m = self._calculate_date_range('ppi')
            self.logger.info(f"PPI query range: {start_m} to {end_m}")
            if start_m <= end_m:
                df = self.pro.cn_ppi(start_m=start_m, end_m=end_m)
                if df is not None and len(df) > 0:
                    new_ppi = df.to_dict('records')
                    existing = historical_data.get('ppi', [])
                    data['ppi'] = self._merge_data(existing, new_ppi, 'month')
                    self.logger.info(f"PPI data: {len(data['ppi'])} records (merged)")
            else:
                self.logger.info("PPI already up to date")
                data['ppi'] = historical_data.get('ppi', [])
        except Exception as e:
            self.logger.warning(f"PPI fetch failed: {e}")
        
        # 4. M0/M1/M2 货币供应 - 月度
        try:
            start_m, end_m = self._calculate_date_range('m2')
            self.logger.info(f"M2 query range: {start_m} to {end_m}")
            if start_m <= end_m:
                df = self.pro.cn_m(start_m=start_m, end_m=end_m)
                if df is not None and len(df) > 0:
                    new_m2 = df.to_dict('records')
                    existing = historical_data.get('m2', [])
                    data['m2'] = self._merge_data(existing, new_m2, 'month')
                    self.logger.info(f"M2 data: {len(data['m2'])} records (merged)")
            else:
                self.logger.info("M2 already up to date")
                data['m2'] = historical_data.get('m2', [])
        except Exception as e:
            self.logger.warning(f"M2 fetch failed: {e}")
        
        # 5. 社融数据 - 从最新开始动态获取
        try:
            start_m, end_m = self._calculate_date_range('cpi')  # 使用月度数据的日期范围
            # 社融数据通常是月度数据，从2024年1月开始查询到最新
            # TuShare的cn_trade接口可能只支持单月查询，我们获取历史数据后再合并
            df = self.pro.cn_trade(start_m='202401', end_m=end_m)
            if df is not None and len(df) > 0:
                new_sf = df.to_dict('records')
                existing = historical_data.get('social_financing', [])
                data['social_financing'] = self._merge_data(existing, new_sf, 'month')
                self.logger.info(f"Social financing: {len(data.get('social_financing', []))} records")
        except Exception as e:
            self.logger.warning(f"Social financing fetch failed: {e}")
        
        # 6. Shibor利率
        try:
            df = self.pro.shibor()
            if df is not None and len(df) > 0:
                data['shibor'] = df.head(30).to_dict('records')
                self.logger.info(f"Shibor data: {len(data['shibor'])} records")
        except Exception as e:
            self.logger.warning(f"Shibor fetch failed: {e}")
        
        # 7. LPR利率
        try:
            df = self.pro.shibor_lpr()
            if df is not None and len(df) > 0:
                data['lpr'] = df.head(30).to_dict('records')
                self.logger.info(f"LPR data: {len(data['lpr'])} records")
        except Exception as e:
            self.logger.warning(f"LPR fetch failed: {e}")
        
        # 8. PMI采购经理人指数 - 月度
        try:
            start_m, end_m = self._calculate_date_range('pmi')
            self.logger.info(f"PMI query range: {start_m} to {end_m}")
            if start_m <= end_m:
                df = self.pro.cn_pmi(start_m=start_m, end_m=end_m)
                if df is not None and len(df) > 0:
                    # 转换列为可读名称
                    pmi_map = {
                        'PMI010100': 'manufacturing_pmi',      # 制造业PMI
                        'PMI020100': 'non_manufacturing_pmi',  # 非制造业商务活动指数
                        'PMI010600': 'manufacturing_production', # 制造业生产指数
                        'PMI020400': 'non_manufacturing_activity', # 非制造业活动指数
                    }
                    df_renamed = df.rename(columns=pmi_map)
                    # 只保留需要的列
                    cols = ['MONTH'] + list(pmi_map.values())
                    available_cols = [c for c in cols if c in df_renamed.columns]
                    new_pmi = df_renamed[available_cols].to_dict('records')
                    existing = historical_data.get('pmi', [])
                    data['pmi'] = self._merge_data(existing, new_pmi, 'MONTH')
                    self.logger.info(f"PMI data: {len(data['pmi'])} records (merged)")
            else:
                self.logger.info("PMI already up to date")
                data['pmi'] = historical_data.get('pmi', [])
        except Exception as e:
            self.logger.warning(f"PMI fetch failed: {e}")
        
        return data
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """转换数据格式"""
        return {
            'date': datetime.now().strftime('%Y-%m'),
            'timestamp': datetime.now().isoformat(),
            'source': 'TuShare',
            'type': 'macro',
            'data': raw_data
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """验证数据"""
        if not data.get('data'):
            self.logger.error("No macro data collected")
            return False
        return True
    
    def get_data_prefix(self) -> str:
        return 'macro'


class StockCollector(BaseCollector):
    """A股行情数据采集器 - TuShare"""
    
    INDEX_CODES = {
        '000001.SH': '上证指数',
        '399001.SZ': '深证成指',
        '399006.SZ': '创业板指',
        '000300.SH': '沪深300',
        '000905.SH': '中证500',
        '000688.SH': '科创50',
        '000852.SH': '中证1000',
    }
    
    def __init__(self, config: Dict[str, Any] = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        import tushare as ts
        token = self.config.get('tushare_token', os.getenv('TUSHARE_TOKEN', ''))
        if token:
            self.pro = ts.pro_api(token)
        else:
            self.pro = None
    
    def fetch(self, start_date: str = None, end_date: str = None) -> Any:
        """从TuShare获取指数数据"""
        if not self.pro:
            raise ValueError("TuShare token not configured")
        
        from datetime import datetime, timedelta
        
        indices = []
        for ts_code, name in self.INDEX_CODES.items():
            try:
                df = self.pro.index_daily(
                    ts_code=ts_code,
                    start_date=(datetime.now() - timedelta(days=5)).strftime('%Y%m%d'),
                    end_date=datetime.now().strftime('%Y%m%d')
                )
                if df is not None and len(df) > 0:
                    latest = df.iloc[-1].to_dict()
                    latest['name'] = name
                    latest['ts_code'] = ts_code
                    indices.append(latest)
            except Exception as e:
                self.logger.warning(f"Failed to fetch {ts_code}: {e}")
        
        return indices
    
    def transform(self, raw_data) -> Dict[str, Any]:
        """转换数据格式"""
        date_str = datetime.now().strftime('%Y-%m-%d')
        
        indices = []
        for item in raw_data:
            try:
                code = item.get('ts_code', '').replace('.SH', '').replace('.SZ', '')
                index = {
                    'date': date_str,
                    'code': code,
                    'name': item.get('name', ''),
                    'open': item.get('open', 0),
                    'high': item.get('high', 0),
                    'low': item.get('low', 0),
                    'close': item.get('close', 0),
                    'pct_chg': item.get('pct_chg', 0),
                    'vol': item.get('vol', 0),
                    'amount': item.get('amount', 0),  # TuShare返回的就是千元
                }
                indices.append(index)
            except Exception as e:
                self.logger.warning(f"Failed to transform: {e}")
        
        return {
            'date': date_str,
            'timestamp': datetime.now().isoformat(),
            'source': 'TuShare',
            'type': 'stock',
            'indices': indices
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        return len(data.get('indices', [])) > 0
    
    def get_data_prefix(self) -> str:
        return 'stock'
        
        indices = []
        for item in raw_data:
            try:
                code = item.get('ts_code', '').replace('SH', '').replace('SZ', '')
                index = {
                    'date': date_str,
                    'code': code,
                    'name': item.get('name', ''),
                    'open': item.get('open', 0),
                    'high': item.get('close', 0),
                    'low': item.get('close', 0),
                    'close': item.get('close', 0),
                    'pct_chg': item.get('pct_chg', 0),
                    'vol': item.get('vol', 0),
                    'amount': item.get('amount', 0),
                }
                indices.append(index)
            except Exception as e:
                self.logger.warning(f"Failed to transform: {e}")
        
        return {
            'date': date_str,
            'timestamp': datetime.now().isoformat(),
            'source': 'Tencent Finance',
            'type': 'stock',
            'indices': indices
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        return len(data.get('indices', [])) > 0
    
    def get_data_prefix(self) -> str:
        return 'stock'


class FuturesCollector(BaseCollector):
    """期货数据采集器 - TuShare"""
    
    FUTURES_CODES = {
        'IF.CFX': '沪深300股指期货',
        'IC.CFX': '中证500股指期货',
        'IH.CFX': '上证50股指期货',
        'IM.CFX': '中证1000股指期货',
    }
    
    def __init__(self, config: Dict[str, Any] = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        token = self.config.get('tushare_token', os.getenv('TUSHARE_TOKEN', ''))
        if token:
            self.pro = ts.pro_api(token)
        else:
            self.pro = None
    
    def fetch(self, start_date: str = None, end_date: str = None) -> Any:
        """获取期货数据"""
        if not self.pro:
            raise ValueError("TuShare token not configured")
        
        data = {}
        
        # 期货合约列表
        try:
            df = self.pro.fut_basic()
            if df is not None and len(df) > 0:
                # 筛选金融期货
                df_cffex = df[df['exchange'] == 'CFFEX']
                data['fut_basic'] = df_cffex.head(50).to_dict('records')
                self.logger.info(f"Futures basic: {len(data['fut_basic'])} records")
        except Exception as e:
            self.logger.warning(f"Futures basic fetch failed: {e}")
        
        # 期货日线行情 - 获取主要期货
        futures_data = []
        for ts_code in self.FUTURES_CODES.keys():
            try:
                df = self.pro.fut_daily(
                    ts_code=ts_code,
                    start_date=(datetime.now() - timedelta(days=5)).strftime('%Y%m%d')
                )
                if df is not None and len(df) > 0:
                    futures_data.extend(df.to_dict('records'))
            except Exception as e:
                self.logger.warning(f"Failed to fetch {ts_code}: {e}")
        
        if futures_data:
            data['fut_daily'] = futures_data
            self.logger.info(f"Futures daily: {len(futures_data)} records")
        
        return data
    
    def transform(self, raw_data) -> Dict[str, Any]:
        return {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat(),
            'source': 'TuShare',
            'type': 'futures',
            'data': raw_data
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        return len(data.get('data', {})) > 0
    
    def get_data_prefix(self) -> str:
        return 'futures'


class GlobalMarketCollector(BaseCollector):
    """全球市场数据采集器 - TuShare港股/美股"""
    
    def __init__(self, config: Dict[str, Any] = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        token = self.config.get('tushare_token', os.getenv('TUSHARE_TOKEN', ''))
        if token:
            self.pro = ts.pro_api(token)
        else:
            self.pro = None
    
    def fetch(self, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """获取全球市场数据"""
        if not self.pro:
            raise ValueError("TuShare token not configured")
        
        data = {}
        
        # 港股 - 恒生指数
        try:
            df = self.pro.hk_daily(
                ts_code='00001.HK',  # 恒生指数
                start_date=(datetime.now() - timedelta(days=5)).strftime('%Y%m%d')
            )
            if df is not None and len(df) > 0:
                data['hk'] = df.tail(5).to_dict('records')
                self.logger.info(f"HK data: {len(data['hk'])} records")
        except Exception as e:
            self.logger.warning(f"HK fetch failed: {e}")
        
        # 港股列表
        try:
            df = self.pro.hk_basic()
            if df is not None and len(df) > 0:
                data['hk_basic'] = len(df)
        except Exception as e:
            self.logger.warning(f"HK basic fetch failed: {e}")
        
        # 美股列表
        try:
            df = self.pro.us_basic()
            if df is not None and len(df) > 0:
                data['us_basic'] = len(df)
        except Exception as e:
            self.logger.warning(f"US basic fetch failed: {e}")
        
        return data
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat(),
            'source': 'TuShare',
            'type': 'global_market',
            'data': raw_data
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        return len(data.get('data', {})) > 0
    
    def get_data_prefix(self) -> str:
        return 'global_market'


class FundFlowCollector(BaseCollector):
    """资金流数据采集器 - TuShare"""
    
    def __init__(self, config: Dict[str, Any] = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        token = self.config.get('tushare_token', os.getenv('TUSHARE_TOKEN', ''))
        if token:
            self.pro = ts.pro_api(token)
        else:
            self.pro = None
    
    def fetch(self, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """获取资金流数据"""
        if not self.pro:
            raise ValueError("TuShare token not configured")
        
        data = {}
        
        # 融资融券 - 汇总数据
        try:
            df = self.pro.margin()
            if df is not None and len(df) > 0:
                # 取最新交易日的汇总数据
                latest = df.head(5).to_dict('records')
                data['margin_summary'] = latest
                self.logger.info(f"Margin summary: {len(latest)} records")
        except Exception as e:
            self.logger.warning(f"Margin summary fetch failed: {e}")
        
        # 融资融券 - 个股明细
        try:
            df = self.pro.margin_detail()
            if df is not None and len(df) > 0:
                data['margin_detail'] = df.head(100).to_dict('records')
                self.logger.info(f"Margin detail: {len(data['margin_detail'])} records")
        except Exception as e:
            self.logger.warning(f"Margin detail fetch failed: {e}")
        
        # 北向资金 - 沪深港通资金流向
        try:
            df = self.pro.moneyflow_hsgt(
                start_date=(datetime.now() - timedelta(days=30)).strftime('%Y%m%d'),
                end_date=datetime.now().strftime('%Y%m%d')
            )
            if df is not None and len(df) > 0:
                data['north_south'] = df.to_dict('records')
                self.logger.info(f"North-south data: {len(data.get('north_south', []))} records")
        except Exception as e:
            self.logger.warning(f"North-south fetch failed: {e}")
        
        # 限售股解禁
        try:
            df = self.pro.share_float()
            if df is not None and len(df) > 0:
                data['share_float'] = df.head(100).to_dict('records')
                self.logger.info(f"Share float: {len(data.get('share_float', []))} records")
        except Exception as e:
            self.logger.warning(f"Share float fetch failed: {e}")
        
        return data
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat(),
            'source': 'TuShare',
            'type': 'fund_flow',
            'data': raw_data
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        return len(data.get('data', {})) > 0
    
    def get_data_prefix(self) -> str:
        return 'fund_flow'


class FundCollector(BaseCollector):
    """基金数据采集器 - TuShare"""
    
    def __init__(self, config: Dict[str, Any] = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        token = self.config.get('tushare_token', os.getenv('TUSHARE_TOKEN', ''))
        if token:
            self.pro = ts.pro_api(token)
        else:
            self.pro = None
    
    def fetch(self, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """获取基金数据"""
        if not self.pro:
            raise ValueError("TuShare token not configured")
        
        data = {}
        
        # 基金列表
        try:
            df = self.pro.fund_basic()
            if df is not None and len(df) > 0:
                data['fund_basic'] = len(df)
                data['sample'] = df.head(10).to_dict('records')
                self.logger.info(f"Fund basic: {data['fund_basic']} records")
        except Exception as e:
            self.logger.warning(f"Fund basic fetch failed: {e}")
        
        # 基金经理
        try:
            df = self.pro.fund_manager()
            if df is not None and len(df) > 0:
                data['fund_manager'] = len(df)
                self.logger.info(f"Fund manager: {data['fund_manager']} records")
        except Exception as e:
            self.logger.warning(f"Fund manager fetch failed: {e}")
        
        return data
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat(),
            'source': 'TuShare',
            'type': 'fund',
            'data': raw_data
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        return len(data.get('data', {})) > 0
    
    def get_data_prefix(self) -> str:
        return 'fund'


class BondCollector(BaseCollector):
    """可转债数据采集器 - TuShare"""
    
    def __init__(self, config: Dict[str, Any] = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        token = self.config.get('tushare_token', os.getenv('TUSHARE_TOKEN', ''))
        if token:
            self.pro = ts.pro_api(token)
        else:
            self.pro = None
    
    def fetch(self, start_date: str = None, end_date: str = None) -> Any:
        """获取可转债数据"""
        if not self.pro:
            raise ValueError("TuShare token not configured")
        
        df = self.pro.cb_basic()
        if df is not None and len(df) > 0:
            return df.to_dict('records')
        return []
    
    def transform(self, raw_data) -> Dict[str, Any]:
        return {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat(),
            'source': 'TuShare',
            'type': 'convertible_bond',
            'data': raw_data
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        return len(data.get('data', [])) > 0
    
    def get_data_prefix(self) -> str:
        return 'cb'


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
