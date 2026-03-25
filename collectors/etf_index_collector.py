#!/usr/bin/env python3
"""
ETF规模数据和指数成分股采集器 - TuShare版本
整合 ETF 规模数据 (etf_share_size) 和指数成分股数据 (index_weight)
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tushare as ts
from collectors.base import BaseCollector, get_tushare_token
from database.db_writer import DBWriter


class EtfIndexCollector(BaseCollector):
    """ETF规模数据和指数成分股采集器"""
    
    # 目标指数列表
    INDEX_LIST = [
        ('000300.SH', 'sh000300', '沪深300'),
        ('000905.SH', 'sh000905', '中证500'),
        ('000852.SH', 'sh000852', '中证1000'),
    ]
    
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
        
        self.db_writer = DBWriter()
    
    def get_latest_trade_date(self) -> str:
        """获取最新交易日"""
        if not self.pro:
            raise ValueError("TuShare token not configured")
        
        today = datetime.now()
        # 获取最近20天的交易日历
        start_date = (today - timedelta(days=30)).strftime('%Y%m%d')
        end_date = today.strftime('%Y%m%d')
        
        cal = self.pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
        cal = cal[cal['is_open'] == 1]
        
        if len(cal) > 0:
            return cal.iloc[-1]['cal_date']
        return today.strftime('%Y%m%d')
    
    def fetch_etf_scale(self, start_date: str = None, end_date: str = None) -> List[Dict]:
        """获取ETF规模数据
        
        Args:
            start_date: 起始日期 (格式：YYYYMMDD)
            end_date: 结束日期 (格式：YYYYMMDD)
        """
        if not self.pro:
            raise ValueError("TuShare token not configured")
        
        # 获取数据库最新日期
        conn = self.db_writer.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(trade_date) FROM etf_share_size")
        db_last_date = cursor.fetchone()[0]
        conn.close()
        
        # 计算采集日期范围：获取最近30天的数据
        today = datetime.now()
        start_date = (today - timedelta(days=30)).strftime('%Y%m%d')
        end_date = today.strftime('%Y%m%d')
        
        self.logger.info(f"ETF规模数据: 数据库最新 {db_last_date}, 采集范围 {start_date} ~ {end_date}")
        
        all_data = []
        actual_latest_date = db_last_date
        
        # 分别获取上海和深圳交易所的ETF
        for exchange in ['SSE', 'SZSE']:
            try:
                df = self.pro.etf_share_size(
                    exchange=exchange,
                    start_date=start_date,
                    end_date=end_date
                )
                if df is not None and len(df) > 0:
                    # 更新实际最新日期
                    actual_dates = df['trade_date'].unique()
                    if len(actual_dates) > 0:
                        actual_latest = max(actual_dates)
                        if not actual_latest_date or actual_latest > actual_latest_date:
                            actual_latest_date = actual_latest
                    
                    # 转换列名
                    df = df.rename(columns={
                        'ts_code': 'ts_code',
                        'etf_name': 'etf_name',
                        'total_share': 'total_share',
                        'total_size': 'total_size',
                        'nav': 'nav',
                        'close': 'close',
                        'exchange': 'exchange'
                    })
                    df['trade_date'] = df['trade_date'].astype(str)
                    all_data.append(df)
                    self.logger.info(f"  {exchange}: 获取 {len(df)} 条")
            except Exception as e:
                self.logger.warning(f"获取 {exchange} ETF失败: {e}")
        
        # 检查是否需要更新：如果实际最新日期大于数据库最新日期
        if db_last_date and actual_latest_date and actual_latest_date <= db_last_date:
            self.logger.info(f"ETF规模数据已是最新 ({db_last_date})")
            return []
        
        if all_data:
            import pandas as pd
            return pd.concat(all_data, ignore_index=True).to_dict('records')
        return []
    
    def fetch_index_weights(self, start_date: str = None, end_date: str = None) -> Dict[str, List[Dict]]:
        """获取指数成分股数据
        
        Returns:
            {index_code: [records]}
        """
        if not self.pro:
            raise ValueError("TuShare token not configured")
        
        if not end_date:
            end_date = self.get_latest_trade_date()
        
        results = {}
        
        for ts_code, db_code, name in self.INDEX_LIST:
            try:
                # 获取最新成分股
                df = self.pro.index_weight(index_code=ts_code)
                
                if df is None or len(df) == 0:
                    continue
                
                # 获取最新日期
                latest_date = df['trade_date'].max()
                
                # 检查数据库是否已是最新
                conn = self.db_writer.get_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT MAX(trade_date) FROM index_weights WHERE index_code = ?",
                    (db_code,)
                )
                last_date = cursor.fetchone()[0]
                conn.close()
                
                if latest_date <= (last_date or '0'):
                    self.logger.info(f"指数 {name} 已是最新 ({latest_date})")
                    continue
                
                # 筛选最新日期的成分股
                df_latest = df[df['trade_date'] == latest_date]
                
                records = []
                for _, row in df_latest.iterrows():
                    records.append({
                        'index_code': db_code,
                        'index_name': name,
                        'trade_date': str(latest_date),
                        'stock_code': row['con_code'],
                        'stock_name': row.get('con_name', ''),
                        'weight': row['weight']
                    })
                
                results[db_code] = records
                self.logger.info(f"  {name}: {len(records)} 只成分股, 日期 {latest_date}")
                
            except Exception as e:
                self.logger.warning(f"获取 {name} 成分股失败: {e}")
        
        return results
    
    def fetch(self, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """获取所有数据
        
        Args:
            start_date: 起始日期 (格式：YYYYMMDD)
            end_date: 结束日期 (格式：YYYYMMDD)
        """
        etf_data = self.fetch_etf_scale(start_date, end_date)
        index_weights = self.fetch_index_weights(start_date, end_date)
        
        return {
            'etf_scale': etf_data,
            'index_weights': index_weights
        }
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """转换数据格式"""
        date_str = datetime.now().strftime('%Y-%m-%d')
        
        return {
            'date': date_str,
            'timestamp': datetime.now().isoformat(),
            'source': 'TuShare',
            'type': 'etf_index',
            'etf_scale': raw_data.get('etf_scale', []),
            'index_weights': raw_data.get('index_weights', {})
        }
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """验证数据"""
        # ETF 规模数据可以为空（已是最新）
        # 指数成分股数据也可以为空（已是最新）
        return True
    
    def save_to_db(self, data: Dict[str, Any]) -> Dict[str, int]:
        """保存数据到数据库
        
        Returns:
            {'etf_scale': count, 'index_weights': count}
        """
        results = {'etf_scale': 0, 'index_weights': 0}
        
        # 保存 ETF 规模数据
        etf_list = data.get('etf_scale', [])
        if etf_list:
            conn = self.db_writer.get_connection()
            cursor = conn.cursor()
            
            for etf in etf_list:
                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO etf_share_size 
                        (trade_date, ts_code, etf_name, total_share, total_size, nav, close, exchange)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        etf.get('trade_date'),
                        etf.get('ts_code'),
                        etf.get('etf_name'),
                        etf.get('total_share'),
                        etf.get('total_size'),
                        etf.get('nav'),
                        etf.get('close'),
                        etf.get('exchange')
                    ))
                    results['etf_scale'] += 1
                except Exception as e:
                    self.logger.debug(f"ETF写入失败: {e}")
            
            conn.commit()
            conn.close()
            self.logger.info(f"ETF规模数据写入 {results['etf_scale']} 条")
        
        # 保存指数成分股数据
        index_weights = data.get('index_weights', {})
        if index_weights:
            conn = self.db_writer.get_connection()
            cursor = conn.cursor()
            
            for index_code, records in index_weights.items():
                for rec in records:
                    try:
                        cursor.execute('''
                            INSERT OR REPLACE INTO index_weights 
                            (index_code, index_name, trade_date, stock_code, stock_name, weight)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (
                            rec.get('index_code'),
                            rec.get('index_name'),
                            rec.get('trade_date'),
                            rec.get('stock_code'),
                            rec.get('stock_name'),
                            rec.get('weight')
                        ))
                        results['index_weights'] += 1
                    except Exception as e:
                        self.logger.debug(f"指数成分股写入失败: {e}")
            
            conn.commit()
            conn.close()
            self.logger.info(f"指数成分股写入 {results['index_weights']} 条")
        
        return results
    
    def get_data_prefix(self) -> str:
        return 'etf_index'
    
    def run(self, date: str = None, save: bool = True, start_date: str = None, end_date: str = None) -> Optional[Dict[str, Any]]:
        """执行采集流程
        
        Args:
            date: 日期字符串 (单个日期)
            save: 是否保存到文件，默认True
            start_date: 起始日期 (格式：YYYYMMDD)
            end_date: 结束日期 (格式：YYYYMMDD)
        """
        try:
            self.logger.info(f"Starting ETF and Index collection")
            
            # 1. 获取原始数据
            raw = self.fetch(start_date=start_date, end_date=end_date)
            
            # 2. 转换格式
            data = self.transform(raw)
            
            # 3. 验证数据
            if not self.validate(data):
                self.logger.error("Data validation failed")
                return None
            
            # 4. 保存到数据库（优先）
            db_results = self.save_to_db(data)
            
            # 5. 保存到文件（可选，遵循原则：同时保存JSON和数据库）
            if save:
                prefix = self.get_data_prefix()
                filepath = self.save(data, prefix, date)
                self.logger.info(f"Data saved to {filepath}")
            
            # 添加数据库写入统计
            data['db_results'] = db_results
            
            return data
            
        except Exception as e:
            self.logger.error(f"Collection failed: {e}")
            raise


def main():
    """测试采集器"""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # 获取 token
    token = get_tushare_token()
    if not token:
        print("请设置 TUSHARE_TOKEN 环境变量或配置 local_config.json")
        return
    
    config = {'tushare_token': token}
    collector = EtfIndexCollector(config)
    
    # 执行采集
    result = collector.run()
    
    if result:
        print(f"\n采集完成!")
        print(f"  ETF规模: {len(result.get('etf_scale', []))} 条")
        index_weights = result.get('index_weights', {})
        for code, items in index_weights.items():
            print(f"  {code}: {len(items)} 只成分股")


if __name__ == '__main__':
    main()
