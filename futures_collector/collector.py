#!/usr/bin/env python3
"""
期货数据采集器 - 使用 TuShare API
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta

# 添加项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import tushare as ts
from collectors.base import get_tushare_token


class FuturesCollector:
    """期货数据采集器 - 使用TuShare"""
    
    def __init__(self, config):
        self.config = config
        token = config.get('tushare_token', '')
        if token:
            self.pro = ts.pro_api(token)
        else:
            self.pro = None
        
        self.logger = logging.getLogger(__name__)
    
    def get_data_prefix(self):
        return 'futures'
    
    def transform(self, data):
        return data
    
    def fetch(self, start_date: str = None, end_date: str = None):
        """获取期货数据
        
        Args:
            start_date: 起始日期 (格式：YYYYMMDD)
            end_date: 结束日期 (格式：YYYYMMDD)
        """
        if not self.pro:
            raise ValueError("TuShare token not configured")
        
        data = {}
        
        # 如果没有提供日期，使用默认
        if not start_date:
            start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        
        today = end_date
        
        # 1. 获取期货基本信息（股指期货）
        try:
            df = self.pro.fut_basic(exchange='CFFEX')
            if df is not None and len(df) > 0:
                # 筛选股指期货
                index_fut = df[df['fut_code'].isin(['IF', 'IC', 'IH', 'IM'])]
                # 筛选未到期合约
                index_fut = index_fut[index_fut['delist_date'].fillna('20990101') >= today]
                data['fut_basic'] = index_fut.to_dict('records')
                self.logger.info(f"Futures basic: {len(data['fut_basic'])} records")
        except Exception as e:
            self.logger.warning(f"Futures basic fetch failed: {e}")
        
        # 2. 获取期货日线数据（近30天）- 采集所有合约
        try:
            start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
            df_daily = []
            
            # 股指期货合约列表
            # 当月(2603)，下月(2604)，下季(2606)，隔季(2609)
            contracts = [
                # 当月合约 (2603)
                'IF2603.CFX', 'IC2603.CFX', 'IH2603.CFX', 'IM2603.CFX',
                # 下月合约 (2604)
                'IF2604.CFX', 'IC2604.CFX', 'IH2604.CFX', 'IM2604.CFX',
                # 下季合约 (2606)
                'IF2606.CFX', 'IC2606.CFX', 'IH2606.CFX', 'IM2606.CFX',
                # 隔季合约 (2609)
                'IF2609.CFX', 'IC2609.CFX', 'IH2609.CFX', 'IM2609.CFX',
            ]
            
            for code in contracts:
                try:
                    df = self.pro.fut_daily(ts_code=code, start_date=start_date, end_date=today)
                    if df is not None and len(df) > 0:
                        df_daily.append(df)
                        self.logger.info(f"  {code}: {len(df)} records")
                except Exception as e:
                    self.logger.warning(f"Futures daily {code} fetch failed: {e}")
            
            if df_daily:
                import pandas as pd
                df_all = pd.concat(df_daily, ignore_index=True)
                data['fut_daily'] = df_all.to_dict('records')
                self.logger.info(f"Futures daily: {len(data['fut_daily'])} records")
        except Exception as e:
            self.logger.warning(f"Futures daily fetch failed: {e}")
        
        return {
            'date': today,
            'data': data
        }
    
    def run(self, data_dir='data', start_date: str = None, end_date: str = None):
        """运行采集器并保存数据
        
        Args:
            data_dir: 数据保存目录
            start_date: 起始日期 (格式：YYYYMMDD)
            end_date: 结束日期 (格式：YYYYMMDD)
        """
        import sqlite3
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        result = self.fetch(start_date=start_date, end_date=end_date)
        
        if result:
            # 1. 保存到JSON文件
            output_file = os.path.join(data_dir, f'futures_{today}.json')
            os.makedirs(data_dir, exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Data saved to {output_file}")
            
            # 2. 保存到数据库
            db_path = os.path.join(data_dir, 'market_data.db')
            if os.path.exists(db_path):
                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    
                    # 写入期货日线数据
                    fut_daily = result.get('data', {}).get('fut_daily', [])
                    for item in fut_daily:
                        try:
                            cursor.execute('''
                                INSERT OR REPLACE INTO futures_daily 
                                (ts_code, trade_date, open, high, low, close, vol, amount, oi)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                item.get('ts_code'),
                                item.get('trade_date'),
                                item.get('open'),
                                item.get('high'),
                                item.get('low'),
                                item.get('close'),
                                item.get('vol'),
                                item.get('amount'),
                                item.get('oi')
                            ))
                        except Exception as e:
                            self.logger.debug(f"Insert error: {e}")
                    
                    conn.commit()
                    conn.close()
                    self.logger.info(f"✓ Futures data written to DB: {len(fut_daily)} records")
                except Exception as e:
                    self.logger.warning(f"Failed to write futures data to DB: {e}")
        
        return result


def main():
    """测试采集器"""
    logging.basicConfig(level=logging.INFO)
    
    config = {'tushare_token': get_tushare_token()}
    collector = FuturesCollector(config)
    
    result = collector.fetch()
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))


if __name__ == '__main__':
    main()
