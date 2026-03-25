#!/usr/bin/env python3
"""
宏观策略分析师系统 - 主程序
"""
import os
import sys
import json
import logging
from datetime import datetime

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from collectors.macro_collector import (
    MacroCollector,
    StockCollector, 
    FuturesCollector,
    GlobalMarketCollector,
    FundFlowCollector,
    FundCollector,
    BondCollector,
)
from collectors.tushare_macro_collector import MoneySupplyCollector, SocialFinancingProCollector
from database.db_writer import DBWriter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def load_local_config():
    """从本地配置文件加载敏感配置"""
    config_path = os.path.join(project_root, 'config', 'local_config.json')
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def main():
    """主入口"""
    # 优先从环境变量读取，其次从本地配置文件读取
    local_config = load_local_config()
    config = {
        'tushare_token': os.getenv('TUSHARE_TOKEN', local_config.get('tushare_token', '')),
        'data_dir': 'data'
    }
    
    logger = logging.getLogger('main')
    logger.info(f"Starting data collection at {datetime.now()}")
    
    results = {}
    
    # 创建数据库写入器
    db_writer = DBWriter()
    
    # 1. A股行情 (TuShare)
    if config.get('tushare_token'):
        try:
            collector = StockCollector(config)
            result = collector.run()
            if result:
                results['stock'] = result
                # 写入数据库
                indices = result.get('indices', [])
                if indices:
                    count = db_writer.write_stock_indices(indices)
                    logger.info(f"✓ Stock: {len(indices)} indices, {count} written to DB")
                else:
                    logger.info(f"✓ Stock: {len(indices)} indices")
        except Exception as e:
            logger.error(f"✗ Stock collection failed: {e}")
        
        # 2. 宏观经济
        try:
            collector = MacroCollector(config)
            result = collector.run()
            if result:
                results['macro'] = result
                logger.info(f"✓ Macro: {list(result.get('data', {}).keys())}")
        except Exception as e:
            logger.error(f"✗ Macro collection failed: {e}")
        
        # 3. 资金流
        try:
            collector = FundFlowCollector(config)
            result = collector.run()
            if result:
                results['fund_flow'] = result
                logger.info(f"✓ Fund flow: {list(result.get('data', {}).keys())}")
        except Exception as e:
            logger.error(f"✗ Fund flow collection failed: {e}")
        
        # 4. 基金
        try:
            collector = FundCollector(config)
            result = collector.run()
            if result:
                results['fund'] = result
                logger.info(f"✓ Fund: {list(result.get('data', {}).keys())}")
        except Exception as e:
            logger.error(f"✗ Fund collection failed: {e}")
        
        # 5. 可转债
        try:
            collector = BondCollector(config)
            result = collector.run()
            if result:
                results['cb'] = result
                logger.info(f"✓ Convertible bond: {len(result.get('data', []))} records")
        except Exception as e:
            logger.error(f"✗ Bond collection failed: {e}")
        
        # 6. 全球市场
        try:
            collector = GlobalMarketCollector(config)
            result = collector.run()
            if result:
                results['global_market'] = result
                logger.info(f"✓ Global market: {list(result.get('data', {}).keys())}")
        except Exception as e:
            logger.error(f"✗ Global market collection failed: {e}")
        
        # 7. 期货数据
        try:
            from futures_collector.collector import FuturesCollector
            collector = FuturesCollector(config)
            result = collector.fetch()
            if result:
                results['futures'] = result
                logger.info(f"✓ Futures: {list(result.get('data', {}).keys())}")
        except Exception as e:
            logger.error(f"✗ Futures collection failed: {e}")
        
        # 8. 货币供应量 M0/M1/M2 (TuShare Pro)
        try:
            collector = MoneySupplyCollector(config)
            result = collector.run()
            if result:
                results['money_supply'] = result
                ms_list = result.get('data', {}).get('money_supply', [])
                if ms_list:
                    count = db_writer.write_money_supply(ms_list)
                    logger.info(f"✓ Money supply: {len(ms_list)} records, {count} written to DB")
                else:
                    logger.info(f"✓ Money supply: {len(ms_list)} records")
        except Exception as e:
            logger.error(f"✗ Money supply collection failed: {e}")
        
        # 9. 社会融资规模 (TuShare Pro)
        try:
            collector = SocialFinancingProCollector(config)
            result = collector.run()
            if result:
                results['social_financing'] = result
                sf_list = result.get('data', {}).get('social_financing', [])
                if sf_list:
                    count = db_writer.write_social_financing(sf_list)
                    logger.info(f"✓ Social financing: {len(sf_list)} records, {count} written to DB")
                else:
                    logger.info(f"✓ Social financing: {len(sf_list)} records")

        except Exception as e:
            logger.error(f"✗ Social financing collection failed: {e}")
        
        # 10. PMI采购经理人指数 (TuShare Pro)
        try:
            import tushare
            pro = tushare.pro_api(config.get('tushare_token', ''))
            df = pro.cn_pmi(start_m='202401', end_m='202503')
            
            if not df.empty:
                pmi_data = {
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'type': 'pmi',
                    'timestamp': datetime.now().isoformat(),
                    'source': 'TuShare Pro',
                    'data': df.to_dict('records')
                }
                results['pmi'] = pmi_data
                # 写入数据库
                pmi_list = df.to_dict('records')
                if pmi_list:
                    count = db_writer.write_pmi(pmi_list)
                    logger.info(f"✓ PMI: {len(df)} records, {count} written to DB")
                else:
                    logger.info(f"✓ PMI: {len(df)} records")
        except Exception as e:
            logger.error(f"✗ PMI collection failed: {e}")
    
    logger.info(f"Data collection completed. Results: {list(results.keys())}")
    return results


if __name__ == '__main__':
    main()
