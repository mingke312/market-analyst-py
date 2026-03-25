#!/usr/bin/env python3
"""
数据采集主程序
统一管理所有数据的采集逻辑：
1. 读取数据库最新日期
2. 计算需要采集的时间范围
3. 调用各数据采集函数

使用方法:
    python collect_data.py -t stock          # 只采集股票指数
    python collect_data.py -t stock,macro  # 采集股票+宏观
    python collect_data.py -t news          # 只采集新闻
    python collect_data.py -t all          # 采集所有类型（包括新闻）
"""
import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_db_latest_date(table_name: str, date_column: str = 'trade_date') -> Optional[str]:
    """获取数据库中某类数据的最新日期"""
    import sqlite3
    
    db_path = os.path.join(project_root, 'data', 'market_data.db')
    if not os.path.exists(db_path):
        return None
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX({date_column}) FROM {table_name}")
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0]:
            return result[0]
    except Exception as e:
        logger.warning(f"Failed to get latest date from {table_name}: {e}")
    
    return None


def calculate_date_range(table_name: str, date_column: str = None, days_back: int = 365) -> Tuple[str, str]:
    """
    计算数据采集的时间范围
    逻辑：读取数据库最新日期，与今天比较，采集中间缺失的数据
    
    Args:
        table_name: 数据库表名
        date_column: 日期列名 (如果为None，使用默认列名)
        days_back: 如果没有历史数据，默认往前取多少天
    
    Returns:
        (start_date, end_date) 格式：YYYYMMDD 或 YYYYMM
    """
    # 默认日期列映射
    default_columns = {
        'stock_indices': 'trade_date',
        'money_supply': 'month',
        'macro_indicators': 'month',
        'cpi': 'month',
        'ppi': 'month',
        'gdp': 'quarter',
        'pmi': 'month',
        'convertible_bonds': 'trade_date',
        'fund_data': 'trade_date',
        'real_estate': 'month',
    }
    
    # 日期格式映射
    date_formats = {
        'trade_date': '%Y%m%d',
        'month': '%Y%m',
        'quarter': '%YQ%m',
    }
    
    if date_column is None:
        date_column = default_columns.get(table_name, 'trade_date')
    
    # 判断是日期还是月度
    is_monthly = date_column in ['month', 'quarter']
    today_str = datetime.now().strftime('%Y%m') if is_monthly else datetime.now().strftime('%Y%m%d')
    today = datetime.now()
    
    # 尝试获取数据库最新日期
    latest_date = get_db_latest_date(table_name, date_column)
    
    if latest_date:
        # 将数据库日期转换为datetime进行比较
        # 先去掉横杠等分隔符
        try:
            clean_date = str(latest_date).replace('-', '').replace('/', '').replace('.', '')
            fmt = date_formats.get(date_column, '%Y-%m-%d')
            latest_dt = datetime.strptime(clean_date[:8], '%Y%m%d') if not is_monthly else datetime.strptime(clean_date[:6], '%Y%m')
            
            # 计算需要补充的天数/月数
            if is_monthly:
                months_diff = (today.year - latest_dt.year) * 12 + today.month - latest_dt.month
                if months_diff <= 1:
                    start_date = today_str
                else:
                    # 上个月的今天
                    start_month = latest_dt.month + 1
                    start_year = latest_dt.year
                    if start_month > 12:
                        start_month = 1
                        start_year += 1
                    start_date = f"{start_year}{start_month:02d}"
            else:
                days_diff = (today - latest_dt).days
                if days_diff <= 1:
                    start_date = today_str
                else:
                    start_date = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
            
            end_date = today_str
            
        except Exception as e:
            logger.warning(f"Failed to parse latest date {latest_date}: {e}")
            if is_monthly:
                start_date = (today - timedelta(days=days_back)).strftime('%Y%m')
            else:
                start_date = (today - timedelta(days=days_back)).strftime('%Y%m%d')
            end_date = today_str
    else:
        # 没有历史数据，默认采集过去一段时间
        if is_monthly:
            start_date = (today - timedelta(days=days_back)).strftime('%Y%m')
        else:
            start_date = (today - timedelta(days=days_back)).strftime('%Y%m%d')
        end_date = today_str
    
    logger.info(f"{table_name}:采集范围 {start_date} ~ {end_date} (最新:{latest_date})")
    return start_date, end_date


def collect_stock_data(start_date: str, end_date: str) -> Dict:
    """采集股票指数数据"""
    from collectors.stock_collector import StockCollector
    from database.db_writer import DBWriter
    
    config = load_config()
    if not config.get('tushare_token'):
        logger.error("TuShare token not configured")
        return {'status': 'error', 'message': 'TuShare token not configured'}
    
    try:
        collector = StockCollector(config)
        result = collector.run(start_date=start_date, end_date=end_date)
        
        if result:
            indices = result.get('indices', [])
            if indices:
                db = DBWriter()
                count = db.write_stock_indices(indices)
                logger.info(f"✓ Stock: {len(indices)} indices, {count} written to DB")
                return {'status': 'ok', 'count': count}
        
        return {'status': 'ok', 'count': 0}
    except Exception as e:
        logger.error(f"✗ Stock collection failed: {e}")
        return {'status': 'error', 'message': str(e)}


def collect_macro_data(start_date: str, end_date: str) -> Dict:
    """采集宏观经济数据"""
    from collectors.macro_collector import MacroCollector
    from database.db_writer import DBWriter
    
    config = load_config()
    if not config.get('tushare_token'):
        logger.error("TuShare token not configured")
        return {'status': 'error', 'message': 'TuShare token not configured'}
    
    try:
        collector = MacroCollector(config)
        result = collector.run(start_date=start_date, end_date=end_date)
        
        if result:
            db = DBWriter()
            # 写入各种宏观数据
            logger.info(f"✓ Macro data collected")
            return {'status': 'ok'}
        
        return {'status': 'ok'}
    except Exception as e:
        logger.error(f"✗ Macro collection failed: {e}")
        return {'status': 'error', 'message': str(e)}


def collect_fund_flow_data(start_date: str, end_date: str) -> Dict:
    """采集资金流向数据"""
    from collectors.fund_flow_collector import FundFlowCollector
    from database.db_writer import DBWriter
    
    config = load_config()
    if not config.get('tushare_token'):
        logger.error("TuShare token not configured")
        return {'status': 'error', 'message': 'TuShare token not configured'}
    
    try:
        collector = FundFlowCollector(config)
        result = collector.run(start_date=start_date, end_date=end_date)
        
        if result:
            db = DBWriter()
            logger.info(f"✓ Fund flow data collected")
            return {'status': 'ok'}
        
        return {'status': 'ok'}
    except Exception as e:
        logger.error(f"✗ Fund flow collection failed: {e}")
        return {'status': 'error', 'message': str(e)}


def collect_fund_data(start_date: str, end_date: str) -> Dict:
    """采集基金数据 - 暂时跳过，模块不存在"""
    logger.info("基金数据采集跳过 (FundCollector模块不存在)")
    return {'status': 'ok', 'message': 'skipped'}


def collect_bond_data(start_date: str, end_date: str) -> Dict:
    """采集可转债数据 - 暂时跳过，模块不存在"""
    logger.info("可转债数据采集跳过 (BondCollector模块不存在)")
    return {'status': 'ok', 'message': 'skipped'}

def collect_global_market_data(start_date: str, end_date: str) -> Dict:
    """采集全球市场数据 - 暂时跳过，模块不存在"""
    logger.info("全球市场数据采集跳过 (GlobalMarketCollector模块不存在)")
    return {'status': 'ok', 'message': 'skipped'}


def collect_futures_data(start_date: str, end_date: str) -> Dict:
    """采集期货数据"""
    from futures_collector.collector import FuturesCollector
    from database.db_writer import DBWriter
    
    config = load_config()
    if not config.get('tushare_token'):
        logger.error("TuShare token not configured")
        return {'status': 'error', 'message': 'TuShare token not configured'}
    
    try:
        collector = FuturesCollector(config)
        result = collector.run(start_date=start_date, end_date=end_date)
        
        if result:
            logger.info(f"✓ Futures data collected")
            return {'status': 'ok'}
        
        return {'status': 'ok'}
    except Exception as e:
        logger.error(f"✗ Futures collection failed: {e}")
        return {'status': 'error', 'message': str(e)}


def collect_etf_index_data(start_date: str = None, end_date: str = None) -> Dict:
    """采集ETF规模数据和指数成分股"""
    from collectors.etf_index_collector import EtfIndexCollector
    
    config = load_config()
    if not config.get('tushare_token'):
        logger.error("TuShare token not configured")
        return {'status': 'error', 'message': 'TuShare token not configured'}
    
    try:
        collector = EtfIndexCollector(config)
        result = collector.run(start_date=start_date, end_date=end_date)
        
        if result:
            db_results = result.get('db_results', {})
            etf_count = db_results.get('etf_scale', 0)
            index_count = db_results.get('index_weights', 0)
            logger.info(f"✓ ETF规模: {etf_count} 条, 指数成分股: {index_count} 条")
            return {'status': 'ok', 'etf_scale': etf_count, 'index_weights': index_count}
        
        return {'status': 'ok', 'message': '数据已是最新'}
    except Exception as e:
        logger.error(f"✗ ETF/Index collection failed: {e}")
        return {'status': 'error', 'message': str(e)}


def collect_news_data() -> Dict:
    """采集新闻数据并写入数据库"""
    import sqlite3
    
    try:
        # 1. 采集新闻数据（不保存到文件）
        from collectors.news_collector import NewsCollector
        collector = NewsCollector()
        result = collector.run(save=False)
        
        if not result:
            logger.warning("No news data collected")
            return {'status': 'warning', 'message': 'No news data collected'}
        
        # 2. 直接从结果中获取数据写入数据库
        news_date = datetime.now().strftime('%Y-%m-%d')
        news_list = result.get('data', {}).get('news', [])
        
        if not news_list:
            logger.warning("No news items in result")
            return {'status': 'warning', 'message': 'No news items'}
        
        # 写入数据库
        db_path = os.path.join(project_root, 'data', 'market_data.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        inserted = 0
        
        for news in news_list:
            try:
                cursor.execute(
                    'INSERT OR IGNORE INTO news (date, title, summary, source, url) VALUES (?, ?, ?, ?, ?)',
                    (news_date, news.get('title', ''), '', news.get('source', ''), news.get('url', ''))
                )
                if cursor.rowcount > 0:
                    inserted += 1
            except Exception as e:
                logger.debug(f"Insert error: {e}")
        
        conn.commit()
        conn.close()
        
        logger.info(f"✓ News: {len(news_list)} collected, {inserted} written to DB")
        return {'status': 'ok', 'collected': len(news_list), 'inserted': inserted}
        
    except Exception as e:
        logger.error(f"✗ News collection failed: {e}")
        return {'status': 'error', 'message': str(e)}


def load_config():
    """加载配置"""
    import json
    config_path = os.path.join(project_root, 'config', 'local_config.json')
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    # 尝试从环境变量
    token = os.environ.get('TUSHARE_TOKEN', '')
    if token:
        return {'tushare_token': token}
    return {}


def main():
    """主入口 - 采集所有缺失的数据"""
    import argparse
    import json
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='数据采集主程序')
    parser.add_argument('-t', '--types', type=str, default='all',
                        help='要采集的数据类型，用逗号分隔。可选值: stock, macro, fund_flow, futures, money_supply, etf_index, all。默认: all')
    args = parser.parse_args()
    
    # 解析数据类型
    if args.types.lower() == 'all':
        target_types = ['stock', 'macro', 'fund_flow', 'futures', 'money_supply', 'etf_index', 'news']
    else:
        target_types = [t.strip() for t in args.types.split(',')]
    
    logger.info("=" * 50)
    logger.info(f"开始数据采集 (类型: {', '.join(target_types)})")
    logger.info("=" * 50)
    
    idx = 0
    
    # 1. 股票指数数据
    if 'stock' in target_types:
        idx += 1
        logger.info(f"\n[{idx}/{len(target_types)}] 股票指数数据...")
        start_date, end_date = calculate_date_range('stock_indices', 'trade_date', days_back=30)
        result = collect_stock_data(start_date, end_date)
    
    # 2. 宏观经济数据
    if 'macro' in target_types:
        idx += 1
        logger.info(f"\n[{idx}/{len(target_types)}] 宏观经济数据...")
        start_date, end_date = calculate_date_range('macro_indicators', 'month', days_back=24)
        result = collect_macro_data(start_date, end_date)
    
    # 3. 资金流向数据
    if 'fund_flow' in target_types:
        idx += 1
        logger.info(f"\n[{idx}/{len(target_types)}] 资金流向数据...")
        start_date, end_date = calculate_date_range('fund_flow', 'trade_date', days_back=30)
        result = collect_fund_flow_data(start_date, end_date)
    
    # 4. 期货数据
    if 'futures' in target_types:
        idx += 1
        logger.info(f"\n[{idx}/{len(target_types)}] 期货数据...")
        start_date, end_date = calculate_date_range('futures_daily', 'trade_date', days_back=30)
        result = collect_futures_data(start_date, end_date)
    
    # 5. 货币供应量数据
    if 'money_supply' in target_types:
        idx += 1
        logger.info(f"\n[{idx}/{len(target_types)}] 货币供应量数据...")
        from collectors.tushare_macro_collector import MoneySupplyCollector
        config = load_config()
        try:
            collector = MoneySupplyCollector(config)
            start_date, end_date = calculate_date_range('money_supply', 'month', days_back=24)
            result = collector.run(start_date=start_date, end_date=end_date)
            logger.info(f"✓ Money supply data collected")
        except Exception as e:
            logger.error(f"✗ Money supply collection failed: {e}")
    
    # 6. ETF规模数据和指数成分股
    if 'etf_index' in target_types:
        idx += 1
        logger.info(f"\n[{idx}/{len(target_types)}] ETF规模数据和指数成分股...")
        result = collect_etf_index_data()
    
    # 7. 新闻数据
    if 'news' in target_types:
        idx += 1
        logger.info(f"\n[{idx}/{len(target_types)}] 新闻数据...")
        result = collect_news_data()
    
    logger.info("\n" + "=" * 50)
    logger.info("数据采集完成")
    logger.info("=" * 50)


if __name__ == '__main__':
    main()
