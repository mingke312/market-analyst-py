#!/usr/bin/env python3
"""
每日A股报告生成系统 - 统一入口
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 数据库模块
try:
    from database.db_writer import DBWriter
    from database.db_writer import DBReader
    HAS_DB = True
except ImportError:
    HAS_DB = False
    DBReader = None
    logger.warning("数据库模块未安装，将使用 JSON 文件存储")


def get_spot_price(code: str) -> float:
    """从数据库获取现货价格"""
    if not HAS_DB or DBReader is None:
        return None
    try:
        db = DBReader()
        conn = db.get_connection()
        cursor = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 将 code 转换为 ts_code 格式
        ts_code_map = {
            'sh000300': '000300', 'sh000905': '000905', 
            'sh000016': '000016', 'sh000852': '000852'
        }
        db_code = ts_code_map.get(code, code.replace('sh', ''))
        
        cursor.execute('''
            SELECT close FROM stock_indices 
            WHERE ts_code = ? AND trade_date = ?
        ''', (db_code, today))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return float(result[0])
    except Exception as e:
        logger.warning(f"从数据库获取现货价格失败 {code}: {e}")
    return None


def get_futures_price(contract: str) -> float:
    """从数据库/JSON文件获取期货价格"""
    try:
        import json
        from datetime import datetime
        
        today = datetime.now().strftime('%Y-%m-%d')
        futures_file = project_root / 'data' / f'futures_{today}.json'
        
        # 尝试从JSON文件读取
        if futures_file.exists():
            with open(futures_file, 'r') as f:
                data = json.load(f)
            
            futures_list = data.get('data', {}).get('fut_daily', [])
            
            # 期货代码映射
            futures_map = {
                'IF2609': 'IF2609', 'IC2609': 'IC2609', 
                'IH2609': 'IH2609', 'IM2609': 'IM2609'
            }
            futures_code = futures_map.get(contract, contract[:2])
            
            for item in futures_list:
                ts_code = item.get('ts_code', '')
                if futures_code in ts_code:
                    return float(item.get('close', 0))
    except Exception as e:
        logger.warning(f"从JSON获取期货价格失败 {contract}: {e}")
    
    # 备选：从数据库读取（如果期货表存在）
    if not HAS_DB or DBReader is None:
        return None
    try:
        import sqlite3
        db_path = project_root / 'data' / 'market_data.db'
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        today_str = datetime.now().strftime('%Y%m%d')
        
        # 期货代码映射
        futures_map = {
            'IF2609': 'IF2609', 'IC2609': 'IC2609', 
            'IH2609': 'IH2609', 'IM2609': 'IM2609'
        }
        futures_code = futures_map.get(contract, contract)
        
        cursor.execute('''
            SELECT close FROM futures_daily 
            WHERE ts_code LIKE ? AND trade_date = ?
        ''', (f'%{futures_code}%', today_str))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return float(result[0])
    except Exception as e:
        logger.warning(f"从数据库获取期货价格失败 {contract}: {e}")
    return None


def get_volume_data_from_db(days: int = 22) -> dict:
    """从数据库获取历史成交额数据"""
    if not HAS_DB or DBReader is None:
        return None
    try:
        import sqlite3
        db_path = project_root / 'data' / 'market_data.db'
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 获取过去N天的数据
        cursor.execute('''
            SELECT trade_date, name, amount/100000 AS amount_yi 
            FROM stock_indices 
            WHERE name IN ('上证指数', '深证成指')
            AND trade_date >= date('now', '-35 days')
            ORDER BY trade_date
        ''')
        
        data = cursor.fetchall()
        conn.close()
        
        # 整理数据
        sse_amounts = {}
        szse_amounts = {}
        dates = set()
        
        for row in data:
            trade_date, name, amount = row
            if name == '上证指数':
                sse_amounts[trade_date] = amount
            elif name == '深证成指':
                szse_amounts[trade_date] = amount
            dates.add(trade_date)
        
        # 合并计算总成交额
        trading_dates = sorted(dates, reverse=True)[:days]
        
        volume_data = {
            'dates': [d.replace('-', '') for d in trading_dates],
            'daily_amounts': [sse_amounts.get(d, 0) + szse_amounts.get(d, 0) for d in trading_dates]
        }
        
        return volume_data
    except Exception as e:
        logger.warning(f"从数据库获取成交额数据失败: {e}")
    return None


def get_pmi_from_db() -> list:
    """从数据库获取PMI数据"""
    if not HAS_DB or DBReader is None:
        return []
    try:
        import sqlite3
        db_path = project_root / 'data' / 'market_data.db'
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT month, manufacturing_pmi, non_manufacturing_pmi 
            FROM pmi 
            ORDER BY month DESC 
            LIMIT 12
        ''')
        
        results = cursor.fetchall()
        conn.close()
        return results
    except Exception as e:
        logger.warning(f"从数据库获取PMI数据失败: {e}")
    return []


def generate_volume_chart(volume_data, output_path=None):
    """生成成交额折线图"""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    
    if not volume_data or not volume_data.get('dates'):
        return None
    
    dates = volume_data.get('dates', [])
    amounts = volume_data.get('daily_amounts', [])
    
    if not dates or not amounts:
        return None
    
    # 格式化日期
    labels = [f"{d[4:6]}/{d[6:]}" for d in dates]
    
    plt.figure(figsize=(10, 5))
    plt.plot(range(len(amounts)), amounts, 'b-o', linewidth=2, markersize=6)
    plt.fill_between(range(len(amounts)), amounts, alpha=0.3)
    
    # 数值标签
    for i, v in enumerate(amounts):
        plt.annotate(f'{v:.0f}', (i, v + 200), ha='center', fontsize=8)
    
    plt.xlabel('Date', fontsize=11)
    plt.ylabel('Amount (亿元)', fontsize=11)
    plt.title('A-Share Market Daily Volume (Shanghai + Shenzhen)', fontsize=12, fontweight='bold')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.xticks(range(len(labels)), labels, rotation=45, ha='right')
    plt.tight_layout()
    
    if output_path is None:
        output_path = project_root / 'data' / f'volume_chart_{datetime.now().strftime("%Y%m%d")}.png'
    
    plt.savefig(output_path, dpi=120)
    plt.close()
    
    return str(output_path)


def load_config():
    """加载配置"""
    config_path = project_root / 'config' / 'local_config.json'
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {}


def main():
    """主入口"""
    logger.info("=" * 50)
    logger.info("开始生成每日A股报告")
    logger.info("=" * 50)
    
    # 加载配置
    config = load_config()
    if not config.get('tushare_token'):
        logger.error("未配置TuShare Token!")
        return
    
    from analyzer.analyzer import Analyzer
    from reporter.reporter import Reporter
    
    # ========== 从数据库读取数据 ==========
    logger.info("\n[1/3] 从数据库读取数据...")
    
    # 1. 读取股票指数数据
    try:
        stock_result = None
        import sqlite3
        db_path = project_root / 'data' / 'market_data.db'
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 读取今日指数数据
        cursor.execute('''
            SELECT ts_code, name, close, pct_chg, volume, amount 
            FROM stock_indices 
            WHERE trade_date = ?
            ORDER BY amount DESC
        ''', (today,))
        
        rows = cursor.fetchall()
        if rows:
            indices = []
            for row in rows:
                indices.append({
                    'code': row[0],
                    'name': row[1],
                    'close': row[2],
                    'pct_chg': row[3],
                    'volume': row[4],
                    'amount': row[5]
                })
            stock_result = {'data': {'indices': indices}}
            logger.info(f"✓ 从数据库读取股票指数: {len(indices)} 条")
        else:
            logger.warning("数据库中没有今日股票数据，请先运行数据采集任务")
        
        conn.close()
    except Exception as e:
        logger.error(f"✗ 读取股票数据失败: {e}")
        stock_result = None
    
    # 2. 读取期货数据
    try:
        futures = []
        import sqlite3
        db_path = project_root / 'data' / 'market_data.db'
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        today_str = datetime.now().strftime('%Y%m%d')
        
        contracts = [
            ('IF', '沪深300', 'sh000300', 'IF2609'),
            ('IC', '中证500', 'sh000905', 'IC2609'),
            ('IH', '上证50', 'sh000016', 'IH2609'),
            ('IM', '中证1000', 'sh000852', 'IM2609'),
        ]
        
        spot_prices = {}
        index_map = {'sh000300': '000300', 'sh000905': '000905', 'sh000016': '000016', 'sh000852': '000852'}
        
        # 读取现货价格
        for sh_code, db_code in index_map.items():
            cursor.execute('SELECT close FROM stock_indices WHERE ts_code = ? AND trade_date = ?', (db_code, today))
            row = cursor.fetchone()
            if row:
                spot_prices[sh_code] = row[0]
        
        # 读取期货价格
        for code, name, spot_code, contract in contracts:
            futures_code = contract  # 使用隔季合约代码，如 'IF2609'
            cursor.execute('SELECT close FROM futures_daily WHERE ts_code LIKE ? AND trade_date = ?', (f'%{futures_code}%', today_str))
            row = cursor.fetchone()
            if row:
                futures.append({
                    'code': code,
                    'name': name,
                    'contract': contract,
                    'close': row[0],
                    'spot_price': spot_prices.get(spot_code)
                })
        
        conn.close()
        logger.info(f"✓ 从数据库读取期货数据: {len(futures)} 条")
    except Exception as e:
        logger.warning(f"读取期货数据失败: {e}")
        futures = []
    
    # 3. 读取历史成交额数据
    try:
        volume_data = get_volume_data_from_db(days=22)
        if volume_data and volume_data.get('dates'):
            logger.info(f"✓ 从数据库读取历史成交额: {len(volume_data['dates'])} 天")
        else:
            logger.warning("无法从数据库获取历史成交额数据")
            volume_data = {'dates': [], 'daily_amounts': []}
    except Exception as e:
        logger.warning(f"读取历史成交额失败: {e}")
        volume_data = {'dates': [], 'daily_amounts': []}
    
    # 4. 新闻数据 - 从数据库读取
    try:
        import sqlite3
        conn = sqlite3.connect(str(project_root / 'data' / 'market_data.db'))
        cursor = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        
        cursor.execute('''
            SELECT title, summary, source FROM news 
            WHERE date = ?
            ORDER BY id DESC
            LIMIT 10
        ''', (today,))
        
        news_rows = cursor.fetchall()
        conn.close()
        
        if news_rows:
            news_result = {'data': {'news': []}}
            for row in news_rows:
                news_result['data']['news'].append({
                    'title': row[0],
                    'summary': row[1],
                    'source': row[2]
                })
            logger.info(f"✓ 从数据库读取新闻: {len(news_rows)} 条")
        else:
            news_result = None
            logger.warning("数据库中没有今日新闻")
    except Exception as e:
        news_result = None
        logger.warning(f"读取新闻失败: {e}")
    
    # 5. 货币供应量、社融、PMI等（暂不读取）
    logger.info("✓ 宏观数据跳过")
    
    # 获取历史成交额数据（用于成交量分析）- 从数据库读取
    try:
        # 从数据库获取历史成交额
        volume_data = get_volume_data_from_db(days=22)
        
        if not volume_data or not volume_data.get('dates'):
            logger.warning("无法从数据库获取成交额数据")
            volume_data = {'dates': [], 'daily_amounts': []}
        else:
            logger.info(f"✓ 历史成交额数据: {len(volume_data.get('dates', []))}天")
    except Exception as e:
        logger.error(f"✗ 获取历史成交额失败: {e}")
        volume_data = {'dates': [], 'daily_amounts': []}
    
    # 8. 生成分析报告
    logger.info("\n[2/3] 分析和报告生成...")
    
    # 构建数据
    data = {
        'stock': stock_result,
        'futures': futures,
        'news': news_result,
        'volume_history': volume_data
    }
    
    try:
        analyzer = Analyzer()
        analysis = analyzer.analyze_from_data(data)
        
        # 生成报告
        reporter = Reporter()
        report = reporter.generate(analysis)
        
        # 生成成交额折线图
        chart_path = generate_volume_chart(volume_data)
        if chart_path:
            logger.info(f"✓ 成交额图表已生成: {chart_path}")
        
        # 保存报告
        today = datetime.now().strftime('%Y-%m-%d')
        report_file = project_root / 'data' / f'report_{today}.md'
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        # 打印基差分析
        basis = analysis.get('market', {}).get('basis', [])
        if basis:
            print("\n📊 基差分析 (隔季合约)")
            print("-" * 70)
            print(f"| {'指数':^10} | {'合约':^8} | {'期货价':^10} | {'现货价':^10} | {'基差':^10} | {'年化基差率':^10} |")
            print("-" * 70)
            for b in basis:
                arrow = "↓" if b['basis'] < 0 else "↑"
                ann = f"{arrow}{abs(b['annualized_basis']):.2f}%"
                print(f"| {b['index']:^10} | {b['contract']:^8} | {b['futures_price']:>10.2f} | {b['spot_price']:>10.2f} | {arrow}{abs(b['basis']):>9.2f} | {ann:>10} |")
        
        # 打印完整报告
        print("\n" + "=" * 60)
        print(report)
        print("=" * 60)
        print(f"\n报告已保存至: {report_file}")
        
    except Exception as e:
        import traceback
        logger.error(f"✗ 分析失败: {e}")
        traceback.print_exc()


if __name__ == '__main__':
    main()
