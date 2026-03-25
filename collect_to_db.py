#!/usr/bin/env python3
"""
统一数据采集和数据库写入脚本
采集所有数据并写入SQLite数据库
"""
import os
import sys
import json
import logging
from datetime import datetime

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('collector')


def load_config():
    """加载配置"""
    config_path = os.path.join(project_root, 'config', 'local_config.json')
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def main():
    """主入口 - 采集所有数据并写入数据库"""
    logger.info("=" * 60)
    logger.info("开始采集数据并写入数据库")
    logger.info("=" * 60)
    
    config = load_config()
    tushare_token = os.getenv('TUSHARE_TOKEN', config.get('tushare_token', ''))
    
    if not tushare_token:
        logger.error("未配置TuShare Token!")
        return
    
    # 创建数据库写入器
    from database.db_writer import DBWriter
    db_writer = DBWriter()
    
    total_written = 0
    
    # ========== 1. A股行情 ==========
    try:
        from collectors.stock_collector import StockCollector
        collector = StockCollector({'tushare_token': tushare_token})
        result = collector.run(save=False)
        if result:
            indices = result.get('indices', [])
            if indices:
                count = db_writer.write_stock_indices(indices)
                logger.info(f"✓ 股票指数写入数据库: {count} 条")
                total_written += count
    except Exception as e:
        logger.error(f"✗ 股票指数采集失败: {e}")
    
    # ========== 2. 货币供应量 M0/M1/M2 ==========
    try:
        from collectors.tushare_macro_collector import MoneySupplyCollector
        collector = MoneySupplyCollector({'tushare_token': tushare_token})
        result = collector.run(save=False)
        if result:
            ms_list = result.get('data', {}).get('money_supply', [])
            if ms_list:
                count = db_writer.write_money_supply(ms_list)
                logger.info(f"✓ 货币供应量写入数据库: {count} 条")
                total_written += count
    except Exception as e:
        logger.error(f"✗ 货币供应量采集失败: {e}")
    
    # ========== 3. 社会融资规模 (TuShare Pro) ==========
    try:
        from collectors.tushare_macro_collector import SocialFinancingProCollector
        collector = SocialFinancingProCollector({'tushare_token': tushare_token})
        result = collector.run(save=False)
        if result:
            sf_list = result.get('data', {}).get('social_financing', [])
            if sf_list:
                count = db_writer.write_social_financing(sf_list)
                logger.info(f"✓ 社会融资写入数据库: {count} 条")
                total_written += count
    except Exception as e:
        logger.error(f"✗ 社会融资采集失败: {e}")
    
    # ========== 4. PMI ==========
    try:
        import tushare
        pro = tushare.pro_api(tushare_token)
        # 获取最近24个月
        df = pro.cn_pmi(start_m='202401', end_m='202503')
        if not df.empty:
            pmi_list = df.to_dict('records')
            if pmi_list:
                count = db_writer.write_pmi(pmi_list)
                logger.info(f"✓ PMI写入数据库: {count} 条")
                total_written += count
    except Exception as e:
        logger.error(f"✗ PMI采集失败: {e}")
    
    # ========== 5. 社融 (央行官网备用) ==========
    try:
        from collectors.social_financing_collector import SocialFinancingCollector
        collector = SocialFinancingCollector()
        result = collector.run(save=False)
        if result:
            sf_list = result.get('data', {}).get('social_financing', [])
            if sf_list:
                # 转换格式以适配数据库
                formatted = []
                for sf in sf_list:
                    formatted.append({
                        'month': sf.get('month', '').replace('.', ''),
                        'total': sf.get('total', 0),
                        'rmb_loan': sf.get('rmb_loan', 0),
                        'foreign_loan': sf.get('foreign_loan', 0),
                        'entrusted_loan': sf.get('entrusted_loan', 0),
                        'trust_loan': sf.get('trust_loan', 0),
                        'ba_acceptance': sf.get('ba_acceptance', 0),
                        'corporate_bond': sf.get('corporate_bond', 0),
                        'gov_bond': sf.get('gov_bond', 0),
                        'stock_financing': sf.get('stock_financing', 0),
                        'abs': sf.get('abs', 0),
                        'loan_writeoff': sf.get('loan_writeoff', 0),
                    })
                count = db_writer.write_social_financing(formatted)
                logger.info(f"✓ 社融(央行)写入数据库: {count} 条")
                total_written += count
    except Exception as e:
        logger.error(f"✗ 社融(央行)采集失败: {e}")
    
    # ========== 6. 房地产数据 ==========
    try:
        from collectors.real_estate_collector import RealEstateCollector
        collector = RealEstateCollector()
        result = collector.run(save=False)
        if result:
            re_data = result.get('data', {}).get('real_estate', [])
            if re_data:
                # 检查是否有写入方法
                logger.info(f"✓ 房地产数据采集: {len(re_data)} 条 (待写入数据库)")
    except Exception as e:
        logger.error(f"✗ 房地产采集失败: {e}")
    
    # ========== 7. 宏观数据 (CPI/PPI/GDP) ==========
    try:
        from collectors.macro_collector import MacroCollector
        collector = MacroCollector({'tushare_token': tushare_token})
        result = collector.run(save=False)
        if result:
            data = result.get('data', {})
            
            # CPI
            cpi_list = data.get('cpi', [])
            if cpi_list:
                count = db_writer.write_cpi(cpi_list)
                logger.info(f"✓ CPI写入数据库: {count} 条")
                total_written += count
            
            # PPI
            ppi_list = data.get('ppi', [])
            if ppi_list:
                count = db_writer.write_ppi_data(ppi_list)
                logger.info(f"✓ PPI写入数据库: {count} 条")
                total_written += count
            
            # GDP
            gdp_list = data.get('gdp', [])
            if gdp_list:
                count = db_writer.write_gdp(gdp_list)
                logger.info(f"✓ GDP写入数据库: {count} 条")
                total_written += count
    except Exception as e:
        logger.error(f"✗ 宏观数据(CPI/PPI/GDP)采集失败: {e}")
    
    # ========== 8. 其他TuShare数据(仅JSON) ==========
    try:
        from collectors.macro_collector import MacroCollector, FundFlowCollector, FundCollector, BondCollector, GlobalMarketCollector
        
        collectors_to_run = [
            ('MacroCollector', MacroCollector),
            ('FundFlowCollector', FundFlowCollector),
            ('FundCollector', FundCollector),
            ('BondCollector', BondCollector),
            ('GlobalMarketCollector', GlobalMarketCollector),
        ]
        
        for name, cls in collectors_to_run:
            try:
                collector = cls({'tushare_token': tushare_token})
                result = collector.run(save=True)  # 保存到JSON
                if result:
                    logger.info(f"✓ {name}: 已保存到JSON")
            except Exception as e:
                logger.warning(f"  {name} 采集失败: {e}")
                
    except Exception as e:
        logger.error(f"✗ 其他TuShare数据采集失败: {e}")
    
    # ========== 8. 验证数据库最新数据 ==========
    try:
        conn = db_writer.get_connection()
        cursor = conn.cursor()
        
        # 货币供应量最新
        cursor.execute("SELECT month, m2, m2_yoy FROM money_supply ORDER BY month DESC LIMIT 1")
        row = cursor.fetchone()
        if row:
            logger.info(f"📊 M2最新: {row[0]}, 数值: {row[1]:,.0f}亿, 同比: {row[2]}%")
        
        # 社融最新
        cursor.execute("SELECT month, total FROM social_financing ORDER BY month DESC LIMIT 1")
        row = cursor.fetchone()
        if row:
            logger.info(f"📊 社融最新: {row[0]}, 增量: {row[1]:,.0f}亿")
        
        conn.close()
    except Exception as e:
        logger.error(f"✗ 验证数据库失败: {e}")
    
    logger.info("=" * 60)
    logger.info(f"数据采集完成! 共写入 {total_written} 条记录到数据库")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
