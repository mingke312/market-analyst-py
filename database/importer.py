"""
数据导入脚本
将 JSON 数据导入到 SQLite 数据库
"""
import sqlite3
import json
import glob
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'market_data.db')
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')


def get_connection():
    """获取数据库连接"""
    return sqlite3.connect(DB_PATH)


def import_money_supply():
    """导入货币供应量数据"""
    conn = get_connection()
    cursor = conn.cursor()
    
    pattern = os.path.join(DATA_DIR, 'money_supply_2026-*.json')
    files = [f for f in glob.glob(pattern) if 'history' not in f]
    
    imported = 0
    for filepath in files:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        # 处理不同格式
        ms_list = data.get('data', [])
        if isinstance(ms_list, dict):
            ms_list = ms_list.get('data', [])
        if isinstance(ms_list, dict):
            ms_list = ms_list.get('money_supply', [])
        
        for ms in ms_list:
            month = ms.get('month', '')
            if month:
                cursor.execute('''
                    INSERT OR REPLACE INTO money_supply 
                    (month, m0, m0_yoy, m0_mom, m1, m1_yoy, m1_mom, m2, m2_yoy, m2_mom)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    month,
                    ms.get('m0'), ms.get('m0_yoy'), ms.get('m0_mom'),
                    ms.get('m1'), ms.get('m1_yoy'), ms.get('m1_mom'),
                    ms.get('m2'), ms.get('m2_yoy'), ms.get('m2_mom')
                ))
                imported += 1
    
    conn.commit()
    count = cursor.execute('SELECT COUNT(*) FROM money_supply').fetchone()[0]
    conn.close()
    
    print(f"📊 货币供应量: 导入 {imported} 条, 数据库共 {count} 条")
    return imported


def import_social_financing():
    """导入社融数据"""
    conn = get_connection()
    cursor = conn.cursor()
    
    pattern = os.path.join(DATA_DIR, 'social_financing_2026-*.json')
    files = glob.glob(pattern)
    
    imported = 0
    for filepath in files:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        # 解析不同格式
        sf_list = data.get('data', {}).get('data', {})
        if isinstance(sf_list, dict):
            sf_list = sf_list.get('social_financing', [])
        
        for sf in sf_list:
            month = sf.get('month', '')
            if month:
                cursor.execute('''
                    INSERT OR REPLACE INTO social_financing
                    (month, total, rmb_loan, foreign_loan, entrusted_loan, 
                     trust_loan, ba_acceptance, corporate_bond, gov_bond, 
                     stock_financing, abs_val, loan_writeoff)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    month, sf.get('total'), sf.get('rmb_loan'), sf.get('foreign_loan'),
                    sf.get('entrusted_loan'), sf.get('trust_loan'), sf.get('ba_acceptance'),
                    sf.get('corporate_bond'), sf.get('gov_bond'), sf.get('stock_financing'),
                    sf.get('abs'), sf.get('loan_writeoff')
                ))
                imported += 1
    
    conn.commit()
    count = cursor.execute('SELECT COUNT(*) FROM social_financing').fetchone()[0]
    conn.close()
    
    print(f"📊 社会融资规模: 导入 {imported} 条, 数据库共 {count} 条")
    return imported


def import_pmi():
    """导入PMI数据"""
    conn = get_connection()
    cursor = conn.cursor()
    
    pattern = os.path.join(DATA_DIR, 'pmi_2026-*.json')
    files = glob.glob(pattern)
    
    imported = 0
    for filepath in files:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        pmi_list = data.get('data', [])
        if isinstance(pmi_list, dict):
            pmi_list = pmi_list.get('data', [])
        
        for pmi in pmi_list:
            month = pmi.get('MONTH', '')
            if month:
                # 转换为 YYYY-MM 格式
                month_formatted = f"{month[:4]}-{month[4:]}"
                cursor.execute('''
                    INSERT OR REPLACE INTO pmi
                    (month, manufacturing_pmi, non_manufacturing_pmi, 
                     manufacturing_production, non_manufacturing_activity)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    month_formatted,
                    pmi.get('manufacturing_pmi'), pmi.get('non_manufacturing_pmi'),
                    pmi.get('manufacturing_production'), pmi.get('non_manufacturing_activity')
                ))
                imported += 1
    
    conn.commit()
    count = cursor.execute('SELECT COUNT(*) FROM pmi').fetchone()[0]
    conn.close()
    
    print(f"📊 PMI数据: 导入 {imported} 条, 数据库共 {count} 条")
    return imported


def import_stock_indices():
    """导入股票指数数据"""
    conn = get_connection()
    cursor = conn.cursor()
    
    pattern = os.path.join(DATA_DIR, 'stock_2026-*.json')
    files = glob.glob(pattern)
    
    imported = 0
    for filepath in files:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        indices = data.get('data', {}).get('indices', [])
        
        for idx in indices:
            # 兼容不同字段名
            trade_date = idx.get('date', idx.get('trade_date', ''))
            ts_code = idx.get('code', idx.get('ts_code', ''))
            if trade_date and ts_code:
                cursor.execute('''
                    INSERT OR REPLACE INTO stock_indices
                    (trade_date, ts_code, name, close, pct_chg, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    trade_date, ts_code, idx.get('name'),
                    idx.get('close'), idx.get('change'),
                    idx.get('volume'), idx.get('amount')
                ))
                imported += 1
    
    conn.commit()
    count = cursor.execute('SELECT COUNT(*) FROM stock_indices').fetchone()[0]
    conn.close()
    
    print(f"📊 股票指数: 导入 {imported} 条, 数据库共 {count} 条")
    return imported


def import_convertible_bonds():
    """导入可转债数据"""
    conn = get_connection()
    cursor = conn.cursor()
    
    pattern = os.path.join(DATA_DIR, 'cb_2026-*.json')
    files = glob.glob(pattern)
    
    imported = 0
    for filepath in files:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        bonds = data.get('data', {}).get('data', [])
        
        for bond in bonds:
            # 兼容字段名
            bond_code = bond.get('cb_code', bond.get('bond_code', ''))
            stock_code = bond.get('stk_code', bond.get('stock_code', ''))
            # 没有日期数据，使用文件日期
            trade_date = os.path.basename(filepath).replace('cb_', '').replace('.json', '')
            
            # 跳过无效记录
            if not bond_code or str(bond_code) == 'nan' or not stock_code:
                continue
            
            cursor.execute('''
                INSERT OR REPLACE INTO convertible_bonds
                (trade_date, bond_code, stock_code, close, prem_rate, yield)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                trade_date, str(bond_code), stock_code,
                None, None, None
            ))
            imported += 1
    
    conn.commit()
    count = cursor.execute('SELECT COUNT(*) FROM convertible_bonds').fetchone()[0]
    conn.close()
    
    print(f"📊 可转债: 导入 {imported} 条, 数据库共 {count} 条")
    return imported


def import_fund_data():
    """导入基金数据"""
    conn = get_connection()
    cursor = conn.cursor()
    
    pattern = os.path.join(DATA_DIR, 'fund_2026-*.json')
    files = glob.glob(pattern)
    
    imported = 0
    for filepath in files:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        # 基金数据格式不同，data 下是多个类型的字典
        fund_dict = data.get('data', {}).get('data', {})
        
        # 处理字典格式的基金数据
        if isinstance(fund_dict, dict):
            for fund_type, fund_list in fund_dict.items():
                if isinstance(fund_list, list):
                    for fund in fund_list:
                        # 尝试获取日期
                        trade_date = fund.get('trade_date', 
                                    os.path.basename(filepath).replace('fund_', '').replace('.json', ''))
                        if trade_date:
                            cursor.execute('''
                                INSERT OR REPLACE INTO fund_data
                                (trade_date, fund_type, net_asset)
                                VALUES (?, ?, ?)
                            ''', (
                                trade_date, fund_type, None
                            ))
                            imported += 1
    
    conn.commit()
    count = cursor.execute('SELECT COUNT(*) FROM fund_data').fetchone()[0]
    conn.close()
    
    print(f"📊 基金数据: 导入 {imported} 条, 数据库共 {count} 条")
    return imported


def main():
    print("=" * 60)
    print("开始导入数据到 SQLite")
    print("=" * 60)
    print(f"数据库: {DB_PATH}")
    print(f"数据目录: {DATA_DIR}")
    print()
    
    import_money_supply()
    import_social_financing()
    import_pmi()
    import_stock_indices()
    import_convertible_bonds()
    import_fund_data()
    
    # 显示数据库统计
    conn = get_connection()
    cursor = conn.cursor()
    print("\n" + "=" * 60)
    print("数据库记录统计")
    print("=" * 60)
    
    tables = ['money_supply', 'social_financing', 'pmi', 'stock_indices', 
              'convertible_bonds', 'fund_data']
    for table in tables:
        count = cursor.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
        print(f"  {table}: {count} 条")
    
    conn.close()
    print("\n✅ 导入完成!")


if __name__ == '__main__':
    main()
