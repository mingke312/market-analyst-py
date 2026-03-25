"""
数据库初始化脚本
创建 SQLite 数据库和表结构
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'market_data.db')

def init_db():
    """初始化数据库和表结构"""
    # 确保 data 目录存在
    db_dir = os.path.dirname(DB_PATH)
    os.makedirs(db_dir, exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print(f"📁 数据库路径: {DB_PATH}")
    
    # 1. 宏观经济指标表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS macro_indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            indicator_type TEXT NOT NULL,
            value REAL,
            yoy REAL,
            mom REAL,
            source TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, indicator_type)
        )
    ''')
    print("✅ 表创建: macro_indicators")
    
    # 2. 货币供应量表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS money_supply (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT NOT NULL UNIQUE,
            m0 REAL, m0_yoy REAL, m0_mom REAL,
            m1 REAL, m1_yoy REAL, m1_mom REAL,
            m2 REAL, m2_yoy REAL, m2_mom REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    print("✅ 表创建: money_supply")
    
    # 3. 社会融资规模表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS social_financing (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT NOT NULL UNIQUE,
            total REAL,
            rmb_loan REAL, foreign_loan REAL,
            entrusted_loan REAL, trust_loan REAL,
            ba_acceptance REAL, corporate_bond REAL,
            gov_bond REAL, stock_financing REAL,
            abs_val REAL, loan_writeoff REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    print("✅ 表创建: social_financing")
    
    # 4. 股票指数表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_indices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            name TEXT,
            close REAL, pct_chg REAL, volume REAL, amount REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(trade_date, ts_code)
        )
    ''')
    print("✅ 表创建: stock_indices")
    
    # 5. 人民币贷款详细表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rmb_loan (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT NOT NULL UNIQUE,
            loan_balance REAL, loan_balance_yoy REAL,
            new_loan REAL,
            household_loan REAL, household_short REAL, household_medium REAL,
            enterprise_loan REAL, enterprise_short REAL, enterprise_medium REAL,
            enterprise_bill REAL,
            foreign_loan REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    print("✅ 表创建: rmb_loan")
    
    # 6. PMI数据表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pmi (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT NOT NULL UNIQUE,
            manufacturing_pmi REAL,
            non_manufacturing_pmi REAL,
            manufacturing_production REAL,
            non_manufacturing_activity REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    print("✅ 表创建: pmi")
    
    # 7. 可转债表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS convertible_bonds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL,
            bond_code TEXT NOT NULL,
            stock_code TEXT,
            close REAL, prem_rate REAL, yield REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(trade_date, bond_code)
        )
    ''')
    print("✅ 表创建: convertible_bonds")
    
    # 8. 基金数据表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fund_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL,
            fund_type TEXT,
            net_asset REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(trade_date, fund_type)
        )
    ''')
    print("✅ 表创建: fund_data")
    
    conn.commit()
    conn.close()
    
    print(f"\n🎉 数据库初始化完成!")
    return DB_PATH


if __name__ == '__main__':
    init_db()
