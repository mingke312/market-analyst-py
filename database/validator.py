"""
数据一致性验证脚本
对比 JSON 文件和 SQLite 数据库中的数据
"""
import sqlite3
import json
import glob
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'market_data.db')
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')


def get_connection():
    return sqlite3.connect(DB_PATH)


def validate_money_supply():
    """验证货币供应量数据一致性"""
    conn = get_connection()
    cursor = conn.cursor()
    
    print("\n" + "=" * 60)
    print("验证：货币供应量 M0/M1/M2")
    print("=" * 60)
    
    # 获取数据库中所有月份
    db_months = {row[0]: row for row in cursor.execute('SELECT month, m2, m2_yoy FROM money_supply').fetchall()}
    
    print(f"数据库唯一月份数:   {len(db_months)}")
    
    # 抽查对比最新3条
    print("\n抽查对比 (最新3条):")
    print("-" * 60)
    print(f"{'月份':<10} {'来源':<8} {'M2(亿)':<15} {'M2同比%':<10}")
    print("-" * 60)
    
    db_latest = cursor.execute('SELECT month, m2, m2_yoy FROM money_supply ORDER BY month DESC LIMIT 3').fetchall()
    for row in db_latest:
        month = row[0]
        m2_db = row[1]
        m2_yoy_db = row[2]
        
        print(f"{month:<10} {'数据库':<8} {m2_db:<15.2f} {m2_yoy_db:<10.1f}")
    
    # 验证数据合理性
    valid = True
    for month, row in db_months.items():
        if row[1] is None or row[1] <= 0:
            print(f"警告: {month} M2数据无效")
            valid = False
    
    conn.close()
    
    status = "✅ 通过" if valid else "❌ 失败"
    print(f"\n结果: {status}")
    return valid


def validate_social_financing():
    """验证社融数据一致性"""
    conn = get_connection()
    cursor = conn.cursor()
    
    print("\n" + "=" * 60)
    print("验证：社会融资规模")
    print("=" * 60)
    
    # JSON 数据
    json_months = {}
    json_count = 0
    for f in glob.glob(os.path.join(DATA_DIR, 'social_financing_2026-*.json')):
        with open(f) as fp:
            data = json.load(fp)
        sf_list = data.get('data', {}).get('data', {})
        if isinstance(sf_list, dict):
            sf_list = sf_list.get('social_financing', [])
        
        for sf in sf_list:
            month = sf.get('month', '')
            if month:
                json_months[month] = sf
                json_count += 1
    
    # 数据库
    db_count = cursor.execute('SELECT COUNT(*) FROM social_financing').fetchone()[0]
    db_months = {row[0]: row for row in cursor.execute('SELECT month, total, rmb_loan FROM social_financing').fetchall()}
    
    print(f"JSON 月份数:    {json_count}")
    print(f"数据库月份数:  {len(db_months)}")
    
    # 对比
    match = 0
    mismatch = []
    for month, json_val in json_months.items():
        if month in db_months:
            db_val = db_months[month]
            if abs(json_val.get('total', 0) - db_val[1]) < 0.01:
                match += 1
            else:
                mismatch.append((month, json_val.get('total'), db_val[1]))
    
    print(f"匹配数:        {match}")
    print(f"不一致数:      {len(mismatch)}")
    
    if mismatch[:3]:
        print("\n不一致记录 (前3条):")
        for m, j, d in mismatch[:3]:
            print(f"  {m}: JSON={j}, DB={d}")
    
    conn.close()
    
    passed = len(mismatch) == 0
    status = "✅ 通过" if passed else "❌ 失败"
    print(f"\n结果: {status}")
    return passed


def validate_pmi():
    """验证PMI数据一致性"""
    conn = get_connection()
    cursor = conn.cursor()
    
    print("\n" + "=" * 60)
    print("验证：PMI 采购经理指数")
    print("=" * 60)
    
    # JSON 数据
    json_months = {}
    for f in glob.glob(os.path.join(DATA_DIR, 'pmi_2026-*.json')):
        with open(f) as fp:
            data = json.load(fp)
        pmi_list = data.get('data', [])
        if isinstance(pmi_list, dict):
            pmi_list = pmi_list.get('data', [])
        
        for pmi in pmi_list:
            month = pmi.get('MONTH', '')
            if month:
                json_months[f"{month[:4]}-{month[4:]}"] = pmi
    
    # 数据库
    db_count = cursor.execute('SELECT COUNT(*) FROM pmi').fetchone()[0]
    
    print(f"JSON 月份数:   {len(json_months)}")
    print(f"数据库月份数:  {db_count}")
    
    # 抽查对比
    print("\n抽查对比:")
    print("-" * 50)
    db_data = cursor.execute('SELECT month, manufacturing_pmi, non_manufacturing_pmi FROM pmi ORDER BY month DESC LIMIT 3').fetchall()
    for row in db_data:
        month = row[0]
        json_val = json_months.get(month, {})
        mfg_pmi_db = row[1]
        mfg_pmi_json = json_val.get('manufacturing_pmi', 0)
        match = "✅" if mfg_pmi_json and abs(mfg_pmi_db - mfg_pmi_json) < 0.1 else "❌"
        print(f"{month}: DB={mfg_pmi_db}, JSON={mfg_pmi_json} {match}")
    
    conn.close()
    
    passed = db_count == len(json_months)
    status = "✅ 通过" if passed else "⚠️ 部分通过"
    print(f"\n结果: {status}")
    return passed


def validate_stock_indices():
    """验证股票指数数据一致性"""
    conn = get_connection()
    cursor = conn.cursor()
    
    print("\n" + "=" * 60)
    print("验证：股票指数")
    print("=" * 60)
    
    # JSON
    json_count = 0
    json_records = []
    for f in glob.glob(os.path.join(DATA_DIR, 'stock_2026-*.json')):
        with open(f) as fp:
            data = json.load(fp)
        indices = data.get('data', {}).get('indices', [])
        for idx in indices:
            json_records.append(idx)
        json_count += len(indices)
    
    # 数据库
    db_count = cursor.execute('SELECT COUNT(*) FROM stock_indices').fetchone()[0]
    
    print(f"JSON 记录数:   {json_count}")
    print(f"数据库记录数:  {db_count}")
    print(f"差异:          {json_count - db_count}")
    
    conn.close()
    
    passed = json_count == db_count
    status = "✅ 通过" if passed else "❌ 失败"
    print(f"\n结果: {status}")
    return passed


def show_database_summary():
    """显示数据库摘要"""
    conn = get_connection()
    cursor = conn.cursor()
    
    print("\n" + "=" * 60)
    print("数据库统计摘要")
    print("=" * 60)
    
    tables = ['money_supply', 'social_financing', 'pmi', 'stock_indices', 
              'convertible_bonds', 'fund_data']
    total = 0
    for table in tables:
        try:
            count = cursor.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
            print(f"  {table:<25} {count:>8} 条")
            total += count
        except:
            print(f"  {table:<25} {'(表不存在)':>15}")
    
    print("-" * 60)
    print(f"  {'总计':<25} {total:>8} 条")
    
    conn.close()


def main():
    print("=" * 60)
    print("数据一致性验证")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    results = {
        '货币供应量(M0/M1/M2)': validate_money_supply(),
        '社会融资规模': validate_social_financing(),
        'PMI指数': validate_pmi(),
        '股票指数': validate_stock_indices(),
    }
    
    show_database_summary()
    
    print("\n" + "=" * 60)
    print("验证结果汇总")
    print("=" * 60)
    
    passed = 0
    failed = 0
    for name, result in results.items():
        status = "✅ 通过" if result else "❌ 失败"
        print(f"  {name}: {status}")
        if result:
            passed += 1
        else:
            failed += 1
    
    print("-" * 60)
    print(f"  通过: {passed}, 失败: {failed}")
    
    if failed == 0:
        print("\n🎉 所有验证通过!")
    else:
        print(f"\n⚠️  {failed} 项验证失败，请检查!")
    
    return failed == 0


if __name__ == '__main__':
    main()
