"""
数据库写入器
提供将数据写入 SQLite 数据库的统一接口
"""
import sqlite3
import os
from datetime import datetime
from typing import Dict, Any, List, Optional

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'market_data.db')


class DBWriter:
    """数据库写入器"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or DB_PATH
    
    def get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        return sqlite3.connect(self.db_path)
    
    # ========== 货币供应量 ==========
    def write_money_supply(self, data: List[Dict]) -> int:
        """写入货币供应量数据"""
        if not data:
            return 0
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        count = 0
        for ms in data:
            month = ms.get('month', '')
            if not month:
                continue
            
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
            count += 1
        
        conn.commit()
        conn.close()
        return count
    
    # ========== 社会融资规模 ==========
    def write_social_financing(self, data: List[Dict]) -> int:
        """写入社会融资规模数据
        
        支持两种数据格式：
        1. 央行官网格式: total, rmb_loan, foreign_loan, entrusted_loan, trust_loan, 
           ba_acceptance, corporate_bond, gov_bond, stock_financing, abs, loan_writeoff
        2. TuShare格式: inc_month (社融增量), month
        """
        if not data:
            return 0
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        count = 0
        for sf in data:
            month = sf.get('month', '')
            if not month:
                continue
            
            # 兼容两种格式
            total = sf.get('total') or sf.get('inc_month')  # TuShare用inc_month
            
            cursor.execute('''
                INSERT OR REPLACE INTO social_financing
                (month, total, rmb_loan, foreign_loan, entrusted_loan, 
                 trust_loan, ba_acceptance, corporate_bond, gov_bond, 
                 stock_financing, abs_val, loan_writeoff)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                month, total, 
                sf.get('rmb_loan'), sf.get('foreign_loan'),
                sf.get('entrusted_loan'), sf.get('trust_loan'), 
                sf.get('ba_acceptance'), sf.get('corporate_bond'), 
                sf.get('gov_bond'), sf.get('stock_financing'),
                sf.get('abs') or sf.get('abs_val'), 
                sf.get('loan_writeoff')
            ))
            count += 1
        
        conn.commit()
        conn.close()
        return count
    
    # ========== PMI 数据 ==========
    def write_pmi(self, data: List[Dict]) -> int:
        """写入PMI数据"""
        if not data:
            return 0
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        count = 0
        for pmi in data:
            month = pmi.get('MONTH', '')
            if not month:
                continue
            
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
            count += 1
        
        conn.commit()
        conn.close()
        return count
    
    # ========== 股票指数 ==========
    def write_stock_indices(self, data: List[Dict]) -> int:
        """写入股票指数数据"""
        if not data:
            return 0
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        count = 0
        for idx in data:
            trade_date = idx.get('date', idx.get('trade_date', ''))
            ts_code = idx.get('code', idx.get('ts_code', ''))
            if not trade_date or not ts_code:
                continue
            
            cursor.execute('''
                INSERT OR REPLACE INTO stock_indices
                (trade_date, ts_code, name, close, pct_chg, volume, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                trade_date, ts_code, idx.get('name'),
                idx.get('close'), idx.get('pct_chg'),
                idx.get('volume'), idx.get('amount')
            ))
            count += 1
        
        conn.commit()
        conn.close()
        return count
    
    # ========== CPI 消费者价格指数 ==========
    def write_cpi(self, data: List[Dict]) -> int:
        """写入CPI数据"""
        if not data:
            return 0
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        count = 0
        for cpi in data:
            month = cpi.get('month', '')
            if not month:
                continue
            
            cursor.execute('''
                INSERT OR REPLACE INTO cpi
                (month, nt_val, nt_yoy, nt_mom, town_val, town_yoy, town_mom, cnt_val, cnt_yoy, cnt_mom)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                month,
                cpi.get('nt_val'), cpi.get('nt_yoy'), cpi.get('nt_mom'),
                cpi.get('town_val'), cpi.get('town_yoy'), cpi.get('town_mom'),
                cpi.get('cnt_val'), cpi.get('cnt_yoy'), cpi.get('cnt_mom')
            ))
            count += 1
        
        conn.commit()
        conn.close()
        return count
    
    # ========== PPI 生产者价格指数 ==========
    def write_ppi_data(self, data: List[Dict]) -> int:
        """写入PPI数据"""
        if not data:
            return 0
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        count = 0
        for ppi in data:
            month = ppi.get('month', '')
            if not month:
                continue
            
            cursor.execute('''
                INSERT OR REPLACE INTO ppi
                (month, ppi_yoy, ppi_mom, ppi_accu)
                VALUES (?, ?, ?, ?)
            ''', (
                month,
                ppi.get('ppi_yoy'), ppi.get('ppi_mom'), ppi.get('ppi_accu')
            ))
            count += 1
        
        conn.commit()
        conn.close()
        return count
    
    # ========== GDP 国内生产总值 ==========
    def write_gdp(self, data: List[Dict]) -> int:
        """写入GDP数据"""
        if not data:
            return 0
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        count = 0
        for gdp in data:
            quarter = gdp.get('quarter', '')
            if not quarter:
                continue
            
            cursor.execute('''
                INSERT OR REPLACE INTO gdp
                (quarter, gdp, gdp_yoy, pi, pi_yoy, si, si_yoy, ti, ti_yoy)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                quarter,
                gdp.get('gdp'), gdp.get('gdp_yoy'),
                gdp.get('pi'), gdp.get('pi_yoy'),
                gdp.get('si'), gdp.get('si_yoy'),
                gdp.get('ti'), gdp.get('ti_yoy')
            ))
            count += 1
        
        conn.commit()
        conn.close()
        return count
    
    # ========== 批量写入 ==========
    def write_all(self, data: Dict[str, Any]) -> Dict[str, int]:
        """批量写入所有类型数据"""
        results = {}
        
        # 货币供应量
        ms_list = data.get('money_supply', [])
        if isinstance(ms_list, dict):
            ms_list = ms_list.get('money_supply', ms_list.get('data', []))
        if ms_list:
            results['money_supply'] = self.write_money_supply(ms_list)
        
        # 社会融资
        sf_list = data.get('social_financing', [])
        if isinstance(sf_list, dict):
            sf_list = sf_list.get('social_financing', sf_list.get('data', []))
        if sf_list:
            results['social_financing'] = self.write_social_financing(sf_list)
        
        # PMI
        pmi_list = data.get('pmi', [])
        if isinstance(pmi_list, dict):
            pmi_list = pmi_list.get('data', pmi_list.get('pmi', []))
        if pmi_list:
            results['pmi'] = self.write_pmi(pmi_list)
        
        # 股票指数
        indices = data.get('stock_indices', [])
        if indices:
            results['stock_indices'] = self.write_stock_indices(indices)
        
        return results


class DBReader:
    """数据库读取器"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or DB_PATH
    
    def get_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)
    
    # ========== 读取货币供应量 ==========
    def get_money_supply(self, months: int = 12) -> List[Dict]:
        """获取货币供应量数据"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT month, m0, m0_yoy, m1, m1_yoy, m2, m2_yoy
            FROM money_supply
            ORDER BY month DESC
            LIMIT ?
        ''', (months,))
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'month': row[0],
                'm0': row[1], 'm0_yoy': row[2],
                'm1': row[3], 'm1_yoy': row[4],
                'm2': row[5], 'm2_yoy': row[6]
            })
        
        conn.close()
        return results
    
    def get_latest_money_supply(self) -> Optional[Dict]:
        """获取最新货币供应量数据"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT month, m0, m0_yoy, m1, m1_yoy, m2, m2_yoy
            FROM money_supply
            ORDER BY month DESC
            LIMIT 1
        ''')
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'month': row[0],
                'm0': row[1], 'm0_yoy': row[2],
                'm1': row[3], 'm1_yoy': row[4],
                'm2': row[5], 'm2_yoy': row[6]
            }
        return None
    
    # ========== 读取社会融资 ==========
    def get_social_financing(self, months: int = 12) -> List[Dict]:
        """获取社会融资数据"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT month, total, rmb_loan, gov_bond, corporate_bond
            FROM social_financing
            ORDER BY month DESC
            LIMIT ?
        ''', (months,))
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'month': row[0],
                'total': row[1], 'rmb_loan': row[2],
                'gov_bond': row[3], 'corporate_bond': row[4]
            })
        
        conn.close()
        return results
    
    # ========== 读取 PMI ==========
    def get_pmi(self, months: int = 12) -> List[Dict]:
        """获取PMI数据"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT month, manufacturing_pmi, non_manufacturing_pmi
            FROM pmi
            ORDER BY month DESC
            LIMIT ?
        ''', (months,))
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'month': row[0],
                'manufacturing_pmi': row[1],
                'non_manufacturing_pmi': row[2]
            })
        
        conn.close()
        return results
    
    def get_latest_pmi(self) -> Optional[Dict]:
        """获取最新PMI数据"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT month, manufacturing_pmi, non_manufacturing_pmi
            FROM pmi
            ORDER BY month DESC
            LIMIT 1
        ''')
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'month': row[0],
                'manufacturing_pmi': row[1],
                'non_manufacturing_pmi': row[2]
            }
        return None
    
    # ========== 读取 CPI ==========
    def get_latest_cpi(self) -> Optional[Dict]:
        """获取最新CPI数据"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT month, nt_val, nt_yoy, nt_mom
            FROM cpi
            ORDER BY month DESC
            LIMIT 1
        ''')
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            # nt_val 是指数，yoy 是同比
            return {
                'month': row[0],
                'value': row[1],  # 指数
                'yoy': row[2],    # 同比
                'mom': row[3]     # 环比
            }
        return None
    
    # ========== 读取 PPI ==========
    def get_latest_ppi(self) -> Optional[Dict]:
        """获取最新PPI数据"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT month, ppi_yoy, ppi_mom
            FROM ppi
            ORDER BY month DESC
            LIMIT 1
        ''')
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'month': row[0],
                'yoy': row[1],   # 同比
                'mom': row[2]     # 环比
            }
        return None
    
    # ========== 读取 GDP ==========
    def get_latest_gdp(self) -> Optional[Dict]:
        """获取最新GDP数据"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT quarter, gdp, gdp_yoy
            FROM gdp
            ORDER BY quarter DESC
            LIMIT 1
        ''')
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            # GDP 是亿元，需要转换为万亿元
            gdp_val = row[1] / 10000 if row[1] else 0
            return {
                'quarter': row[0],
                'value': round(gdp_val, 2),  # 万亿元
                'yoy': row[2]                # 同比
            }
        return None
    
    # ========== 读取股票指数 ==========
    def get_stock_indices(self, trade_date: str = None) -> List[Dict]:
        """获取股票指数数据"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if trade_date:
            cursor.execute('''
                SELECT trade_date, ts_code, name, close, pct_chg, volume
                FROM stock_indices
                WHERE trade_date = ?
                ORDER BY ts_code
            ''', (trade_date,))
        else:
            cursor.execute('''
                SELECT trade_date, ts_code, name, close, pct_chg, volume
                FROM stock_indices
                ORDER BY trade_date DESC, ts_code
                LIMIT 100
            ''')
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'trade_date': row[0],
                'ts_code': row[1],
                'name': row[2],
                'close': row[3],
                'pct_chg': row[4],
                'volume': row[5]
            })
        
        conn.close()
        return results


# 便捷函数
def get_db_reader() -> DBReader:
    return DBReader()


def get_db_writer() -> DBWriter:
    return DBWriter()
