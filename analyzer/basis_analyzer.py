"""
基差分析算法模块
从数据库获取期货和现货数据，计算基差和年化基差率
"""
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path


class BasisAnalyzer:
    """基差分析器"""
    
    # 指数代码映射 (ts_code -> (db_code, name))
    INDEX_MAP = {
        '000300': ('000300', '沪深300'),
        '000905': ('000905', '中证500'),
        '000016': ('000016', '上证50'),
        '000852': ('000852', '中证1000'),
    }
    
    # 期货代码到现货代码的映射（使用隔季合约）
    FUTURES_TO_SPOT = {
        'IF2609': ('000300', '沪深300'),
        'IC2609': ('000905', '中证500'),
        'IH2609': ('000016', '上证50'),
        'IM2609': ('000852', '中证1000'),
    }
    
    # 期货合约映射（隔季合约）
    FUTURES_MAP = {
        'IF2609': 'IF2609',   # 沪深300股指期货
        'IC2609': 'IC2609',   # 中证500股指期货
        'IH2609': 'IH2609',   # 上证50股指期货
        'IM2609': 'IM2609',   # 中证1000股指期货
    }
    
    # 隔季合约到期天数（按交易日估算）
    DAYS_TO_EXPIRY = 136
    
    def __init__(self, db_path: str = None):
        if db_path is None:
            # 项目根目录
            project_root = Path(__file__).parent.parent
            self.db_path = project_root / 'data' / 'market_data.db'
        else:
            self.db_path = Path(db_path)
    
    def get_spot_price(self, db_code: str, trade_date: str = None) -> Optional[float]:
        """从数据库获取现货价格
        
        Args:
            db_code: 数据库代码，如 '000300'
            trade_date: 交易日期
        """
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y-%m-%d')
        
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT close FROM stock_indices 
            WHERE ts_code = ? AND trade_date = ?
        ''', (db_code, trade_date))
        
        result = cursor.fetchone()
        conn.close()
        
        return float(result[0]) if result else None
    
    def get_futures_price(self, futures_code: str, trade_date: str = None) -> Optional[float]:
        """从数据库获取期货价格
        
        Args:
            futures_code: 期货代码，如 'IF'
            trade_date: 交易日期
        """
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')
        
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        # 模糊匹配期货代码
        cursor.execute('''
            SELECT close FROM futures_daily 
            WHERE ts_code LIKE ? AND trade_date = ?
        ''', (f'%{futures_code}%', trade_date))
        
        result = cursor.fetchone()
        conn.close()
        
        return float(result[0]) if result else None
    
    def calculate_basis(self, spot_price: float, futures_price: float) -> Dict:
        """计算基差和年化基差率
        
        Args:
            spot_price: 现货价格
            futures_price: 期货价格
        
        Returns:
            基差数据字典
        """
        # 基差 = 现货价 - 期货价
        basis = spot_price - futures_price
        
        # 基差率 = 基差 / 现货价 × 100%
        basis_pct = (basis / spot_price) * 100 if spot_price > 0 else 0
        
        # 年化基差率 = 基差率 × (252 / 到期天数)
        annual_basis_pct = basis_pct * (252 / self.DAYS_TO_EXPIRY)
        
        return {
            'basis': basis,
            'basis_pct': basis_pct,
            'annual_basis_pct': annual_basis_pct
        }
    
    def analyze(self, trade_date: str = None) -> List[Dict]:
        """分析基差
        
        Args:
            trade_date: 交易日期，默认为今天
        
        Returns:
            基差分析结果列表
        """
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y-%m-%d')
        
        results = []
        
        for futures_code, (db_code, name) in self.FUTURES_TO_SPOT.items():
            # 获取现货价格
            spot_price = self.get_spot_price(db_code, trade_date)
            
            # 获取期货价格
            contract = self.FUTURES_MAP.get(futures_code, '')
            fut_price = self.get_futures_price(futures_code, trade_date.replace('-', ''))
            
            if spot_price and fut_price:
                basis_data = self.calculate_basis(spot_price, fut_price)
                
                results.append({
                    'name': name,
                    'contract': contract,
                    'spot_price': spot_price,
                    'futures_price': fut_price,
                    'basis': basis_data['basis'],
                    'basis_pct': basis_data['basis_pct'],
                    'annual_basis_pct': basis_data['annual_basis_pct'],
                    'days_to_expiry': self.DAYS_TO_EXPIRY
                })
        
        return results
    
    def get_table_format(self, results: List[Dict] = None, trade_date: str = None) -> str:
        """获取表格格式的基差分析结果
        
        Args:
            results: 基差分析结果，如果为None则重新计算
            trade_date: 交易日期
        
        Returns:
            表格格式的字符串
        """
        if results is None:
            results = self.analyze(trade_date)
        
        if not results:
            return "暂无基差数据"
        
        lines = [
            "=" * 76,
            "📊 基差分析 (隔季合约)",
            "=" * 76,
            f"{'指数':<12} {'合约':<8} {'期货价':<10} {'现货价':<10} {'基差':<10} {'年化基差率':<12}",
            "-" * 76
        ]
        
        for r in results:
            arrow = '↓' if r['basis'] > 0 else '↑'
            lines.append(
                f"{r['name']:<12} {r['contract']:<8} {r['futures_price']:<10.2f} "
                f"{r['spot_price']:<10.2f} {arrow}{abs(r['basis']):<9.2f} "
                f"{arrow}{abs(r['annual_basis_pct']):.2f}%"
            )
        
        lines.extend([
            "=" * 76,
            f"注: 隔季合约，距到期约{self.DAYS_TO_EXPIRY}个交易日，年化按252交易日计算"
        ])
        
        return '\n'.join(lines)


def analyze_basis(trade_date: str = None) -> List[Dict]:
    """基差分析入口函数
    
    Args:
        trade_date: 交易日期，默认为今天
    
    Returns:
        基差分析结果列表
    """
    analyzer = BasisAnalyzer()
    return analyzer.analyze(trade_date)


def get_basis_table(trade_date: str = None) -> str:
    """获取表格格式的基差分析结果
    
    Args:
        trade_date: 交易日期，默认为今天
    
    Returns:
        表格格式的字符串
    """
    analyzer = BasisAnalyzer()
    return analyzer.get_table_format(trade_date=trade_date)


if __name__ == '__main__':
    # 测试
    results = analyze_basis()
    print(get_basis_table(results))
