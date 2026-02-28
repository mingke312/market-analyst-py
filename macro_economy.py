#!/usr/bin/env python3
"""
宏观经济数据模块
包含：GDP、CPI、PPI、房地产、央行数据等

由于服务器网络限制，部分数据使用静态数据+标注
"""

from datetime import datetime
from typing import Dict
import json


class MacroEconomyData:
    """宏观经济数据类"""
    
    # 静态数据（当网络不可用时使用）
    STATIC_DATA = {
        'gdp': {
            'name': '中国GDP',
            'value': 126.06,  # 万亿元
            'yoy': 5.0,  # 2024年同比
            'quarter': '2024Q4',
            'source': '国家统计局',
            'note': '2024年全年数据'
        },
        'cpi': {
            'name': '中国CPI',
            'value': 0.2,  # 2025年1月
            'yoy': 0.2,
            'month': '2025-01',
            'source': '国家统计局',
            'note': '2025年1月数据'
        },
        'ppi': {
            'name': '中国PPI',
            'value': -2.3,  # 2025年1月
            'yoy': -2.3,
            'month': '2025-01',
            'source': '国家统计局',
            'note': '2025年1月数据'
        },
        'pmi': {
            'name': '中国PMI',
            'value': 50.1,
            'yoy': 0,
            'month': '2025-02',
            'source': '统计局',
            'note': '2025年2月官方PMI'
        },
        'central_bank': [
            {'name': '7天逆回购利率', 'value': '1.50%', 'source': '央行', 'date': '2025-02'},
            {'name': '1年期LPR', 'value': '3.45%', 'source': '央行', 'date': '2025-02'},
            {'name': '5年期以上LPR', 'value': '3.95%', 'source': '央行', 'date': '2025-02'},
            {'name': 'MLF利率', 'value': '2.50%', 'source': '央行', 'date': '2025-02'},
            {'name': 'SLF利率(隔夜)', 'value': '2.45%', 'source': '央行', 'date': '2025-02'},
        ],
        'real_estate': {
            'investment_yoy': -10.4,  # 2024年
            'sales_yoy': -15.3,
            'source': '国家统计局',
            'note': '2024年数据'
        }
    }
    
    def get_all(self) -> Dict:
        """获取所有宏观经济数据"""
        
        return {
            'date': datetime.now().strftime("%Y-%m-%d"),
            'type': 'macro_economy',
            'timestamp': datetime.now().isoformat(),
            'data': self.STATIC_DATA,
            'note': '静态数据，如需更新请运行本地收集器'
        }
    
    def get_summary(self) -> str:
        """获取摘要文本"""
        
        data = self.STATIC_DATA
        lines = []
        
        lines.append("📊 宏观经济数据")
        lines.append("=" * 40)
        
        # GDP
        gdp = data.get('gdp', {})
        if gdp:
            lines.append(f"\n【GDP】")
            lines.append(f"  {gdp.get('name')}: {gdp.get('value')} 万亿元")
            lines.append(f"  同比: {gdp.get('yoy')}%")
            lines.append(f"  季度: {gdp.get('quarter')}")
        
        # CPI
        cpi = data.get('cpi', {})
        if cpi:
            lines.append(f"\n【CPI】")
            lines.append(f"  同比: {cpi.get('yoy')}%")
            lines.append(f"  时期: {cpi.get('month')}")
        
        # PPI
        ppi = data.get('ppi', {})
        if ppi:
            lines.append(f"\n【PPI】")
            lines.append(f"  同比: {ppi.get('yoy')}%")
            lines.append(f"  时期: {ppi.get('month')}")
        
        # PMI
        pmi = data.get('pmi', {})
        if pmi:
            lines.append(f"\n【PMI】")
            lines.append(f"  数值: {pmi.get('value')}")
            lines.append(f"  时期: {pmi.get('month')}")
        
        # 央行
        cb = data.get('central_bank', [])
        if cb:
            lines.append(f"\n【央行政策】")
            for item in cb:
                lines.append(f"  {item['name']}: {item['value']}")
        
        # 房地产
        re = data.get('real_estate', {})
        if re:
            lines.append(f"\n【房地产】")
            lines.append(f"  投资同比: {re.get('investment_yoy')}%")
            lines.append(f"  销售同比: {re.get('sales_yoy')}%")
        
        return "\n".join(lines)


def main():
    import sys
    
    data = MacroEconomyData()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--json':
        result = data.get_all()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(data.get_summary())


if __name__ == "__main__":
    main()
