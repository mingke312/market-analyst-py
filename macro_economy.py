#!/usr/bin/env python3
"""
宏观经济数据模块
包含：GDP、CPI、PPI、PMI、社会消费品、工业投资、进出口、房地产、央行数据等
"""

from datetime import datetime
from typing import Dict
import json


class MacroEconomyData:
    """宏观经济数据类"""
    
    # 完整的静态宏观经济数据
    STATIC_DATA = {
        # ============ 经济增长 ============
        'gdp': {
            'name': '中国GDP',
            'value': 126.06,  # 万亿元
            'yoy': 5.0,
            'quarter': '2024Q4',
            'source': '国家统计局',
            'note': '2024年全年GDP'
        },
        
        # ============ 通胀数据 ============
        'cpi': {
            'name': '中国CPI',
            'value': 0.2,  # 2025年1月
            'yoy': 0.2,
            'mom': -0.7,  # 环比
            'month': '2025-01',
            'source': '国家统计局',
            'note': '2025年1月数据'
        },
        'ppi': {
            'name': '中国PPI',
            'value': -2.3,  # 2025年1月
            'yoy': -2.3,
            'mom': -0.2,
            'month': '2025-01',
            'source': '国家统计局',
            'note': '2025年1月数据'
        },
        
        # ============ PMI ============
        'pmi': {
            'name': '制造业PMI',
            'value': 50.1,
            'yoy': 0,
            'month': '2025-02',
            'source': '统计局',
            'note': '2025年2月官方PMI'
        },
        'pmi_services': {
            'name': '非制造业PMI',
            'value': 50.8,
            'month': '2025-02',
            'source': '统计局',
            'note': '2025年2月'
        },
        
        # ============ 消费 ============
        'retail': {
            'name': '社会消费品零售总额',
            'value': 4.0,  # 万亿元
            'yoy': 3.5,  # 同比
            'month': '2025-01',
            'source': '国家统计局',
            'note': '2025年1月数据'
        },
        'online_retail': {
            'name': '网上零售额',
            'yoy': 8.3,
            'month': '2025-01',
            'source': '国家统计局',
            'note': '实物商品网上零售额同比'
        },
        
        # ============ 投资 ============
        'fixed_investment': {
            'name': '全国固定资产投资',
            'yoy': 3.2,
            'month': '2025-01',
            'source': '国家统计局',
            'note': '2025年1-2月累计同比'
        },
        'real_estate_investment': {
            'name': '房地产投资',
            'yoy': -10.4,
            'month': '2025-01',
            'source': '国家统计局',
            'note': '2024年全年数据'
        },
        'manufacturing_investment': {
            'name': '制造业投资',
            'yoy': 9.2,
            'month': '2025-01',
            'source': '国家统计局',
            'note': '2024年全年数据'
        },
        
        # ============ 工业 ============
        'industrial_addition': {
            'name': '工业增加值',
            'yoy': 5.8,
            'month': '2025-01',
            'source': '国家统计局',
            'note': '2025年1月规模以上工业增加值同比'
        },
        'industrial_profit': {
            'name': '工业企业利润',
            'yoy': -4.7,
            'month': '2024-12',
            'source': '国家统计局',
            'note': '2024年全年数据'
        },
        
        # ============ 进出口 ============
        'exports': {
            'name': '出口金额',
            'value': 3345.0,  # 亿美元
            'yoy': 10.3,
            'month': '2025-01',
            'source': '海关总署',
            'note': '2025年1月美元计'
        },
        'imports': {
            'name': '进口金额',
            'value': 2215.0,  # 亿美元
            'yoy': 1.5,
            'month': '2025-01',
            'source': '海关总署',
            'note': '2025年1月美元计'
        },
        'trade_balance': {
            'name': '贸易顺差',
            'value': 1130.0,  # 亿美元
            'month': '2025-01',
            'source': '海关总署'
        },
        
        # ============ 房地产 ============
        # 国家统计局数据
        'real_estate': {
            'name': '房地产核心指标',
            'investment_yoy': -13.9,  # 2025年1-9月
            'investment_residential_yoy': -12.9,
            'new_start_yoy': -18.9,
            'new_start_residential_yoy': -18.3,
            'completion_yoy': -15.3,
            'completion_residential_yoy': -17.1,
            'sales_area_yoy': -5.5,
            'sales_area_residential_yoy': -5.6,
            'sales_amount_yoy': -7.9,
            'sales_amount_residential_yoy': -7.6,
            'inventory': 75928,  # 万平方米
            'month': '2025-09',
            'source': '国家统计局',
        },
        
        # 中指院数据
        'real_estate_zhongzhi': {
            'name': '中指院房价数据',
            'new_home_100city': {
                'name': '百城新建商品住宅价格',
                'mom': 0.09,  # 2025年8月
                'yoy': 2.68,
            },
            'second_hand_home': {
                'name': '二手房价格',
                'mom': -0.74,
                'yoy': -7.38,
            },
            'rental_50city': {
                'name': '50城租赁均价',
                'residential_mom': -0.39,
                'residential_yoy': -3.76,
            },
            'top100_sales': {
                'name': 'Top100房企销售',
                'value_1_9': 26065.9,  # 亿元
                'yoy': -12.2,
                'sep_yoy': 11.9,
                'month': '2025-09',
            },
            'tier_city_new_home': {
                'name': '一二三线城市新建住宅价格环比',
                'tier1': -0.3,
                'tier2': -0.4,
                'tier3': -0.4,
            },
            'tier_city_second_hand': {
                'name': '一二三线城市二手住宅价格环比',
                'tier1': -1.0,
                'tier2': -0.7,
                'tier3': -0.6,
            },
            'source': '中指院',
            'month': '2025-08/09',
        },
        
        # ============ 央行政策 ============
        'central_bank': [
            {'name': '7天逆回购利率', 'value': '1.50%', 'date': '2025-02', 'source': '央行'},
            {'name': '14天逆回购利率', 'value': '1.70%', 'date': '2025-02', 'source': '央行'},
            {'name': '1年期LPR', 'value': '3.45%', 'date': '2025-02', 'source': '央行'},
            {'name': '5年期以上LPR', 'value': '3.95%', 'date': '2025-02', 'source': '央行'},
            {'name': 'MLF利率(1年)', 'value': '2.50%', 'date': '2025-02', 'source': '央行'},
            {'name': 'SLF利率(隔夜)', 'value': '2.45%', 'date': '2025-02', 'source': '央行'},
            {'name': '存款准备金率(大型)', 'value': '12.50%', 'date': '2025-02', 'source': '央行'},
            {'name': '存款准备金率(中型)', 'value': '10.50%', 'date': '2025-02', 'source': '央行'},
        ],
        
        # ============ 货币供应 ============
        'm2': {
            'name': 'M2货币供应',
            'value': 318.0,  # 万亿元
            'yoy': 7.3,
            'month': '2025-01',
            'source': '央行',
            'note': '广义货币M2同比'
        },
        'm1': {
            'name': 'M1货币供应',
            'value': 95.0,  # 万亿元
            'yoy': 0.5,
            'month': '2025-01',
            'source': '央行'
        },
        'm0': {
            'name': 'M0货币供应',
            'value': 12.0,  # 万亿元
            'yoy': 12.5,
            'month': '2025-01',
            'source': '央行'
        },
        
        # ============ 新增社融 ============
        'social_financing': {
            'name': '社会融资规模',
            'value': 6.5,  # 万亿元
            'yoy': 9.0,
            'month': '2025-01',
            'source': '央行',
            'note': '2025年1月新增社融'
        }
    }
    
    def save_to_file(self, filepath: str = None) -> str:
        """保存数据到JSON文件"""
        import os
        
        if filepath is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
            data_dir = os.path.join(os.path.dirname(__file__), 'data')
            os.makedirs(data_dir, exist_ok=True)
            filepath = os.path.join(data_dir, f"macro_{date_str}.json")
        
        result = self.get_all()
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        return filepath
    
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
        lines.append("=" * 50)
        
        # GDP
        gdp = data.get('gdp')
        if gdp:
            lines.append(f"\n【{gdp['name']}】")
            lines.append(f"  数值: {gdp.get('value')} 万亿元")
            lines.append(f"  同比: {gdp.get('yoy')}%")
            lines.append(f"  季度: {gdp.get('quarter')}")
        
        # CPI & PPI
        cpi = data.get('cpi')
        ppi = data.get('ppi')
        if cpi or ppi:
            lines.append(f"\n【通胀数据】")
            if cpi:
                lines.append(f"  CPI: 同比 {cpi.get('yoy')}%, 环比 {cpi.get('mom')}%, {cpi.get('month')}")
            if ppi:
                lines.append(f"  PPI: 同比 {ppi.get('yoy')}%, {ppi.get('month')}")
        
        # PMI
        pmi = data.get('pmi')
        if pmi:
            lines.append(f"\n【PMI】")
            lines.append(f"  制造业PMI: {pmi.get('value')}, {pmi.get('month')}")
            pmi_s = data.get('pmi_services')
            if pmi_s:
                lines.append(f"  非制造业: {pmi_s.get('value')}")
        
        # 消费
        retail = data.get('retail')
        if retail:
            lines.append(f"\n【消费】")
            lines.append(f"  社会消费品零售: {retail.get('yoy')}%, {retail.get('month')}")
            online = data.get('online_retail')
            if online:
                lines.append(f"  网上零售额同比: {online.get('yoy')}%")
        
        # 投资
        lines.append(f"\n【投资】")
        fi = data.get('fixed_investment')
        if fi:
            lines.append(f"  固定资产投资: {fi.get('yoy')}%, {fi.get('month')}")
        re = data.get('real_estate_investment')
        if re:
            lines.append(f"  房地产投资: {re.get('yoy')}%, {re.get('month')}")
        mi = data.get('manufacturing_investment')
        if mi:
            lines.append(f"  制造业投资: {mi.get('yoy')}%, {mi.get('month')}")
        
        # 工业
        ia = data.get('industrial_addition')
        if ia:
            lines.append(f"\n【工业】")
            lines.append(f"  工业增加值: {ia.get('yoy')}%, {ia.get('month')}")
        
        # 进出口
        exp = data.get('exports')
        imp = data.get('imports')
        if exp and imp:
            lines.append(f"\n【进出口】")
            lines.append(f"  出口: {exp.get('value')}亿美元, 同比 {exp.get('yoy')}%")
            lines.append(f"  进口: {imp.get('value')}亿美元, 同比 {imp.get('yoy')}%")
            tb = data.get('trade_balance')
            if tb:
                lines.append(f"  贸易顺差: {tb.get('value')}亿美元")
        
        # 央行
        cb = data.get('central_bank', [])
        if cb:
            lines.append(f"\n【央行政策】")
            for item in cb[:5]:
                lines.append(f"  {item['name']}: {item['value']}")
        
        # 货币供应
        m2 = data.get('m2')
        if m2:
            lines.append(f"\n【货币供应】")
            lines.append(f"  M2: {m2.get('value')}万亿元, 同比 {m2.get('yoy')}%")
        
        # 社融
        sf = data.get('social_financing')
        if sf:
            lines.append(f"\n【社会融资】")
            lines.append(f"  新增: {sf.get('value')}万亿元, 同比 {sf.get('yoy')}%")
        
        # 房地产
        re = data.get('real_estate')
        if re:
            lines.append(f"\n【房地产】")
            lines.append(f"  投资: {re.get('investment_yoy')}%")
            lines.append(f"  销售: {re.get('sales_yoy')}%")
        
        return "\n".join(lines)


def main():
    import sys
    import os
    
    data = MacroEconomyData()
    
    if '--save' in sys.argv:
        # 保存到文件
        filepath = data.save_to_file()
        print(f"✅ 数据已保存到: {filepath}")
    elif len(sys.argv) > 1 and sys.argv[1] == '--json':
        result = data.get_all()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(data.get_summary())


if __name__ == "__main__":
    main()
