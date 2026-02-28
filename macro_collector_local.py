#!/usr/bin/env python3
"""
宏观数据收集器 - 本地运行版
用于在没有网络限制的环境下运行（如本地电脑）

使用方法:
    python3 macro_collector_local.py

输出:
    macro_YYYY-MM-DD.json
"""

import akshare as ak
from datetime import datetime
import json
import os


class MacroCollectorLocal:
    """本地版宏观数据收集器"""
    
    def __init__(self):
        self.date = datetime.now().strftime("%Y-%m-%d")
    
    def collect_all(self):
        """收集所有数据"""
        
        print(f"📊 开始收集宏观数据 ({self.date})")
        print("=" * 50)
        
        result = {
            'date': self.date,
            'type': 'macro',
            'timestamp': datetime.now().isoformat(),
            'data': {}
        }
        
        # A股指数
        print("📈 获取A股指数...")
        try:
            df = ak.stock_zh_index_spot_em()
            codes = {
                'sh000001': '上证指数', 'sh000300': '沪深300', 'sh000905': '中证500',
                'sh000852': '中证1000', 'sz399001': '深证成指', 'sh000016': '上证50',
                'sh000688': '科创50', 'sz399006': '创业板指'
            }
            china_indices = []
            for code, name in codes.items():
                row = df[df['代码'] == code]
                if not row.empty:
                    price = row.iloc[0]['最新价']
                    change = row.iloc[0]['涨跌幅']
                    china_indices.append({
                        'name': name, 'code': code,
                        'price': float(price) if price != '--' else 0,
                        'change_percent': float(change) if change != '--' else 0,
                        'currency': 'CNY'
                    })
            result['data']['china_indices'] = china_indices
            print(f"   ✅ 获取 {len(china_indices)} 个指数")
        except Exception as e:
            print(f"   ❌ 获取失败: {e}")
            result['data']['china_indices'] = []
        
        # 港股
        print("📈 获取港股指数...")
        try:
            df = ak.stock_hk_index_spot_em()
            row = df[df['代码'] == 'HSI']
            if not row.empty:
                result['data']['hk_index'] = {
                    'name': '恒生指数', 'code': 'HSI',
                    'price': float(row.iloc[0]['最新价']),
                    'change_percent': float(row.iloc[0]['涨跌幅']),
                    'currency': 'HKD'
                }
                print(f"   ✅ 恒生指数")
        except Exception as e:
            print(f"   ❌ 获取失败: {e}")
        
        # 美股
        print("📈 获取美股指数...")
        try:
            # 道琼斯
            df = ak.index_usdj(symbol="DJI")
            if not df.empty:
                latest = df.iloc[-1]
                result['data']['us_indices'] = [{
                    'name': '道琼斯', 'code': 'DJI',
                    'price': float(latest['收盘']),
                    'change_percent': float(latest.get('涨跌幅', 0)),
                    'currency': 'USD'
                }]
                print(f"   ✅ 道琼斯指数")
        except Exception as e:
            print(f"   ❌ 获取失败: {e}")
        
        # 黄金
        print("📈 获取黄金价格...")
        try:
            df = ak.futures_cj伦敦金属()
            for _, row in df.iterrows():
                name = str(row.get('品种', ''))
                if '黄金' in name or 'Au' in name:
                    result['data']['gold'] = {
                        'name': '伦敦金',
                        'price': float(row.get('最新价', 0)),
                        'unit': '美元/盎司',
                        'currency': 'USD'
                    }
                    print(f"   ✅ 伦敦金")
                    break
        except Exception as e:
            print(f"   ❌ 获取失败: {e}")
        
        # 原油
        print("📈 获取原油价格...")
        try:
            df = ak.futures_cj能源()
            if not df.empty:
                row = df.iloc[0]
                result['data']['oil'] = {
                    'name': row.get('品种', '原油'),
                    'price': float(row.get('最新价', 0)),
                    'unit': '美元/桶',
                    'currency': 'USD'
                }
                print(f"   ✅ 原油价格")
        except Exception as e:
            print(f"   ❌ 获取失败: {e}")
        
        # 国债收益率
        print("📈 获取国债收益率...")
        try:
            df = ak.bond_china_yield()
            bonds = []
            for _, row in df.head(5).iterrows():
                bonds.append({
                    'name': f"国债{row.get('期限', '')}年",
                    'yield': float(row.get('收益率', 0)),
                    'currency': 'CNY'
                })
            result['data']['bonds'] = bonds
            print(f"   ✅ {len(bonds)} 个期限")
        except Exception as e:
            print(f"   ❌ 获取失败: {e}")
        
        # GDP
        print("📈 获取GDP...")
        try:
            df = ak.macro_china_gdp()
            if not df.empty:
                latest = df.iloc[-1]
                result['data']['macro'] = {
                    'gdp': {
                        'name': '中国GDP',
                        'value': float(latest.get('GDP', 0)),
                        'yoy': float(latest.get('GDP同比', 0)),
                        'quarter': str(latest.get('季度', ''))
                    }
                }
                print(f"   ✅ GDP")
        except Exception as e:
            print(f"   ❌ 获取失败: {e}")
        
        # CPI
        print("📈 获取CPI...")
        try:
            df = ak.macro_china_cpi()
            if not df.empty:
                latest = df.iloc[-1]
                if 'macro' not in result['data']:
                    result['data']['macro'] = {}
                result['data']['macro']['cpi'] = {
                    'name': '中国CPI',
                    'value': float(latest.get('CPI', 0)),
                    'yoy': float(latest.get('CPI同比', 0)),
                    'month': str(latest.get('月份', ''))
                }
                print(f"   ✅ CPI")
        except Exception as e:
            print(f"   ❌ 获取失败: {e}")
        
        print("=" * 50)
        print("✅ 数据收集完成!")
        
        return result
    
    def save(self, result):
        """保存到文件"""
        # 创建输出目录
        output_dir = os.path.join(os.path.dirname(__file__), 'data')
        os.makedirs(output_dir, exist_ok=True)
        
        filename = f"macro_{self.date}.json"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"\n📁 数据已保存到: {filepath}")
        return filepath


def main():
    collector = MacroCollectorLocal()
    result = collector.collect_all()
    collector.save(result)
    
    print("\n" + "=" * 50)
    print("📊 数据汇总")
    print("=" * 50)
    
    data = result['data']
    
    if data.get('china_indices'):
        print(f"A股指数: {len(data['china_indices'])}个")
        for idx in data['china_indices'][:3]:
            print(f"  {idx['name']}: {idx['price']:.2f} ({idx['change_percent']:+.2f}%)")
    
    if data.get('hk_index'):
        idx = data['hk_index']
        print(f"港股: {idx['name']} {idx['price']:.2f}")
    
    if data.get('us_indices'):
        for idx in data['us_indices']:
            print(f"美股: {idx['name']} {idx['price']:.2f}")
    
    if data.get('gold'):
        print(f"黄金: {data['gold']['price']:.2f} 美元/盎司")
    
    if data.get('oil'):
        print(f"原油: {data['oil']['price']:.2f} 美元/桶")
    
    if data.get('macro'):
        for name, item in data['macro'].items():
            print(f"{item['name']}: {item.get('value', 'N/A')}")


if __name__ == "__main__":
    main()
