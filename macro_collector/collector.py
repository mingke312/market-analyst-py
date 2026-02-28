#!/usr/bin/env python3
"""
宏观数据收集模块 - 基于 Akshare
"""

import akshare as ak
from typing import Dict, List, Optional
from datetime import datetime


class MacroCollector:
    """宏观数据收集器 - 基于 Akshare"""
    
    def fetch_china_indices(self) -> List[Dict]:
        """获取中国主要指数"""
        results = []
        try:
            df = ak.stock_zh_index_spot_em()
            
            codes = ['sh000001', 'sh000300', 'sh000905', 'sh000852', 'sz399001', 'sh000016', 'sh000688', 'sz399006']
            names = {'sh000001': '上证指数', 'sh000300': '沪深300', 'sh000905': '中证500', 
                    'sh000852': '中证1000', 'sz399001': '深证成指', 'sh000016': '上证50', 
                    'sh000688': '科创50', 'sz399006': '创业板指'}
            
            for code in codes:
                row = df[df['代码'] == code]
                if not row.empty:
                    price = row.iloc[0]['最新价']
                    change = row.iloc[0]['涨跌幅']
                    results.append({
                        'name': names.get(code, code),
                        'code': code,
                        'price': float(price) if price != '--' else 0,
                        'change_percent': float(change) if change != '--' else 0,
                        'currency': 'CNY'
                    })
        except Exception as e:
            print(f"获取A股指数失败: {e}")
        
        return results
    
    def fetch_hk_index(self) -> Optional[Dict]:
        """获取港股恒生指数"""
        try:
            df = ak.stock_hk_index_spot_em()
            row = df[df['代码'] == 'HSI']
            if not row.empty:
                return {
                    'name': '恒生指数',
                    'code': 'HSI',
                    'price': float(row.iloc[0]['最新价']),
                    'change_percent': float(row.iloc[0]['涨跌幅']),
                    'currency': 'HKD'
                }
        except Exception as e:
            print(f"获取港股指数失败: {e}")
        
        return None
    
    def fetch_us_indices(self) -> List[Dict]:
        """获取美股指数"""
        results = []
        try:
            # 道琼斯工业平均指数
            df = ak.index_usdj(symbol="DJI")
            if not df.empty:
                latest = df.iloc[-1]
                results.append({
                    'name': '道琼斯工业指数',
                    'code': 'DJI',
                    'price': float(latest['收盘']),
                    'change_percent': float(latest['涨跌幅']) if '涨跌幅' in latest else 0,
                    'currency': 'USD'
                })
        except Exception as e:
            print(f"获取美股指数失败: {e}")
        
        return results
    
    def fetch_gold_price(self) -> Optional[Dict]:
        """获取黄金价格"""
        try:
            df = ak.futures_cj伦敦金属()
            for _, row in df.iterrows():
                name = str(row.get('品种', ''))
                if '黄金' in name or 'Au' in name:
                    return {
                        'name': '伦敦金',
                        'price': float(row.get('最新价', 0)),
                        'unit': '美元/盎司',
                        'currency': 'USD'
                    }
        except Exception as e:
            print(f"获取黄金价格失败: {e}")
        
        return None
    
    def fetch_china_bonds(self) -> List[Dict]:
        """获取中国国债收益率"""
        results = []
        try:
            df = ak.bond_china_yield()
            if not df.empty:
                for _, row in df.head(5).iterrows():
                    results.append({
                        'name': f"国债{row.get('期限', '')}年",
                        'yield': float(row.get('收益率', 0)),
                        'currency': 'CNY'
                    })
        except Exception as e:
            print(f"获取国债收益率失败: {e}")
        
        return results
    
    def fetch_macro_gdp(self) -> Optional[Dict]:
        """获取中国GDP"""
        try:
            df = ak.macro_china_gdp()
            if not df.empty:
                latest = df.iloc[-1]
                return {
                    'name': '中国GDP',
                    'value': float(latest.get('GDP', 0)),
                    'yoy': float(latest.get('GDP同比', 0)),
                    'quarter': str(latest.get('季度', ''))
                }
        except Exception as e:
            print(f"获取GDP失败: {e}")
        
        return None
    
    def fetch_macro_cpi(self) -> Optional[Dict]:
        """获取中国CPI"""
        try:
            df = ak.macro_china_cpi()
            if not df.empty:
                latest = df.iloc[-1]
                return {
                    'name': '中国CPI',
                    'value': float(latest.get('CPI', 0)),
                    'yoy': float(latest.get('CPI同比', 0)),
                    'month': str(latest.get('月份', ''))
                }
        except Exception as e:
            print(f"获取CPI失败: {e}")
        
        return None
    
    def collect_all(self) -> Dict:
        """收集所有宏观数据"""
        
        result = {
            'date': datetime.now().strftime("%Y-%m-%d"),
            'type': 'macro',
            'timestamp': datetime.now().isoformat(),
            'data': {}
        }
        
        print("📊 获取A股指数...")
        result['data']['china_indices'] = self.fetch_china_indices()
        
        print("📊 获取港股指数...")
        result['data']['hk_index'] = self.fetch_hk_index()
        
        print("📊 获取宏观指标...")
        gdp = self.fetch_macro_gdp()
        cpi = self.fetch_macro_cpi()
        
        result['data']['macro'] = {}
        if gdp:
            result['data']['macro']['gdp'] = gdp
        if cpi:
            result['data']['macro']['cpi'] = cpi
        
        print("📊 获取国债收益率...")
        result['data']['bonds'] = self.fetch_china_bonds()
        
        return result


def main():
    import json
    import sys
    
    collector = MacroCollector()
    result = collector.collect_all()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--json':
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print("=" * 50)
        print("📊 宏观数据 (基于 Akshare)")
        print("=" * 50)
        
        data = result['data']
        
        if data.get('china_indices'):
            print(f"\n【A股指数】")
            for idx in data['china_indices']:
                print(f"  {idx['name']}: {idx['price']:.2f} ({idx['change_percent']:+.2f}%)")
        
        if data.get('hk_index'):
            print(f"\n【港股】")
            idx = data['hk_index']
            print(f"  {idx['name']}: {idx['price']:.2f} ({idx['change_percent']:+.2f}%)")
        
        if data.get('macro'):
            print(f"\n【宏观指标】")
            for name, item in data['macro'].items():
                print(f"  {item['name']}: {item.get('value', 'N/A')} (同比: {item.get('yoy', 'N/A')}%)")
        
        if data.get('bonds'):
            print(f"\n【国债收益率】")
            for bond in data['bonds'][:3]:
                print(f"  {bond['name']}: {bond['yield']:.2f}%")
        
        print("\n✅ 数据获取完成")


if __name__ == "__main__":
    main()
