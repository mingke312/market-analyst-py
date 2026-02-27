#!/usr/bin/env python3
"""
数据分析模块
分析行情、期货、新闻数据，生成分析结果
"""

from typing import Dict, List, Optional
from datetime import datetime
import json

from storage.storage import Storage
from utils.trading_calendar import get_trading_days_to_expiry, get_contract_expiry


# 指数代码映射
INDEX_CODE_MAP = {
    'sh000001': '上证指数',
    'sz399001': '深证成指',
    'sh000300': '沪深300',
    'sh000905': '中证500',
    'sh000852': '中证1000',
    'sh000016': '上证50',
    'sh000688': '科创50',
    'sz399006': '创业板指',
}

# 期货与现货映射
FUTURES_SPOT_MAP = {
    'IF': 'sh000300',  # 沪深300
    'IC': 'sh000905',  # 中证500
    'IM': 'sh000852',  # 中证1000
    'IH': 'sh000016',  # 上证50
}


class Analyzer:
    """数据分析器"""
    
    def __init__(self, storage: Storage = None):
        self.storage = storage or Storage()
    
    def analyze(self, date: str = None) -> Dict:
        """
        执行完整分析
        
        Args:
            date: 日期，默认今天
        
        Returns:
            分析结果
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        # 加载数据
        market_data = self.storage.load_market(date)
        futures_data = self.storage.load_futures(date)
        news_data = self.storage.load_news(date)
        
        # 执行各项分析
        market_analysis = self.analyze_market(market_data)
        basis_analysis = self.analyze_basis(market_data, futures_data)
        news_analysis = self.analyze_news(news_data)
        
        # 生成结论
        conclusion = self.generate_conclusion(market_analysis, basis_analysis, news_analysis)
        
        return {
            'date': date,
            'market': market_analysis,
            'basis': basis_analysis,
            'news': news_analysis,
            'conclusion': conclusion,
            'timestamp': datetime.now().isoformat()
        }
    
    def analyze_market(self, market_data: Optional[Dict]) -> Dict:
        """分析行情数据"""
        if not market_data or not market_data.get('data'):
            return {
                'changes': {'daily': []},
                'volume': {'trend': '数据不足', 'interpretation': '需要更多历史数据'}
            }
        
        data = market_data['data']
        
        # 涨跌幅排行
        changes = sorted(
            data,
            key=lambda x: x.get('change_percent', 0),
            reverse=True
        )
        
        daily_changes = [
            {
                'name': item.get('name', ''),
                'code': item.get('code', ''),
                'price': item.get('price', 0),
                'change_percent': item.get('change_percent', 0)
            }
            for item in changes
        ]
        
        # 成交量趋势（简化版，需要历史数据）
        volumes = [item.get('volume', 0) for item in data]
        avg_volume = sum(volumes) / len(volumes) if volumes else 0
        
        if avg_volume > 0:
            trend = "正常"
            interpretation = "成交量处于正常水平"
        else:
            trend = "数据不足"
            interpretation = "需要更多历史数据"
        
        return {
            'changes': {
                'daily': daily_changes
            },
            'volume': {
                'trend': trend,
                'interpretation': interpretation,
                'avg_volume': avg_volume
            }
        }
    
    def analyze_basis(self, market_data: Optional[Dict], futures_data: Optional[Dict]) -> List[Dict]:
        """
        分析基差
        
        Args:
            market_data: 现货数据
            futures_data: 期货数据
        
        Returns:
            基差分析列表
        """
        if not market_data or not futures_data:
            return []
        
        # 构建现货价格映射
        spot_prices = {}
        for item in market_data.get('data', []):
            code = item.get('code')
            price = item.get('price')
            if code and price:
                spot_prices[code] = price
        
        results = []
        
        futures = futures_data.get('data', {})
        
        for futures_code, contracts in futures.items():
            # 获取对应的现货指数代码
            spot_code = FUTURES_SPOT_MAP.get(futures_code)
            spot_price = spot_prices.get(spot_code, 0)
            
            if not spot_price:
                continue
            
            for contract_type, contract_data in contracts.items():
                if not contract_data:
                    continue
                
                futures_price = contract_data.get('price', 0)
                if not futures_price:
                    continue
                
                # 计算基差
                basis = futures_price - spot_price
                basis_percent = (basis / spot_price) * 100
                
                # 计算距到期日
                trading_days = get_trading_days_to_expiry(contract_type)
                
                # 如果距0交易日（已到期），使用下个周期
                if trading_days <= 0:
                    trading_days = 15 if contract_type == '当月' else (35 if contract_type == '下季' else 70)
                
                # 计算年化基差率
                annualized_basis = basis_percent * (365 / trading_days)
                
                results.append({
                    'index': futures_code,
                    'index_name': INDEX_CODE_MAP.get(spot_code, futures_code),
                    'contract': contract_type,
                    'futures_price': futures_price,
                    'spot_price': spot_price,
                    'basis': round(basis, 2),
                    'basis_percent': round(basis_percent, 2),
                    'annualized_basis': round(annualized_basis, 2),
                    'trading_days': trading_days,
                })
        
        # 按年化基差率排序（从大到小）
        results.sort(key=lambda x: x['annualized_basis'], reverse=True)
        
        return results
    
    def analyze_news(self, news_data: Optional[Dict]) -> Dict:
        """分析新闻数据"""
        if not news_data or not news_data.get('data'):
            return {
                'count': 0,
                'summary': '无新闻数据',
                'high_importance': [],
                'categories': {},
                'sentiment': '中性'
            }
        
        data = news_data['data']
        
        # 高重要性新闻
        high_importance = [
            {
                'title': item.get('title', ''),
                'category': item.get('category', ''),
                'source': item.get('source', ''),
                'importance': item.get('importance', '')
            }
            for item in data
            if item.get('importance') == '高'
        ][:5]
        
        # 分类统计
        categories = {}
        for item in data:
            cat = item.get('category', '其他')
            categories[cat] = categories.get(cat, 0) + 1
        
        # 市场情绪判断
        sentiment = self._judge_sentiment(data)
        
        return {
            'count': len(data),
            'summary': f'共{len(data)}条新闻',
            'high_importance': high_importance,
            'categories': categories,
            'sentiment': sentiment
        }
    
    def _judge_sentiment(self, news_data: List[Dict]) -> str:
        """判断市场情绪"""
        positive_keywords = ['利好', '上涨', '涨停', '突破', '增长', '反弹', '大涨', '看涨']
        negative_keywords = ['利空', '下跌', '跌停', '回落', '下滑', '大跌', '看跌', '风险']
        
        positive_count = 0
        negative_count = 0
        
        for item in news_data:
            text = (item.get('title', '') + item.get('summary', '')).lower()
            
            if any(kw in text for kw in positive_keywords):
                positive_count += 1
            if any(kw in text for kw in negative_keywords):
                negative_count += 1
        
        if positive_count > negative_count + 2:
            return '偏多'
        elif negative_count > positive_count + 2:
            return '偏空'
        else:
            return '中性'
    
    def generate_conclusion(
        self,
        market_analysis: Dict,
        basis_analysis: List[Dict],
        news_analysis: Dict
    ) -> Dict:
        """生成综合结论"""
        # 市场判断
        market_views = []
        
        # 基差判断
        if basis_analysis:
            discounts = sum(1 for b in basis_analysis if b['basis'] < 0)
            premiums = sum(1 for b in basis_analysis if b['basis'] > 0)
            
            if discounts > premiums:
                market_views.append("期货整体贴水，市场预期偏空")
            elif premiums > discounts:
                market_views.append("期货整体升水，市场预期偏多")
            else:
                market_views.append("期货基差平衡")
        
        # 风险提示
        risk_alerts = []
        
        if news_analysis.get('categories', {}).get('宏观政策'):
            risk_alerts.append("有宏观政策相关新闻，建议关注")
        
        # 投资建议
        recommendations = []
        
        sentiment = news_analysis.get('sentiment', '中性')
        if sentiment == '偏多':
            recommendations.append("建议适度加仓")
        elif sentiment == '偏空':
            recommendations.append("建议保持谨慎")
        else:
            recommendations.append("建议保持观望")
        
        # 仓位建议
        if market_views and '偏空' in market_views[0]:
            recommendations.append("控制仓位在50%-70%")
        
        return {
            'market_view': market_views[0] if market_views else "震荡整理",
            'risk_alerts': risk_alerts,
            'recommendations': recommendations
        }


def main():
    """命令行入口"""
    import sys
    
    analyzer = Analyzer()
    
    date = sys.argv[1] if len(sys.argv) > 1 else None
    result = analyzer.analyze(date)
    
    if len(sys.argv) > 2 and sys.argv[2] == '--json':
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        # 打印涨跌幅排行
        print("📈 涨跌幅排行")
        print("-" * 40)
        
        changes = result['market']['changes']['daily']
        for item in changes:
            change = f"{item['change_percent']:+.2f}%"
            print(f"{item['name']:8s}: {item['price']:>8.2f}  {change}")
        
        # 打印基差分析
        if result['basis']:
            print("\n📊 基差分析")
            print("-" * 40)
            
            for item in result['basis']:
                arrow = "↓" if item['basis'] < 0 else "↑"
                ann = f"{item['annualized_basis']:+.2f}%"
                print(f"{item['index']}{item['contract']:2s}: 现{item['spot_price']:.2f} 期{item['futures_price']:.2f} {arrow}{abs(item['basis']):.2f} ({ann})")
        
        # 打印结论
        print("\n📋 综合结论")
        print("-" * 40)
        print(f"判断: {result['conclusion']['market_view']}")
        for rec in result['conclusion']['recommendations']:
            print(f"- {rec}")


if __name__ == "__main__":
    main()
