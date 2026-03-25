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

# 一年交易日
TRADING_DAYS_PER_YEAR = 250  # A股一年交易日

# 指数代码映射
INDEX_CODE_MAP = {
    'sh000001': '上证指数',
    'sz399001': '深证成指',
    'sh000300': '沪深300',
    'sh000905': '中证500',
    'sh000852': '中证1000',
    'sh000804': '中证2000',
    'sh000016': '上证50',
    'sh000688': '科创50',
    'sz399006': '创业板指',
}

# 期货与现货映射
FUTURES_SPOT_MAP = {
    'IF': ('sh000300', '沪深300'),
    'IC': ('sh000905', '中证500'),
    'IM': ('sh000852', '中证1000'),
    'IH': ('sh000016', '上证50'),
}


class Analyzer:
    """数据分析器"""
    
    def __init__(self, storage: Storage = None):
        self.storage = storage or Storage()
    
    def analyze(self, date: str = None) -> Dict:
        """
        执行完整分析（从文件读取数据）
        
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
        basis_analysis = self.analyze_basis(market_data, futures_data, date)
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
    
    def analyze_from_data(self, data: Dict) -> Dict:
        """
        执行完整分析（直接传入数据）
        
        Args:
            data: 包含 stock, futures, news 的字典
        
        Returns:
            分析结果
        """
        date = datetime.now().strftime("%Y-%m-%d")
        
        # 处理行情数据
        market_data = None
        if 'stock' in data and data['stock']:
            stock = data['stock']
            # StockCollector返回的是 {'date':..., 'indices':[...]}
            # 需要转换为 {'data': {'indices': [...]}}
            if stock and 'indices' in stock:
                indices = stock.get('indices', [])
            elif stock:
                indices = stock.get('data', {}).get('indices', [])
            else:
                indices = []
            market_data = {'date': date, 'data': indices}
        
        # 处理期货数据
        futures_data = data.get('futures', [])
        
        # 处理新闻数据
        news_data = data.get('news', {})
        # 确保news_data有正确的格式
        if isinstance(news_data, dict) and 'data' in news_data:
            news_data = news_data['data']
        # 再次检查是否有嵌套的data
        if isinstance(news_data, dict) and 'data' in news_data:
            news_data = news_data['data']
        # news应该是列表
        if isinstance(news_data, dict) and 'news' in news_data:
            news_data = {'data': news_data['news']}
        
        # 执行各项分析
        # 先获取 volume_history
        volume_history = data.get('volume_history')
        
        market_analysis = self.analyze_market(market_data)
        
        # 将 volume_history 传入
        if volume_history and volume_history.get('daily_amounts'):
            market_analysis['volume_history'] = volume_history
        
        basis_analysis = self.analyze_basis_from_data(market_data, futures_data, date)
        news_analysis = self.analyze_news(news_data)
        
        # 将基差分析加入market_analysis
        market_analysis['basis'] = basis_analysis
        
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
            key=lambda x: x.get('pct_chg', 0),
            reverse=True
        )
        
        daily_changes = [
            {
                'name': item.get('name', ''),
                'code': item.get('code', ''),
                'close': item.get('close', 0),
                'change_percent': item.get('pct_chg', 0),
                'volume': item.get('volume', 0),
                'amount': item.get('amount', 0),  # 单位是千元
            }
            for item in changes
        ]
        
        # 成交量趋势 - 从 market_data 中获取
        volume_history = market_data.get('volume_history', {})
        if volume_history and volume_history.get('daily_amounts'):
            daily_amounts = volume_history.get('daily_amounts', [])
            avg_volume = sum(daily_amounts) / len(daily_amounts) if daily_amounts else 0
            total_volume = sum(daily_amounts) if daily_amounts else 0
        else:
            # 备选：使用当日数据
            volumes = [item.get('amount', 0) / 100000 for item in changes]  # 千元转亿元
            avg_volume = sum(volumes) / len(volumes) if volumes else 0
            total_volume = sum(volumes) if volumes else 0
        
        # 今日成交额
        today_amount = sum(item.get('amount', 0) / 100000 for item in changes) if changes else 0
        
        if avg_volume > 0:
            if today_amount > avg_volume * 1.2:
                trend = "放量上涨 📈"
                interpretation = f"较昨日放量{(today_amount/avg_volume - 1)*100:.1f}%，市场活跃度提升"
            elif today_amount < avg_volume * 0.8:
                trend = "缩量下跌 📉"
                interpretation = f"较昨日缩量{(1 - today_amount/avg_volume)*100:.1f}%，市场活跃度下降"
            else:
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
                'avg_volume': avg_volume,
                'today_amount': today_amount
            }
        }
    
    def analyze_basis(self, market_data: Optional[Dict], futures_data: Optional[Dict], date: str = None) -> List[Dict]:
        """
        分析基差 - 从TuShare实时获取数据（从文件读取时使用）
        
        Args:
            market_data: 现货数据
            futures_data: 期货数据（未使用，现在从TuShare实时获取）
            date: 日期
        
        Returns:
            基差分析列表
        """
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        try:
            import tushare as ts
            from collectors.base import get_tushare_token
            
            token = get_tushare_token()
            if not token:
                return []
            
            pro = ts.pro_api(token)
        except Exception as e:
            self.logger.warning(f"TuShare connection failed: {e}")
            return []
        
        results = []
        
        # 只获取隔季合约
        contracts = [
            ('IF', '沪深300', '隔季', 'IF2609', 136),
            ('IC', '中证500', '隔季', 'IC2609', 136),
            ('IH', '上证50', '隔季', 'IH2609', 136),
            ('IM', '中证1000', '隔季', 'IM2609', 136),
        ]
        
        # 获取指数收盘价
        index_codes = {
            'sh000300': '000300.SH',
            'sh000905': '000905.SH',
            'sh000016': '000016.SH',
            'sh000852': '000852.SH',
        }
        
        spot_prices = {}
        for sh_code, ts_code in index_codes.items():
            try:
                df = pro.index_daily(ts_code=ts_code, start_date=date, end_date=date)
                if len(df) > 0:
                    spot_prices[sh_code] = df.iloc[-1]['close']
            except:
                pass
        
        # 获取期货数据
        for fut_code, index_name, contract_type, contract_name, days_to_expiry in contracts:
            spot_code = FUTURES_SPOT_MAP.get(fut_code, ('', ''))[0] if isinstance(FUTURES_SPOT_MAP[fut_code], tuple) else FUTURES_SPOT_MAP[fut_code]
            spot_price = spot_prices.get(spot_code)
            
            if not spot_price:
                continue
            
            # 获取期货结算价
            try:
                df_fut = pro.fut_daily(ts_code=f"{contract_name}.CFX", trade_date=date)
                if len(df_fut) > 0:
                    futures_price = df_fut.iloc[-1]['settle']
                else:
                    continue
            except:
                continue
            
            if not futures_price:
                continue
            
            # 计算基差
            basis = futures_price - spot_price
            basis_percent = (basis / spot_price) * 100
            
            # 计算年化基差率
            annualized_basis = basis_percent * (TRADING_DAYS_PER_YEAR / days_to_expiry)
            
            results.append({
                'index': fut_code,
                'index_name': index_name,
                'contract': contract_type,
                'futures_price': round(futures_price, 2),
                'spot_price': round(spot_price, 2),
                'basis': round(basis, 2),
                'basis_percent': round(basis_percent, 2),
                'annualized_basis': round(annualized_basis, 2),
                'trading_days': days_to_expiry,
            })
        
        # 按年化基差率排序
        results.sort(key=lambda x: x['annualized_basis'], reverse=True)
        
        return results
    
    def analyze_basis_from_data(self, market_data: Optional[Dict], futures_data: List[Dict], date: str = None) -> List[Dict]:
        """
        分析基差 - 使用直接传入的期货数据
        
        Args:
            market_data: 现货数据
            futures_data: 期货数据列表
            date: 日期
        
        Returns:
            基差分析列表
        """
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        results = []
        
        if not market_data or not market_data.get('data'):
            return results
        
        # 构建现货价格映射
        spot_prices = {}
        for item in market_data['data']:
            code = item.get('code', '')
            name = item.get('name', '')
            close = item.get('close', 0)
            
            # 指数代码映射
            if '沪深300' in name or code == '000300':
                spot_prices['sh000300'] = close
            elif '中证500' in name or code == '000905':
                spot_prices['sh000905'] = close
            elif '上证50' in name or code == '000016':
                spot_prices['sh000016'] = close
            elif '中证1000' in name or code == '000852':
                spot_prices['sh000852'] = close
        
        # 处理期货数据（只使用隔季合约）
        for f in futures_data:
            code = f.get('code', '')
            name = f.get('name', '')
            contract = f.get('contract', '')
            
            # 只处理隔季合约
            if '2609' not in contract:
                continue
            
            # 优先使用期货数据中自带的现货价格，否则从spot_prices获取
            spot_price = f.get('spot_price')
            if not spot_price:
                spot_code_map = {
                    'IF': 'sh000300',
                    'IC': 'sh000905',
                    'IH': 'sh000016',
                    'IM': 'sh000852'
                }
                spot_code = spot_code_map.get(code)
                spot_price = spot_prices.get(spot_code)
            
            if not spot_price:
                continue
            
            # 使用收盘价计算基差
            futures_price = f.get('close', 0)
            if not futures_price:
                continue
            
            # 计算基差
            basis = futures_price - spot_price
            basis_percent = (basis / spot_price) * 100
            
            # 年化基差率（使用收盘价，距到期136天）
            days_to_expiry = 136
            annualized_basis = basis_percent * (TRADING_DAYS_PER_YEAR / days_to_expiry)
            
            results.append({
                'index': name,
                'code': code,
                'contract': '隔季',
                'futures_price': futures_price,
                'spot_price': spot_price,
                'basis': round(basis, 2),
                'basis_percent': round(basis_percent, 2),
                'annualized_basis': round(annualized_basis, 2),
                'trading_days': days_to_expiry
            })
        
        # 按年化基差率排序
        results.sort(key=lambda x: x['annualized_basis'], reverse=True)
        
        return results
    
    def analyze_news(self, news_data: Optional[Dict]) -> Dict:
        """分析新闻数据"""
        if not news_data or not news_data.get('data'):
            return {
                'count': 0,
                'summary': '无新闻数据',
                'high_importance': [],
                'all_news': [],
                'categories': {},
                'sentiment': '中性'
            }
        
        data = news_data['data']
        
        # 所有新闻（用于展示标题）
        all_news = [
            {
                'title': item.get('title', ''),
                'category': item.get('category', ''),
                'source': item.get('source', ''),
            }
            for item in data
        ][:20]  # 保留最新20条
        
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
            'all_news': all_news,
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
