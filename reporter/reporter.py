#!/usr/bin/env python3
"""
报告生成模块
生成Markdown格式的每日报告
"""

from typing import Dict, List
from datetime import datetime
import json


class Reporter:
    """报告生成器"""
    
    def __init__(self):
        pass
    
    def generate(self, analysis: Dict) -> str:
        """
        生成报告
        
        Args:
            analysis: 分析结果
        
        Returns:
            Markdown格式报告
        """
        date = analysis.get('date', datetime.now().strftime("%Y-%m-%d"))
        
        lines = []
        
        # 标题
        lines.append("# 📈 市场每日简报")
        lines.append(f"**日期**: {date}")
        lines.append("")
        lines.append("---")
        lines.append("")
        
        # 一、行情分析
        lines.extend(self._generate_market_section(analysis.get('market', {}), analysis.get('basis', [])))
        
        # 二、新闻分析
        lines.extend(self._generate_news_section(analysis.get('news', {})))
        
        # 三、综合结论
        lines.extend(self._generate_conclusion_section(analysis.get('conclusion', {})))
        
        # 结尾
        lines.append("")
        lines.append("---")
        lines.append("")
        lines.append("*本报告由 AI 自动生成*")
        
        return "\n".join(lines)
    
    def _generate_market_section(self, market: Dict, basis: List[Dict] = None) -> List[str]:
        """生成行情分析部分"""
        if basis is None:
            basis = []
        
        lines = []
        
        lines.append("## 一、行情分析")
        lines.append("")
        
        # 1.1 涨跌幅排行
        lines.append("### 1.1 涨跌幅排行")
        lines.append("")
        lines.append("| 指数 | 收盘价 | 涨跌幅 | 成交额(亿) |")
        lines.append("|:-----|-------:|-------:|----------:|")
        
        changes = market.get('changes', {}).get('daily', [])
        for item in changes:
            change = item.get('change_percent', 0)
            change_str = f"**{change:+.2f}%**" if change > 0 else f"{change:+.2f}%"
            # TuShare的amount单位是千元，转换为亿：除以100000
            amount = item.get('amount', 0)
            if amount and amount > 0:
                amount_str = f"{amount / 1e5:.1f}"
            else:
                amount_str = "-"
            # 收盘价
            close = item.get('close', 0)
            if close:
                close_str = f"{close:.2f}"
            else:
                close_str = "-"
            lines.append(f"| {item['name']} | {close_str} | {change_str} | {amount_str} |")
        
        lines.append("")
        
        # 1.2 成交量分析
        lines.append("### 1.2 成交量分析")
        lines.append("")
        
        # 获取历史成交量数据
        volume_history = market.get('volume_history', {})
        volume_data = market.get('changes', {}).get('daily', [])
        
        if volume_history and volume_history.get('daily_amounts'):
            # 使用历史数据计算平均
            historical_amounts = volume_history.get('daily_amounts', [])
            avg_amount = sum(historical_amounts) / len(historical_amounts) if historical_amounts else 0
            days_count = len(historical_amounts)
            
            # 今日成交额
            sse_amount = next((item.get('amount', 0) for item in volume_data if '上证' in item.get('name', '')), 0)
            sz_amount = next((item.get('amount', 0) for item in volume_data if '深证' in item.get('name', '')), 0)
            total_amount = sse_amount + sz_amount
            
            # 找出最大和最小成交额（从历史数据）
            max_vol = ('深证成指', max(historical_amounts)) if historical_amounts else ('', 0)
            min_vol = ('科创50', min(historical_amounts)) if historical_amounts else ('', 0)
            
            # 判断趋势
            if len(historical_amounts) >= 2:
                latest = historical_amounts[0] if historical_amounts else 0
                prev = historical_amounts[1] if len(historical_amounts) > 1 else 0
                if latest > prev * 1.1:
                    trend = "放量上涨 📈"
                    interpretation = f"较昨日放量{((latest/prev)-1)*100:.1f}%，市场活跃度提升"
                elif latest < prev * 0.9:
                    trend = "缩量下跌 📉"
                    interpretation = f"较昨日缩量{((1-latest/prev)*100):.1f}%，市场情绪谨慎"
                elif latest > avg_amount:
                    trend = "高于平均 📊"
                    interpretation = f"成交额高于近{days_count}日平均，市场交投活跃"
                else:
                    trend = "低于平均 📊"
                    interpretation = f"成交额低于近{days_count}日平均，市场观望情绪浓厚"
            else:
                trend = "数据不足"
                interpretation = "需要更多历史数据进行趋势判断"
            
            lines.append(f"**今日总成交额(沪市+深市)**: {total_amount/1e5:.1f}亿元")
            lines.append("")
            lines.append(f"| 指标 | 数值 |")
            lines.append("|:-----|------:|")
            lines.append(f"| 成交额 | {total_amount/1e5:.1f}亿 |")
            lines.append(f"| 日均成交额(近{days_count}日) | {avg_amount:.1f}亿 |")
            lines.append(f"| 最高成交额 | {max_vol[0]}: {max_vol[1]:.1f}亿 |")
            lines.append(f"| 最低成交额 | {min_vol[0]}: {min_vol[1]:.1f}亿 |")
            lines.append("")
            lines.append(f"**趋势判断**: {trend}")
            lines.append(f"**解读**: {interpretation}")
        else:
            lines.append("- 暂无成交量数据")
        
        lines.append("")
        
        # 1.3 基差分析
        if basis:
            lines.extend(self.generate_basis_section(basis))
        
        return lines
    
    def generate_basis_section(self, basis: List[Dict]) -> List[str]:
        """生成基差分析部分"""
        lines = []
        
        if not basis:
            return lines
        
        lines.append("### 1.3 基差分析")
        lines.append("")
        lines.append("| 指数 | 合约 | 期货价 | 现货价 | 基差 | 年化基差率 |")
        lines.append("|------|------|--------|--------|------|------------|")
        
        for item in basis:
            arrow = "↓" if item['basis'] < 0 else "↑"
            ann = f"{arrow}{abs(item['annualized_basis']):.2f}%"
            index_name = item.get('index_name') or item.get('index', '')
            lines.append(
                f"| {index_name} | {item['contract']} | "
                f"{item['futures_price']:.2f} | {item['spot_price']:.2f} | "
                f"{arrow}{abs(item['basis']):.2f} | {ann} |"
            )
        
        # 添加交易日说明
        if basis:
            trading_days = basis[0].get('trading_days', 0)
            lines.append("")
            lines.append(f"> 交易日计算说明：距到期日 {trading_days} 个交易日")
        
        lines.append("")
        
        return lines
    
    def _generate_news_section(self, news: Dict) -> List[str]:
        """生成新闻分析部分"""
        lines = []
        
        lines.append("---")
        lines.append("")
        lines.append("## 二、新闻分析")
        lines.append("")
        
        # 2.1 新闻概况
        lines.append("### 2.1 新闻概况")
        lines.append("")
        
        lines.append(f"- 总数: {news.get('count', 0)} 条")
        sentiment = news.get('sentiment', '中性')
        lines.append(f"- 市场情绪: **{sentiment}**")
        
        lines.append("")
        
        # 2.2 今日新闻标题
        lines.append("### 2.2 今日新闻标题")
        lines.append("")
        
        # 获取原始新闻数据
        all_news = news.get('all_news', [])
        if all_news:
            # 显示最新10条
            for i, item in enumerate(all_news[:10], 1):
                title = item.get('title', '')[:50]  # 截断过长标题
                source = item.get('source', '')
                lines.append(f"{i}. **{title}**")
                if source:
                    lines.append(f"   - 来源: {source}")
                lines.append("")
        else:
            # 如果没有原始数据，显示高重要性新闻
            high_news = news.get('high_importance', [])
            if high_news:
                for i, item in enumerate(high_news[:10], 1):
                    lines.append(f"**{i}. {item.get('title', '')}**")
                    lines.append(f"   - 分类: {item.get('category', '其他')}")
                    lines.append("")
            else:
                lines.append("*暂无重要新闻*")
        
        lines.append("")
        
        return lines
    
    def _generate_conclusion_section(self, conclusion: Dict) -> List[str]:
        """生成综合结论部分"""
        lines = []
        
        lines.append("---")
        lines.append("")
        lines.append("## 三、综合结论")
        lines.append("")
        
        # 3.1 市场判断
        lines.append("### 3.1 市场判断")
        lines.append("")
        lines.append(conclusion.get('market_view', '震荡整理'))
        lines.append("")
        
        # 3.2 风险提示
        lines.append("### 3.2 风险提示")
        lines.append("")
        
        risk_alerts = conclusion.get('risk_alerts', [])
        if risk_alerts:
            for alert in risk_alerts:
                lines.append(f"- {alert}")
        else:
            lines.append("- 无明显风险提示")
        
        lines.append("")
        
        # 3.3 投资建议
        lines.append("### 3.3 投资建议")
        lines.append("")
        
        recommendations = conclusion.get('recommendations', [])
        if recommendations:
            for rec in recommendations:
                lines.append(f"- {rec}")
        else:
            lines.append("- 建议保持观望")
        
        lines.append("")
        
        return lines
    
    def to_feishu(self, analysis: Dict) -> str:
        """
        生成飞书消息格式（简化版）
        
        Args:
            analysis: 分析结果
        
        Returns:
            飞书消息文本
        """
        date = analysis.get('date', datetime.now().strftime("%Y-%m-%d"))
        
        lines = []
        
        lines.append(f"📈 市场每日简报 {date}")
        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━━━")
        lines.append("")
        
        # 行情
        lines.append("## 一、行情分析")
        lines.append("")
        lines.append("### 涨跌幅排行")
        lines.append("")
        
        changes = analysis.get('market', {}).get('changes', {}).get('daily', [])
        for item in changes[:5]:
            change = f"{item['change_percent']:+.2f}%"
            # 成交金额
            amount = item.get('amount', 0)
            if amount and amount > 0:
                amount_str = f" 成交{amount/1e8:.0f}亿"
            else:
                amount_str = ""
            lines.append(f"{item['name']}: {item['price']:.2f} ({change}){amount_str}")
        
        lines.append("")
        
        # 基差
        basis = analysis.get('basis', [])
        if basis:
            lines.append("### 基差分析")
            lines.append("")
            
            for item in basis[:5]:
                arrow = "↓" if item['basis'] < 0 else "↑"
                ann = f"{item['annualized_basis']:+.2f}%"
                lines.append(
                    f"{item['index']}{item['contract']}: {arrow}{abs(item['basis']):.2f} ({ann})"
                )
        
        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━━━")
        lines.append("")
        
        # 新闻
        lines.append("## 二、新闻分析")
        lines.append("")
        
        news = analysis.get('news', {})
        sentiment = news.get('sentiment', '中性')
        lines.append(f"总数: {news.get('count', 0)}条 | 市场情绪: {sentiment}")
        
        lines.append("")
        
        high_news = news.get('high_importance', [])
        if high_news:
            for i, item in enumerate(high_news[:3], 1):
                lines.append(f"{i}. {item['title'][:40]}")
                lines.append(f"   [{item.get('category', '其他')}]")
        
        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━━━")
        lines.append("")
        
        # 结论
        lines.append("## 三、综合结论")
        lines.append("")
        
        conclusion = analysis.get('conclusion', {})
        lines.append(f"判断: {conclusion.get('market_view', '震荡整理')}")
        
        for rec in conclusion.get('recommendations', []):
            lines.append(f"- {rec}")
        
        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━━━")
        lines.append("")
        lines.append("🤖 本报告由 AI 自动生成")
        
        return "\n".join(lines)


def main():
    """命令行入口"""
    import sys
    
    # 读取分析结果
    if len(sys.argv) > 1:
        with open(sys.argv[1], 'r', encoding='utf-8') as f:
            analysis = json.load(f)
    else:
        # 测试数据
        analysis = {
            'date': datetime.now().strftime("%Y-%m-%d"),
            'market': {
                'changes': {'daily': [
                    {'name': '上证指数', 'price': 4146.63, 'change_percent': -0.01},
                    {'name': '深证成指', 'price': 14503.79, 'change_percent': 0.19},
                ]},
                'volume': {'trend': '正常', 'interpretation': '成交量处于正常水平'}
            },
            'basis': [],
            'news': {
                'count': 20,
                'sentiment': '中性',
                'high_importance': []
            },
            'conclusion': {
                'market_view': '震荡整理',
                'risk_alerts': [],
                'recommendations': ['建议保持观望']
            }
        }
    
    reporter = Reporter()
    
    if len(sys.argv) > 2 and sys.argv[2] == '--feishu':
        print(reporter.to_feishu(analysis))
    else:
        print(reporter.generate(analysis))


if __name__ == "__main__":
    main()
