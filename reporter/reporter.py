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
        lines.extend(self._generate_market_section(analysis.get('market', {})))
        
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
    
    def _generate_market_section(self, market: Dict) -> List[str]:
        """生成行情分析部分"""
        lines = []
        
        lines.append("## 一、行情分析")
        lines.append("")
        
        # 1.1 涨跌幅排行
        lines.append("### 1.1 涨跌幅排行")
        lines.append("")
        lines.append("| 指数 | 涨跌幅 |")
        lines.append("|------|--------|")
        
        changes = market.get('changes', {}).get('daily', [])
        for item in changes:
            change = f"{item['change_percent']:+.2f}%"
            lines.append(f"| {item['name']} | {change} |")
        
        lines.append("")
        
        # 1.2 成交量
        lines.append("### 1.2 成交量")
        lines.append("")
        
        volume = market.get('volume', {})
        lines.append(f"- 趋势: {volume.get('trend', '数据不足')}")
        lines.append(f"- 解读: {volume.get('interpretation', '需要更多历史数据')}")
        
        lines.append("")
        
        # 1.3 基差分析
        # 这里需要从analysis中获取basis数据
        # 暂时省略，在主流程中会单独处理
        
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
            lines.append(
                f"| {item['index_name']} | {item['contract']} | "
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
        
        # 2.2 重要新闻
        lines.append("### 2.2 重要新闻")
        lines.append("")
        
        high_news = news.get('high_importance', [])
        if high_news:
            for i, item in enumerate(high_news, 1):
                lines.append(f"**{i}. {item['title']}**")
                lines.append(f"- 分类: {item.get('category', '其他')}")
                lines.append("")
        else:
            lines.append("*暂无高重要性新闻*")
        
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
            lines.append(f"{item['name']}: {item['price']:.2f} ({change})")
        
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
