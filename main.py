#!/usr/bin/env python3
"""
A股市场每日分析系统 - 主程序
"""

import sys
import argparse
import logging
from datetime import datetime
from typing import Optional

from market_collector.collector import MarketCollector
from futures_collector.collector import FuturesCollector
from news_collector.collector import NewsCollector
from analyzer.analyzer import Analyzer
from reporter.reporter import Reporter
from storage.storage import Storage
from utils.data_quality import generate_quality_report

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AShareAnalyzer:
    """A股分析系统主类"""
    
    def __init__(self):
        self.storage = Storage()
        self.market_collector = MarketCollector()
        self.futures_collector = FuturesCollector()
        self.news_collector = NewsCollector()
        self.analyzer = Analyzer(self.storage)
        self.reporter = Reporter()
    
    def collect_data(self, date: str = None) -> dict:
        """
        收集所有数据
        
        Args:
            date: 日期
        
        Returns:
            收集结果
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        logger.info(f"开始收集数据 for {date}")
        
        results = {
            'date': date,
            'market': None,
            'futures': None,
            'news': None
        }
        
        # 1. 收集行情数据
        logger.info("收集行情数据...")
        try:
            market_data = self.market_collector.collect_all()
            self.storage.save_market(market_data['data'], date)
            results['market'] = market_data
            logger.info(f"行情数据: {market_data.get('count', 0)}个指数")
        except Exception as e:
            logger.error(f"行情数据收集失败: {e}")
        
        # 2. 收集期货数据
        logger.info("收集期货数据...")
        try:
            futures_data = self.futures_collector.collect_all()
            self.storage.save_futures(futures_data['data'], date)
            results['futures'] = futures_data
            logger.info("期货数据收集完成")
        except Exception as e:
            logger.error(f"期货数据收集失败: {e}")
        
        # 3. 收集新闻数据
        logger.info("收集新闻数据...")
        try:
            news_data = self.news_collector.collect_all()
            self.storage.save_news(news_data['data'], date)
            results['news'] = news_data
            logger.info(f"新闻数据: {news_data.get('count', 0)}条")
        except Exception as e:
            logger.error(f"新闻数据收集失败: {e}")
        
        return results
    
    def check_quality(self, date: str = None) -> dict:
        """
        检查数据质量
        
        Args:
            date: 日期
        
        Returns:
            质量检查结果
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        logger.info("检查数据质量...")
        
        market_data = self.storage.load_market(date)
        futures_data = self.storage.load_futures(date)
        news_data = self.storage.load_news(date)
        
        result = generate_quality_report(market_data, futures_data, news_data)
        
        logger.info(f"质量评分: {result.score}/100, 通过: {result.passed}")
        
        return {
            'score': result.score,
            'passed': result.passed,
            'report': result.report
        }
    
    def analyze(self, date: str = None) -> dict:
        """
        执行分析
        
        Args:
            date: 日期
        
        Returns:
            分析结果
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        logger.info(f"执行分析 for {date}")
        
        result = self.analyzer.analyze(date)
        
        # 保存分析结果
        self.storage.save_analysis(result, date)
        
        return result
    
    def generate_report(self, date: str = None, format: str = 'markdown') -> str:
        """
        生成报告
        
        Args:
            date: 日期
            format: 格式 (markdown/feishu)
        
        Returns:
            报告文本
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        logger.info(f"生成报告 for {date}")
        
        # 分析
        analysis = self.analyzer.analyze(date)
        
        # 生成报告
        if format == 'feishu':
            report = self.reporter.to_feishu(analysis)
        else:
            # 添加基差分析部分
            report = self.reporter.generate(analysis)
            # 插入基差分析
            if analysis.get('basis'):
                lines = report.split('\n')
                # 在"1.2 成交量"后插入基差分析
                for i, line in enumerate(lines):
                    if line == '### 1.2 成交量':
                        # 找到下一个空行
                        j = i + 1
                        while j < len(lines) and lines[j]:
                            j += 1
                        # 插入基差部分
                        basis_lines = self.reporter.generate_basis_section(analysis['basis'])
                        lines = lines[:j] + basis_lines + lines[j:]
                        break
                report = '\n'.join(lines)
        
        return report
    
    def run_full(self, date: str = None) -> dict:
        """
        完整流程：收集 -> 质量检查 -> 分析 -> 报告
        
        Args:
            date: 日期
        
        Returns:
            流程结果
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        logger.info(f"=== 开始完整流程 for {date} ===")
        
        # 1. 收集数据
        logger.info("步骤1: 收集数据")
        collect_result = self.collect_data(date)
        
        # 2. 质量检查
        logger.info("步骤2: 质量检查")
        quality_result = self.check_quality(date)
        
        if not quality_result['passed']:
            logger.warning(f"数据质量未通过: {quality_result['score']}/100")
            # 这里可以添加重试逻辑
        
        # 3. 分析
        logger.info("步骤3: 数据分析")
        analysis = self.analyzer.analyze(date)
        
        # 4. 生成报告
        logger.info("步骤4: 生成报告")
        report = self.generate_report(date, format='feishu')
        
        logger.info("=== 流程完成 ===")
        
        return {
            'date': date,
            'collect': collect_result,
            'quality': quality_result,
            'analysis': analysis,
            'report': report
        }


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(description='A股市场每日分析系统')
    parser.add_argument('command', choices=['collect', 'quality', 'analyze', 'report', 'run'],
                       help='命令')
    parser.add_argument('--date', '-d', help='日期 (YYYY-MM-DD)')
    parser.add_argument('--format', '-f', choices=['markdown', 'feishu'], default='feishu',
                       help='报告格式')
    
    args = parser.parse_args()
    
    analyzer = AShareAnalyzer()
    
    if args.command == 'collect':
        result = analyzer.collect_data(args.date)
        print(f"数据收集完成: {result}")
    
    elif args.command == 'quality':
        result = analyzer.check_quality(args.date)
        print(result['report'])
    
    elif args.command == 'analyze':
        result = analyzer.analyze(args.date)
        print(result)
    
    elif args.command == 'report':
        report = analyzer.generate_report(args.date, args.format)
        print(report)
    
    elif args.command == 'run':
        result = analyzer.run_full(args.date)
        print(result['report'])


if __name__ == "__main__":
    main()
