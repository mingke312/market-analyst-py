#!/usr/bin/env python3
"""
数据质量检查模块
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import json

from utils.data_validator import (
    validate_market_data,
    validate_futures_data,
    validate_news_data,
    validate_basis_data
)


@dataclass
class QualityResult:
    """质量检查结果"""
    score: int
    passed: bool
    issues: List[str]
    warnings: List[str]
    report: str


def check_market_data(data: Optional[Dict]) -> Dict:
    """检查行情数据质量"""
    issues = []
    warnings = []
    
    if not data or not data.get('data'):
        return {'issues': ['行情数据为空'], 'warnings': [], 'score': 0}
    
    # 验证数据
    result = validate_market_data(data['data'])
    issues.extend(result.errors)
    warnings.extend(result.warnings)
    
    # 计算分数
    base_score = 100
    score = base_score - len(issues) * 20 - len(warnings) * 5
    score = max(0, score)
    
    return {
        'issues': issues,
        'warnings': warnings,
        'score': score,
        'count': len(data.get('data', []))
    }


def check_futures_data(data: Optional[Dict]) -> Dict:
    """检查期货数据质量"""
    issues = []
    warnings = []
    
    if not data or not data.get('data'):
        return {'issues': ['期货数据为空'], 'warnings': [], 'score': 0}
    
    result = validate_futures_data(data['data'])
    issues.extend(result.errors)
    warnings.extend(result.warnings)
    
    # 计算合约完整度
    futures = data.get('data', {})
    total = 0
    filled = 0
    for code, contracts in futures.items():
        expected = 2 if code == 'IH' else 3
        total += expected
        for ct in ['当月', '下季', '隔季']:
            if ct == '隔季' and code == 'IH':
                continue
            if contracts.get(ct) and contracts[ct].get('price'):
                filled += 1
    
    if filled < total:
        warnings.append(f"期货合约不完整: {filled}/{total}")
    
    base_score = 100
    score = base_score - len(issues) * 15 - len(warnings) * 5
    score = max(0, score)
    
    return {
        'issues': issues,
        'warnings': warnings,
        'score': score,
        'filled': filled,
        'total': total
    }


def check_news_data(data: Optional[Dict]) -> Dict:
    """检查新闻数据质量"""
    issues = []
    warnings = []
    
    if not data or not data.get('data'):
        return {'issues': ['新闻数据为空'], 'warnings': [], 'score': 0}
    
    result = validate_news_data(data['data'])
    issues.extend(result.errors)
    warnings.extend(result.warnings)
    
    if len(data['data']) < 5:
        warnings.append(f"新闻数量过少: {len(data['data'])}条")
    
    base_score = 100
    score = base_score - len(issues) * 10 - len(warnings) * 3
    score = max(0, score)
    
    return {
        'issues': issues,
        'warnings': warnings,
        'score': score,
        'count': len(data['data'])
    }


def check_basis_data(data: Optional[Dict]) -> Dict:
    """检查基差数据质量"""
    issues = []
    warnings = []
    
    if not data or not data.get('data'):
        return {'issues': ['基差数据为空'], 'warnings': [], 'score': 0}
    
    result = validate_basis_data(data['data'])
    issues.extend(result.errors)
    warnings.extend(result.warnings)
    
    base_score = 100
    score = base_score - len(issues) * 10 - len(warnings) * 3
    score = max(0, score)
    
    return {
        'issues': issues,
        'warnings': warnings,
        'score': score,
        'count': len(data.get('data', []))
    }


def generate_quality_report(
    market_data: Optional[Dict] = None,
    futures_data: Optional[Dict] = None,
    news_data: Optional[Dict] = None,
    basis_data: Optional[Dict] = None
) -> QualityResult:
    """
    生成数据质量报告
    
    Args:
        market_data: 行情数据
        futures_data: 期货数据
        news_data: 新闻数据
        basis_data: 基差数据
    
    Returns:
        QualityResult: 质量结果
    """
    market_check = check_market_data(market_data)
    futures_check = check_futures_data(futures_data)
    news_check = check_news_data(news_data)
    basis_check = check_basis_data(basis_data)
    
    # 计算总分
    scores = [market_check['score'], futures_check['score'], news_check['score']]
    if basis_check['score'] > 0:
        scores.append(basis_check['score'])
    
    total_score = sum(scores) // len(scores) if scores else 0
    
    # 生成报告
    report = "📋 数据质量报告\n" + "━" * 20 + "\n\n"
    
    # 行情数据
    report += f"【行情数据】 {market_check['score']}分\n"
    if market_check['issues']:
        report += "  ❌ 问题:\n"
        for i in market_check['issues']:
            report += f"    - {i}\n"
    if market_check['warnings']:
        report += "  ⚠️ 警告:\n"
        for w in market_check['warnings']:
            report += f"    - {w}\n"
    if not market_check['issues'] and not market_check['warnings']:
        report += "  ✅ 正常\n"
    report += "\n"
    
    # 期货数据
    report += f"【期货数据】 {futures_check['score']}分"
    if futures_check.get('filled') is not None:
        report += f" ({futures_check['filled']}/{futures_check['total']}合约)"
    report += "\n"
    if futures_check['issues']:
        report += "  ❌ 问题:\n"
        for i in futures_check['issues']:
            report += f"    - {i}\n"
    if futures_check['warnings']:
        report += "  ⚠️ 警告:\n"
        for w in futures_check['warnings']:
            report += f"    - {w}\n"
    if not futures_check['issues'] and not futures_check['warnings']:
        report += "  ✅ 正常\n"
    report += "\n"
    
    # 新闻数据
    report += f"【新闻数据】 {news_check['score']}分"
    if news_check.get('count'):
        report += f" ({news_check['count']}条)"
    report += "\n"
    if news_check['issues']:
        report += "  ❌ 问题:\n"
        for i in news_check['issues']:
            report += f"    - {i}\n"
    if news_check['warnings']:
        report += "  ⚠️ 警告:\n"
        for w in news_check['warnings']:
            report += f"    - {w}\n"
    if not news_check['issues'] and not news_check['warnings']:
        report += "  ✅ 正常\n"
    report += "\n"
    
    # 总评
    report += "━" * 20 + "\n"
    report += f"总分: {total_score}/100\n"
    
    if total_score >= 90:
        report += "✅ 优秀\n"
    elif total_score >= 70:
        report += "✅ 合格\n"
    elif total_score >= 50:
        report += "⚠️ 一般\n"
    else:
        report += "❌ 较差，需要重新采集\n"
    
    return QualityResult(
        score=total_score,
        passed=total_score >= 70,
        issues=market_check['issues'] + futures_check['issues'] + news_check['issues'],
        warnings=market_check['warnings'] + futures_check['warnings'] + news_check['warnings'],
        report=report
    )


if __name__ == "__main__":
    # 测试
    result = generate_quality_report(
        market_data={'data': []},
        futures_data={'data': {}},
        news_data={'data': []}
    )
    print(result.report)
