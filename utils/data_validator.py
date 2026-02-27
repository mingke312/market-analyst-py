#!/usr/bin/env python3
"""
数据验证模块
"""

from typing import Dict, List, Any, Tuple
from dataclasses import dataclass


@dataclass
class ValidationResult:
    """验证结果"""
    valid: bool
    errors: List[str]
    warnings: List[str]


class MarketDataValidator:
    """行情数据验证器"""
    
    REQUIRED_FIELDS = ['code', 'name', 'price', 'change_percent']
    REQUIRED_INDICES = [
        'sh000001',  # 上证指数
        'sz399001', # 深证成指
        'sh000300', # 沪深300
        'sh000905', # 中证500
        'sh000852', # 中证1000
        'sh000016', # 上证50
        'sh000688', # 科创50
        'sz399006', # 创业板指
    ]
    
    def validate(self, data: List[Dict]) -> ValidationResult:
        errors = []
        warnings = []
        
        if not data:
            return ValidationResult(False, ["数据为空"], [])
        
        # 检查必需的指数
        codes = {item.get('code') for item in data if item.get('code')}
        missing = set(self.REQUIRED_INDICES) - codes
        if missing:
            errors.append(f"缺少指数: {missing}")
        
        # 验证每条数据
        for item in data:
            result = self._validate_item(item)
            errors.extend(result.errors)
            warnings.extend(result.warnings)
        
        return ValidationResult(len(errors) == 0, errors, warnings)
    
    def _validate_item(self, item: Dict) -> ValidationResult:
        errors = []
        warnings = []
        
        # 必填字段
        for field in self.REQUIRED_FIELDS:
            if not item.get(field):
                errors.append(f"缺少字段: {field}")
        
        # 价格验证
        price = item.get('price', 0)
        if price and price <= 0:
            errors.append(f"价格异常: {price}")
        
        # 涨跌幅验证
        change = item.get('change_percent', 0)
        if change and (change < -20 or change > 20):
            warnings.append(f"涨跌幅异常: {change}%")
        
        return ValidationResult(len(errors) == 0, errors, warnings)


class FuturesDataValidator:
    """期货数据验证器"""
    
    REQUIRED_CONTRACTS = ['IF', 'IC', 'IM', 'IH']
    CONTRACT_TYPES = ['当月', '下季', '隔季']
    
    def validate(self, data: Dict) -> ValidationResult:
        errors = []
        warnings = []
        
        if not data:
            return ValidationResult(False, ["期货数据为空"], [])
        
        # 检查必需的指数
        for code in self.REQUIRED_CONTRACTS:
            if code not in data:
                errors.append(f"缺少期货数据: {code}")
                continue
            
            # 检查每个合约
            for ct in self.CONTRACT_TYPES:
                if code == 'IH' and ct == '隔季':
                    continue  # IH可能没有隔季
                
                if ct not in data[code]:
                    warnings.append(f"缺少 {code} {ct} 合约数据")
                    continue
                
                contract = data[code][ct]
                if not contract or not contract.get('price'):
                    errors.append(f"缺少 {code} {ct} 价格")
        
        return ValidationResult(len(errors) == 0, errors, warnings)


class NewsDataValidator:
    """新闻数据验证器"""
    
    CATEGORIES = ['宏观政策', '行业动态', '国际市场', '公司重大事项', '其他']
    
    def validate(self, data: List[Dict]) -> ValidationResult:
        errors = []
        warnings = []
        
        if not data:
            return ValidationResult(False, ["新闻数据为空"], [])
        
        if len(data) < 5:
            warnings.append(f"新闻数量过少: {len(data)}条")
        
        for item in data:
            if not item.get('title'):
                errors.append("缺少标题")
            
            if not item.get('category'):
                errors.append("缺少分类")
            elif item['category'] not in self.CATEGORIES:
                warnings.append(f"未知分类: {item['category']}")
        
        return ValidationResult(len(errors) == 0, errors, warnings)


class BasisDataValidator:
    """基差数据验证器"""
    
    def validate(self, data: List[Dict]) -> ValidationResult:
        errors = []
        warnings = []
        
        for item in data:
            if not item.get('index'):
                errors.append("缺少指数代码")
            if not item.get('contract'):
                errors.append("缺少合约类型")
            if not item.get('futures_price'):
                errors.append("缺少期货价格")
            if not item.get('spot_price'):
                errors.append("缺少现货价格")
            
            # 基差合理性检查
            basis = item.get('basis', 0)
            if basis and abs(basis) > 1000:
                warnings.append(f"基差绝对值较大: {basis}")
            
            ann_basis = item.get('annualized_basis', 0)
            if ann_basis and abs(ann_basis) > 50:
                warnings.append(f"年化基差异常: {ann_basis}%")
        
        return ValidationResult(len(errors) == 0, errors, warnings)


def validate_market_data(data: List[Dict]) -> ValidationResult:
    """验证行情数据"""
    return MarketDataValidator().validate(data)


def validate_futures_data(data: Dict) -> ValidationResult:
    """验证期货数据"""
    return FuturesDataValidator().validate(data)


def validate_news_data(data: List[Dict]) -> ValidationResult:
    """验证新闻数据"""
    return NewsDataValidator().validate(data)


def validate_basis_data(data: List[Dict]) -> ValidationResult:
    """验证基差数据"""
    return BasisDataValidator().validate(data)
