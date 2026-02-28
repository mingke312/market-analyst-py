#!/usr/bin/env python3
"""
宏观经济数据质量检查模块
检查数据完整性、异常值、与历史数据对比
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class MacroDataQualityChecker:
    """宏观经济数据质量检查器"""
    
    # 期望的数据字段（必需）
    REQUIRED_FIELDS = [
        'gdp', 'cpi', 'ppi', 'pmi',
        'retail', 'fixed_investment',
        'industrial_addition', 'exports', 'imports',
        'central_bank', 'm2', 'real_estate'
    ]
    
    # 数值字段的合理范围（用于异常检测）
    VALUE_RANGES = {
        'gdp': {'min': 110, 'max': 140, 'unit': '万亿元', 'field': 'value'},
        'cpi': {'min': -2, 'max': 5, 'unit': '%', 'field': 'yoy'},
        'ppi': {'min': -5, 'max': 5, 'unit': '%', 'field': 'yoy'},
        'pmi': {'min': 48, 'max': 52, 'unit': '', 'field': 'value'},
        'm2': {'min': 250, 'max': 350, 'unit': '万亿元', 'field': 'value'},
        'fixed_investment': {'min': -5, 'max': 15, 'unit': '%', 'field': 'yoy'},
        'real_estate': {'min': -30, 'max': 10, 'unit': '%', 'field': 'investment_yoy'},
    }
    
    def __init__(self, data_dir: str = None):
        if data_dir is None:
            data_dir = os.path.join(os.path.dirname(__file__), 'data')
        self.data_dir = data_dir
    
    def load_latest(self, days_back: int = 30) -> List[Dict]:
        """加载最近N天的数据"""
        files = []
        if not os.path.exists(self.data_dir):
            return []
        
        for f in os.listdir(self.data_dir):
            if f.startswith('macro_') and f.endswith('.json'):
                files.append(f)
        
        files.sort(reverse=True)
        return files[:days_back]
    
    def check_completeness(self, data: Dict) -> Dict:
        """检查数据完整性"""
        issues = []
        warnings = []
        
        if not data or 'data' not in data:
            return {'issues': ['数据为空'], 'warnings': [], 'score': 0}
        
        fields = data.get('data', {})
        
        # 检查必需字段
        for field in self.REQUIRED_FIELDS:
            if field not in fields or not fields[field]:
                issues.append(f"缺失字段: {field}")
        
        # 检查嵌套字段
        # GDP
        if 'gdp' in fields and fields['gdp']:
            gdp = fields['gdp']
            if not gdp.get('value'):
                warnings.append("GDP数值缺失")
        
        # CPI
        if 'cpi' in fields and fields['cpi']:
            cpi = fields['cpi']
            if not cpi.get('yoy'):
                warnings.append("CPI同比缺失")
        
        # 央行数据
        if 'central_bank' in fields:
            cb = fields['central_bank']
            if not cb or len(cb) < 3:
                warnings.append("央行数据不足")
        
        # 计算分数
        base = 100
        score = base - len(issues) * 20 - len(warnings) * 5
        score = max(0, score)
        
        return {
            'issues': issues,
            'warnings': warnings,
            'score': score,
            'total_fields': len(self.REQUIRED_FIELDS),
            'filled_fields': len([f for f in self.REQUIRED_FIELDS if f in fields and fields[f]])
        }
    
    def check_value_ranges(self, data: Dict) -> Dict:
        """检查数值是否在合理范围内"""
        issues = []
        warnings = []
        
        fields = data.get('data', {})
        
        # 检查各字段
        check_fields = {
            'gdp': fields.get('gdp', {}),
            'cpi': fields.get('cpi', {}),
            'ppi': fields.get('ppi', {}),
            'pmi': fields.get('pmi', {}),
            'm2': fields.get('m2', {}),
            'fixed_investment': fields.get('fixed_investment', {}),
        }
        
        for field_name, field_data in check_fields.items():
            if not field_data:
                continue
            
            if field_name in self.VALUE_RANGES:
                range_info = self.VALUE_RANGES[field_name]
                check_field = range_info.get('field', 'yoy')
                
                # 检查对应字段
                value = field_data.get(check_field)
                if value is not None:
                    if value < range_info['min'] or value > range_info['max']:
                        issues.append(
                            f"{field_name} {check_field}异常: {value}{range_info['unit']} (合理范围: {range_info['min']} ~ {range_info['max']})"
                        )
        
        score = 100 - len(issues) * 15
        score = max(0, score)
        
        return {'issues': issues, 'warnings': [], 'score': score}
    
    def compare_with_history(self, data: Dict, history_files: List[str]) -> Dict:
        """与历史数据对比"""
        issues = []
        warnings = []
        
        if not history_files or len(history_files) < 2:
            return {'issues': [], 'warnings': ['历史数据不足'], 'score': 100}
        
        # 加载最近的历史数据
        current_data = data.get('data', {})
        
        try:
            # 对比上一条数据
            prev_file = history_files[0]
            with open(os.path.join(self.data_dir, prev_file), 'r') as f:
                prev_data = json.load(f).get('data', {})
            
            # 对比关键指标
            compare_fields = ['gdp', 'cpi', 'ppi', 'm2']
            
            for field in compare_fields:
                if field not in current_data or field not in prev_data:
                    continue
                
                curr = current_data[field]
                prev = prev_data[field]
                
                if not curr or not prev:
                    continue
                
                # 对比同比
                curr_yoy = curr.get('yoy')
                prev_yoy = prev.get('yoy')
                
                if curr_yoy and prev_yoy:
                    diff = abs(curr_yoy - prev_yoy)
                    # 差异超过10个百分点视为异常
                    if diff > 10:
                        issues.append(
                            f"{field}环比变化过大: {curr_yoy}% vs {prev_yoy}% (变化{diff:.1f}%)"
                        )
        
        except Exception as e:
            warnings.append(f"历史对比失败: {e}")
        
        score = 100 - len(issues) * 10
        score = max(0, score)
        
        return {'issues': issues, 'score': score}
    
    def check_all(self) -> Dict:
        """执行所有检查"""
        
        # 获取今天的数据
        today = datetime.now().strftime("%Y-%m-%d")
        today_file = f"macro_{today}.json"
        today_path = os.path.join(self.data_dir, today_file)
        
        if not os.path.exists(today_path):
            return {
                'date': today,
                'status': 'no_data',
                'message': '今日数据尚未生成，请先运行收集任务'
            }
        
        with open(today_path, 'r') as f:
            data = json.load(f)
        
        # 加载历史数据
        history_files = self.load_latest()
        
        # 执行各项检查
        completeness = self.check_completeness(data)
        ranges = self.check_value_ranges(data)
        history = self.compare_with_history(data, history_files[1:])  # 排除今天
        
        # 综合评分
        total_score = (completeness['score'] + ranges['score'] + history['score']) // 3
        
        # 汇总问题
        all_issues = completeness['issues'] + ranges['issues'] + history['issues']
        all_warnings = completeness['warnings'] + ranges['warnings'] + history['warnings']
        
        result = {
            'date': today,
            'status': 'pass' if total_score >= 70 else 'fail',
            'score': total_score,
            'completeness': completeness,
            'ranges': ranges,
            'history': history,
            'issues': all_issues,
            'warnings': all_warnings,
            'summary': self._generate_summary(total_score, all_issues, all_warnings)
        }
        
        return result
    
    def _generate_summary(self, score: int, issues: List[str], warnings: List[str]) -> str:
        """生成摘要"""
        
        lines = []
        lines.append("📋 宏观经济数据质量报告")
        lines.append("=" * 50)
        lines.append(f"\n日期: {datetime.now().strftime('%Y-%m-%d')}")
        lines.append(f"质量评分: {score}/100")
        
        if score >= 90:
            lines.append("状态: ✅ 优秀")
        elif score >= 70:
            lines.append("状态: ✅ 合格")
        elif score >= 50:
            lines.append("状态: ⚠️ 一般")
        else:
            lines.append("状态: ❌ 需关注")
        
        if issues:
            lines.append("\n❌ 问题:")
            for issue in issues[:5]:  # 最多显示5条
                lines.append(f"  - {issue}")
        
        if warnings:
            lines.append("\n⚠️ 警告:")
            for warn in warnings[:5]:
                lines.append(f"  - {warn}")
        
        if not issues and not warnings:
            lines.append("\n✅ 数据质量良好，无异常")
        
        return "\n".join(lines)


def main():
    import sys
    
    checker = MacroDataQualityChecker()
    result = checker.check_all()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--json':
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(result['summary'])


if __name__ == "__main__":
    main()
