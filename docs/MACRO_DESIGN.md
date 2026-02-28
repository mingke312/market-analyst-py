# 宏观数据收集模块 - 设计文档

## 1. 模块架构

```
┌─────────────────────────────────────────┐
│            宏观数据收集器                │
├─────────────────────────────────────────┤
│  macro_collector/                       │
│  ├── collector.py                       │  ← 主收集器
│  └── __init__.py                        │
├─────────────────────────────────────────┤
│  数据输出:                               │
│  {                                      │
│    "date": "2026-02-28",               │
│    "type": "macro",                    │
│    "data": {                           │
│      "global_indices": [...],          │
│      "hk_indices": [...],              │
│      "commodities": {...},             │
│      "currency": [...],                │
│      "bonds": [...]                    │
│    }                                    │
│  }                                      │
└─────────────────────────────────────────┘
```

## 2. 数据格式

### 2.1 全球指数

```json
{
  "global_indices": [
    {"name": "S&P 500", "price": 5234.18, "change_percent": 0.5, "currency": "USD"},
    {"name": "纳斯达克", "price": 16742.39, "change_percent": 0.8, "currency": "USD"},
    {"name": "道琼斯", "price": 39127.14, "change_percent": 0.3, "currency": "USD"}
  ]
}
```

### 2.2 大宗商品

```json
{
  "commodities": {
    "gold": {"name": "黄金", "price": 528.50, "unit": "元/克", "currency": "CNY"},
    "oil": {"name": "布伦特原油", "price": 83.45, "unit": "美元/桶", "currency": "USD"}
  }
}
```

### 2.3 汇率

```json
{
  "currency": [
    {"name": "美元/人民币", "price": 7.24, "currency": "CNY"},
    {"name": "欧元/美元", "price": 1.08, "currency": "USD"}
  ]
}
```

### 2.4 债券

```json
{
  "bonds": [
    {"name": "国债1年", "yield": 1.85, "currency": "CNY"},
    {"name": "国债5年", "yield": 2.15, "currency": "CNY"},
    {"name": "国债10年", "yield": 2.35, "currency": "CNY"}
  ]
}
```

## 3. 收集策略

| 数据类型 | 优先级 | 超时 | 重试 |
|----------|--------|------|------|
| 股票指数 | 高 | 10s | 3次 |
| 外汇 | 高 | 10s | 3次 |
| 大宗商品 | 中 | 15s | 2次 |
| 债券 | 低 | 15s | 2次 |

## 4. 与现有系统集成

```python
# 在 main.py 中添加
from macro_collector.collector import MacroCollector

# 收集宏观数据
macro_collector = MacroCollector()
macro_data = macro_collector.collect_all()
storage.save_macro(macro_data)
```

## 5. 文件结构

```
market-analyst-py/
├── macro_collector/
│   ├── collector.py       # 宏观数据收集
│   └── __init__.py
├── market_collector/      # A股行情
├── futures_collector/     # 期货数据
├── news_collector/       # 新闻数据
├── analyzer/             # 分析引擎
├── reporter/             # 报告生成
└── storage/              # 数据存储
```
