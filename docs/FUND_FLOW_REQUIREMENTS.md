# 资金流数据采集需求文档

## 1. 概述

新增A股资金流分析所需的两项数据采集：
- ETF申赎数据
- 新基金发行数据

## 2. ETF申赎数据

### 2.1 数据来源
- 东方财富网 ETF 资金流向页面
- URL: `http://fund.eastmoney.com/ETFjjgm.html`

### 2.2 采集字段
| 字段 | 说明 |
|------|------|
| trade_date | 交易日期 |
| ts_code | ETF代码 |
| etf_name | ETF名称 |
| net_inflow | 资金净流入（万元） |
| net_inflow_rate | 资金净流入率(%) |
| net_inflow_share | 净申购份额（万份） |
| exchange | 交易所（SSE/SZSE） |

### 2.3 更新频率
每日采集（T+1）

## 3. 新基金发行数据

### 3.1 数据来源
- 东方财富网 新基金发行页面
- URL: `http://fund.eastmoney.com/data/xg_xjjj.html`

### 3.2 采集字段
| 字段 | 说明 |
|------|------|
| issue_date | 发行日期 |
| fund_name | 基金名称 |
| fund_type | 基金类型（股票/混合/债券等） |
| issue_scale | 发行规模（亿元） |
| issue_status | 发行状态（已完成/发行中/待发行） |

### 3.3 更新频率
每日采集

## 4. 数据库设计

### 4.1 etf_net_flow（ETF资金流向）
```sql
CREATE TABLE etf_net_flow (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL,
    ts_code TEXT NOT NULL,
    etf_name TEXT,
    net_inflow REAL,
    net_inflow_rate REAL,
    net_inflow_share REAL,
    exchange TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trade_date, ts_code)
);
```

### 4.2 new_fund_issue（新基金发行）
```sql
CREATE TABLE new_fund_issue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_date TEXT NOT NULL,
    fund_name TEXT NOT NULL,
    fund_type TEXT,
    issue_scale REAL,
    issue_status TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(issue_date, fund_name)
);
```

## 5. 整合计划

在 `collect_data.py` 中添加 `-t etf_net_flow` 和 `-t new_fund` 参数，
并将其加入 `-t all` 的默认采集列表。
