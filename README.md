# A股市场每日分析系统 (Python版)

A股市场每日自动分析系统，从多个数据源收集市场数据，生成分析报告并推送至飞书。

## 功能特性

- **行情数据收集** - 8个主要A股指数（腾讯财经）
- **期货数据收集** - 股指期货基差分析（东方财富）
- **新闻数据收集** - 市场新闻聚合（东方财富、新浪、凤凰）
- **基差分析** - 年化基差率计算
- **质量检查** - 数据完整性验证
- **每日报告** - 自动生成并推送

## 系统架构

```
数据收集层 → 数据存储 → 质量检查 → 数据分析 → 报告生成 → 飞书推送
```

## 项目结构

```
market-analyst-py/
├── main.py                      # 主程序入口
├── market_collector/            # 行情数据收集
├── futures_collector/           # 期货数据收集
├── news_collector/             # 新闻数据收集
├── analyzer/                   # 数据分析
├── reporter/                   # 报告生成
├── storage/                   # 数据存储
└── utils/                     # 工具模块
    ├── trading_calendar.py    # 交易日历
    ├── data_validator.py     # 数据验证
    ├── data_retry.py         # 重试机制
    └── data_quality.py       # 质量检查
```

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt

# 完整流程
python3 main.py run

# 分步执行
python3 main.py collect        # 收集数据
python3 main.py quality        # 质量检查
python3 main.py analyze        # 数据分析
python3 main.py report         # 生成报告
```

## 使用示例

```bash
# 收集今日数据
python3 main.py collect

# 检查数据质量
python3 main.py quality

# 生成分析报告（飞书格式）
python3 main.py report --format feishu

# 执行完整流程
python3 main.py run
```

## 配置

- 数据存储目录: `data/`
- 交易日历: 自动识别2026年节假日
- 质量阈值: 70分（满分100）

## 文档

- [需求文档](./docs/REQUIREMENTS.md)
- [设计文档](./docs/DESIGN.md)
- [架构文档](./docs/ARCHITECTURE.md)

## 依赖

- Python 3.8+
- urllib (标准库)
- json (标准库)

##  License

MIT
