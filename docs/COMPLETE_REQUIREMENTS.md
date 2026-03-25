# 宏观策略分析师系统 - 需求文档 (V3.0)

**版本**: 3.0  
**更新日期**: 2026-03-01  
**状态**: 完善中

---

## 1. 项目概述

### 1.1 项目目标
成为宏观策略分析师，每日自动收集数据，分析市场、产出分析报告。

### 1.2 核心原则
- **所有数据必须从本地数据库读取**
- **没有的数据先开发程序采集存储，再使用**
- **每日产出分析报告**

---

## 2. 数据分类与规格

### 2.1 A股行情数据

#### 2.1.1 数据项规格

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 日期 | 2026-03-01 |
| code | string | 指数代码 | 000001 |
| name | string | 指数名称 | 上证指数 |
| open | float | 开盘价 | 4150.00 |
| high | float | 最高价 | 4180.00 |
| low | float | 最低价 | 4140.00 |
| close | float | 收盘价 | 4162.88 |
| change | float | 涨跌幅(%) | 0.39 |
| volume | float | 成交量(手) | 682047646 |
| amount | float | 成交额(元) | 1072367831609.8 |
| turnover_rate | float | 换手率(%) | 2.5 |
| pe | float | 市盈率(倍) | 18.5 |
| pb | float | 市净率(倍) | 1.8 |

#### 2.1.2 数据源

| 指数 | 代码 | 来源 | API |
|------|------|------|-----|
| 上证指数 | 000001 | 东方财富 | push2.eastmoney.com |
| 深证成指 | 399001 | 东方财富 | push2.eastmoney.com |
| 创业板指 | 399006 | 东方财富 | push2.eastmoney.com |
| 沪深300 | 000300 | 东方财富 | push2.eastmoney.com |
| 中证500 | 000905 | 东方财富 | push2.eastmoney.com |
| 科创50 | 000688 | 东方财富 | push2.eastmoney.com |

#### 2.1.3 采集频率
- 每日 16:00（交易日）
- 数据格式：JSON

---

### 2.2 期货数据

#### 2.2.1 数据项规格

| 字段 | 类型 | 说明 |
|------|------|------|
| date | string | 日期 |
| code | string | 合约代码 |
| name | string | 合约名称 |
| open | float | 开盘价 |
| high | float | 最高价 |
| low | float | 最低价 |
| close | float | 收盘价 |
| change | float | 涨跌幅(%) |
| settle | float | 结算价 |
| volume | float | 成交量 |
| amount | float | 成交额 |
| contract_month | string | 合约月份 |

#### 2.2.2 数据源

| 期货 | 代码 | 期限 | 来源 |
|------|------|------|------|
| 沪深300股指期货 | IF | 当月/下月/下季/隔季 | 东方财富 |
| 中证500股指期货 | IC | 当月/下月/下季/隔季 | 东方财富 |
| 中证1000股指期货 | IM | 当月/下月/下季/隔季 | 东方财富 |
| 上证50股指期货 | IH | 当月/下月/下季/隔季 | 东方财富 |

#### 2.2.3 采集频率
- 每日 16:00

---

### 2.3 宏观经济数据

#### 2.3.1 GDP数据

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 数据月份 | 2025Q4 |
| value | float | GDP值(亿元) | 1401879.2 |
| yoy | float | 同比(%) | 5.0 |
| quarter | string | 季度 | 2025Q4 |

**来源**: TuShare (cn_gdp)

#### 2.3.2 CPI数据

| 字段 | 类型 | 说明 |
|------|------|------|
| date | string | 月份 |
| yoy | float | 同比(%) |
| mom | float | 环比(%) |

**来源**: TuShare (cn_cpi)

#### 2.3.3 PPI数据

| 字段 | 类型 | 说明 |
|------|------|------|
| date | string | 月份 |
| yoy | float | 同比(%) |
| mom | float | 环比(%) |

**来源**: TuShare (cn_ppi)

#### 2.3.4 PMI数据

| 字段 | 类型 | 说明 |
|------|------|------|
| date | string | 月份 |
| manufacturing | float | 制造业PMI |
| non_manufacturing | float | 非制造业PMI |

**来源**: TuShare (macro_cn_pmi)

#### 2.3.5 采集频率
- 每月 08:00（数据发布日后）

---

### 2.4 货币金融数据

#### 2.4.1 M0/M1/M2

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 月份 | 2026-01 |
| m0 | float | M0(万亿元) | 14.61 |
| m0_yoy | float | M0同比(%) | 2.7 |
| m1 | float | M1(万亿元) | 117.97 |
| m1_yoy | float | M1同比(%) | 4.9 |
| m2 | float | M2(万亿元) | 347.19 |
| m2_yoy | float | M2同比(%) | 9.0 |
| m0_mom | float | M0环比(%) | 2.8 |
| m1_mom | float | M1环比(%) | 2.3 |
| m2_mom | float | M2环比(%) | 1.0 |

**来源**: TuShare (cn_m)

#### 2.4.2 社会融资规模

| 字段 | 类型 | 说明 |
|------|------|------|
| date | string | 月份 |
| stock | float | 存量(万亿元) |
| stock_yoy | float | 存量同比(%) |
| increment | float | 当月增量(万亿元) |
| increment_yoy | float | 增量同比(亿元) |

#### 2.4.3 社融结构

| 字段 | 类型 | 说明 |
|------|------|------|
| rmb_loan | float | 人民币贷款(万亿元) |
| foreign_loan | float | 外币贷款(万亿元) |
| entrusted_loan | float | 委托贷款(万亿元) |
| trust_loan | float | 信托贷款(万亿元) |
| corporate_bond | float | 企业债券(万亿元) |
| gov_bond | float | 政府债券(万亿元) |
| stock_financing | float | 股票融资(万亿元) |

#### 2.4.4 贷款数据

| 字段 | 类型 | 说明 |
|------|------|------|
| date | string | 月份 |
| household_short | float | 住户短期贷款(万亿元) |
| household_long | float | 住户中长期贷款(万亿元) |
| enterprise_short | float | 企业短期贷款(万亿元) |
| enterprise_long | float | 企业中长期贷款(万亿元) |

#### 2.4.5 利率数据

##### LPR贷款基础利率

| 字段 | 类型 | 说明 |
|------|------|------|
| date | string | 日期 |
| lpr_1y | float | 1年期LPR(%) |
| lpr_5y | float | 5年期以上LPR(%) |

**来源**: TuShare (shibor_lpr)

##### Shibor利率

| 字段 | 类型 | 说明 |
|------|------|------|
| date | string | 日期 |
| shibor_on | float | 隔夜Shibor(%) |
| shibor_1w | float | 1周Shibor(%) |
| shibor_2w | float | 2周Shibor(%) |
| shibor_1m | float | 1月Shibor(%) |
| shibor_3m | float | 3月Shibor(%) |
| shibor_6m | float | 6月Shibor(%) |
| shibor_9m | float | 9月Shibor(%) |
| shibor_1y | float | 1年Shibor(%) |

**来源**: TuShare (shibor)

---

### 2.5 房地产数据

#### 2.5.1 统计局数据 - 开发投资

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 月份 | 2025-12 |
| total_investment | float | 房地产开发投资(亿元) | 100000 |
| total_investment_yoy | float | 房地产开发投资同比(%) | -10.4 |
| residential_investment | float | 住宅开发投资(亿元) | 75000 |
| residential_investment_yoy | float | 住宅开发投资同比(%) | -12.9 |

#### 2.5.2 统计局数据 - 销售

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 月份 | 2025-12 |
| new_home_sales_area | float | 新建商品房销售面积(万平方米) | 95000 |
| new_home_sales_area_yoy | float | 新建商品房销售面积同比(%) | -5.5 |
| residential_sales_area | float | 住宅销售面积(万平方米) | 85000 |
| residential_sales_area_yoy | float | 住宅销售面积同比(%) | -5.6 |
| new_home_sales_amount | float | 新建商品房销售额(亿元) | 100000 |
| new_home_sales_amount_yoy | float | 新建商品房销售额同比(%) | -7.9 |
| residential_sales_amount | float | 住宅销售额(亿元) | 90000 |
| residential_sales_amount_yoy | float | 住宅销售额同比(%) | -7.6 |

#### 2.5.3 统计局数据 - 开工竣工

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 月份 | 2025-12 |
| new_start_area | float | 新开工面积(万平方米) | 100000 |
| new_start_area_yoy | float | 新开工面积同比(%) | -18.9 |
| residential_new_start | float | 住宅新开工面积(万平方米) | 75000 |
| residential_new_start_yoy | float | 住宅新开工面积同比(%) | -18.3 |
| completion_area | float | 竣工面积(万平方米) | 60000 |
| completion_area_yoy | float | 竣工面积同比(%) | -15.3 |
| residential_completion | float | 住宅竣工面积(万平方米) | 45000 |
| residential_completion_yoy | float | 住宅竣工面积同比(%) | -17.1 |

#### 2.5.4 统计局数据 - 待售

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 月份 | 2025-12 |
| inventory | float | 商品房待售面积(万平方米) | 75928 |
| residential_inventory | float | 住宅待售面积(万平方米) | 38000 |

#### 2.5.5 中指院数据 - 百城房价

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 月份 | 2025-12 |
| new_home_price | float | 百城新建住宅均价(元/平方米) | 18000 |
| new_home_price_mom | float | 百城新建住宅均价环比(%) | 0.09 |
| new_home_price_yoy | float | 百城新建住宅均价同比(%) | 2.68 |
| second_hand_price | float | 二手房均价(元/平方米) | 15000 |
| second_hand_price_mom | float | 二手房均价环比(%) | -0.74 |
| second_hand_price_yoy | float | 二手房均价同比(%) | -7.38 |

#### 2.5.6 中指院数据 - 租赁

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 月份 | 2025-12 |
| rental_50city | float | 50城住宅租赁均价(元/平方米/月) | 35 |
| rental_50city_mom | float | 50城租赁均价环比(%) | -0.39 |
| rental_50city_yoy | float | 50城租赁均价同比(%) | -3.76 |

#### 2.5.7 中指院数据 - 分线城市

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 月份 | 2025-12 |
| tier1_new_home_mom | float | 一线城市新房价格环比(%) | -0.3 |
| tier2_new_home_mom | float | 二线城市新房价格环比(%) | -0.4 |
| tier3_new_home_mom | float | 三线城市新房价格环比(%) | -0.4 |
| tier1_second_hand_mom | float | 一线城市二手房价格环比(%) | -1.0 |
| tier2_second_hand_mom | float | 二线城市二手房价格环比(%) | -0.7 |
| tier3_second_hand_mom | float | 三线城市二手房价格环比(%) | -0.6 |

#### 2.5.8 中指院数据 - 房企销售

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 月份 | 2025-12 |
| top100_sales_amount | float | Top100房企销售金额(亿元) | 26065.9 |
| top100_sales_yoy | float | Top100房企销售金额同比(%) | -12.2 |
| top100_sales_monthly_yoy | float | Top100单月销售同比(%) | 11.9 |

#### 2.5.9 中指院数据 - 土地

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 月份 | 2025-12 |
| land_transaction_area | float | 土地成交面积(万平方米) | 5000 |
| land_transaction_area_yoy | float | 土地成交面积同比(%) | -20.5 |
| land_transaction_amount | float | 土地成交金额(亿元) | 3000 |
| land_transaction_amount_yoy | float | 土地成交金额同比(%) | -25.0 |
| land_price | float | 土地成交均价(元/平方米) | 6000 |
| land_price_yoy | float | 土地成交均价同比(%) | -5.7 |

#### 2.5.10 70城房价指数

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 月份 | 2025-12 |
| city70_new_home_index | float | 70城新建住宅价格指数 | 98.5 |
| city70_new_home_mom | float | 70城新建住宅环比 | -0.2 |
| city70_second_hand_index | float | 70城二手住宅价格指数 | 95.0 |
| city70_second_hand_mom | float | 70城二手住宅环比 | -0.5 |

---

### 2.6 新闻数据

#### 2.6.1 数据项规格

| 字段 | 类型 | 说明 |
|------|------|------|
| date | string | 发布日期 |
| title | string | 标题 |
| source | string | 来源 |
| category | string | 分类 |
| importance | string | 重要程度(高/中/低) |
| url | string | 原文链接 |

#### 2.6.2 分类

- 宏观政策
- 行业动态
- 国际市场
- 公司重大事项
- 其他

**来源**: 格隆汇 / 新浪财经

---

### 2.7 分析师研究报告

#### 2.7.1 数据项规格

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 发布日期 | 2026-03-01 |
| analyst_name | string | 分析师姓名 | 张三 |
| analyst_org | string | 分析师机构 | 中金公司 |
| title | string | 报告标题 | 2026年宏观展望 |
| summary | string | 摘要 | 报告核心观点... |
| rating | string | 评级 | 买入/增持/中性/减持/卖出 |
| target_price | float | 目标价(元) | 5000 |
| target_price_change | float | 目标价涨幅(%) | 20.5 |
| industry | string | 所属行业 | 宏观策略 |
| tags | string | 标签 | GDP/货币政策/房地产 |
| url | string | 原文链接 | https://... |
| content | string | 详细内容(可选) | 报告正文... |

#### 2.7.2 评级类型

- 买入
- 增持
- 中性
- 减持
- 卖出

#### 2.7.3 数据源

| 来源 | 说明 |
|------|------|
| 同花顺 | iFinD |
| 东方财富 | Choice |
| Wind | 万得 |
| 券商官网 | 中金/华泰/中信等 |

---

### 2.8 全球市场数据

| 字段 | 类型 | 说明 |
|------|------|------|
| date | string | 日期 |
| sp500_price | float | 标普500点位 |
| sp500_change | float | 标普500涨跌幅(%) |
| nasdaq_price | float | 纳斯达克点位 |
| nasdaq_change | float | 纳斯达克涨跌幅(%) |
| hsi_price | float | 恒生指数点位 |
| hsi_change | float | 恒生指数涨跌幅(%) |
| gold_price | float | 黄金价格(美元/盎司) |
| oil_price | float | 原油价格(美元/桶) |

---

### 2.9 A股资金流数据

#### 2.9.1 融资融券数据（两融）

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 日期 | 2026-03-01 |
| margin_balance | float | 融资余额(亿元) | 15000 |
| margin_buy | float | 融资买入额(亿元) | 500 |
| margin_repay | float | 融资偿还额(亿元) | 450 |
| margin_net | float | 融资净买入(亿元) | 50 |
| short_balance | float | 融券余额(亿元) | 800 |
| short_sell | float | 融券卖出额(亿元) | 100 |
| short_repay | float | 融券偿还额(亿元) | 90 |
| short_net | float | 融券净卖出(亿元) | 10 |
| total_balance | float | 两融余额合计(亿元) | 15800 |
| margin_change | float | 融资余额变化(亿元) | 50 |
| short_change | float | 融券余额变化(亿元) | 10 |

#### 2.9.2 基金申赎数据 - 公募

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 日期 | 2026-03-01 |
| public_fund_net | float | 公募基金净申购(亿元) | 200 |
| public_fund_subscription | float | 公募基金申购(亿元) | 500 |
| public_fund_redemption | float | 公募基金赎回(亿元) | 300 |
| public_fund_nav | float | 公募基金总净值(万亿元) | 25 |

#### 2.9.3 基金申赎数据 - 私募

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 日期 | 2026-03-01 |
| private_fund_net | float | 私募基金净申购(亿元) | 50 |
| private_fund_subscription | float | 私募基金申购(亿元) | 150 |
| private_fund_redemption | float | 私募基金赎回(亿元) | 100 |
| private_fund_nav | float | 私募基金总净值(万亿元) | 5.5 |

#### 2.9.4 基金申赎数据 - ETF股票型

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 日期 | 2026-03-01 |
| etf_stock_net | float | ETF股票型净申购(亿元) | 100 |
| etf_stock_subscription | float | ETF股票型申购(亿元) | 200 |
| etf_stock_redemption | float | ETF股票型赎回(亿元) | 100 |
| etf_stock_nav | float | ETF股票型总净值(万亿元) | 2.0 |

#### 2.9.5 基金申赎数据 - ETF债券型

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 日期 | 2026-03-01 |
| etf_bond_net | float | ETF债券型净申购(亿元) | 30 |
| etf_bond_subscription | float | ETF债券型申购(亿元) | 80 |
| etf_bond_redemption | float | ETF债券型赎回(亿元) | 50 |
| etf_bond_nav | float | ETF债券型总净值(万亿元) | 0.8 |

#### 2.9.6 IPO数据

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 日期 | 2026-03-01 |
| ipo_amount | float | IPO募集资金(亿元) | 300 |
| ipo_count | int | IPO数量(家) | 10 |
| ipo_pe_avg | float | IPO平均市盈率 | 25.5 |

#### 2.9.7 定增数据

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 日期 | 2026-03-01 |
| sp_amount | float | 定增募集资金(亿元) | 500 |
| sp_count | int | 定增数量(家) | 20 |
| sp_discount_avg | float | 平均折扣率(%) | 80 |

#### 2.9.8 可转债数据

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 日期 | 2026-03-01 |
| cb_amount | float | 可转债发行金额(亿元) | 100 |
| cb_count | int | 可转债数量(只) | 5 |
| cb_conversion_price | float | 转股价(元) | 25.0 |

#### 2.9.9 资金流入 - 北向资金

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 日期 | 2026-03-01 |
| north_buy | float | 沪股通买入(亿元) | 50 |
| north_sell | float | 沪股通卖出(亿元) | 40 |
| north_net | float | 沪股通净买入(亿元) | 10 |
| south_buy | float | 深股通买入(亿元) | 60 |
| south_sell | float | 深股通卖出(亿元) | 45 |
| south_net | float | 深股通净买入(亿元) | 15 |
| north_south_net | float | 南北向合计净流入(亿元) | 25 |

#### 2.9.10 资金流入 - 新增投资者

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 日期 | 2026-03-01 |
| new_accounts | int | 新增股票账户(万户) | 150 |
| new_accounts_change | float | 新增账户环比(%) | 10.5 |

#### 2.9.11 资金流入 - 新发基金

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 日期 | 2026-03-01 |
| new_fund_raised | float | 新成立基金募资(亿元) | 300 |
| new_fund_count | int | 新成立基金数量(只) | 50 |

#### 2.9.12 资金流出 - 股东减持

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 日期 | 2026-03-01 |
| reduction_amount | float | 股东减持金额(亿元) | 100 |
| reduction_count | int | 减持公司数量(家) | 50 |

#### 2.9.13 资金流出 - 印花税

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| date | string | 日期 | 2026-03-01 |
| stamp_duty | float | 印花税收入(亿元) | 50 |

#### 2.9.14 数据源

| 数据类型 | 来源 |
|----------|------|
| 融资融券 | 东方财富 / TuShare |
| 基金申赎 | 基金业协会 / TuShare |
| IPO/定增/可转债 | 东方财富 / TuShare |
| 北向资金 | 东方财富 / TuShare |
| 新增投资者/新发基金/股东减持/印花税 | 东方财富 / TuShare |

---

## 3. 数据存储规格

### 3.1 文件命名规范

```
{数据类型}_{日期}.json

例如：
stock_2026-03-01.json
macro_2026-03-01.json
futures_2026-03-01.json
news_2026-03-01.json
pboc_2026_01.json
```

### 3.2 目录结构

```
data/
├── stock_YYYY-MM-DD.json      # A股行情
├── futures_YYYY-MM-DD.json    # 期货数据
├── macro_YYYY-MM-DD.json      # 每日宏观数据汇总
├── pboc_YYYY_MM.json          # 央行月度数据
├── news_YYYY-MM-DD.json       # 新闻数据
└── global_YYYY-MM-DD.json     # 全球市场数据
```

### 3.3 数据格式

```json
{
  "date": "2026-03-01",
  "timestamp": "2026-03-01T08:00:00",
  "source": "东方财富/TuShare/央行",
  "data": {
    // 具体数据
  }
}
```

---

## 4. 数据质量要求

### 4.1 完整性检查

| 数据类型 | 必填字段 | 最低完整率 |
|----------|----------|------------|
| 股票行情 | price, change, volume | 100% |
| 期货数据 | price, change | 100% |
| 宏观数据 | GDP, M2 | 90% |
| 新闻数据 | title, date | 90% |

### 4.2 合理性检查

| 数据类型 | 检查项 | 范围 |
|----------|--------|------|
| 股票价格 | price | > 0 |
| 涨跌幅 | change | -20 ~ 20 |
| 成交量 | volume | > 0 |
| M2同比 | m2_yoy | 0 ~ 20 |

### 4.3 评分标准

- 100分：完美
- 90-99分：优秀
- 70-89分：合格
- <70分：不合格，需重采

---

## 5. 待确认事项

- [ ] 确认数据项是否完整
- [ ] 确认采集频率
- [ ] 确认存储路径
- [ ] 确认质量检查标准

---

## 6. 数据源汇总表

| 数据类型 | 主要来源 | 备选 | Token |
|----------|----------|------|-------|
| A股行情 | 东方财富API | - | ❌ |
| 期货数据 | 东方财富API | - | ❌ |
| GDP | TuShare | 统计局网站 | ✅ |
| CPI/PPI | TuShare | 统计局网站 | ✅ |
| M2/社融 | TuShare | 央行官网 | ✅ |
| 利率 | TuShare | 央行官网 | ✅ |
| 房地产 | TuShare | 中指院 | ✅ |
| 新闻 | 格隆汇 | 新浪财经 | ❌ |
| 全球市场 | 腾讯财经 | - | ❌ |

---

*文档版本：3.0*
*待用户确认后生效*
