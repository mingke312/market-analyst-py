# 宏观策略分析师系统 - 程序设计文档

**版本**: 3.0  
**更新日期**: 2026-03-01  
**参考需求**: COMPLETE_REQUIREMENTS.md V3.0

---

## 1. 系统架构

### 1.1 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                        main.py                               │
│                    (定时任务入口)                            │
└─────────────────────────┬───────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
   ┌──────────┐     ┌──────────┐     ┌──────────┐
   │数据采集层│     │ 数据存储层│     │ 分析层   │
   └────┬─────┘     └────┬─────┘     └────┬─────┘
        │                 │                 │
        ▼                 ▼                 ▼
   collectors/          storage/         analyzer/
   - stock_collector   - json_storage   - market_analyzer
   - futures_collector                   - macro_analyzer
   - macro_collector                     - news_analyzer
   - news_collector
   - fund_flow_collector
```

### 1.2 模块设计原则

- **单一职责**：每个模块只负责一件事
- **开闭原则**：对扩展开放，对修改关闭
- **依赖注入**：通过配置注入依赖，便于测试

---

## 2. 数据采集模块 (collectors/)

### 2.1 基类设计

```python
# collectors/base.py
from abc import ABC, abstractmethod
from typing import Dict, Any, List
from datetime import datetime
import logging

class BaseCollector(ABC):
    """数据采集基类"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.storage = JSONStorage(config.get('data_dir', 'data'))
    
    @abstractmethod
    def fetch(self) -> Dict[str, Any]:
        """获取原始数据"""
        pass
    
    @abstractmethod
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """转换数据格式"""
        pass
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """验证数据"""
        return True
    
    def save(self, data: Dict[str, Any], prefix: str) -> str:
        """保存到本地"""
        date_str = datetime.now().strftime('%Y-%m-%d')
        return self.storage.save(data, prefix, date_str)
    
    def run(self) -> Dict[str, Any]:
        """执行采集流程"""
        try:
            raw = self.fetch()
            if not raw:
                self.logger.warning("No raw data fetched")
                return None
            
            data = self.transform(raw)
            
            if not self.validate(data):
                self.logger.error("Data validation failed")
                return None
            
            prefix = self.get_data_prefix()
            filepath = self.save(data, prefix)
            self.logger.info(f"Data saved to {filepath}")
            
            return data
        except Exception as e:
            self.logger.error(f"Collection failed: {e}")
            raise
    
    @abstractmethod
    def get_data_prefix(self) -> str:
        """获取数据前缀"""
        pass
```

### 2.2 A股行情采集器

```python
# collectors/stock_collector.py
from .base import BaseCollector
from typing import Dict, Any, List
import requests
from datetime import datetime

class StockCollector(BaseCollector):
    """A股行情数据采集器"""
    
    # 指数代码映射
    INDEX_CODES = {
        '000001': '上证指数',
        '399001': '深证成指',
        '399006': '创业板指',
        '000300': '沪深300',
        '000905': '中证500',
        '000688': '科创50',
    }
    
    API_URL = "https://push2.eastmoney.com/api/qt/ulist.np/get"
    
    def fetch(self) -> Dict[str, Any]:
        """从东方财富API获取行情"""
        params = {
            'fltt': 2,
            'fields': 'f1,f2,f3,f4,f5,f6,f7,f12,f13,f14',
            'secids': ','.join(self.INDEX_CODES.keys()),
            '_': str(int(datetime.now().timestamp() * 1000))
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://data.eastmoney.com/'
        }
        
        response = requests.get(
            self.API_URL, 
            params=params, 
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        result = response.json()
        return result.get('data', {}).get('diff', [])
    
    def transform(self, raw_data: List[Dict]) -> Dict[str, Any]:
        """转换为标准格式"""
        date_str = datetime.now().strftime('%Y-%m-%d')
        
        indices = []
        for item in raw_data:
            index = {
                'date': date_str,
                'code': item.get('f12'),
                'name': item.get('f14'),
                'open': item.get('f17'),    # 开盘价
                'high': item.get('f15'),   # 最高价
                'low': item.get('f16'),    # 最低价
                'close': item.get('f2'),   # 收盘价
                'change': item.get('f3'),  # 涨跌幅
                'volume': item.get('f5'),  # 成交量
                'amount': item.get('f6'),  # 成交额
                'turnover_rate': item.get('f8'),  # 换手率
                # PE/PB 需要额外接口获取
            }
            indices.append(index)
        
        return {
            'date': date_str,
            'timestamp': datetime.now().isoformat(),
            'source': '东方财富',
            'type': 'stock',
            'indices': indices
        }
    
    def get_data_prefix(self) -> str:
        return 'stock'
```

### 2.3 期货数据采集器

```python
# collectors/futures_collector.py
from .base import BaseCollector
from typing import Dict, Any, List
import requests
from datetime import datetime

class FuturesCollector(BaseCollector):
    """期货行情数据采集器"""
    
    # 股指期货代码
    FUTURES_CODES = {
        'IF': {'name': '沪深300股指期货', 'exchange': 'CFFEX'},
        'IC': {'name': '中证500股指期货', 'exchange': 'CFFEX'},
        'IM': {'name': '中证1000股指期货', 'exchange': 'CFFEX'},
        'IH': {'name': '上证50股指期货', 'exchange': 'CFFEX'},
    }
    
    # 合约期限
    CONTRACT_TERMS = ['当月', '下月', '下季', '隔季']
    
    API_URL = "https://push2.eastmoney.com/api/qt/stock/kline/get"
    
    def fetch(self) -> Dict[str, Any]:
        """获取期货行情"""
        # 实现获取逻辑
        pass
    
    def transform(self, raw_data: Dict) -> Dict[str, Any]:
        """转换数据格式"""
        date_str = datetime.now().strftime('%Y-%m-%d')
        
        futures = []
        for code, info in self.FUTURES_CODES.items():
            for term in self.CONTRACT_TERMS:
                # 处理每个合约
                pass
        
        return {
            'date': date_str,
            'timestamp': datetime.now().isoformat(),
            'source': '东方财富',
            'type': 'futures',
            'contracts': futures
        }
    
    def get_data_prefix(self) -> str:
        return 'futures'
```

### 2.4 宏观经济数据采集器

```python
# collectors/macro_collector.py
from .base import BaseCollector
from typing import Dict, Any
import tushare as ts

class MacroCollector(BaseCollector):
    """宏观经济数据采集器"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        token = config.get('tushare_token', '')
        if token:
            self.pro = ts.pro_api(token)
        else:
            self.pro = None
    
    def fetch(self) -> Dict[str, Any]:
        """从TuShare获取宏观数据"""
        if not self.pro:
            raise ValueError("TuShare token not configured")
        
        data = {}
        
        # GDP
        try:
            gdp = self.pro.cn_gdp(quarter='2024')
            data['gdp'] = gdp.to_dict('records')
        except Exception as e:
            self.logger.warning(f"GDP fetch failed: {e}")
        
        # M0/M1/M2 货币供应量
        try:
            m2_data = self.pro.cn_m(start_m='202401', end_m='202512')
            data['m2'] = m2_data.to_dict('records')  # 包含M0/M1/M2及同环
        except Exception as e:
            self.logger.warning(f"M2 fetch failed: {e}")
        
        # CPI
        try:
            cpi = self.pro.cn_cpi(start_m='202401', end_m='202512')
            data['cpi'] = cpi.to_dict('records')
        except Exception as e:
            self.logger.warning(f"CPI fetch failed: {e}")
        
        # PPI
        try:
            ppi = self.pro.cn_ppi(start_m='202401', end_m='202512')
            data['ppi'] = ppi.to_dict('records')
        except Exception as e:
            self.logger.warning(f"PPI fetch failed: {e}")
        
        return data
    
    def transform(self, raw_data: Dict) -> Dict[str, Any]:
        """转换数据格式"""
        from datetime import datetime
        
        return {
            'date': datetime.now().strftime('%Y-%m'),
            'timestamp': datetime.now().isoformat(),
            'source': 'TuShare',
            'type': 'macro',
            'data': raw_data
        }
    
    def get_data_prefix(self) -> str:
        return 'macro'
```

### 2.5 资金流数据采集器

```python
# collectors/fund_flow_collector.py
from .base import BaseCollector
from typing import Dict, Any

class FundFlowCollector(BaseCollector):
    """A股资金流数据采集器"""
    
    def fetch(self) -> Dict[str, Any]:
        """获取资金流数据"""
        # 融资融券
        # 北向资金
        # 基金申赎
        # IPO/定增
        # 新增投资者
        # 股东减持
        # 印花税
        pass
```

### 2.6 分析师研究报告采集器

```python
# collectors/analyst_report_collector.py
from .base import BaseCollector
from typing import Dict, Any, List
import requests
from datetime import datetime

class AnalystReportCollector(BaseCollector):
    """分析师研究报告采集器"""
    
    # 数据源配置
    SOURCES = {
        'tonghuashun': '同花顺iFinD',
        'eastmoney': '东方财富Choice',
        'wind': '万得',
    }
    
    def fetch(self) -> List[Dict[str, Any]]:
        """获取研究报告"""
        reports = []
        
        # 从各数据源获取
        # 1. 同花顺
        # 2. 东方财富
        # 3. Wind
        # 4. 券商官网
        
        return reports
    
    def transform(self, raw_data: List[Dict]) -> Dict[str, Any]:
        """转换为标准格式"""
        date_str = datetime.now().strftime('%Y-%m-%d')
        
        reports = []
        for item in raw_data:
            report = {
                'date': item.get('publish_date'),
                'analyst_name': item.get('analyst_name'),
                'analyst_org': item.get('org_name'),
                'title': item.get('title'),
                'summary': item.get('summary'),
                'rating': item.get('rating'),
                'target_price': item.get('target_price'),
                'target_price_change': item.get('target_price_change'),
                'industry': item.get('industry'),
                'tags': item.get('tags', []),
                'url': item.get('url'),
                'content': item.get('content'),
            }
            reports.append(report)
        
        return {
            'date': date_str,
            'timestamp': datetime.now().isoformat(),
            'source': '同花顺/东方财富/Wind',
            'type': 'analyst_report',
            'reports': reports
        }
    
    def get_data_prefix(self) -> str:
        return 'analyst_report'
```
    
    def transform(self, raw_data: Dict) -> Dict[str, Any]:
        """转换数据格式"""
        return {
            'date': '',  # 填充日期
            'timestamp': '',  # 填充时间戳
            'source': '东方财富/TuShare',
            'type': 'fund_flow',
            # 融资融券
            'margin': {
                'balance': 0,      # 融资余额
                'buy': 0,          # 融资买入
                'repay': 0,        # 融资偿还
                'net': 0,          # 融资净买入
            },
            'short': {
                'balance': 0,     # 融券余额
                'sell': 0,         # 融券卖出
                'repay': 0,        # 融券偿还
                'net': 0,          # 融券净卖出
            },
            # 北向资金
            'north_south': {
                'north_buy': 0,    # 沪股通买入
                'north_sell': 0,   # 沪股通卖出
                'north_net': 0,    # 沪股通净买入
                'south_buy': 0,   # 深股通买入
                'south_sell': 0,   # 深股通卖出
                'south_net': 0,    # 深股通净买入
            },
            # 基金申赎
            'fund': {
                'public': {},     # 公募
                'private': {},    # 私募
                'etf_stock': {},  # ETF股票型
                'etf_bond': {},   # ETF债券型
            },
            # IPO/定增
            'ipo': {},
            'sp': {},           # 定增
            'cb': {},           # 可转债
            # 新增投资者
            'new_investors': {},
            # 股东减持
            'reduction': {},
            # 印花税
            'stamp_duty': {},
        }
    
    def get_data_prefix(self) -> str:
        return 'fund_flow'
```

---

## 3. 数据存储模块 (storage/)

### 3.1 JSON存储

```python
# storage/json_storage.py
import json
import os
import glob
from typing import Dict, Any, Optional
from datetime import datetime

class JSONStorage:
    """JSON文件存储"""
    
    def __init__(self, base_dir: str = 'data'):
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)
    
    def save(self, data: Dict[str, Any], prefix: str, date: str = None) -> str:
        """保存数据到JSON文件"""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        filepath = os.path.join(self.base_dir, f'{prefix}_{date}.json')
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        return filepath
    
    def load(self, prefix: str, date: str = None) -> Optional[Dict[str, Any]]:
        """加载数据"""
        if date is None:
            # 查找最新的文件
            pattern = os.path.join(self.base_dir, f'{prefix}_*.json')
            files = glob.glob(pattern)
            if not files:
                return None
            filepath = max(files)
        else:
            filepath = os.path.join(self.base_dir, f'{prefix}_{date}.json')
            if not os.path.exists(filepath):
                return None
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def list_files(self, prefix: str = None) -> list:
        """列出数据文件"""
        if prefix:
            pattern = os.path.join(self.base_dir, f'{prefix}_*.json')
        else:
            pattern = os.path.join(self.base_dir, '*.json')
        return sorted(glob.glob(pattern))
```

---

## 4. 数据分析模块 (analyzer/)

### 4.1 行情分析器

```python
# analyzer/market_analyzer.py
from typing import Dict, Any, List

class MarketAnalyzer:
    """行情分析器"""
    
    def analyze(self, stock_data: Dict, futures_data: Dict = None) -> Dict[str, Any]:
        """分析行情数据"""
        result = {
            'index_performance': self._analyze_index_performance(stock_data),
            'volume_analysis': self._analyze_volume(stock_data),
            'turnover_rate': self._analyze_turnover(stock_data),
        }
        
        if futures_data:
            result['basis_analysis'] = self._analyze_basis(futures_data, stock_data)
        
        return result
    
    def _analyze_index_performance(self, data: Dict) -> List[Dict]:
        """分析指数涨跌幅排行"""
        indices = data.get('indices', [])
        sorted_indices = sorted(
            indices, 
            key=lambda x: x.get('change', 0), 
            reverse=True
        )
        return sorted_indices
    
    def _analyze_volume(self, data: Dict) -> Dict:
        """分析成交量"""
        indices = data.get('indices', [])
        total_volume = sum(idx.get('volume', 0) for idx in indices)
        return {'total_volume': total_volume}
    
    def _analyze_turnover(self, data: Dict) -> Dict:
        """分析换手率"""
        indices = data.get('indices', [])
        avg_turnover = sum(idx.get('turnover_rate', 0) for idx in indices) / len(indices) if indices else 0
        return {'avg_turnover_rate': avg_turnover}
    
    def _analyze_basis(self, futures_data: Dict, stock_data: Dict) -> Dict:
        """分析基差"""
        # 基差 = 期货价格 - 现货价格
        # 年化基差率 = (基差 / 现货价格) * (365 / 距到期交易日)
        pass
```

### 4.2 宏观分析器

```python
# analyzer/macro_analyzer.py
from typing import Dict, Any

class MacroAnalyzer:
    """宏观数据分析器"""
    
    def analyze(self, macro_data: Dict) -> Dict[str, Any]:
        """分析宏观数据"""
        result = {
            'gdp': self._analyze_gdp(macro_data.get('gdp')),
            'money_supply': self._analyze_money_supply(macro_data.get('m2')),
            'social_financing': self._analyze_social_financing(macro_data.get('social_financing')),
            'outlook': self._generate_outlook(macro_data),
        }
        return result
    
    def _analyze_gdp(self, gdp_data: Dict) -> Dict:
        """分析GDP"""
        if not gdp_data:
            return {}
        
        yoy = gdp_data.get('yoy', 0)
        
        if yoy > 5:
            status = '快速增长'
        elif yoy > 0:
            status = '低速增长'
        else:
            status = '负增长'
        
        return {'yoy': yoy, 'status': status}
    
    def _analyze_money_supply(self, m2_data: Dict) -> Dict:
        """分析货币供应"""
        if not m2_data:
            return {}
        
        yoy = m2_data.get('m2_yoy', 0)
        
        if yoy > 8:
            status = '宽松'
        elif yoy > 5:
            status = '稳健'
        else:
            status = '紧缩'
        
        return {'m2_yoy': yoy, 'status': status}
    
    def _analyze_social_financing(self, sf_data: Dict) -> Dict:
        """分析社融"""
        if not sf_data:
            return {}
        
        yoy = sf_data.get('stock_yoy', 0)
        
        if yoy > 10:
            status = '快速增长'
        elif yoy > 5:
            status = '稳健增长'
        else:
            status = '放缓'
        
        return {'yoy': yoy, 'status': status}
    
    def _generate_outlook(self, data: Dict) -> Dict:
        """生成宏观展望"""
        # 综合各项指标生成结论
        return {'conclusion': ''}
```

---

## 5. 主程序 (main.py)

```python
# main.py
import logging
from datetime import datetime
from collectors.stock_collector import StockCollector
from collectors.futures_collector import FuturesCollector
from collectors.macro_collector import MacroCollector
from collectors.fund_flow_collector import FundFlowCollector
from storage.json_storage import JSONStorage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """主入口"""
    config = {
        'tushare_token': 'YOUR_TOKEN_HERE',
        'data_dir': 'data'
    }
    
    logger = logging.getLogger('main')
    logger.info(f"Starting data collection at {datetime.now()}")
    
    # 采集数据
    collectors = [
        StockCollector(config),
        FuturesCollector(config),
        MacroCollector(config),
        FundFlowCollector(config),
        AnalystReportCollector(config),
        RealEstateCollector(config),
    ]
    
    results = {}
    for collector in collectors:
        try:
            logger.info(f"Running {collector.__class__.__name__}")
            result = collector.run()
            results[collector.get_data_prefix()] = result
        except Exception as e:
            logger.error(f"Collection failed: {e}")
    
    logger.info(f"Data collection completed. Results: {results.keys()}")
    return results

if __name__ == '__main__':
    main()
```

---

## 6. 目录结构

```
market-analyst-py/
├── collectors/                  # 数据采集模块
│   ├── __init__.py
│   ├── base.py                # 基类
│   ├── stock_collector.py     # A股行情
│   ├── futures_collector.py   # 期货数据
│   ├── macro_collector.py    # 宏观数据
│   ├── fund_flow_collector.py # 资金流数据
│   ├── news_collector.py     # 新闻数据
│   ├── analyst_report_collector.py # 分析师研究报告
│   └── real_estate_collector.py # 房地产数据
│
├── storage/                   # 存储模块
│   ├── __init__.py
│   └── json_storage.py       # JSON存储
│
├── analyzer/                  # 分析模块
│   ├── __init__.py
│   ├── market_analyzer.py    # 行情分析
│   ├── macro_analyzer.py    # 宏观分析
│   └── news_analyzer.py     # 新闻分析
│
├── models/                    # 数据模型
│   ├── __init__.py
│   └── data_models.py        # 数据类定义
│
├── utils/                     # 工具模块
│   ├── __init__.py
│   ├── config.py             # 配置
│   ├── trading_calendar.py  # 交易日历
│   └── data_quality.py      # 质量检查
│
├── data/                      # 数据目录
│   └── .gitkeep
│
├── main.py                    # 主入口
├── requirements.txt           # 依赖
└── README.md                 # 说明文档
```

---

## 7. 依赖

```
tushare>=1.4.0
requests>=2.25.0
playwright>=1.40.0
pandas>=1.3.0
python-dateutil>=2.8.0
```

---

*文档版本：3.0*
*更新日期：2026-03-01*
