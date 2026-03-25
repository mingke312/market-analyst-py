"""
新闻数据采集器 - 从白名单来源爬取
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from datetime import datetime
import json
import re
from typing import Dict, List, Any
from bs4 import BeautifulSoup

from collectors.base import BaseCollector


class NewsCollector(BaseCollector):
    """新闻数据采集器 - 白名单来源"""
    
    # 白名单新闻源
    SOURCES = [
        {
            "name": "新浪财经",
            "url": "https://finance.sina.com.cn/stock/",
            "category": "财经"
        },
        {
            "name": "华尔街见闻", 
            "url": "https://www.wallstreetcn.com/",
            "category": "财经"
        }
    ]
    
    def __init__(self, config: Dict = None):
        if config is None:
            config = {}
        super().__init__(config)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Encoding': 'utf-8'
        })
        # 强制使用 UTF-8 编码
        self.session.encoding = 'utf-8'
    
    def _extract_sina_news(self, html: str) -> List[Dict]:
        """从新浪财经提取新闻"""
        news_list = []
        soup = BeautifulSoup(html, 'html.parser')
        
        # 查找新闻链接
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # 过滤条件：文本不为空，链接有效，是新闻链接
            if text and len(text) > 5 and ('stock' in href or 'news' in href or 'finance' in href):
                if any(x in href for x in ['sina.com.cn', 'finance.sina.com.cn']):
                    news_list.append({
                        'title': text[:200],
                        'url': href if href.startswith('http') else f'https://finance.sina.com.cn{href}',
                        'source': '新浪财经',
                        'category': '财经'
                    })
        
        # 去重
        seen = set()
        unique_news = []
        for n in news_list:
            if n['url'] not in seen:
                seen.add(n['url'])
                unique_news.append(n)
        
        return unique_news[:50]  # 限制数量
    
    def _extract_eastmoney_news(self, html: str) -> List[Dict]:
        """从东方财富网提取新闻"""
        news_list = []
        soup = BeautifulSoup(html, 'html.parser')
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            if text and len(text) > 5 and 'eastmoney' in href:
                news_list.append({
                    'title': text[:200],
                    'url': href if href.startswith('http') else f'https://www.eastmoney.com{href}',
                    'source': '东方财富网',
                    'category': '证券'
                })
        
        seen = set()
        unique_news = []
        for n in news_list:
            if n['url'] not in seen:
                seen.add(n['url'])
                unique_news.append(n)
        
        return unique_news[:30]
    
    def fetch(self, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """从白名单来源爬取新闻
        
        Args:
            start_date: 起始日期 (格式：YYYYMMDD)
            end_date: 结束日期 (格式：YYYYMMDD)
        """
        all_news = []
        
        for source in self.SOURCES:
            try:
                self.logger.info(f"Fetching from {source['name']}...")
                response = self.session.get(source['url'], timeout=15)
                response.raise_for_status()
                
                # 自动检测编码
                import re
                match = re.search(r'charset=([^\s\">]+)', response.text)
                if match:
                    response.encoding = match.group(1)
                elif response.headers.get('Content-Type', '').find('charset') > 0:
                    response.encoding = response.headers['Content-Type'].split('charset=')[-1]
                else:
                    response.encoding = 'utf-8'
                
                if 'sina' in source['url']:
                    news = self._extract_sina_news(response.text)
                elif 'eastmoney' in source['url']:
                    news = self._extract_eastmoney_news(response.text)
                else:
                    continue
                
                all_news.extend(news)
                self.logger.info(f"{source['name']}: {len(news)} articles")
                
            except Exception as e:
                self.logger.warning(f"Failed to fetch {source['name']}: {e}")
        
        # 去重
        seen = set()
        unique_news = []
        for n in all_news:
            if n['url'] not in seen:
                seen.add(n['url'])
                unique_news.append(n)
        
        return {
            'news': unique_news[:100],
            'sources': [s['name'] for s in self.SOURCES]
        }
    
    def transform(self, raw_data: Dict) -> Dict[str, Any]:
        return {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat(),
            'source': '白名单新闻源',
            'type': 'news',
            'data': raw_data
        }
    
    def validate(self, data: Dict) -> bool:
        return len(data.get('data', {}).get('news', [])) > 0
    
    def get_data_prefix(self) -> str:
        return 'news'


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    
    collector = NewsCollector()
    result = collector.run()
    
    if result:
        print(f"\n=== 新闻数据 ({len(result['data']['news'])}条) ===")
        for n in result['data']['news'][:10]:
            print(f"  - {n['title'][:50]}... [{n['source']}]")
