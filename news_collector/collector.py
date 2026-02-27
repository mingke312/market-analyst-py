#!/usr/bin/env python3
"""
新闻数据收集模块 v2
支持多个新闻源：新浪财经、凤凰财经
"""

import re
import json
from typing import Dict, List
from datetime import datetime
import urllib.request
import urllib.parse


class NewsCollector:
    """新闻数据收集器"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.sources = []
    
    def fetch_from_sina(self) -> List[Dict]:
        """从新浪财经获取新闻"""
        results = []
        
        try:
            encoded_keyword = urllib.parse.quote('A股')
            url = f"https://search.sina.com.cn/?q={encoded_keyword}&c=news&sort=time"
            
            req = urllib.request.Request(url, headers=self.headers)
            
            with urllib.request.urlopen(req, timeout=10) as response:
                content = response.read().decode('utf-8', errors='ignore')
                
                # 提取标题和链接
                pattern = r'<a href="(https?://[^"]+)"[^>]*>([^<]+)</a>'
                matches = re.findall(pattern, content)
                
                seen = set()
                for url, title in matches[:15]:
                    if title.strip() and len(title) > 10 and url not in seen:
                        if 'sina.com.cn' in url or 'finance.sina' in url:
                            seen.add(url)
                            category = self._classify_news(title, '')
                            results.append({
                                'title': title.strip()[:100],
                                'url': url,
                                'source': '新浪财经',
                                'category': category,
                                'importance': self._get_importance(title, ''),
                                'timestamp': datetime.now().isoformat(),
                                'summary': '',
                            })
                            
        except Exception as e:
            print(f"新浪财经获取失败: {e}")
        
        return results
    
    def fetch_from_phoenix(self) -> List[Dict]:
        """从凤凰财经获取新闻"""
        results = []
        
        try:
            # 凤凰网财经频道
            url = "https://news.ifeng.com/"
            
            req = urllib.request.Request(url, headers=self.headers)
            
            with urllib.request.urlopen(req, timeout=10) as response:
                content = response.read().decode('utf-8', errors='ignore')
                
                # 提取财经相关新闻
                pattern = r'<a href="(https?://[^\s]+)"[^>]*title="([^"]+)"[^>]*>'
                matches = re.findall(pattern, content)
                
                seen = set()
                for url, title in matches[:20]:
                    title = title.strip()
                    # 过滤：长度合适、是财经相关内容
                    if (title and len(title) >= 10 and len(title) <= 80 and 
                        url not in seen and 'ifeng.com' in url):
                        
                        # 过滤无关链接
                        if any(x in url for x in ['finance', 'stock', 'money', 'biz', 'news']):
                            seen.add(url)
                            category = self._classify_news(title, '')
                            results.append({
                                'title': title[:100],
                                'url': url,
                                'source': '凤凰财经',
                                'category': category,
                                'importance': self._get_importance(title, ''),
                                'timestamp': datetime.now().isoformat(),
                                'summary': '',
                            })
                            
        except Exception as e:
            print(f"凤凰财经获取失败: {e}")
        
        return results
    
    def fetch_from_eastmoney(self) -> List[Dict]:
        """从东方财富获取新闻"""
        results = []
        
        try:
            url = "https://stock.eastmoney.com/"
            
            req = urllib.request.Request(url, headers=self.headers)
            
            with urllib.request.urlopen(req, timeout=10) as response:
                content = response.read().decode('utf-8', errors='ignore')
                
                # 提取新闻标题
                pattern = r'title="([^"]+)"'
                matches = re.findall(pattern, content)
                
                seen = set()
                for title in matches:
                    title = title.strip()
                    # 过滤：长度合适、是财经相关内容
                    if (title and 10 <= len(title) <= 60 and title not in seen):
                        
                        # 过滤无关标题
                        if any(x in title for x in ['股', '板块', '涨停', '跌停', '指数', '期货', '宏观', '政策', '财报', '业绩', 'A股', '美股', '港股']):
                            seen.add(title)
                            category = self._classify_news(title, '')
                            results.append({
                                'title': title[:100],
                                'url': "https://stock.eastmoney.com/",
                                'source': '东方财富',
                                'category': category,
                                'importance': self._get_importance(title, ''),
                                'timestamp': datetime.now().isoformat(),
                                'summary': '',
                            })
                            
        except Exception as e:
            print(f"东方财富获取失败: {e}")
        
        return results
    
    def fetch_from_wallstreetcn(self) -> List[Dict]:
        """从华尔街见闻获取新闻（需要认证，仅尝试）"""
        results = []
        
        try:
            # 尝试RSS或公开API
            url = "https://www.wallstreetcn.com/news"
            
            req = urllib.request.Request(url, headers=self.headers)
            
            with urllib.request.urlopen(req, timeout=10) as response:
                content = response.read().decode('utf-8', errors='ignore')
                
                # 尝试提取新闻标题
                patterns = [
                    r'<a[^>]*href="/news/([^"]+)"[^>]*>([^<]+)</a>',
                    r'"title":"([^"]+)"',
                ]
                
                for pattern in patterns:
                    matches = re.findall(pattern, content)
                    for match in matches[:10]:
                        if isinstance(match, tuple):
                            title = match[1] if len(match) > 1 else match[0]
                        else:
                            title = match
                        
                        title = title.strip()
                        if title and 10 <= len(title) <= 80:
                            results.append({
                                'title': title[:100],
                                'url': f"https://www.wallstreetcn.com/news/{match[0] if isinstance(match, tuple) else ''}",
                                'source': '华尔街见闻',
                                'category': self._classify_news(title, ''),
                                'importance': self._get_importance(title, ''),
                                'timestamp': datetime.now().isoformat(),
                                'summary': '',
                            })
                        break
                            
        except Exception as e:
            print(f"华尔街见闻获取失败: {e}")
        
        return results
    
    def fetch_all(self) -> List[Dict]:
        """从所有源获取新闻"""
        all_news = []
        
        # 新浪财经
        sina_news = self.fetch_from_sina()
        all_news.extend(sina_news)
        print(f"新浪财经: {len(sina_news)}条")
        
        # 凤凰财经
        phoenix_news = self.fetch_from_phoenix()
        all_news.extend(phoenix_news)
        print(f"凤凰财经: {len(phoenix_news)}条")
        
        # 东方财富
        eastmoney_news = self.fetch_from_eastmoney()
        all_news.extend(eastmoney_news)
        print(f"东方财富: {len(eastmoney_news)}条")
        
        # 华尔街见闻 (可能失败)
        try:
            wsnews = self.fetch_from_wallstreetcn()
            all_news.extend(wsnews)
            print(f"华尔街见闻: {len(wsnews)}条")
        except:
            pass
        
        # 去重 (使用URL和标题组合)
        seen = set()
        unique_results = []
        for news in all_news:
            # 使用URL+标题作为唯一标识
            key = (news['url'], news['title'][:30])
            if key not in seen:
                seen.add(key)
                unique_results.append(news)
        
        return unique_results[:20]
    
    def _classify_news(self, title: str, content: str) -> str:
        """分类新闻"""
        text = (title + content).lower()
        
        if any(kw in text for kw in ['降息', '降准', '加息', '通胀', 'gdp', '经济', '政策', '央行', '财政部', '证监会', '货币']):
            return '宏观政策'
        elif any(kw in text for kw in ['美股', '港股', '美联储', '欧洲', '日本', '韩国', '关税', '贸易', '特朗普', '拜登']):
            return '国际市场'
        elif any(kw in text for kw in ['涨停', '跌停', '并购', '重组', '上市', 'ipo', '财报', '业绩', '分红', 'a股', '股市', '大盘', '指数']):
            return '公司重大事项'
        elif any(kw in text for kw in ['新能源', '半导体', '医药', '银行', '地产', '汽车', '科技', 'ai', '人工智能', '芯片', '光伏']):
            return '行业动态'
        else:
            return '其他'
    
    def _get_importance(self, title: str, content: str) -> str:
        """判断重要性"""
        text = (title + content).lower()
        
        high_importance = ['央行', '降息', '降准', '加息', '关税', '重大', '涨停', '跌停', 
                          '突发', '重磅', '利好', '利空', '政策', '监管', '证监会', '美股',
                          '崩盘', '暴涨', '大跌', '突破', '历史']
        
        if any(kw in text for kw in high_importance):
            return '高'
        return '中'
    
    def collect_all(self) -> Dict:
        """收集所有新闻"""
        data = self.fetch_all()
        
        return {
            'date': datetime.now().strftime("%Y-%m-%d"),
            'type': 'news',
            'sources': ['新浪财经', '凤凰财经', '华尔街见闻'],
            'count': len(data),
            'data': data,
            'timestamp': datetime.now().isoformat()
        }


def main():
    """命令行入口"""
    import sys
    
    collector = NewsCollector()
    result = collector.collect_all()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--json':
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"📰 新闻数据 ({result['count']}条)")
        print("-" * 50)
        
        # 按来源统计
        sources_count = {}
        for item in result['data']:
            src = item['source']
            sources_count[src] = sources_count.get(src, 0) + 1
        
        print(f"来源: {sources_count}")
        
        # 按分类统计
        categories = {}
        for item in result['data']:
            cat = item['category']
            categories[cat] = categories.get(cat, 0) + 1
        
        print(f"分类: {categories}")
        
        print("-" * 50)
        print("\n重要新闻:")
        for item in result['data'][:5]:
            if item['importance'] == '高':
                print(f"⭐ [{item['source']}] {item['title'][:50]}")
                print(f"   [{item['category']}]")


if __name__ == "__main__":
    main()
