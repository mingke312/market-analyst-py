# 本地运行指南

由于服务器网络受限（无法访问国内金融网站），宏观数据收集需要在**本地电脑**运行。

## 环境要求

- Python 3.8+
- akshare: `pip install akshare`

## 运行步骤

### 1. 安装依赖

```bash
pip install akshare
```

### 2. 运行脚本

```bash
cd market-analyst-py
python3 macro_collector_local.py
```

### 3. 上传数据

运行后会在 `data/` 目录生成 `macro_YYYY-MM-DD.json` 文件。

**上传方式（选一种）：**

#### 方式 A: GitHub
```bash
# 提交到 GitHub
git add data/macro_*.json
git commit -m "add macro data"
git push
```
服务器会自动同步。

#### 方式 B: 飞书上传
把生成的 JSON 文件发送到飞书，服务器可以读取。

#### 方式 C: SCP
```bash
scp data/macro_2026-02-28.json user@server:/path/to/data/
```

## 数据文件格式

```json
{
  "date": "2026-02-28",
  "type": "macro",
  "data": {
    "china_indices": [...],
    "hk_index": {...},
    "us_indices": [...],
    "gold": {...},
    "oil": {...},
    "bonds": [...],
    "macro": {
      "gdp": {...},
      "cpi": {...}
    }
  }
}
```

## 自动运行

可以设置本地定时任务（如每日早上8点）自动运行：

```bash
# Mac/Linux
crontab -e
0 8 * * * cd /path/to/market-analyst-py && python3 macro_collector_local.py

# Windows 任务计划程序
```

## 文件位置

- 脚本: `market-analyst-py/macro_collector_local.py`
- 数据: `market-analyst-py/data/macro_YYYY-MM-DD.json`
