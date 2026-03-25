#!/usr/bin/env python3
"""
期货基差年化收益率计算模块
"""
import tushare as ts
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

# 一年交易日（A股）
TRADING_DAYS_PER_YEAR = 244


def get_trading_days_to_expiry(trade_date: str, expiry_month: str, pro) -> int:
    """
    计算距到期日的交易日数
    
    算法步骤:
    1. 计算到期日（每月第三周周五）
    2. 获取从交易日至到期日的交易日历
    3. 统计交易日数量（不包括今日）
    
    参数:
        trade_date: 交易日期，格式 'YYYYMMDD'
        expiry_month: 到期月份，格式 'YYYYMM'
        pro: TuShare pro 对象
    
    返回:
        距到期的交易日数量
    """
    # 1. 计算到期日（第三周周五）
    year = int(expiry_month[:4])
    month = int(expiry_month[4:])
    
    first_day = datetime(year, month, 1)
    # 找到该月第一个周五 (weekday: Monday=0, Friday=4)
    days_to_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_to_friday)
    # 第三周周五 = 第一周周五 + 2周
    expiry_date = first_friday + timedelta(weeks=2)
    expiry_str = expiry_date.strftime('%Y%m%d')
    
    # 2. 获取交易日历
    df_cal = pro.trade_cal(
        exchange='SSE',
        start_date=trade_date,
        end_date=expiry_str
    )
    
    # 3. 计算交易日数量（不包括今日）
    trading_days = df_cal[
        (df_cal['cal_date'] > trade_date) &  # 不包括今日
        (df_cal['cal_date'] <= expiry_str) &  # 到期日为止
        (df_cal['is_open'] == 1)               # 只算交易日
    ]
    
    return len(trading_days)


def calculate_basis_annual_return(spot: float, future: float, 
                                  trading_days_to_expiry: int) -> float:
    """
    计算基差年化收益率
    
    公式:
    年化收益率 = (期货价格 - 现货价格) / 现货价格 × (244 / 距到期交易日)
    
    参数:
        spot: 现货价格
        future: 期货价格
        trading_days_to_expiry: 距到期交易日
    
    返回:
        年化收益率（百分比）
    """
    if spot <= 0 or trading_days_to_expiry <= 0:
        return 0.0
    
    basis = future - spot
    annual_return = (basis / spot) * (TRADING_DAYS_PER_YEAR / trading_days_to_expiry) * 100
    
    return annual_return


if __name__ == '__main__':
    from collectors.base import get_tushare_token
    
    token = get_tushare_token()
    pro = ts.pro_api(token)
    
    # 测试
    trade_date = '20260311'
    
    for contract, month in [('当月', '202603'), ('下月', '202604')]:
        days = get_trading_days_to_expiry(trade_date, month, pro)
        print(f"{contract}: {days} 天")
