#!/usr/bin/env python3
"""
交易日历模块
计算交易日、到期日
"""

from datetime import datetime, timedelta
from typing import List, Optional
import json
import os

# 2026年节假日
HOLIDAYS_2026 = [
    "2026-01-01",  # 元旦
    "2026-01-28",  # 春节
    "2026-01-29",
    "2026-01-30",
    "2026-01-31",
    "2026-02-01",
    "2026-02-02",
    "2026-02-03",
    "2026-02-14",  # 春节调休
    "2026-02-15",  # 春节调休
    "2026-04-04",  # 清明节
    "2026-04-05",
    "2026-04-06",
    "2026-05-01",  # 劳动节
    "2026-05-02",
    "2026-05-03",
    "2026-05-04",
    "2026-05-05",
    "2026-06-19",  # 端午节
    "2026-06-20",
    "2026-06-21",
    "2026-10-01",  # 国庆节
    "2026-10-02",
    "2026-10-03",
    "2026-10-04",
    "2026-10-05",
    "2026-10-06",
    "2026-10-07",
    "2026-10-10",  # 国庆调休
]

# 期货到期日 (每月第三个周五)
FUTURES_EXPIRY_2026 = [
    "2026-01-16",  # 1月
    "2026-02-13",  # 2月
    "2026-03-20",  # 3月
    "2026-04-17",  # 4月
    "2026-05-15",  # 5月
    "2026-06-19",  # 6月
    "2026-07-17",  # 7月
    "2026-08-21",  # 8月
    "2026-09-18",  # 9月
    "2026-10-16",  # 10月
    "2026-11-20",  # 11月
    "2026-12-18",  # 12月
]


def is_weekend(date: datetime) -> bool:
    """判断是否周末"""
    return date.weekday() >= 5  # 5=周六, 6=周日


def is_holiday(date: datetime) -> bool:
    """判断是否节假日"""
    date_str = date.strftime("%Y-%m-%d")
    return date_str in HOLIDAYS_2026


def is_trading_day(date: datetime) -> bool:
    """判断是否交易日"""
    if is_weekend(date):
        return False
    if is_holiday(date):
        return False
    return True


def get_trading_days_between(start_date: datetime, end_date: datetime) -> int:
    """计算两个日期之间的交易日天数"""
    count = 0
    current = start_date
    while current <= end_date:
        if is_trading_day(current):
            count += 1
        current += timedelta(days=1)
    return count


def get_next_trading_day(date: datetime) -> datetime:
    """获取下一个交易日"""
    current = date + timedelta(days=1)
    while not is_trading_day(current):
        current += timedelta(days=1)
    return current


def get_futures_expiry_date(year: int, month: int) -> datetime:
    """获取期货到期日（当月第三个周五）"""
    # 找到当月第一天
    first_day = datetime(year, month, 1)
    # 找到第一个周五
    days_until_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_until_friday)
    # 第三个周五
    third_friday = first_friday + timedelta(weeks=2)
    return third_friday


def get_contract_expiry(contract_type: str, year: int = None, month: int = None) -> datetime:
    """
    获取合约到期日
    contract_type: "当月", "下季", "隔季"
    """
    if year is None:
        year = datetime.now().year
    if month is None:
        month = datetime.now().month
    
    if contract_type == "当月":
        return get_futures_expiry_date(year, month)
    elif contract_type == "下季":
        # 下季 = 当月后的下个月
        if month == 12:
            return get_futures_expiry_date(year + 1, 1)
        else:
            return get_futures_expiry_date(year, month + 1)
    elif contract_type == "隔季":
        # 隔季 = 当月后的下下个月
        if month >= 11:
            return get_futures_expiry_date(year + 1, month - 10)
        else:
            return get_futures_expiry_date(year, month + 2)
    else:
        raise ValueError(f"Unknown contract type: {contract_type}")


def get_trading_days_to_expiry(contract_type: str, from_date: datetime = None) -> int:
    """获取距到期日的交易日天数"""
    if from_date is None:
        from_date = datetime.now()
    
    expiry = get_contract_expiry(contract_type, from_date.year, from_date.month)
    return get_trading_days_between(from_date, expiry)


if __name__ == "__main__":
    # 测试
    today = datetime.now()
    print(f"今天: {today.strftime('%Y-%m-%d')}")
    print(f"是否交易日: {is_trading_day(today)}")
    
    for ct in ["当月", "下季", "隔季"]:
        expiry = get_contract_expiry(ct, today.year, today.month)
        days = get_trading_days_to_expiry(ct, today)
        print(f"{ct}到期日: {expiry.strftime('%Y-%m-%d')}, 距{days}个交易日")
