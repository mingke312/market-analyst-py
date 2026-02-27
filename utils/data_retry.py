#!/usr/bin/env python3
"""
重试机制模块
"""

import time
import asyncio
from functools import wraps
from typing import Callable, Any, Optional
import logging

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BASE_DELAY = 10  # 秒


class RetryError(Exception):
    """重试耗尽异常"""
    pass


def retry_with_backoff(
    max_retries: int = MAX_RETRIES,
    base_delay: float = BASE_DELAY,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,)
):
    """
    指数退避重试装饰器
    
    Args:
        max_retries: 最大重试次数
        base_delay: 基础延迟（秒）
        backoff_factor: 退避因子
        exceptions: 需要捕获的异常类型
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def sync_wrapper(*args, **kwargs) -> Any:
            last_error = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_error = e
                    if attempt < max_retries:
                        delay = base_delay * (backoff_factor ** (attempt - 1))
                        logger.warning(f"第{attempt}次尝试失败: {e}, {delay:.1f}秒后重试...")
                        time.sleep(delay)
                    else:
                        logger.error(f"达到最大重试次数({max_retries}): {e}")
            raise RetryError(f"重试{max_retries}次后仍失败: {last_error}")
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs) -> Any:
            last_error = None
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_error = e
                    if attempt < max_retries:
                        delay = base_delay * (backoff_factor ** (attempt - 1))
                        logger.warning(f"第{attempt}次尝试失败: {e}, {delay:.1f}秒后重试...")
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"达到最大重试次数({max_retries}): {e}")
            raise RetryError(f"重试{max_retries}次后仍失败: {last_error}")
        
        # 根据函数类型返回对应包装器
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    
    return decorator


async def with_timeout(coro, timeout: float):
    """
    带超时的异步执行
    
    Args:
        coro: 协程
        timeout: 超时时间（秒）
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        raise TimeoutError(f"操作超时 ({timeout}秒)")


class RetryConfig:
    """重试配置"""
    
    MARKET_DATA = {
        'max_retries': 3,
        'timeout': 15,
    }
    
    FUTURES_DATA = {
        'max_retries': 3,
        'timeout': 60,
    }
    
    NEWS_DATA = {
        'max_retries': 3,
        'timeout': 20,
    }
    
    @classmethod
    def get_config(cls, task_type: str) -> dict:
        """获取任务类型的重试配置"""
        return getattr(cls, task_type.upper(), cls.MARKET_DATA)


def retry_task(task_type: str = 'market_data'):
    """
    任务重试装饰器
    
    Usage:
        @retry_task('futures_data')
        async def fetch_futures():
            ...
    """
    config = RetryConfig.get_config(task_type)
    return retry_with_backoff(
        max_retries=config['max_retries'],
        exceptions=(TimeoutError, ConnectionError, OSError)
    )
