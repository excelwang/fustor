import pytest
import time
from fustor_core.clock.logical_clock import LogicalClock

def test_logical_clock_buffer_full_eviction():
    """测试缓冲区满时旧值的正确移除逻辑 (Lines 77-80)"""
    # 模拟一个小容量的 deque 来快速测试，但由于 maxlen 是硬编码的 10000，
    # 我们需要插入超过 10000 个采样。
    clock = LogicalClock()
    
    # 插入 10000 个值为 10 的 skew
    for _ in range(10000):
        clock.update(time.time() - 10)
    
    assert clock.get_skew() == 10.0
    assert len(clock._global_buffer) == 10000
    assert clock._global_histogram[10] == 10000
    
    # 插入一个新的值 20，这应该会导致一个 10 被移除
    clock.update(time.time() - 20)
    
    assert len(clock._global_buffer) == 10000
    assert clock._global_histogram[10] == 9999
    assert clock._global_histogram[20] == 1
    
    # 插入 10000 个 20，直到 10 完全消失
    for _ in range(10000):
        clock.update(time.time() - 20)
        
    assert clock._global_histogram[10] == 0
    assert 10 not in clock._global_histogram
    assert clock.get_skew() == 20.0

def test_logical_clock_update_exception_handling():
    """测试 update 中的异常处理 (Lines 85-87)"""
    clock = LogicalClock()
    # 传入会导致减法异常的对象
    res = clock.update("invalid_time")
    # 应该静默失败并返回当前水位（物理时间）
    assert isinstance(res, float)

def test_logical_clock_empty_histogram_skew():
    """测试直方图为空时的 skew 计算 (Line 103)"""
    clock = LogicalClock()
    # 没有任何采样时
    assert clock._get_global_skew_locked() is None
    assert clock.get_skew() == 0.0

def test_logical_clock_tie_breaker():
    """测试 Mode 出现平局时的 Tie-breaker 逻辑"""
    clock = LogicalClock()
    # 插入相同频率的两个偏移
    clock.update(time.time() - 10)
    clock.update(time.time() - 20)
    
    # 频率都是 1，应该取较小的 skew (10)
    assert clock.get_skew() == 10.0
    
    # 增加 20 的频率
    clock.update(time.time() - 20)
    assert clock.get_skew() == 20.0
