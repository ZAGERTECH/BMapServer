import threading
from datetime import time, datetime, timedelta
from dataclasses import dataclass
import time
from datetime import datetime, time as dt_time

from BMap import TrafficManager


@dataclass
class TrafficTaskConfig:
    """
    任务配置结构体
    """
    start_time: dt_time      # 开始时间 (例如 9:00)
    end_time: dt_time        # 结束时间 (例如 18:00)
    interval_seconds: int    # 时段内的轮询间隔 (秒)
    segment_table_path: str  # 路段数据文件路径


def is_current_in_schedule(config: TrafficTaskConfig) -> bool:
    """
    判断当前系统时间是否在配置的作业时间段内。
    支持跨午夜的时间段配置（例如 23:00 到 02:00）。
    Args:
        config (TrafficTaskConfig): 包含 start_time 和 end_time 的配置对象。
    Returns:
        bool:如果在时间段内返回 True，否则返回 False。
    """
    now_time = datetime.now().time()

    if config.start_time <= config.end_time:
        return config.start_time <= now_time <= config.end_time
    else:
        # 处理跨天情况
        return now_time >= config.start_time or now_time <= config.end_time

# 线程控制事件，允许外部信号来停止线程
traffic_monitor_task_end_event = threading.Event()


def traffic_monitor_task(taskConfig: TrafficTaskConfig):
    TrafficManagerObj = TrafficManager(config_file=taskConfig.segment_table_path)
    current_worker_thread = None

    interval = taskConfig.interval_seconds

    # 首次基准计算
    now = datetime.now()
    seconds_since_midnight = (now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
    # 计算距离下一个整刻度还要多久
    seconds_to_wait = (interval - (seconds_since_midnight % interval)) % interval
    # 设定刚性的下一次运行时间
    next_run_time = now + timedelta(seconds=seconds_to_wait)

    print(f"线程启动，首次对齐时间: {next_run_time.strftime('%H:%M:%S')}")

    while traffic_monitor_task_end_event.is_set():
        try:
            # 先等待，再执行
            # 只有当时间到达 next_run_time 时，才跳出这个循环往下走
            while True:
                now = datetime.now()
                sleep_seconds = (next_run_time - now).total_seconds()

                if sleep_seconds <= 0:
                    break  # 时间到了，跳出等待循环n
                # 打印倒计时
                if int(sleep_seconds) % 10 == 0:
                    print(f"距离下一次调用({next_run_time.strftime('%H:%M:%S')})还有 {int(sleep_seconds)} 秒...",end='\n')
                time.sleep(min(1, sleep_seconds))

            # 时间到了，执行任务逻辑
            if is_current_in_schedule(taskConfig):
                if current_worker_thread is not None and current_worker_thread.is_alive():
                    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 警告：上一轮任务尚未结束，跳过本次调度！")
                else:
                    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 启动子线程执行 API 查询...")
                    current_worker_thread = threading.Thread(
                        target=TrafficManagerObj.task_query_all_segments,
                        daemon=True
                    )
                    current_worker_thread.start()
            else:
                # 不在时段内：什么都不做，直接跳过，准备计算下一次
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 当前不在运行时间段内，跳过。")
                pass

            # 设定下一次目标
            # 无论刚才是否执行，都强制把目标推向下一个刻度 (例如 12:10:00)
            next_run_time += timedelta(seconds=interval)

            # 防滞后 (处理电脑休眠的情况)
            # 如果 next_run_time 依然落后于当前时间超过一个周期，说明系统可能休眠过
            if next_run_time < datetime.now() - timedelta(seconds=interval):
                print("\n检测到时间严重滞后（可能由于系统休眠），重新对齐...")
                now = datetime.now()
                seconds_since_midnight = (now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
                seconds_to_wait = (interval - (seconds_since_midnight % interval)) % interval
                next_run_time = now + timedelta(seconds=seconds_to_wait)

        except Exception as e:
            print(f"\n线程运行出错: {e}")
            time.sleep(5)  # 出错后稍微等待再重试