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

    # 第一次启动时，对齐到整点 (例如 00, 05, 10 分) ---
    # 这里的逻辑是：找到当前时间之后的第一个“整刻度”
    now = datetime.now()
    # 计算从当天0点开始过了多少秒
    seconds_since_midnight = (now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
    # 计算距离下一个整刻度还要多久
    seconds_to_wait = interval - (seconds_since_midnight % interval)
    # 设定刚性的下一次运行时间
    next_run_time = now + timedelta(seconds=seconds_to_wait)

    print(f"线程启动，首次对齐时间: {next_run_time.strftime('%H:%M:%S')}")

    while traffic_monitor_task_end_event.is_set():
        try:
            # 检查是否在允许的时间段内
            if is_current_in_schedule(taskConfig):


                if current_worker_thread is not None and current_worker_thread.is_alive():
                    print(f"警告：上一轮任务未结束")
                else:
                    # 启动线程...
                    current_worker_thread = threading.Thread(target=TrafficManagerObj.task_query_all_segments,
                                                             daemon=True)
                    current_worker_thread.start()

                # 无论当前耗时多久，强制在这个基准上加间隔
                next_run_time += timedelta(seconds=interval)

                # 倒计时逻辑
                while True:
                    now = datetime.now()
                    sleep_seconds = (next_run_time - now).total_seconds()

                    if sleep_seconds <= 0:
                        break  # 时间到了，跳出等待循环，去执行任务

                    # 打印倒计时并睡眠
                    print(f"距离下一次调用({next_run_time.strftime('%H:%M:%S')})还有 {int(sleep_seconds)} 秒...",
                          end='\n')
                    # 动态睡眠：如果在最后1秒内，就睡得短一点，提高精度
                    time.sleep(min(1, sleep_seconds))

            else:
                # 不在时段内，休眠并重新计算下一次整点
                time.sleep(1)
                # 重新对齐 next_run_time，确保进入时间段的第一秒就能响应，而不是非要等到整点
                now = datetime.now()
                if now > next_run_time:
                    seconds_since_midnight = (
                                now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
                    seconds_to_wait = interval - (seconds_since_midnight % interval)
                    next_run_time = now + timedelta(seconds=seconds_to_wait)

        except Exception as e:
            print(f"出错: {e}")
            time.sleep(5)