import threading
from datetime import time, datetime
from dataclasses import dataclass
import time
from datetime import datetime, time as dt_time

@dataclass
class TrafficTaskConfig:
    """
    任务配置结构体
    """
    start_time: dt_time      # 开始时间 (例如 9:00)
    end_time: dt_time        # 结束时间 (例如 18:00)
    interval_seconds: int    # 时段内的轮询间隔 (秒)
    api_url: str             # API 地址
    api_ak: str              # 百度 AK
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

# 线程控制事件，允许外部信号来停止线程 traffic_monitor_task_end_event.set() 来停止线程，
traffic_monitor_task_end_event = threading.Event()

def traffic_monitor_task(taskConfig: TrafficTaskConfig):
    """
    后台工作线程：在指定时间段内轮询百度 API。
    """

    START_TIME = taskConfig.start_time
    END_TIME = taskConfig.end_time


    print(f"线程启动，计划时段: {START_TIME} - {END_TIME}")

    while traffic_monitor_task_end_event.is_set():
        try:
            if is_current_in_schedule(taskConfig):
                # 在时段内：调用 API
                print("当前在时段内，调用百度API...")

                print(f"[{datetime.now()}] 进等待...\n")

                counter = 0

                while counter < taskConfig.interval_seconds:
                    print(f"距离下一次调用还有 {taskConfig.interval_seconds - counter} 秒...", end='\r')
                    time.sleep(1)
                    counter += 1

            else:
                # 不在时段内：每秒查询一次时间
                print(f"当前不在时段内，1秒后重试...", end='\r') # 打印太多会刷屏，可以注释掉
                time.sleep(1)

        except KeyboardInterrupt:
            # 允许外部强制中断
            break
        except Exception as e:
            print(f"线程运行出错: {e}")
            time.sleep(5) # 出错后稍微等待再重试，防止死循环刷报错