import threading
from datetime import time, datetime
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
    """
    后台工作线程：在指定时间段内轮询百度 API。
    """

    START_TIME = taskConfig.start_time
    END_TIME = taskConfig.end_time

    # 构造TrafficManager对象
    TrafficManagerObj = TrafficManager(config_file=taskConfig.segment_table_path)

    # 用于记录当前正在运行的子线程，初始为空
    current_worker_thread: threading.Thread = None

    print(f"线程启动，计划时段: {START_TIME} - {END_TIME}")

    while traffic_monitor_task_end_event.is_set():
        try:
            if is_current_in_schedule(taskConfig):
                # 检查上一个子线程是否还在运行
                if current_worker_thread is not None and current_worker_thread.is_alive():
                    print(f"[{datetime.now()}] 警告：上一轮任务尚未结束，跳过本次调度！")
                else:
                    print(f"[{datetime.now()}] 启动子线程执行 API 查询...")

                    # 创建并启动子线程
                    # target 指向你要执行的那个函数
                    current_worker_thread = threading.Thread(
                        target=TrafficManagerObj.task_query_all_segments,
                        daemon=True  # 设置为守护线程，主程序退出它也退
                    )
                    current_worker_thread.start()

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