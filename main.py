import threading
from ApiSchedule import traffic_monitor_task, TrafficTaskConfig, traffic_monitor_task_end_event
from datetime import time as dt_time

def debug():
    print("Debugging BMapServer...")
    traffic_monitor_task_end_event.set()  # 确保线程可以正常运行
    traffic_monitor_thread = threading.Thread(target=traffic_monitor_task, args=(myConfig, ), daemon=True)
    traffic_monitor_thread.start()
    print("Traffic monitor thread started.")
    traffic_monitor_thread.join()  # 等待线程结束（在实际应用中可能不需要，因为它是一个后台线程）


def main():
    print("Hello, BMapServer!")
    debug()




if __name__ == '__main__':

    myConfig = TrafficTaskConfig(
        start_time=dt_time(0, 0),
        end_time=dt_time(23, 59),
        interval_seconds=300,
        api_url="",
        api_ak="",
        segment_table_path="")

    main()