import threading
from TimeSchedule import traffic_monitor_task, TrafficTaskConfig, traffic_monitor_task_end_event
from datetime import time as dt_time
from SocketServer import start_traffic_server

def debug():
    print("Debugging BMapServer...")
    server = start_traffic_server(myConfig)
    traffic_monitor_task_end_event.clear() # 确保线程可以正常运行
    traffic_monitor_thread = threading.Thread(target=traffic_monitor_task, args=(myConfig, ), daemon=True)
    traffic_monitor_thread.start()
    print("Traffic monitor thread started.")
    traffic_monitor_thread.join()  # 等待线程结束


def main():
    print("Hello, BMapServer!")
    debug()

if __name__ == '__main__':

    myConfig = TrafficTaskConfig(
        start_time=dt_time(0, 0),
        end_time=dt_time(23, 59),
        interval_seconds=30, # 不宜低于30S
        segment_table_path="road_segment.csv",
        server_ip= "0.0.0.0",
        server_port= 8888
    )

    main()