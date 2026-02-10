"""
BMap.py
用于实现百度地图API的调用、存储和数据处理功能。
"""
import csv
import requests
import json
import time
import os
from datetime import datetime
from typing import List, Tuple

from globals import RoutineBMapData, g_data_lock, g_history_data, RoadSegment, TrafficResult


# ================= 辅助函数：安全读取 =================

def get_latest_history_safe() -> List[RoutineBMapData]:
    """
    线程安全地获取当前全局历史数据的副本。
    供外部线程（如UI显示、数据分析）调用。
    Args:
        None
    Returns:
        List[RoutineBMapData]: 返回历史数据的浅拷贝列表。
                               如果无数据则返回空列表。
    """
    snapshot = []
    with g_data_lock:
        if len(g_history_data) > 0:
            # 将 deque 转换为 list 并拷贝，防止锁释放后数据被修改
            # 注意：这里做的是浅拷贝 list()，如果 TrafficResult 是不可变对象通常够用
            # 如果需要绝对隔离，可以使用 copy.deepcopy(list(g_history_data))
            snapshot = list(g_history_data)
    return snapshot


# ================= 核心管理类 =================

class TrafficManager:
    """交通数据管理器，负责配置加载、API轮询、数据存储及内存容器维护。"""

    def __init__(self, config_file: str, output_dir: str = "./data"):
        """初始化管理器，创建输出目录、加载配置。
        Args:
            config_file (str): CSV 配置文件路径。
            output_dir (str): 结果文件存储目录，默认为 "./data"。
        Returns:
            None
        """
        self.segments: List[RoadSegment] = []
        self.output_dir = output_dir

        # 加载配置
        self.load_config(config_file)

        # 准备输出文件路径
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        current_time_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.csv_filename = os.path.join(output_dir, f"{current_time_str}_Result.csv")
        self.log_filename = os.path.join(output_dir, f"{current_time_str}_RawJson.txt")

        # 初始化 CSV 表头
        self.init_csv_header()

    def load_config(self, file_path: str) -> None:
        """从 CSV 文件加载路段配置信息到内存。
        Args:
            file_path (str): CSV 文件的完整路径。
        Returns:
            None
        """
        try:
            with open(file_path, mode='r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    seg = RoadSegment(
                        id=int(row['id']) + 1,  # ID 从1开始
                        name=row.get('name', '').strip(),
                        direction=row.get('direction', '').strip(),
                        grade=int(row['grade']),
                        start_lat=float(row['start_lat']),
                        start_lon=float(row['start_lon']),
                        end_lat=float(row['end_lat']),
                        end_lon=float(row['end_lon']),
                        traffic_url=row['traffic_url'],
                        route_url=row['route_url']
                    )
                    self.segments.append(seg)
            print(f"[System] 成功加载 {len(self.segments)} 条路段配置。")
        except Exception as e:
            print(f"[Error] 加载配置文件失败: {e}")

    def init_csv_header(self) -> None:
        """初始化 CSV 结果文件的表头，若文件不存在则创建。
        Args:
            None
        Returns:
            None
        """
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Time", "SegID", "TrafficStatus", "JamDirection", "Speed(km/h)"])

    def fetch_traffic_status(self, seg: RoadSegment) -> Tuple[int, int, str]:
        """调用百度 API 获取交通拥堵态势，并解析拥堵方向。
        Args:
            seg (RoadSegment): 当前要查询的路段对象。
        Returns:
            Tuple[int, int, str]: (拥堵等级, 拥堵方向, 原始JSON)。
        """
        retry_count = 0
        max_retries = 5
        traffic_stat = 0
        jam_drct = 0

        while retry_count < max_retries:
            try:
                time.sleep(0.3)
                response = requests.get(seg.traffic_url, timeout=5)
                if response.status_code != 200:
                    raise Exception(f"HTTP {response.status_code}")

                data = response.json()
                if "成功" not in data.get("message", ""):
                    retry_count += 1
                    continue

                traffic_stat = int(data.get("evaluation", {}).get("status", 0))
                jam_drct = 0
                road_traffic_list = data.get("road_traffic", [])

                for rt in road_traffic_list:
                    if seg.name and seg.name != rt.get("road_name", ""):
                        continue
                    for section in rt.get("congestion_sections", []):
                        section_desc = section.get("section_desc", "")
                        if seg.direction and seg.direction in section_desc:
                            jam_drct = 1
                            break
                        elif section_desc:
                            jam_drct = -1
                    if jam_drct == 1:
                        break

                return traffic_stat, jam_drct, json.dumps(data, ensure_ascii=False)

            except Exception as e:
                retry_count += 1
                print(f"[Error] 获取交通状态失败 (Seg {seg.id}): {e}")

        return -2, -2, "{}"

    def fetch_route_speed(self, seg: RoadSegment) -> Tuple[float, str]:
        """调用百度 API 获取路径规划数据，并计算平均车速。
        Args:
            seg (RoadSegment): 当前要查询的路段对象。
        Returns:
            Tuple[float, str]: (车速km/h, 原始JSON)。
        """
        retry_count = 0
        max_retries = 5
        speed = 0.0

        while retry_count < max_retries:
            try:
                time.sleep(0.3)
                response = requests.get(seg.route_url, timeout=5)
                if response.status_code != 200:
                    raise Exception(f"HTTP {response.status_code}")

                data = response.json()
                if "成功" not in data.get("message", ""):
                    retry_count += 1
                    continue

                result = data.get("result", [])
                if result:
                    dist = float(result[0]["distance"]["value"])
                    dur = float(result[0]["duration"]["value"])
                    speed = (dist / dur) * 3.6 if dur > 0 else 0.0

                return speed, json.dumps(data, ensure_ascii=False)

            except Exception as e:
                retry_count += 1
                print(f"[Error] 获取路径规划失败 (Seg {seg.id}): {e}")

        return -2.0, "{}"

    def save_result(self, result: TrafficResult) -> None:
        """将单次查询结果写入 CSV 和 JSON 日志文件。
        Args:
            result (TrafficResult): 包含查询结果的对象。
        Returns:
            None
        """
        try:
            with open(self.csv_filename, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    result.timestamp, result.seg_id,
                    result.traffic_status, result.jam_direction,
                    f"{result.speed:.2f}"
                ])


            with open(self.log_filename, mode='a', encoding='utf-8') as f:
                f.write(f"[{result.timestamp}] [ID:{result.seg_id}] TRAFFIC: {result.raw_json_traffic}\n")
                f.write(f"[{result.timestamp}] [ID:{result.seg_id}] ROUTE:   {result.raw_json_route}\n")
        except Exception as e:
            print(f"[Error] 文件写入失败: {e}")

    def task_query_all_segments(self) -> RoutineBMapData:
        """执行一次完整的轮询任务，遍历所有路段，保存数据，并更新内存中的历史容器。
        Args:
            None
        Returns:
            RoutineBMapData: 返回当前轮询收集到的所有路段数据列表。
        """
        now_str = datetime.now().strftime("%Y%m%d.%H%M%S")
        print(f"\n[Cycle] 开始轮询 - {now_str}",end='\n')

        # 创建本轮数据的容器 (routine_bMap_data)
        current_routine_data: RoutineBMapData = []

        for seg in self.segments:
            print(f"Processing Seg {seg.id}...", end='\n')

            # 获取数据
            t_stat, j_drct, t_json = self.fetch_traffic_status(seg)
            spd, r_json = self.fetch_route_speed(seg)

            # 创建结构体对象
            res = TrafficResult(
                seg_id=seg.id, timestamp=now_str,
                traffic_status=t_stat, jam_direction=j_drct, speed=spd,
                raw_json_traffic=t_json, raw_json_route=r_json
            )

            # 持久化存储到文件
            self.save_result(res)

            # 添加到本轮内存容器
            current_routine_data.append(res)

            time.sleep(0.3)  # 避免请求过快

        # 轮询结束后，将本轮数据添加到全局历史容器
        with g_data_lock:
            g_history_data.append(current_routine_data)

        try:
            with open(self.csv_filename, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([])
            with open(self.log_filename, mode='a', encoding='utf-8') as f:
                f.write("\n")
        except Exception as e:
            print(f"[Error] 文件写入失败: {e}")


        return current_routine_data