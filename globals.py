import threading
from collections import deque
from dataclasses import dataclass
from typing import List, Deque
# ================= 数据结构定义 =================

@dataclass
class RoadSegment:
    """路段配置数据结构。"""
    id: int
    name: str
    direction: str
    grade: int
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    traffic_url: str
    route_url: str


@dataclass
class TrafficResult:
    """单次查询结果的数据结构。"""
    seg_id: int
    timestamp: str
    traffic_status: int
    jam_direction: int
    speed: float
    raw_json_traffic: str
    raw_json_route: str


# 定义容器类型别名
RoutineBMapData = List[TrafficResult]  # 单轮数据容器 (16个路段)
HistoryBMapData = Deque[RoutineBMapData]  # 历史数据容器 (20轮)


# ================= 全局变量与锁 =================

# 定义互斥锁，用于保护 g_history_data 的读写安全
g_data_lock = threading.Lock()

# 全局历史数据容器，最大长度20，自动挤出旧数据
g_history_data: HistoryBMapData = deque(maxlen=20)

