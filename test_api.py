from BMapClient import BMapClient


def fetch_and_analyze():
    # 指向你 main.py 中定义的 IP 和 端口
    client = BMapClient(host="127.0.0.1", port=8888)

    # 1. 获取 ID 为 1 的路段最近 5 次的历史速度
    res = client.read_segment(seg_id=1, history_count=5)
    if res["success"]:
        records = res["data"].get("seg_01", [])
        speeds = [r["speed"] for r in records]
        print(f"路段 1 最近的平均速度: {sum(speeds) / len(speeds):.2f} km/h")

    # 2. 获取全路网快照
    all_data = client.read_all()
    print("当前全路网数据已同步。")


if __name__ == "__main__":
    fetch_and_analyze()