from BMapClient import BMapClient


def fetch_and_analyze():
    # 指向服务器的 IP 和 端口
    # client = BMapClient(host="127.0.0.1", port=8888)
    client = BMapClient(host="119.84.246.217", port=62073)

    try:
        # 调用 read_all 方法
        result = client.read_all()

        if result["success"]:
            # 此时 data 包含所有路段（seg_01, seg_02...）的数据
            all_segments_data = result["data"]
            print(f"成功获取到 {len(all_segments_data)} 个路段的全量历史数据")

            # 打印其中一个路段看看
            for seg_id, history in all_segments_data.items():
                print(f"路段 {seg_id} 当前缓存了 {len(history)} 条记录")
        else:
            print(f"读取失败: {result['message']}")

    finally:
        client.close()


# def fetch_single_segment():#调用一个的
#     # 连接 frp 映射后的公网地址
#     client = BMapClient(host="119.84.246.217", port=62073)
#
#     try:
#         # --- 核心操作：读取 ID 为 1 的路段，获取最近 5 条数据 ---
#         target_id = 1
#         count = 5
#         result = client.read_segment(seg_id=target_id, history_count=count)
#
#         if result["success"]:
#             # 注意：服务器返回的数据 Key 是字符串格式的 "seg_01" (自动补零)
#             key = f"seg_{target_id:02d}"
#             segment_history = result["data"].get(key, [])
#
#             print(f"--- 路段 {target_id} 数据 (最近 {len(segment_history)} 条) ---")
#             for record in segment_history:
#                 print(f"时间: {record['time']} | 速度: {record['speed']} km/h | 状态码: {record['trafficStatus']}")
#         else:
#             print(f"读取失败: {result['message']}")
#
#     finally:
#         client.close()

if __name__ == "__main__":
    fetch_and_analyze()