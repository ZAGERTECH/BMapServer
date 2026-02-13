import socketserver
import json
import threading
from TimeSchedule import TrafficTaskConfig

# 引入全局变量
from globals import g_history_data, g_data_lock, TrafficResult


class JsonResponse:
    @staticmethod
    def make(success, msg, data=None):
        """
        生成标准 JSON 响应字符串。
        :param success: 响应码
        :param msg: 附带的信息
        :param data: 数据段
        :return: 生成的Json串
        """
        return json.dumps({
            "code": 200 if success else 400,
            "success": success,
            "message": msg,
            "data": data
        }, ensure_ascii=False)


class TrafficDataAccess:
    """数据访问：安全读取全局变量 g_history_data"""

    def _fmt(self, res: TrafficResult):
        return {
            "time": res.timestamp, "segID": res.seg_id,
            "trafficStatus": res.traffic_status, "jamDirection": res.jam_direction,
            "speed": res.speed
        }

    def get_data(self, seg_id=0, count=0, read_all=False):
        res_dict = {}
        with g_data_lock:
            # 拷贝数据快照
            snapshot = list(g_history_data)

        # 如果不是 read_all，则只取最近 count 条
        if not read_all and count > 0:
            snapshot = snapshot[-count:]

        for frame in snapshot:
            for seg_res in frame:
                # 如果是 read_all 或者 ID 匹配
                if read_all or seg_res.seg_id == seg_id:
                    key = f"seg_{seg_res.seg_id:02d}"
                    if key not in res_dict: res_dict[key] = []
                    res_dict[key].append(self._fmt(seg_res))
        return res_dict


db = TrafficDataAccess()


class TrafficTCPHandler(socketserver.StreamRequestHandler):
    def handle(self):
        # 获取客户端 IP 和 端口
        client_ip = self.client_address[0]
        client_port = self.client_address[1]

        # 打印连接信息
        print(f"\n[Server] 新客户端连接: {client_ip}:{client_port}")

        try:
            while True:
                # 读取数据
                data = self.request.recv(409600)
                if not data:
                    break

                req_str = data.decode('utf-8').strip()
                if not req_str:
                    continue

                # 打印接收到的原始数据
                print(f"[Server] 收到来自 {client_ip} 的消息: {req_str}")

                try:
                    req = json.loads(req_str)
                    action = req.get('action')

                    if action == 'read':
                        seg_id = int(req.get('segID', 0))
                        count = int(req.get('hisTime', 1))
                        print(f"[Server] 执行动作: read (SegID={seg_id}, Count={count})")

                        data = db.get_data(seg_id=seg_id, count=count)
                        self.wfile.write(JsonResponse.make(True, "OK", data).encode('utf-8'))

                    elif action == 'readall':
                        print(f"[Server] 执行动作: readall")
                        data = db.get_data(read_all=True)
                        self.wfile.write(JsonResponse.make(True, "OK", data).encode('utf-8'))
                    else:
                        print(f"[Server] 未知动作: {action}")
                        self.wfile.write(JsonResponse.make(False, "Unknown action").encode('utf-8'))

                except Exception as e:
                    print(f"[Server] 处理请求出错: {e}")
                    self.wfile.write(JsonResponse.make(False, str(e)).encode('utf-8'))
        except ConnectionResetError:
            print(f"[Server] 客户端 {client_ip} 强制断开连接")
        except Exception as e:
            print(f"[Server] 通信异常: {e}")
        finally:
            print(f"[Server] 客户端断开: {client_ip}:{client_port}")


def start_traffic_server(taskConfig: TrafficTaskConfig) -> socketserver.ThreadingTCPServer:
    """启动 TCP 服务 (Daemon线程)"""
    # 配置服务器地址和端口
    SERVER_HOST = taskConfig.server_ip
    SERVER_PORT = taskConfig.server_port

    socketserver.ThreadingTCPServer.allow_reuse_address = True
    server = socketserver.ThreadingTCPServer((SERVER_HOST, SERVER_PORT), TrafficTCPHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server