import socket
import json
import time
from typing import Optional, Dict, Any

class BMapClient:
    """
    百度地图交通数据服务器客户端 SDK
    用于简化与 TrafficServer 的 TCP/JSON 通信。
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 8888, timeout: float = 10.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None

    def connect(self):
        """建立连接"""
        if self.sock is None:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(self.timeout)
            self.sock.connect((self.host, self.port))

    def close(self):
        """关闭连接"""
        if self.sock:
            self.sock.close()
            self.sock = None

    def _send_request(self, request_dict: Dict[str, Any]) -> Dict[str, Any]:
        """内部方法：发送 JSON 并解析返回结果"""
        try:
            self.connect()
            # 发送数据
            msg = json.dumps(request_dict).encode('utf-8')
            self.sock.sendall(msg)

            # 接收返回 (4096 字节缓存)
            response_data = self.sock.recv(4096)
            if not response_data:
                raise ConnectionError("Server closed connection without response.")

            return json.loads(response_data.decode('utf-8'))
        except Exception as e:
            return {"code": 500, "success": False, "message": f"Client Error: {str(e)}", "data": None}

    def read_segment(self, seg_id: int, history_count: int = 1) -> Dict[str, Any]:
        """
        读取特定路段的历史数据。
        :param seg_id: 路段 ID
        :param history_count: 获取最近几条历史记录 (对应服务器 hisTime)
        """
        payload = {
            "action": "read",
            "segID": seg_id,
            "hisTime": history_count
        }
        return self._send_request(payload)

    def read_all(self) -> Dict[str, Any]:
        """
        读取服务器内存中缓存的所有路段数据。
        """
        payload = {"action": "readall"}
        return self._send_request(payload)

    # 支持 with 语句 (上下文管理器)
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()