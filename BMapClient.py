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
        # 这里的默认值只是为了本地方便，不需要硬编码公网IP
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
        """改进后的方法：循环接收大数据量 JSON"""
        try:
            self.connect()
            # 1. 发送请求
            msg = json.dumps(request_dict).encode('utf-8')
            self.sock.sendall(msg)

            # 2. 循环接收数据
            raw_data = b""
            while True:
                # 每次尝试读取 8KB
                chunk = self.sock.recv(8192)
                if not chunk:
                    break
                raw_data += chunk

                # 简单的完整性校验：尝试解析。如果数据还没收完，json.loads 会报错，我们继续收。
                try:
                    decoded_str = raw_data.decode('utf-8')
                    # 如果字符串以 { 开始并以 } 结束，尝试解析
                    if decoded_str.strip().startswith('{') and decoded_str.strip().endswith('}'):
                        return json.loads(decoded_str)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    # 数据不完整或编码未完成，继续接收
                    continue

            return json.loads(raw_data.decode('utf-8'))

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