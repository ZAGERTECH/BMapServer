"""
console_logger.py

改进点：
- 只要有输出就立刻追加写入 TXT（不等待程序结束；也不等待必须输出到换行才写）
- 仍然按“行”加时间前缀：[YYYY-MM-DD HH:MM:SS]
  具体策略：每行第一次写入时加前缀，之后同一行的后续片段不重复加前缀，
           直到遇到 '\n' 才认为进入下一行。

用法：
    from console_logger import install_console_logger
    install_console_logger(log_dir="./logs")
"""

from __future__ import annotations

import os
import sys
import threading
from datetime import datetime
from typing import Optional, TextIO


class _RealtimeLinePrefixTee:
    """
    tee: 同时输出到控制台 + 文件。
    realtime: 任何片段写入都立即落盘（flush）。
    line-prefix: 每行只在“行首”加一次时间前缀。
    """

    def __init__(self, original_stream: TextIO, log_fp: TextIO, lock: threading.Lock):
        self._original = original_stream
        self._log_fp = log_fp
        self._lock = lock
        self._at_line_start = True  # 当前是否处于“新的一行开头”

    def write(self, s: str) -> int:
        # 1) 继续正常输出到控制台
        n = self._original.write(s)
        self._original.flush()

        if not s:
            return n

        # 2) 立刻写入文件（不等换行）
        with self._lock:
            i = 0
            while i < len(s):
                ch = s[i]

                # 如果在新行开头，先写时间前缀
                if self._at_line_start:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self._log_fp.write(f"[{ts}] ")
                    self._at_line_start = False

                # 写入当前字符
                self._log_fp.write(ch)

                # 遇到换行：下一次写入应被视为新的一行
                if ch == "\n":
                    self._at_line_start = True

                i += 1

            # 强制刷新到磁盘，保证“有输出就写入”
            self._log_fp.flush()

        return n

    def flush(self) -> None:
        self._original.flush()
        with self._lock:
            self._log_fp.flush()

    def isatty(self) -> bool:
        return getattr(self._original, "isatty", lambda: False)()

    def fileno(self) -> int:
        return self._original.fileno()

    @property
    def encoding(self):
        return getattr(self._original, "encoding", "utf-8")


def install_console_logger(
    log_dir: str = "./logs",
    log_file: Optional[str] = None,
    also_capture_stderr: bool = True,
    encoding: str = "utf-8",
) -> str:
    """
    安装控制台输出捕获器：把 stdout（以及可选 stderr）实时写入 txt。

    参数：
      log_dir: 日志目录（当 log_file 未指定时使用）
      log_file: 指定完整文件路径；若不指定则自动用时间生成文件名
      also_capture_stderr: 是否同时捕获 stderr（异常栈等）
      encoding: 文件编码（默认 utf-8）

    返回：
      实际日志文件路径
    """
    if log_file is None:
        os.makedirs(log_dir, exist_ok=True)
        fname = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + "_server_console.txt"
        log_file = os.path.join(log_dir, fname)
    else:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)) or ".", exist_ok=True)

    lock = threading.Lock()
    log_fp = open(log_file, "a", encoding=encoding)

    sys.stdout = _RealtimeLinePrefixTee(sys.stdout, log_fp, lock)  # type: ignore[assignment]
    if also_capture_stderr:
        sys.stderr = _RealtimeLinePrefixTee(sys.stderr, log_fp, lock)  # type: ignore[assignment]

    # 这条也会进日志
    print(f"Console logger installed. Logging to: {log_file}")

    return log_file