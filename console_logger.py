"""
console_logger.py

功能：
1) 实时记录服务器控制台输出（stdout / stderr）。
2) 按行写入 txt 文件，每行增加时间前缀：[YYYY-MM-DD HH:MM:SS] 内容
3) 同时仍然输出到原控制台（不影响你原本在终端看到的输出）

用法（建议在 main.py 一开始调用）：
    from console_logger import install_console_logger
    install_console_logger(log_dir="./logs")   # 或指定 log_file="xxx.txt"
"""

from __future__ import annotations

import os
import sys
import threading
from datetime import datetime
from typing import Optional, TextIO


class _LinePrefixTee:
    """
    将写入内容：
    - 原样转发到原控制台流（stdout/stderr）
    - 并按“行”写入日志文件，前缀时间戳
    """

    def __init__(self, original_stream: TextIO, log_fp: TextIO, lock: threading.Lock):
        self._original = original_stream
        self._log_fp = log_fp
        self._lock = lock
        self._buf = ""  # 用于缓存不完整的一行

    def write(self, s: str) -> int:
        # 1) 先照常输出到控制台（不改变体验）
        n = self._original.write(s)
        self._original.flush()

        # 2) 再写入文件：按行加时间戳
        if not s:
            return n

        with self._lock:
            self._buf += s
            while "\n" in self._buf:
                line, self._buf = self._buf.split("\n", 1)
                self._write_line_to_file(line)

            # 注意：这里不强制把没换行的 buf 写入文件
            # 避免出现半行就落盘导致重复时间前缀。
            self._log_fp.flush()

        return n

    def _write_line_to_file(self, line: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # line 可能是空串（比如打印了一个纯换行），也要记录
        self._log_fp.write(f"[{ts}] {line}\n")

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
    安装控制台输出捕获器：把 stdout（以及可选 stderr）写入 txt。

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

    # 行级写入锁：避免多线程同时写导致行交错
    lock = threading.Lock()

    # line-buffered：尽量实时写入（但我们也会每次 write 后 flush）
    log_fp = open(log_file, "a", encoding=encoding, buffering=1)

    # 替换 sys.stdout / sys.stderr
    sys.stdout = _LinePrefixTee(sys.stdout, log_fp, lock)  # type: ignore[assignment]
    if also_capture_stderr:
        sys.stderr = _LinePrefixTee(sys.stderr, log_fp, lock)  # type: ignore[assignment]

    # 记录一条启动信息（可删）
    print(f"Console logger installed. Logging to: {log_file}")

    return log_file