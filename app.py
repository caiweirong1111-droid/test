#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import html
import os
import re
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


BASE = Path(__file__).resolve().parent
LOG_FILE = BASE / "events.log"


def redact_log_line(line):
    line = re.sub(r"https?://\S+", "[URL已隐藏]", line)
    line = re.sub(r"(提取码[:：]\s*)\S+", r"\1[已隐藏]", line)
    return line


def read_log_tail(max_lines=220):
    if not LOG_FILE.exists():
        return "events.log 暂无内容"
    lines = LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(redact_log_line(line) for line in lines[-max_lines:])


def read_signal_tail(max_lines=80):
    if not LOG_FILE.exists():
        return "events.log 暂无内容。服务刚启动时请稍等几秒。"

    keywords = (
        "正在连接闲鱼",
        "获取 token",
        "/reg",
        "REG-REPLY",
        "ackDiff",
        "syncPushPackage.data_count",
        "收到闲鱼消息",
        "订单状态=",
        "DRY-RUN",
        "发货消息",
        "自动发货成功",
        "连接失败",
        "handler异常",
        "闲鱼监听启动失败",
    )
    signals = []
    last_heartbeat = ""
    for line in LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines():
        clean = redact_log_line(line)
        if "heartbeat /!" in clean:
            last_heartbeat = clean
            continue
        if any(k in clean for k in keywords):
            signals.append(clean)

    if last_heartbeat:
        signals.append(last_heartbeat)
    if not signals:
        return "暂时没有关键事件。请刷新首页唤醒服务后，再用另一个闲鱼账号发送：云端测试123"
    return "\n".join(signals[-max_lines:])


def log_file_info():
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    if not LOG_FILE.exists():
        return f"当前服务时间 {now} · events.log 暂无内容"
    modified = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(LOG_FILE.stat().st_mtime))
    return f"当前服务时间 {now} · events.log updated {modified}"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/logs":
            return self.handle_logs(parsed)
        if parsed.path == "/healthz":
            return self.send_text("ok\n")
        return self.handle_home()

    def handle_home(self):
        body = render_page(
            title="闲鱼自动发货运行中",
            subtitle="最近关键事件每 5 秒自动刷新。请用另一个闲鱼账号发：云端测试123",
            text=read_signal_tail(),
        )
        return self.send_html(body)

    def handle_logs(self, parsed):
        token = os.environ.get("LOG_VIEW_TOKEN", "")
        query = parse_qs(parsed.query)
        supplied = (query.get("token") or [""])[0]
        if not token:
            return self.send_text(
                "日志页未启用。请在 Render 环境变量设置 LOG_VIEW_TOKEN 后重部署。\n",
                status=403,
            )
        if supplied != token:
            return self.send_text("Forbidden\n", status=403)

        body = render_page(
            title="闲鱼自动发货完整日志",
            subtitle="完整尾部日志每 5 秒自动刷新，URL/提取码已隐藏。",
            text=read_log_tail(),
        )
        return self.send_html(body)

    def send_text(self, text, status=200):
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, text, status=200):
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return


def render_page(title, subtitle, text):
    escaped = html.escape(text)
    info = html.escape(log_file_info())
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="5">
  <title>闲鱼自动发货日志</title>
  <style>
    body {{ margin: 0; background: #111; color: #eee; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }}
    header {{ position: sticky; top: 0; padding: 12px 16px; background: #1b1b1b; border-bottom: 1px solid #333; }}
    h1 {{ margin: 0 0 6px; font-size: 18px; }}
    p {{ margin: 0; color: #bdbdbd; font-size: 13px; }}
    main {{ padding: 16px; }}
    pre {{ white-space: pre-wrap; word-break: break-word; line-height: 1.45; font-size: 13px; }}
  </style>
</head>
<body>
  <header>
    <h1>{html.escape(title)}</h1>
    <p>{html.escape(subtitle)} · {info}</p>
  </header>
  <main><pre>{escaped}</pre></main>
</body>
</html>
"""


def run_delivery():
    try:
        import mini_delivery

        asyncio.run(mini_delivery.main())
    except Exception:
        print("闲鱼监听启动失败：", flush=True)
        print(traceback.format_exc(), flush=True)


def main():
    threading.Thread(target=run_delivery, daemon=True).start()
    port = int(os.environ.get("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Web Service 已启动：http://0.0.0.0:{port}", flush=True)
    print("闲鱼监听后台线程已启动", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
