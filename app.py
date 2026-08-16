#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import mini_delivery


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = "闲鱼自动发货运行中\n".encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return


def run_delivery():
    asyncio.run(mini_delivery.main())


def main():
    threading.Thread(target=run_delivery, daemon=True).start()
    port = int(os.environ.get("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Web Service 已启动：http://0.0.0.0:{port}", flush=True)
    print("闲鱼监听已在后台启动", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
