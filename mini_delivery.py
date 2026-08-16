#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
闲鱼 Mini 自动发货器 v0.4
重点修复：
1) 识别闲鱼“系统订单消息”，不再只做全局关键词猜测。
2) 解析 reminderContent / redReminder / detailNotice / reminderNotice / extJson/updateKey。
3) DRY-RUN 下打印订单状态和关键字段，便于先把“付款检测”跑通。
4) 普通私信也打印一行摘要，用来验证 WebSocket 确实在收消息。
"""

import asyncio
import base64
import json
import os
import re
import sqlite3
import struct
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any
import ssl
import certifi
import websockets

# 强制 WebSocket 使用 certifi CA，避免 macOS/Python 证书链偶发失效。
_CA_FILE = certifi.where()
os.environ["SSL_CERT_FILE"] = _CA_FILE
os.environ["REQUESTS_CA_BUNDLE"] = _CA_FILE
_SSL_CONTEXT = ssl.create_default_context(cafile=_CA_FILE)
_ORIGINAL_WS_CONNECT = websockets.connect

def _ws_connect_with_certifi(*args, **kwargs):
    if args and str(args[0]).startswith("wss://"):
        kwargs.setdefault("ssl", _SSL_CONTEXT)
    return _ORIGINAL_WS_CONNECT(*args, **kwargs)

websockets.connect = _ws_connect_with_certifi

BASE = Path(__file__).resolve().parent
VENDOR = BASE / "vendor" / "XianYuApis"
sys.path.insert(0, str(VENDOR))
os.chdir(VENDOR)

from goofish_live import XianyuLive
from message import make_text
from utils.goofish_utils import decrypt, generate_mid, generate_uuid, get_session_cookies_str

CONFIG = BASE / "config.json"
DB = BASE / "delivery.db"
LOG = BASE / "events.log"

PENDING_SHIP_TEXTS = (
    "我已付款，等待你发货",
    "已付款，待发货",
    "买家已付款",
    "付款完成",
    "记得及时发货",
    "等待你发货",
    "等待卖家发货",
    "去发货",
)
PENDING_PAYMENT_TEXTS = (
    "我已拍下，待付款",
    "买家已拍下，待付款",
    "等待买家付款",
)
CANCELLED_TEXTS = ("交易关闭", "退款成功", "钱款已原路退返")


def log(s: str):
    print(s, flush=True)
    with LOG.open("a", encoding="utf-8") as f:
        f.write(s.rstrip() + "\n")


def mask_id(v, keep=4):
    if v is None:
        return "None"
    s = str(v)
    if len(s) <= keep * 2:
        return "*" * len(s)
    return f"{s[:keep]}...{s[-keep:]}"


def load_config():
    cfg = json.loads(os.path.expandvars(CONFIG.read_text(encoding="utf-8")))
    for a in cfg.get("accounts", []):
        cookie = ""
        cookie_env = str(a.get("cookie_env", "") or "").strip()
        if cookie_env:
            cookie = os.environ.get(cookie_env, "")

        # 本地兼容兜底：部署配置使用 cookie_env，不把 cookies/ 提交进仓库。
        if not cookie:
            cookie_file = str(a.get("cookie_file", "") or "").strip()
            if cookie_file:
                p = BASE / cookie_file
                if p.exists():
                    cookie = p.read_text(encoding="utf-8")

        a["cookie"] = cookie.replace("\r", "").replace("\n", "").strip()
        a["_cookie_env_name"] = cookie_env
    return cfg


def cookie_key_summary(cookie: str):
    keys = []
    for part in str(cookie or "").split(";"):
        if "=" not in part:
            continue
        key = part.split("=", 1)[0].strip()
        if key:
            keys.append(key)
    required = ["_m_h5_tk", "_m_h5_tk_enc", "cookie2", "unb", "sgcookie", "cna"]
    missing = [k for k in required if k not in keys]
    return {
        "length": len(cookie or ""),
        "key_count": len(keys),
        "sample_keys": keys[:12],
        "missing_common_keys": missing,
    }


def safe_response_summary(obj):
    if isinstance(obj, dict):
        out = {
            "keys": list(obj.keys())[:12],
        }
        if "ret" in obj:
            out["ret"] = obj.get("ret")
        if "code" in obj:
            out["code"] = obj.get("code")
        data = obj.get("data")
        if isinstance(data, dict):
            out["data_keys"] = list(data.keys())[:12]
        return out
    return {"type": type(obj).__name__}


def init_db():
    c = sqlite3.connect(DB)
    c.execute(
        "CREATE TABLE IF NOT EXISTS delivered("
        "account TEXT,event_key TEXT,created_at DATETIME DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(account,event_key))"
    )
    c.commit()
    c.close()


def seen(a, k):
    c = sqlite3.connect(DB)
    r = c.execute(
        "SELECT 1 FROM delivered WHERE account=? AND event_key=?", (a, k)
    ).fetchone()
    c.close()
    return bool(r)


def mark(a, k):
    c = sqlite3.connect(DB)
    c.execute(
        "INSERT OR IGNORE INTO delivered(account,event_key) VALUES(?,?)", (a, k)
    )
    c.commit()
    c.close()


def walk(x, path="root"):
    if isinstance(x, dict):
        for k, v in x.items():
            p = f"{path}.{k}"
            yield p, v
            yield from walk(v, p)
    elif isinstance(x, list):
        for i, v in enumerate(x):
            p = f"{path}[{i}]"
            yield p, v
            yield from walk(v, p)


def text_blob(x):
    return "\n".join(
        str(v) for _, v in walk(x) if isinstance(v, (str, int, float))
    )


def pick(x, names):
    names = {n.lower() for n in names}
    for path, v in walk(x):
        key = re.split(r"[.\[\]]+", path)[-1].lower()
        if key in names and isinstance(v, (str, int)):
            return str(v)
    return None


def load_json_dict(v):
    if isinstance(v, dict):
        return v
    if not isinstance(v, str) or not v.strip():
        return {}
    try:
        x = json.loads(v)
        return x if isinstance(x, dict) else {}
    except Exception:
        return {}


def decode_ws_message(raw: dict):
    """
    返回 [(来源, 已解析对象), ...]
    兼容 syncPushPackage 直接 JSON 和 protobuf/加密消息。
    """
    out = [("raw", raw)]
    try:
        d = raw["body"]["syncPushPackage"]["data"][0]["data"]
    except Exception:
        return out

    if isinstance(d, dict):
        out.append(("sync_dict", d))
        return out

    if not isinstance(d, str):
        return out

    try:
        x = json.loads(d)
        out.append(("sync_json", x))
    except Exception:
        pass

    try:
        z = decrypt(d)
        if isinstance(z, bytes):
            z = z.decode("utf-8", "ignore")
        x = json.loads(z)
        out.append(("decrypted", x))
    except Exception:
        pass

    return out


def extract_system_meta(obj: dict):
    """
    兼容闲鱼解密后的系统/聊天消息结构。
    注意：WebSocket 顶层包的 "1" 有时是 list；只有解密后的消息 "1"
    才通常是 dict。遇到 list 必须跳过，不能直接 .get()。
    """
    empty = {
        "reminder_content": "",
        "red_reminder": "",
        "detail_notice": "",
        "reminder_notice": "",
        "reminder_title": "",
        "sender_id": "",
        "cid": "",
        "update_key": "",
        "task_name": "",
        "card_title": "",
        "button_text": "",
    }

    if not isinstance(obj, dict):
        return empty, {}

    m1 = obj.get("1", {})
    if not isinstance(m1, dict):
        return empty, {}

    m10 = m1.get("10", {})
    if not isinstance(m10, dict):
        m10 = {}

    m6 = m1.get("6", {})
    if not isinstance(m6, dict):
        m6 = {}

    m63 = m6.get("3", {})
    if not isinstance(m63, dict):
        m63 = {}

    payload = load_json_dict(m63.get("5", ""))
    ext = load_json_dict(m10.get("extJson", ""))
    biz = load_json_dict(m10.get("bizTag", ""))

    fields = {
        "reminder_content": str(m10.get("reminderContent", "") or "").strip(),
        "red_reminder": str(m10.get("redReminder", "") or "").strip(),
        "detail_notice": str(m10.get("detailNotice", "") or "").strip(),
        "reminder_notice": str(m10.get("reminderNotice", "") or "").strip(),
        "reminder_title": str(m10.get("reminderTitle", "") or "").strip(),
        "sender_id": str(m10.get("senderUserId", "") or "").strip(),
        "cid": str(m1.get("2", "") or "").split("@")[0],
        "update_key": str(ext.get("updateKey", "") or "").strip(),
        "task_name": str(biz.get("taskName", "") or "").strip(),
    }

    # 尝试卡片标题/按钮
    try:
        ex = payload["dxCard"]["item"]["main"]["exContent"]
        fields["card_title"] = str(ex.get("title", "") or "").strip()
        fields["button_text"] = str(
            (ex.get("button") or {}).get("text", "") or ""
        ).strip()
    except Exception:
        fields["card_title"] = ""
        fields["button_text"] = ""

    return fields, payload


def status_from_meta(meta: dict, blob: str):
    candidates = [
        meta.get("red_reminder", ""),
        meta.get("reminder_content", ""),
        meta.get("detail_notice", ""),
        meta.get("reminder_notice", ""),
        meta.get("button_text", ""),
        meta.get("card_title", ""),
    ]
    joined = "\n".join(candidates) + "\n" + blob

    if any(x in joined for x in CANCELLED_TEXTS):
        return "cancelled"
    if any(x in joined for x in PENDING_SHIP_TEXTS):
        return "pending_ship"
    if any(x in joined for x in PENDING_PAYMENT_TEXTS):
        return "pending_payment"
    return None


def extract_long_id_from_update_key(s):
    if not s:
        return None
    nums = re.findall(r"\d{10,}", s)
    return nums[0] if nums else None


def extract_event(raw):
    decoded = decode_ws_message(raw)

    best = {
        "status": None,
        "item_id": None,
        "order_id": None,
        "sender_id": None,
        "cid": None,
        "title": None,
        "summary": "",
        "source": "",
    }

    summaries = []

    for source, obj in decoded:
        if not isinstance(obj, dict):
            continue

        blob = text_blob(obj)
        meta, payload = extract_system_meta(obj)
        status = status_from_meta(meta, blob)

        # 订单/商品字段既可能在解密消息，也可能藏在卡片 payload 中。
        combined = {"obj": obj, "payload": payload}
        item_id = pick(combined, ["itemId", "item_id", "auctionId", "goodsId"])
        order_id = pick(
            combined, ["orderId", "order_id", "tradeId", "bizOrderId", "trade_id"]
        )
        if not order_id:
            order_id = extract_long_id_from_update_key(meta.get("update_key", ""))

        title = pick(
            combined, ["itemTitle", "goodsTitle", "auctionTitle", "title"]
        )

        # system meta 比泛化 pick 更可靠
        sender_id = meta.get("sender_id") or pick(
            combined, ["senderUserId", "sendUserId", "buyerId"]
        )
        cid = meta.get("cid") or pick(combined, ["cid", "conversationId"])
        if cid and "@goofish" in cid:
            cid = cid.split("@")[0]

        visible = [
            meta.get("reminder_title"),
            meta.get("reminder_content"),
            meta.get("red_reminder"),
            meta.get("detail_notice"),
            meta.get("reminder_notice"),
            meta.get("card_title"),
            meta.get("button_text"),
        ]
        visible = [x for x in visible if x]
        if visible:
            summaries.append(f"{source}: " + " | ".join(visible))

        # 优先采用能识别出订单状态的对象
        if status or (not best["source"] and visible):
            best.update(
                {
                    "status": status or best["status"],
                    "item_id": item_id or best["item_id"],
                    "order_id": order_id or best["order_id"],
                    "sender_id": sender_id or best["sender_id"],
                    "cid": cid or best["cid"],
                    "title": title or best["title"],
                    "source": source,
                }
            )

    best["summary"] = " || ".join(summaries)[:2000]
    return best


def match_rule(cfg, e):
    # 先 item_id，再标题关键词；若只有 1 条规则且已明确 pending_ship，
    # DRY-RUN 阶段允许作为调试兜底，正式发货仍建议填 item_id/关键词。
    for r in cfg.get("rules", []):
        iid = str(r.get("item_id", "")).strip()
        kw = str(r.get("title_keyword", "")).strip()
        if iid and iid == str(e.get("item_id") or ""):
            return r
        if kw and kw in str(e.get("title") or ""):
            return r
        if kw and kw in str(e.get("summary") or ""):
            return r

    rules = cfg.get("rules", [])
    if cfg.get("dry_run", True) and e.get("status") == "pending_ship" and len(rules) == 1:
        return rules[0]
    return None



class MessagePackDecoder:
    """轻量 MessagePack 解码器，按当前闲鱼开源项目的实现思路。"""
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0
        self.length = len(data)

    def read_byte(self):
        if self.pos >= self.length:
            raise ValueError("Unexpected end of data")
        b = self.data[self.pos]
        self.pos += 1
        return b

    def read_bytes(self, n):
        if self.pos + n > self.length:
            raise ValueError("Unexpected end of data")
        out = self.data[self.pos:self.pos+n]
        self.pos += n
        return out

    def u8(self): return self.read_byte()
    def u16(self): return struct.unpack(">H", self.read_bytes(2))[0]
    def u32(self): return struct.unpack(">I", self.read_bytes(4))[0]
    def u64(self): return struct.unpack(">Q", self.read_bytes(8))[0]
    def i8(self): return struct.unpack(">b", self.read_bytes(1))[0]
    def i16(self): return struct.unpack(">h", self.read_bytes(2))[0]
    def i32(self): return struct.unpack(">i", self.read_bytes(4))[0]
    def i64(self): return struct.unpack(">q", self.read_bytes(8))[0]
    def f32(self): return struct.unpack(">f", self.read_bytes(4))[0]
    def f64(self): return struct.unpack(">d", self.read_bytes(8))[0]

    def string(self, n):
        return self.read_bytes(n).decode("utf-8", errors="ignore")

    def array(self, n):
        return [self.value() for _ in range(n)]

    def map(self, n):
        d = {}
        for _ in range(n):
            k = self.value()
            v = self.value()
            d[k] = v
        return d

    def value(self):
        b = self.read_byte()

        if b <= 0x7f: return b
        if 0x80 <= b <= 0x8f: return self.map(b & 0x0f)
        if 0x90 <= b <= 0x9f: return self.array(b & 0x0f)
        if 0xa0 <= b <= 0xbf: return self.string(b & 0x1f)
        if b == 0xc0: return None
        if b == 0xc2: return False
        if b == 0xc3: return True
        if b == 0xc4: return self.read_bytes(self.u8())
        if b == 0xc5: return self.read_bytes(self.u16())
        if b == 0xc6: return self.read_bytes(self.u32())
        if b == 0xca: return self.f32()
        if b == 0xcb: return self.f64()
        if b == 0xcc: return self.u8()
        if b == 0xcd: return self.u16()
        if b == 0xce: return self.u32()
        if b == 0xcf: return self.u64()
        if b == 0xd0: return self.i8()
        if b == 0xd1: return self.i16()
        if b == 0xd2: return self.i32()
        if b == 0xd3: return self.i64()
        if b == 0xd9: return self.string(self.u8())
        if b == 0xda: return self.string(self.u16())
        if b == 0xdb: return self.string(self.u32())
        if b == 0xdc: return self.array(self.u16())
        if b == 0xdd: return self.array(self.u32())
        if b == 0xde: return self.map(self.u16())
        if b == 0xdf: return self.map(self.u32())
        if b >= 0xe0: return b - 0x100
        raise ValueError(f"Unknown format byte: {b:02x}")



def decode_msgpack_stream(raw: bytes, max_objects=64):
    """
    连续解码同一个 buffer 中的多个 MessagePack 对象。
    旧 JS 报 “end of buffer not reached” 正说明当前 payload 可能不是单一对象。
    返回所有成功解出的对象。
    """
    dec = MessagePackDecoder(raw)
    values = []
    while dec.pos < dec.length and len(values) < max_objects:
        start = dec.pos
        try:
            values.append(dec.value())
        except Exception:
            # 防止死循环
            if dec.pos <= start:
                dec.pos = start + 1
            break
    return values, dec.pos, dec.length

def json_safe(obj):
    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8")
        except Exception:
            return obj.decode("utf-8", errors="ignore")
    raise TypeError(f"{type(obj)} not JSON serializable")


def modern_decrypt(data: str):
    """
    base64 -> MessagePack stream。
    v0.4.8 不再只取第一个对象，而是把整个 buffer 连续解完。
    """
    if not isinstance(data, str):
        data = str(data)

    s = data.strip()
    missing = len(s) % 4
    if missing:
        s += "=" * (4 - missing)

    raw = base64.b64decode(s)
    values, consumed, total = decode_msgpack_stream(raw)

    def conv_keys_only(x):
        if isinstance(x, dict):
            out = {}
            for k, v in x.items():
                if isinstance(k, bytes):
                    k = k.decode("utf-8", errors="ignore")
                else:
                    k = str(k)
                out[k] = conv_keys_only(v)
            return out
        if isinstance(x, list):
            return [conv_keys_only(v) for v in x]
        return x

    values = [conv_keys_only(v) for v in values]

    # 一个对象时保持旧结构；多个对象时用一个明确容器包起来，后续递归扫描全部对象。
    if len(values) == 1:
        return values[0]
    return {
        "__msgpack_stream__": values,
        "__stream_consumed__": consumed,
        "__stream_total__": total,
    }


def parse_upstream_push(message):
    """
    当前兼容路径：
    1) 取 syncPushPackage.data[0].data
    2) 先尝试 base64 -> UTF-8 JSON
    3) 再调用上游项目自带的 JS MessagePack 解码器
    4) 最后才使用纯 Python MessagePack 作为诊断兜底

    实时聊天/订单推送使用的是 MessagePack map。上游 JS 解码器能正确
    还原 decoded["1"]["10"]；简化版 Python 解码器对部分 record/连续帧
    只能读到外层 list，因此不能把它当作实时消息的首选解码路径。
    """
    result = {
        "is_sync_push": False,
        "decoded": None,
        "decode_mode": "",
        "error": "",
        "raw_type": "",
        "raw_len": 0,
    }

    try:
        body = message.get("body", {}) if isinstance(message, dict) else {}
        spp = body.get("syncPushPackage", {}) if isinstance(body, dict) else {}
        arr = spp.get("data", []) if isinstance(spp, dict) else []
        if not isinstance(arr, list) or not arr:
            return result

        result["is_sync_push"] = True
        raw = arr[0].get("data") if isinstance(arr[0], dict) else None
        result["raw_type"] = type(raw).__name__

        if isinstance(raw, dict):
            result["decoded"] = raw
            result["decode_mode"] = "dict"
            return result

        if not isinstance(raw, str):
            result["error"] = f"data不是str/dict，而是 {type(raw).__name__}"
            return result

        result["raw_len"] = len(raw)

        # 当前成熟实现首先尝试 base64 -> JSON
        try:
            s = raw.strip()
            missing = len(s) % 4
            if missing:
                s += "=" * (4 - missing)
            decoded_text = base64.b64decode(s).decode("utf-8")
            parsed = json.loads(decoded_text)
            result["decoded"] = parsed
            result["decode_mode"] = "base64-json"
            return result
        except Exception:
            pass

        # 实时聊天和订单卡片：优先使用上游项目已经验证过的 JS 解码器。
        # decrypt() 返回 JSON 字符串；失败时继续走纯 Python 诊断兜底。
        try:
            legacy = decrypt(raw)
            if isinstance(legacy, bytes):
                legacy = legacy.decode("utf-8", errors="ignore")
            parsed = json.loads(legacy) if isinstance(legacy, str) else legacy
            if isinstance(parsed, (dict, list)):
                result["decoded"] = parsed
                result["decode_mode"] = "upstream-js-msgpack"
                return result
        except Exception:
            pass

        # 最后尝试简化 MessagePack，只用于发现未知结构和打印诊断信息。
        try:
            result["decoded"] = modern_decrypt(raw)
            result["decode_mode"] = "msgpack"
            return result
        except Exception as e:
            result["error"] = f"{type(e).__name__}: {e}"
            return result

    except Exception as e:
        result["error"] = f"{type(e).__name__}: {e}"
        return result


def extract_direct_chat(decoded):
    """
    直接读取上游示例使用的字段：
    decoded["1"]["10"]["reminderTitle"/"senderUserId"/"reminderContent"]
    decoded["1"]["2"] 为 cid。
    """
    if not isinstance(decoded, dict):
        return {}

    m1 = decoded.get("1")
    if not isinstance(m1, dict):
        return {}

    m10 = m1.get("10")
    if not isinstance(m10, dict):
        m10 = {}

    cid = m1.get("2", "")
    cid = str(cid or "")
    if "@goofish" in cid:
        cid = cid.split("@")[0]

    return {
        "reminder_title": str(m10.get("reminderTitle", "") or "").strip(),
        "reminder_content": str(m10.get("reminderContent", "") or "").strip(),
        "sender_id": str(m10.get("senderUserId", "") or "").strip(),
        "cid": cid,
        "red_reminder": str(m10.get("redReminder", "") or "").strip(),
        "detail_notice": str(m10.get("detailNotice", "") or "").strip(),
        "reminder_notice": str(m10.get("reminderNotice", "") or "").strip(),
    }


def iter_nodes(obj, path="root"):
    """递归遍历任意 dict/list 结构。"""
    yield path, obj
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from iter_nodes(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from iter_nodes(v, f"{path}[{i}]")


def find_key_values(obj, wanted_keys):
    wanted = {str(k).lower() for k in wanted_keys}
    found = []
    for path, node in iter_nodes(obj):
        if isinstance(node, dict):
            for k, v in node.items():
                if str(k).lower() in wanted:
                    found.append((f"{path}.{k}", v))
    return found


def extract_trade_ids(decoded):
    """从显式字段、提醒链接和 updateKey 中提取商品/订单 ID。"""
    item_hits = find_key_values(
        decoded, ["itemId", "item_id", "auctionId", "goodsId"]
    )
    order_hits = find_key_values(
        decoded, ["orderId", "order_id", "tradeId", "bizOrderId", "trade_id"]
    )

    item_id = str(item_hits[0][1]) if item_hits else None
    order_id = str(order_hits[0][1]) if order_hits else None

    for _, node in iter_nodes(decoded):
        if not isinstance(node, str):
            continue

        if not item_id:
            m = re.search(r"(?:itemId|item_id|auctionId|goodsId)[=:](\d{6,})", node)
            if m:
                item_id = m.group(1)

        if not order_id:
            m = re.search(
                r"(?:bizOrderId|orderId|order_id|tradeId|trade_id|"
                r"order_detail\?id)[=:](\d{10,})",
                node,
            )
            if m:
                order_id = m.group(1)

        # 常见 updateKey: cid:bizOrderId:1_status_role
        if not order_id:
            m = re.search(r"\d{6,}:(\d{10,}):\d+_[A-Za-z_]+", node)
            if m:
                order_id = m.group(1)

        if item_id and order_id:
            break

    return item_id, order_id


def compact_structure(obj):
    """只输出结构，不把整条消息刷满终端。"""
    if isinstance(obj, dict):
        parts = []
        for k, v in list(obj.items())[:8]:
            if isinstance(v, dict):
                parts.append(f"{k}:dict({len(v)})")
            elif isinstance(v, list):
                parts.append(f"{k}:list({len(v)})")
            else:
                parts.append(f"{k}:{type(v).__name__}")
        return "{" + ", ".join(parts) + "}"
    if isinstance(obj, list):
        types = [type(x).__name__ for x in obj[:8]]
        return f"list(len={len(obj)}, first_types={types})"
    return f"{type(obj).__name__}"


def type_summary(v):
    if isinstance(v, dict):
        return f"dict({len(v)})"
    if isinstance(v, list):
        return f"list({len(v)})"
    if isinstance(v, str):
        return f"str(len={len(v)})"
    if isinstance(v, bytes):
        return f"bytes(len={len(v)})"
    return type(v).__name__


def top_keys(obj, limit=12):
    if isinstance(obj, dict):
        return [str(k) for k in list(obj.keys())[:limit]]
    if isinstance(obj, list):
        return [f"[{i}]:{type_summary(v)}" for i, v in enumerate(obj[:limit])]
    return []


def clone_with_single_sync_entry(message, entry):
    body = message.get("body", {}) if isinstance(message, dict) else {}
    spp = body.get("syncPushPackage", {}) if isinstance(body, dict) else {}
    clone = dict(message)
    clone_body = dict(body)
    clone_spp = dict(spp)
    clone_spp["data"] = [entry]
    clone_body["syncPushPackage"] = clone_spp
    clone["body"] = clone_body
    return clone



_BASE64_RE = re.compile(r'^[A-Za-z0-9+/=_-]+$')

def looks_base64_text(s):
    if not isinstance(s, str):
        return False
    t = s.strip()
    return len(t) >= 20 and _BASE64_RE.match(t) is not None


def decode_nested_strings(obj, max_depth=4):
    """
    递归解内层 payload。
    支持：
    - str: base64 -> JSON / MessagePack
    - bytes: 直接尝试 UTF-8 JSON / MessagePack
    """
    def decode_bytes(raw: bytes):
        # bytes -> utf8 json
        try:
            txt = raw.decode("utf-8")
            parsed = json.loads(txt)
            return parsed, "bytes-json"
        except Exception:
            pass

        # bytes -> msgpack stream
        try:
            vals, consumed, total = decode_msgpack_stream(raw)
            if vals:
                parsed = vals[0] if len(vals) == 1 else {
                    "__msgpack_stream__": vals,
                    "__stream_consumed__": consumed,
                    "__stream_total__": total,
                }
                if isinstance(parsed, (dict, list)):
                    return parsed, "bytes-msgpack-stream"
        except Exception:
            pass

        return None, ""

    def decode_str(s):
        if not isinstance(s, str):
            return None, ""
        t = s.strip()
        if len(t) < 8:
            return None, ""

        # 尝试原始 JSON
        try:
            parsed = json.loads(t)
            if isinstance(parsed, (dict, list)):
                return parsed, "str-json"
        except Exception:
            pass

        # 尝试 base64
        if looks_base64_text(t):
            try:
                if len(t) % 4:
                    t += "=" * (4 - len(t) % 4)
                raw = base64.b64decode(t)
                parsed, mode = decode_bytes(raw)
                if parsed is not None:
                    return parsed, "base64-" + mode
            except Exception:
                pass

        return None, ""

    def normalize_keys(x):
        if isinstance(x, dict):
            out = {}
            for k, v in x.items():
                if isinstance(k, bytes):
                    k = k.decode("utf-8", errors="ignore")
                else:
                    k = str(k)
                out[k] = normalize_keys(v)
            return out
        if isinstance(x, list):
            return [normalize_keys(v) for v in x]
        return x

    def walk_decode(x, depth, path="root"):
        if depth > max_depth:
            return x, []

        notes = []

        if isinstance(x, bytes):
            dec, mode = decode_bytes(x)
            if dec is not None:
                notes.append((path, mode, len(x)))
                dec = normalize_keys(dec)
                return walk_decode(dec, depth + 1, path)
            return x, notes

        if isinstance(x, str):
            dec, mode = decode_str(x)
            if dec is not None:
                notes.append((path, mode, len(x)))
                dec = normalize_keys(dec)
                return walk_decode(dec, depth + 1, path)
            return x, notes

        if isinstance(x, dict):
            out = {}
            for k, v in x.items():
                v2, child = walk_decode(v, depth + 1, f"{path}.{k}")
                out[str(k)] = v2
                notes.extend(child)
            return out, notes

        if isinstance(x, list):
            out = []
            for i, v in enumerate(x):
                v2, child = walk_decode(v, depth + 1, f"{path}[{i}]")
                out.append(v2)
                notes.extend(child)
            return out, notes

        return x, notes

    return walk_decode(obj, 0)

def extract_recursive_message(decoded):
    """
    新格式兼容：不假设 decoded["1"] 一定是 dict。
    在整个解码树里递归寻找 reminderContent / senderUserId / reminderTitle / cid。
    """
    out = {
        "reminder_content": "",
        "reminder_title": "",
        "chat_text": "",
        "sender_id": "",
        "cid": "",
        "red_reminder": "",
        "detail_notice": "",
        "reminder_notice": "",
    }

    key_map = {
        "remindercontent": "reminder_content",
        "remindertitle": "reminder_title",
        "senderuserid": "sender_id",
        "redreminder": "red_reminder",
        "detailnotice": "detail_notice",
        "remindernotice": "reminder_notice",
    }

    for path, node in iter_nodes(decoded):
        if not isinstance(node, dict):
            continue
        for k, v in node.items():
            lk = str(k).lower()
            if lk in key_map and not out[key_map[lk]]:
                if isinstance(v, bytes):
                    v = v.decode("utf-8", errors="ignore")
                out[key_map[lk]] = str(v or "").strip()

        if not out["chat_text"] and str(node.get("contentType", "")) == "1":
            text_node = node.get("text")
            if isinstance(text_node, dict) and text_node.get("text"):
                out["chat_text"] = str(text_node.get("text") or "").strip()

        # MessagePack 普通文本常见落点：decoded["1"]["6"]["3"]["2"]。
        if not out["chat_text"] and "2" in node:
            v = node.get("2")
            parent_keys = {str(k) for k in node.keys()}
            if isinstance(v, str) and v.strip() and {"1", "2"}.issubset(parent_keys):
                if (
                    len(v.strip()) <= 500
                    and "@goofish" not in v
                    and re.search(r"[\u4e00-\u9fff]", v)
                ):
                    out["chat_text"] = v.strip()

        # cid / conversationId 也递归找
        if not out["cid"]:
            for ck in ("cid", "conversationId", "conversation_id"):
                if ck in node and node.get(ck):
                    out["cid"] = str(node.get(ck)).split("@")[0]
                    break
        if not out["cid"]:
            for ck in ("2", "4"):
                v = node.get(ck)
                if isinstance(v, str) and v.endswith("@goofish"):
                    out["cid"] = v.split("@")[0]
                    break

    return out


def collect_interesting_strings(decoded, limit=12):
    """
    当字段名发生变化时，用人类可读字符串反推结构。
    只取少量短字符串，避免终端刷屏。
    """
    vals = []
    seen = set()
    for path, node in iter_nodes(decoded):
        if not isinstance(node, str):
            continue
        s = node.strip()
        if not s or len(s) > 220:
            continue
        # 优先中文、测试文本、订单相关词
        interesting = (
            "测试123" in s
            or any(w in s for w in ("付款", "发货", "订单", "交易", "买家", "卖家"))
            or re.search(r"[\u4e00-\u9fff]", s)
        )
        if not interesting:
            continue
        if s in seen:
            continue
        seen.add(s)
        vals.append((path, s))
        if len(vals) >= limit:
            break
    return vals


def split_sync_push_entries(message):
    try:
        if not isinstance(message, dict):
            return [message]
        body = message.get("body")
        if not isinstance(body, dict):
            return [message]
        spp = body.get("syncPushPackage")
        if not isinstance(spp, dict):
            return [message]
        arr = spp.get("data")
        if not isinstance(arr, list) or len(arr) <= 1:
            return [message]

        packets = []
        for entry in arr:
            packets.append(clone_with_single_sync_entry(message, entry))
        return packets
    except Exception:
        return [message]


class Live(XianyuLive):
    def __init__(self, cfg):
        super().__init__(cfg["cookie"])
        self.cfg = cfg
        self.name = cfg.get("name", "店铺")
        self._last_probe_signature = None
        self._last_nested_sig = None
        self._last_stream_sig = None
        self._frame_seq = 0
        self._reg_mid = ""
        self._ackdiff_mid = ""
        self._ackdiff_sent = False
        self._hb_count = 0
        self._dry_run_seen = set()
        self._pending_sends = {}

    async def init(self, ws):
        data = self.xianyu.get_token()
        token = data.get("data", {}).get("accessToken", "") if isinstance(data, dict) else ""
        if not token:
            log(
                f"[{self.name}] ⚠️ 获取 token 失败；"
                f"cookie_env={self.cfg.get('_cookie_env_name') or '-'} "
                f"cookie_diag={cookie_key_summary(self.cfg.get('cookie', ''))} "
                f"token_resp={safe_response_summary(data)}"
            )
            raise RuntimeError("获取 token 失败")

        self._reg_mid = generate_mid()
        reg_msg = {
            "lwp": "/reg",
            "headers": {
                "cache-header": "app-key token ua wv",
                "app-key": "444e9908a51d1cb236a27862abc769c9",
                "token": token,
                "ua": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 "
                    "Safari/537.36 DingTalk(2.1.5) OS(Windows/10) "
                    "Browser(Chrome/133.0.0.0) DingWeb/2.1.5 IMPaaS DingWeb/2.1.5"
                ),
                "dt": "j",
                "wv": "im:3,au:3,sy:6",
                "sync": "0,0;0;0;",
                "did": self.device_id,
                "mid": self._reg_mid,
            },
        }
        await ws.send(json.dumps(reg_msg))
        log(f"[{self.name}] ➡️ /reg 已发送 mid={mask_id(self._reg_mid)}")

    async def send_ackdiff(self, ws, reason=""):
        current_time = int(time.time() * 1000)
        self._ackdiff_mid = generate_mid()
        ackdiff_msg = {
            "lwp": "/r/SyncStatus/ackDiff",
            "headers": {"mid": self._ackdiff_mid},
            "body": [
                {
                    "pipeline": "sync",
                    "tooLong2Tag": "PNM,1",
                    "channel": "sync",
                    "topic": "sync",
                    "highPts": 0,
                    "pts": current_time * 1000,
                    "seq": 0,
                    "timestamp": current_time,
                }
            ],
        }
        await ws.send(json.dumps(ackdiff_msg))
        self._ackdiff_sent = True
        suffix = f" reason={reason}" if reason else ""
        log(f"[{self.name}] ➡️ /r/SyncStatus/ackDiff 已发送 mid={mask_id(self._ackdiff_mid)}{suffix}")

    async def heart_beat(self, ws):
        while True:
            mid = generate_mid()
            await ws.send(json.dumps({"lwp": "/!", "headers": {"mid": mid}}))
            self._hb_count += 1
            if self._hb_count == 1 or self._hb_count % 4 == 0:
                log(f"[{self.name}] 💓 heartbeat /! 已发送 count={self._hb_count} mid={mask_id(mid)}")
            await asyncio.sleep(15)

    def make_server_ack(self, message):
        headers = message.get("headers", {}) if isinstance(message, dict) else {}
        ack = {
            "code": 200,
            "headers": {
                "mid": headers.get("mid") or generate_mid(),
                "sid": headers.get("sid") or "",
            },
        }
        for key in ("app-key", "ua", "dt"):
            if key in headers:
                ack["headers"][key] = headers[key]
        return ack

    async def send_text_confirmed(self, ws, cid, toid, text, timeout=12):
        mid = generate_mid()
        msg = {
            "lwp": "/r/MessageSend/sendByReceiverScope",
            "headers": {"mid": mid},
            "body": [
                {
                    "uuid": generate_uuid(),
                    "cid": f"{cid}@goofish",
                    "conversationType": 1,
                    "content": {
                        "contentType": 101,
                        "custom": {
                            "type": 1,
                            "data": base64.b64encode(
                                json.dumps(
                                    {"contentType": 1, "text": {"text": text}},
                                    ensure_ascii=False,
                                ).encode("utf-8")
                            ).decode("utf-8"),
                        },
                    },
                    "redPointPolicy": 0,
                    "extension": {"extJson": "{}"},
                    "ctx": {"appVersion": "1.0", "platform": "web"},
                    "mtags": {},
                    "msgReadStatusSetting": 1,
                },
                {
                    "actualReceivers": [
                        f"{toid}@goofish",
                        f"{self.myid}@goofish",
                    ]
                },
            ],
        }

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._pending_sends[mid] = fut
        try:
            await ws.send(json.dumps(msg, ensure_ascii=False))
            log(f"[{self.name}] ➡️ 发货消息已发送，等待服务端确认 mid={mask_id(mid)}")
            reply = await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._pending_sends.pop(mid, None)

        code = reply.get("code") if isinstance(reply, dict) else None
        if code != 200:
            raise RuntimeError(f"sendByReceiverScope 服务端未确认成功，code={code}")
        return reply

    async def wire_trace(self, seq, message):
        if not isinstance(message, dict):
            log(f"[{self.name}] 🧵 frame#{seq} 非 JSON 对象：{type_summary(message)}")
            return

        headers = message.get("headers", {})
        body = message.get("body", {})
        mid = headers.get("mid") if isinstance(headers, dict) else ""
        lwp = message.get("lwp", "")
        code = message.get("code", "")

        marks = []
        if mid and mid == self._reg_mid:
            marks.append("REG-REPLY")
        if mid and mid == self._ackdiff_mid:
            marks.append("ACKDIFF-REPLY")
        mark_text = f" [{' '.join(marks)}]" if marks else ""

        body_keys = list(body.keys()) if isinstance(body, dict) else []
        header_keys = list(headers.keys()) if isinstance(headers, dict) else []
        log(
            f"[{self.name}] 🧵 frame#{seq}{mark_text} "
            f"lwp={lwp or '-'} code={code or '-'} "
            f"mid={mask_id(mid) if mid else '-'} "
            f"headers={header_keys} body_keys={body_keys if body_keys else type_summary(body)}"
        )

        if marks:
            ok = code == 200
            log(f"[{self.name}] {'✅' if ok else '⚠️'} {'/'.join(marks)} code={code}")

        spp = body.get("syncPushPackage") if isinstance(body, dict) else None
        if not isinstance(spp, dict):
            return
        arr = spp.get("data")
        if not isinstance(arr, list):
            log(f"[{self.name}] 🧵 frame#{seq} syncPushPackage.data={type_summary(arr)}")
            return

        log(f"[{self.name}] 🧵 frame#{seq} syncPushPackage.data_count={len(arr)}")
        for idx, entry in enumerate(arr):
            if not isinstance(entry, dict):
                log(f"[{self.name}]    data[{idx}] entry={type_summary(entry)}")
                continue

            raw = entry.get("data")
            parsed = parse_upstream_push(clone_with_single_sync_entry(message, entry))
            decoded = parsed.get("decoded")
            decoded_top = top_keys(decoded)
            if isinstance(decoded, dict):
                field_types = [f"{k}:{type_summary(v)}" for k, v in list(decoded.items())[:10]]
            elif isinstance(decoded, list):
                field_types = [type_summary(v) for v in decoded[:10]]
            else:
                field_types = []

            log(
                f"[{self.name}]    data[{idx}] "
                f"objectType={entry.get('objectType', '-')} bizType={entry.get('bizType', '-')} "
                f"raw={type_summary(raw)} mode={parsed.get('decode_mode') or '-'} "
                f"top={decoded_top} fields={field_types} "
                f"error={parsed.get('error') or '-'}"
            )

    async def main(self):
        headers = {
            "Cookie": get_session_cookies_str(self.xianyu.session),
            "Host": "wss-goofish.dingtalk.com",
            "Connection": "Upgrade",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
            ),
            "Origin": "https://www.goofish.com",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }
        threading.Thread(target=self.user_alive, daemon=True).start()
        async with websockets.connect(self.base_url, extra_headers=headers) as websocket:
            asyncio.create_task(self.init(websocket))
            asyncio.create_task(self.heart_beat(websocket))
            async for raw_frame in websocket:
                self._frame_seq += 1
                seq = self._frame_seq
                log(
                    f"[{self.name}] 🧵 TEXT frame#{seq} "
                    f"type={type(raw_frame).__name__} len={len(raw_frame) if hasattr(raw_frame, '__len__') else '-'}"
                )
                try:
                    message = json.loads(raw_frame)
                except Exception as e:
                    log(f"[{self.name}] ⚠️ frame#{seq} JSON 解析失败：{type(e).__name__}: {e}")
                    continue

                await self.wire_trace(seq, message)
                ack = self.make_server_ack(message)
                await websocket.send(json.dumps(ack))
                log(
                    f"[{self.name}] ⬅️ frame#{seq} ACK 已发送 "
                    f"mid={mask_id(ack['headers'].get('mid'))} sid={'yes' if ack['headers'].get('sid') else 'no'}"
                )

                headers = message.get("headers", {}) if isinstance(message, dict) else {}
                recv_mid = headers.get("mid") if isinstance(headers, dict) else ""
                pending = self._pending_sends.get(recv_mid)
                if pending and not pending.done():
                    pending.set_result(message)
                    log(
                        f"[{self.name}] ✅ 发货消息服务端回包 "
                        f"mid={mask_id(recv_mid)} code={message.get('code')}"
                    )

                if (
                    not self._ackdiff_sent
                    and isinstance(headers, dict)
                    and headers.get("mid") == self._reg_mid
                    and message.get("code") == 200
                ):
                    await self.send_ackdiff(websocket, reason="after-reg-200")

                await self.handle_message(message, websocket)

    async def handle_message(self, m, ws):
        packets = split_sync_push_entries(m)
        if len(packets) > 1:
            log(f"[{self.name}] 📦 本次 syncPushPackage 含 {len(packets)} 条 data，逐条解析")
        for idx, packet in enumerate(packets):
            if len(packets) > 1:
                log(f"[{self.name}]    ↳ 解析 data[{idx}]")
            await self._handle_one_message(packet, ws)

    async def _handle_one_message(self, m, ws):
        try:
            parsed = parse_upstream_push(m)
            if not parsed["is_sync_push"]:
                return

            if parsed["error"]:
                log(
                    f"[{self.name}] 📡 收到推送，但解码失败："
                    f"type={parsed['raw_type']} len={parsed['raw_len']} "
                    f"error={parsed['error']}"
                )
                return

            decoded = parsed["decoded"]
            if not isinstance(decoded, (dict, list)):
                return

            # v0.4.6：当前实测外层 decoded["1"] 是 list，list 内还有字符串载荷。
            # 再递归解一层/多层，直到拿到真正聊天字典。
            expanded, nested_notes = decode_nested_strings(decoded)
            if nested_notes:
                note_sig = tuple(nested_notes[:6])
                if note_sig != getattr(self, "_last_nested_sig", None):
                    self._last_nested_sig = note_sig
                    for where, mode, ln in nested_notes[:6]:
                        log(f"[{self.name}] 🧩 发现内层载荷 {where} -> {mode}, len={ln}")
            decoded = expanded

            # v0.4.8：如果一个 buffer 里其实串了多个 msgpack 对象，打印一次数量。
            if isinstance(decoded, dict) and "__msgpack_stream__" in decoded:
                frames = decoded.get("__msgpack_stream__") or []
                sig = (len(frames), decoded.get("__stream_consumed__"), decoded.get("__stream_total__"))
                if sig != getattr(self, "_last_stream_sig", None):
                    self._last_stream_sig = sig
                    log(
                        f"[{self.name}] 🧱 MessagePack 连续帧："
                        f"{len(frames)} 个对象，consumed={sig[1]}/{sig[2]}"
                    )

            direct = extract_recursive_message(decoded)

            if direct.get("reminder_content") or direct.get("reminder_title"):
                display_text = direct.get("chat_text") or direct.get("reminder_content", "")
                log(
                    f"[{self.name}] 📨 收到闲鱼消息 "
                    f"(mode={parsed['decode_mode']}): "
                    f"{direct.get('reminder_title','')} | {display_text} "
                    f"cid={mask_id(direct.get('cid'))} sender={mask_id(direct.get('sender_id'))}"
                )

            # 递归找订单状态文本
            full_blob = text_blob(decoded) if isinstance(decoded, dict) else "\n".join(
                str(v) for _, v in iter_nodes(decoded)
                if isinstance(v, (str, int, float))
            )
            pseudo_meta = {
                "red_reminder": direct.get("red_reminder", ""),
                "reminder_content": direct.get("reminder_content", ""),
                "detail_notice": direct.get("detail_notice", ""),
                "reminder_notice": direct.get("reminder_notice", ""),
                "button_text": "",
                "card_title": "",
            }
            status = status_from_meta(pseudo_meta, full_blob)

            if status:
                title_hits = find_key_values(decoded, ["itemTitle","goodsTitle","auctionTitle","title"])
                cid_hits = find_key_values(decoded, ["cid","conversationId","conversation_id"])

                item_id, order_id = extract_trade_ids(decoded)
                title = str(title_hits[0][1]) if title_hits else None
                cid = direct.get("cid")
                if not cid and cid_hits:
                    cid = str(cid_hits[0][1]).split("@")[0]

                e = {
                    "status": status,
                    "item_id": item_id,
                    "order_id": order_id,
                    "sender_id": direct.get("sender_id"),
                    "cid": cid,
                    "title": title,
                    "summary": " | ".join(
                        x for x in [
                            direct.get("reminder_title"),
                            direct.get("reminder_content"),
                            direct.get("red_reminder"),
                            direct.get("detail_notice"),
                            direct.get("reminder_notice"),
                        ] if x
                    ),
                }

                log(
                    f"[{self.name}] 🧾 订单状态={status} "
                    f"order_id={mask_id(order_id)} item_id={mask_id(item_id)} "
                    f"cid={mask_id(cid)} buyer={mask_id(direct.get('sender_id'))} title={title}"
                )

                if status != "pending_ship":
                    return

                missing = []
                if not e.get("cid"):
                    missing.append("cid")
                if not e.get("sender_id"):
                    missing.append("buyer/sender_id")
                if not (e.get("order_id") or e.get("item_id")):
                    missing.append("order_id/item_id")
                if missing:
                    log(
                        f"[{self.name}] ⚠️ 已识别“已付款待发货”，"
                        f"但缺少关键字段：{', '.join(missing)}；暂不触发 dry-run/发送。"
                    )
                    return

                r = match_rule(self.cfg, e)
                if not r:
                    log(f"[{self.name}] ⚠️ 已识别“已付款待发货”，但没有匹配商品规则。")
                    return

                key = order_id or f"{cid}:{item_id}:{r.get('title_keyword','')}"
                if self.cfg.get("dry_run", True):
                    if key in self._dry_run_seen:
                        return
                    self._dry_run_seen.add(key)
                    log(
                        f"[{self.name}] 🧪 DRY-RUN：已识别付款，本应发送：\n"
                        f"{r['delivery_text']}"
                    )
                    return

                toid = e.get("sender_id")
                if not cid or not toid:
                    log(f"[{self.name}] ⚠️ 已识别付款，但缺少 cid/sender_id；暂不真实发货。")
                    return

                if seen(self.name, key):
                    return

                try:
                    await self.send_text_confirmed(ws, cid, toid, r["delivery_text"])
                except Exception as e:
                    log(f"[{self.name}] ⚠️ 发货消息发送失败/未确认：{e}；不会标记已发货。")
                    return
                mark(self.name, key)
                log(f"[{self.name}] ✅ 自动发货成功")
                return

            # 没找到标准字段时，输出一次结构 + 可读字符串探针
            sig = (
                parsed["decode_mode"],
                compact_structure(decoded),
            )

            if sig != self._last_probe_signature:
                self._last_probe_signature = sig
                log(
                    f"[{self.name}] 🔎 新结构 "
                    f"(mode={parsed['decode_mode']}): {compact_structure(decoded)}"
                )
                # 专门补充 decoded["1"] 是 list 时的内部结构。
                one = decoded.get("1") if isinstance(decoded, dict) else None
                if isinstance(one, list):
                    previews = [compact_structure(x) for x in one[:5]]
                    log(f"[{self.name}]    decoded['1'] 前5项结构：{previews}")
                    if one and isinstance(one[0], dict):
                        stats = []
                        for k, v in one[0].items():
                            ln = len(v) if isinstance(v, (str, bytes, list, dict)) else "-"
                            stats.append(f"{k}:{type(v).__name__}(len={ln})")
                        log(f"[{self.name}]    list[0] 字段类型：{stats}")

        except Exception:
            log(f"[{self.name}] handler异常：\n{traceback.format_exc()}")


async def run(c):
    try:
        log(f"[{c.get('name')}] 正在连接闲鱼…")
        await Live(c).main()
    except Exception as e:
        log(f"[{c.get('name')}] 连接失败/断开：{e}")
        log("测试安全模式：不会自动重连。修好问题后请手动重新启动。")


async def main():
    init_db()
    cfg = load_config()
    acs = [a for a in cfg.get("accounts", []) if a.get("enabled", True)]
    miss = [a.get("name") for a in acs if not a.get("cookie")]
    if miss:
        print("以下账号还没有填写 Cookie 环境变量：", "、".join(miss))
        return

    print("闲鱼 Mini 自动发货器 v0.5.0-realtime")
    print(f"SSL CA: {_CA_FILE}")
    print(f"已加载 {len(acs)} 个账号")
    await asyncio.gather(*(run(a) for a in acs))


if __name__ == "__main__":
    asyncio.run(main())
