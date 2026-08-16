# 闲鱼 Mini 自动发货 Render 部署

这是已跑通的轻量版闲鱼自动发货程序。当前版本保留现有 WebSocket、付款识别、`item_id` 精确匹配和 `sendByReceiverScope` 发送确认逻辑，没有重写协议。

## Render 设置

服务类型使用 Background Worker。

Build Command:

```bash
pip install --upgrade pip && pip install -r requirements.txt
```

Start Command:

```bash
python mini_delivery.py
```

环境变量：

```bash
XIANYU_COOKIE_SHOP_A=你的店铺A完整Cookie
PYTHONUNBUFFERED=1
```

## 安全默认值

仓库里的 `config.json` 默认保留 `dry_run=true`，防止云端首次启动就真实发送。确认 Render 日志里店铺A能完成 `/reg`、`ackDiff`、心跳和订单推送后，再把店铺A的 `dry_run` 改为 `false` 并提交部署。

`cookies/`、`.venv/`、`events.log`、`delivery.db` 和备份文件都已加入 `.gitignore`，不要把这些运行态或敏感文件提交到仓库。

## 说明

上游 `vendor/XianYuApis` 的解码链路依赖 PyExecJS。Render Linux 环境需要存在可用的 JavaScript runtime，通常是 Node.js；如果日志出现 ExecJS runtime 不可用，需要在 Render 环境中启用 Node.js runtime 支持后再启动。
