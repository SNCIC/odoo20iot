# 服务编排模板

服务统一通过宿主机用户级 systemd 启动，进程实际运行在 `devbox` 容器内，避免
`svc-query` 访问容器内 PostgreSQL 时落到宿主机网络命名空间。

## 配置

创建 `/home/xfusion/etc/odoo20iot.env`，权限设为 `0600`，至少包含：

```dotenv
IOT_QUERY_DEV_TOKEN=replace-with-a-random-token
IOT_QUERY_AUTH_MODE=dev
IOT_QUERY_DEV_PROJECT_ID=1
IOT_QUERY_ALLOW_DEV_LAN=true
IOT_CONFIG_KEY=replace-with-the-existing-key
```

生产环境应切换 `IOT_QUERY_AUTH_MODE=jwt`，并配置 JWKS、issuer、audience 和 Redis 吊销表。

## 安装

```bash
mkdir -p /home/xfusion/etc
chmod 700 /home/xfusion/etc
chmod 600 /home/xfusion/etc/odoo20iot.env
systemctl --user daemon-reload
systemctl --user enable --now odoo20iot-svc-query.service
```

真实密钥禁止写入 unit、命令行、Git 或日志。
