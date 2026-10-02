# mTLS 设备证书

网关在配置 `IOT_MQTT_TLS_CLIENT_CA` 后启用 `RequireAndVerifyClientCert`。
认证层还会检查客户端证书已经过 TLS 校验，并要求证书 `Subject.CommonName`
严格等于数据库中的 `t_device.device_key`；错误 CN、错误 CA、过期证书都会拒绝。

## 签发与轮换

CA 私钥只能留在受控主机，不得提交 Git、写入环境文件或出现在命令行历史中。
使用脚本签发或轮换设备证书：

```bash
deploy/certs/issue-device-cert.sh \
  --device-key IOT20-ACCEPT-001 \
  --ca-cert /home/xfusion/etc/certs/iot-gateway/ca.crt \
  --ca-key /home/xfusion/etc/certs/iot-gateway/ca.key \
  --out-dir /home/xfusion/etc/certs/iot-gateway/devices
```

脚本先在临时目录完成签发和校验，再安装到目标目录；私钥权限为 `0600`，证书有效期为 365 天。
轮换时直接对同一 `device-key` 再执行一次；旧证书在客户端替换前仍可使用。
吊销/禁用设备通过 `t_device.status` 或凭据目录撤销字段完成，不能只删除本地
证书文件后继续保留设备为 active。

## 启用方式

先为所有 mTLS 设备签发证书并把其认证模式设为 `mtls`，再在受控维护窗口设置：

```dotenv
IOT_MQTT_TLS_CERT=/home/xfusion/etc/certs/iot-gateway/server.crt
IOT_MQTT_TLS_KEY=/home/xfusion/etc/certs/iot-gateway/server.key
IOT_MQTT_TLS_CLIENT_CA=/home/xfusion/etc/certs/iot-gateway/ca.crt
```

不要在尚未为设备准备证书时启用该变量；启用后所有 MQTT 客户端都必须提交受信任
客户端证书。网关当前仍允许不设置 `IOT_MQTT_TLS_CLIENT_CA` 的单向 TLS 模式。
