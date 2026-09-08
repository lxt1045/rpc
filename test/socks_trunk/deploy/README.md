# deploy

本目录为 systemd 部署示例。二进制路径和配置路径请按实际环境替换。

## 服务端

```bash
sudo cp systemd/socks-trunk-server.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now socks-trunk-server
```

`/etc/socks-trunk/server.env` 示例：

```env
SOCKS_TRUNK_TOKEN=replace-with-a-strong-token
```

## 客户端

```bash
sudo cp systemd/socks-trunk-client.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now socks-trunk-client
```

`/etc/socks-trunk/client.env` 示例：

```env
SOCKS_TRUNK_TOKEN=replace-with-the-same-token
```
