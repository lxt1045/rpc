#!/usr/bin/env bash
# Local integration smoke test for socks_faux_trunk_kcp.
#
# 需要 Linux + root/CAP_NET_RAW（faux_tcp 使用 AF_PACKET + raw IP socket），
# 且需要 iptables 或 nft（faux_tcp 会自动装/卸 RST 抑制规则）。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
WORK="$(mktemp -d)"
EMBED_CONF="$ROOT/filesystem/static/conf/default.yml"
TOKEN="faux-test-token-$(date +%s)"
SERVER_ADDR="127.0.0.1:18097"
SOCKS_ADDR="127.0.0.1:11090"
ECHO_ADDR="127.0.0.1:28091"
METRICS_ADDR="127.0.0.1:16061"

if [[ "$(id -u)" != "0" ]]; then
  echo "SKIP: 需要 root/CAP_NET_RAW（faux_tcp 走 raw socket）" >&2
  exit 0
fi
if ! command -v iptables >/dev/null 2>&1 && ! command -v nft >/dev/null 2>&1; then
  echo "SKIP: 需要 iptables 或 nft 来抑制内核 RST" >&2
  exit 0
fi

SERVER_PID=""
CLIENT_PID=""
ECHO_PID=""
cleanup() {
  set +e
  [[ -n "$CLIENT_PID" ]] && kill "$CLIENT_PID" 2>/dev/null
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null
  [[ -n "$ECHO_PID" ]] && kill "$ECHO_PID" 2>/dev/null
  if [[ -n "${EMBED_CONF_SAVED:-}" && -f "$WORK/default.yml.bak" ]]; then
    cp "$WORK/default.yml.bak" "$EMBED_CONF"
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

if [[ ! -f "$EMBED_CONF" ]]; then
  echo "embedded config not found: $EMBED_CONF" >&2
  exit 1
fi
cp "$EMBED_CONF" "$WORK/default.yml.bak"
EMBED_CONF_SAVED=1

cat > "$EMBED_CONF" <<EOF
debug: true
token: "$TOKEN"
max-clients: 16
metrics-addr: "$METRICS_ADDR"
conn:
  addr: "$SERVER_ADDR"
client-conn:
  addr: "$SERVER_ADDR"
faux_tcp:
  mss: 1448
  keepalive_seconds: 25
  handshake_timeout_ms: 1000
  handshake_retries: 3
  heal_delay_ms: 200
  manual_firewall: false
trunk_kcp:
  conv: 123456789
  min_conns: 1
  max_conns: 3
  max_virtual_conn: 32
tls:
  enabled: true
  host: "speedtest.cn"
  ca-cert: "static/ca/root-cert.pem"
  server-cert: "static/ca/server-cert.pem"
  server-key: "static/ca/server-key.pem"
  client-cert: "static/ca/client-cert.pem"
  client-key: "static/ca/client-key.pem"
socks: "$SOCKS_ADDR"
http: ""
log:
  log-level: error
  to-console: true
EOF

# 证书：filesystem/static/ca/*.pem 不入 git，缺失时用 test/cert 现场生成
CA_DIR="$ROOT/filesystem/static/ca"
if [[ ! -f "$CA_DIR/server-cert.pem" ]]; then
  echo "generating certs via test/cert ..."
  mkdir -p "$CA_DIR"
  (cd "$REPO_ROOT" && go test -run TestMake -count=1 ./test/cert >/dev/null)
  for f in root-cert.pem server-cert.pem server-key.pem client-cert.pem client-key.pem; do
    cp "$REPO_ROOT/test/cert/ca/$f" "$CA_DIR/$f"
  done
fi

(cd "$REPO_ROOT" && go build -o "$WORK/server" ./test/socks_faux_trunk_kcp/cmd/socks-faux-trunk-kcp-server)
(cd "$REPO_ROOT" && go build -o "$WORK/client" ./test/socks_faux_trunk_kcp/cmd/socks-faux-trunk-kcp-client)

python3 - "$ECHO_ADDR" <<'PY' &
import http.server, socketserver, sys
host, port = sys.argv[1].split(':')
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'faux-integration-ok'
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass
socketserver.TCPServer.allow_reuse_address = True
with socketserver.TCPServer((host, int(port)), H) as httpd:
    httpd.serve_forever()
PY
ECHO_PID=$!

"$WORK/server" & SERVER_PID=$!
"$WORK/client" & CLIENT_PID=$!

for _ in $(seq 1 100); do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "server exited early (check root/CAP_NET_RAW 与 RST 抑制规则)" >&2
    exit 1
  fi
  if curl -fsS -x "socks5h://$SOCKS_ADDR" "http://$ECHO_ADDR/ok" 2>/dev/null | grep -q faux-integration-ok; then
    echo "integration test passed"
    exit 0
  fi
  sleep 0.2
done

echo "integration test failed" >&2
exit 1
