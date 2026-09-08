#!/usr/bin/env bash
# Local integration smoke test for socks_trunk_kcp.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
WORK="$(mktemp -d)"
EMBED_CONF="$ROOT/filesystem/static/conf/default.yml"
TOKEN="kcp-test-token-$(date +%s)"
SERVER_ADDR="127.0.0.1:18086"
SOCKS_ADDR="127.0.0.1:11080"
ECHO_ADDR="127.0.0.1:28080"
METRICS_ADDR="127.0.0.1:16060"

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
trunk_kcp:
  conv: 123456789
  min_conns: 1
  max_conns: 3
  max_virtual_conn: 32
socks: "$SOCKS_ADDR"
http: ""
conn:
  addr: "$SERVER_ADDR"
  host: "speedtest.cn"
  enable-tls: true
  read-concurrency: 1
  write-concurrency: 1
  tls:
    ca-cert: "static/ca/root-cert.pem"
    server-cert: "static/ca/server-cert.pem"
    server-key: "static/ca/server-key.pem"
client-conn:
  addr: "$SERVER_ADDR"
  host: "speedtest.cn"
  enable-tls: true
  read-concurrency: 1
  write-concurrency: 1
  tls:
    ca-cert: "static/ca/root-cert.pem"
    client-cert: "static/ca/client-cert.pem"
    client-key: "static/ca/client-key.pem"
log:
  log-level: error
  to-console: true
EOF

(cd "$REPO_ROOT" && go build -o "$WORK/server" ./test/socks_trunk_kcp/cmd/socks-trunk-kcp-server)
(cd "$REPO_ROOT" && go build -o "$WORK/client" ./test/socks_trunk_kcp/cmd/socks-trunk-kcp-client)

python3 - "$ECHO_ADDR" <<'PY' &
import http.server, socketserver, sys
host, port = sys.argv[1].split(':')
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'integration-ok'
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

for _ in $(seq 1 80); do
  if curl -fsS "http://$METRICS_ADDR/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
curl -fsS "http://$METRICS_ADDR/healthz" >/dev/null

for _ in $(seq 1 80); do
  if curl -fsS -x "socks5h://$SOCKS_ADDR" "http://$ECHO_ADDR/ok" 2>/dev/null | grep -q integration-ok; then
    echo "integration test passed"
    exit 0
  fi
  sleep 0.2
done

echo "integration test failed" >&2
exit 1
