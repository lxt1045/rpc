#!/usr/bin/env bash
# Local integration smoke test for socks_trunk:
# 1. Starts a local HTTP echo server
# 2. Starts the socks-trunk server and client
# 3. Sends a request through the client's SOCKS5 proxy to the echo server
#
# Requires: go, curl, python3
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
WORK="$(mktemp -d)"
TOKEN="integration-test-token-$(date +%s)"
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
  rm -rf "$WORK"
}
trap cleanup EXIT

# Build binaries.
(cd "$REPO_ROOT" && go build -o "$WORK/server" ./test/socks_trunk/cmd/socks-trunk-server)
(cd "$REPO_ROOT" && go build -o "$WORK/client" ./test/socks_trunk/cmd/socks-trunk-client)

cat > "$WORK/server.yaml" <<EOF
debug: true
token: "$TOKEN"
max-clients: 16
max-conns-per-client: 16
metrics-addr: "$METRICS_ADDR"
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
log:
  log-level: error
  to-console: true
EOF

cat > "$WORK/client.yaml" <<EOF
debug: true
token: "$TOKEN"
trunk:
  min-conns: 2
  max-conns: 4
  reconnect-sec: 2
  health-check-sec: 2
socks: "$SOCKS_ADDR"
http: ""
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

# HTTP echo server.
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

# Start server and client.
"$WORK/server" -config "$WORK/server.yaml" & SERVER_PID=$!
"$WORK/client" -config "$WORK/client.yaml" & CLIENT_PID=$!

# Wait for health endpoint.
for _ in $(seq 1 50); do
  if curl -fsS "http://$METRICS_ADDR/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
curl -fsS "http://$METRICS_ADDR/healthz" >/dev/null

# Wait for SOCKS proxy to accept.
for _ in $(seq 1 50); do
  if curl -fsS -x "socks5h://$SOCKS_ADDR" "http://$ECHO_ADDR/ok" 2>/dev/null | grep -q integration-ok; then
    echo "integration test passed"
    exit 0
  fi
  sleep 0.2
done

echo "integration test failed: proxy did not become ready" >&2
exit 1
