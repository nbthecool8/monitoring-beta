#!/usr/bin/env bash
set -Eeuo pipefail

# --- pick docker compose binary ---
if docker compose version >/dev/null 2>&1; then
  DC="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  DC="docker-compose"
else
  echo "❌ Docker Compose CLI not found." >&2
  exit 1
fi

# --- pick compose file (supports your dockercompose.yml name) ---
FILE=""
for f in dockercompose.yml docker-compose.yml compose.yaml compose.yml; do
  [[ -f "$f" ]] && FILE="$f" && break
done
[[ -n "$FILE" ]] || { echo "❌ No compose file found."; exit 1; }

echo "▶ Using compose file: $FILE"
$DC -f "$FILE" up -d --build
$DC -f "$FILE" ps

# --- tiny waiters so UIs are ready for screenshots ---
wait_http() {
  local url="$1"; local name="$2"; local timeout="${3:-120}"
  echo -n "⏳ Waiting for $name"
  for ((i=0;i<timeout;i++)); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "  → READY"
      return 0
    fi
    echo -n "."
    sleep 1
  done
  echo "  → (timeout, may still be starting)"
  return 1
}

HOST="${HOST_ADDR:-localhost}"
wait_http "http://${HOST}:8080/healthz" "demo-app"
wait_http "http://${HOST}:9090/-/ready" "Prometheus"
wait_http "http://${HOST}:3000/login"  "Grafana" 60
wait_http "http://${HOST}:9093"        "Alertmanager" 30
wait_http "http://${HOST}:18080/containers/" "cAdvisor" 30 || true

cat <<EOF

✅ Services started. Open these:
- App:        http://${HOST}:8080/   (health: /healthz)
- Prometheus: http://${HOST}:9090/
- Grafana:    http://${HOST}:3000/   (admin / admin)
- Alertmanager: http://${HOST}:9093/
- cAdvisor:   http://${HOST}:18080/

Tip: to watch logs →  $DC -f "$FILE" logs -f --tail=200
EOF
