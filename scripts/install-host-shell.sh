#!/bin/bash
# Install host-based shell API as a systemd service.
# Run once: bash /root/streamdb/scripts/install-host-shell.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SHELL_SCRIPT="$SCRIPT_DIR/host-shell.py"
SHELL_PORT=7777

# 1. Copy shell script
chmod +x "$SHELL_SCRIPT"

# 2. Create systemd service
cat > /etc/systemd/system/host-shell.service << EOF
[Unit]
Description=Host Shell API
After=network.target docker.service

[Service]
Type=simple
ExecStart=/usr/bin/python3 $SHELL_SCRIPT
Environment=SHELL_PORT=$SHELL_PORT
Restart=always
RestartSec=2
OOMScoreAdjust=-900

[Install]
WantedBy=multi-user.target
EOF

# 3. Enable and start
systemctl daemon-reload
systemctl enable host-shell.service
systemctl restart host-shell.service

# 4. Add Traefik file provider config for routing sh.plaio.cc -> host:7777
TRAEFIK_DIR="/root/traefik"
if [ -d "$TRAEFIK_DIR" ]; then
    mkdir -p "$TRAEFIK_DIR/dynamic"

    cat > "$TRAEFIK_DIR/dynamic/host-shell.yml" << EOFYML
http:
  routers:
    host-shell:
      rule: "Host(\`sh.plaio.cc\`)"
      entryPoints:
        - websecure
      tls:
        certResolver: letsencrypt
      service: host-shell
  services:
    host-shell:
      loadBalancer:
        servers:
          - url: "http://172.17.0.1:${SHELL_PORT}"
EOFYML

    echo "Traefik dynamic config written to $TRAEFIK_DIR/dynamic/host-shell.yml"
fi

# 5. Stop old Docker-based shell if exists
for name in shell claude-shell shell-api web-shell; do
    if docker ps -a --format '{{.Names}}' | grep -q "^${name}$"; then
        docker update --restart=no "$name" 2>/dev/null || true
        docker stop "$name" 2>/dev/null || true
        echo "Stopped old container: $name"
    fi
done

# 6. Verify
sleep 1
if curl -s --max-time 5 "http://127.0.0.1:${SHELL_PORT}/" | grep -q "Shell API"; then
    echo "Host shell is running on port $SHELL_PORT"
else
    echo "WARNING: Shell may not have started. Check: systemctl status host-shell"
fi

echo "Done. Shell accessible at http://0.0.0.0:${SHELL_PORT}/run?cmd=whoami"
