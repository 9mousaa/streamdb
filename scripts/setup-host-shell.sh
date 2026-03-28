#!/bin/bash
# Run this ON THE HOST (via nsenter) to install the host shell
set -e
cp /root/streamdb/scripts/host-shell.py /opt/host-shell.py
chmod +x /opt/host-shell.py
cp /root/streamdb/scripts/host-shell.service /etc/systemd/system/host-shell.service
systemctl daemon-reload
systemctl enable host-shell
systemctl restart host-shell
sleep 1
curl -s http://127.0.0.1:7777/ || echo "WARN: shell not responding yet"
echo "DONE"
