#!/usr/bin/env bash
# Run as root to bound host diagnostics; application logs rotate in Compose.
set -euo pipefail
if [[ ${EUID} -ne 0 ]]; then
  echo "Run with sudo: sudo bash scripts/configure_host_log_retention.sh" >&2
  exit 1
fi

install -d -m 0755 /etc/systemd/journald.conf.d
cat > /etc/systemd/journald.conf.d/60-ndp-retention.conf <<'EOF'
[Journal]
SystemMaxUse=100M
RuntimeMaxUse=50M
MaxRetentionSec=7day
MaxFileSec=1day
EOF

# Update the existing rsyslog rule so files are managed by exactly one rule.
python3 - <<'PY'
from pathlib import Path
p = Path('/etc/logrotate.d/rsyslog')
text = p.read_text()
backup = Path('/var/backups/ndp-log-retention/rsyslog')
backup.parent.mkdir(parents=True, exist_ok=True)
if not backup.exists():
    backup.write_text(text)
import re
text = re.sub(r'(?m)^\s*rotate\s+\d+\s*$', '\trotate 7', text)
text = re.sub(r'(?m)^\s*(weekly|monthly|daily)\s*$', '\tdaily', text)
if not re.search(r'(?m)^\s*su\s+', text):
    text = text.replace('{', '{\n\tsu root adm', 1)
if 'maxsize ' not in text:
    text = text.replace('{', '{\n\tmaxsize 10M', 1)
p.write_text(text)
PY
log_validation_output=$(mktemp)
trap 'rm -f "$log_validation_output"' EXIT
if ! logrotate --debug /etc/logrotate.conf >"$log_validation_output" 2>&1; then
  cat "$log_validation_output" >&2
  exit 1
fi
rm -f "$log_validation_output"
trap - EXIT
systemctl restart systemd-journald
journalctl --rotate
journalctl --vacuum-size=100M --vacuum-time=7d
logrotate --force /etc/logrotate.d/rsyslog
install -d -m 0755 /etc/systemd/system/logrotate.timer.d
cat > /etc/systemd/system/logrotate.timer.d/60-ndp-retention.conf <<'EOF'
[Timer]
OnCalendar=
OnCalendar=hourly
RandomizedDelaySec=5min
EOF
systemctl daemon-reload
systemctl enable logrotate.timer
systemctl restart logrotate.timer
journalctl --disk-usage
