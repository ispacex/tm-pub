#!/usr/bin/env bash
# Put this tree and your tm.toml on the monitor host and (re)start the monitor container.
# Touches $TM_DIR and the tm-monitor container only.
#
#   TM_PUBLIC_HOST=tm.example.org deploy/deploy.sh root@monitor.example.org
#
#   TM_DEPLOY_HOST  ssh target, if not given as the first argument
#   TM_PUBLIC_HOST  the monitor's public host name (for the reverse proxy and its certificate)
#   TM_DIR          the monitor's directory on the host (default /srv/tm)
set -euo pipefail
HOST=${1:-${TM_DEPLOY_HOST:?usage: deploy/deploy.sh user@host (or set TM_DEPLOY_HOST)}}
: "${TM_PUBLIC_HOST:?set TM_PUBLIC_HOST to the monitor's public host name}"
DIR=${TM_DIR:-/srv/tm}
here=$(cd "$(dirname "$0")/.." && pwd)
test -f "$here/tm.toml" || { echo "no tm.toml: copy tm.example.toml to tm.toml and edit it"; exit 2; }
ssh -o BatchMode=yes "$HOST" "test -f '$DIR/tm.env' || { echo 'no $DIR/tm.env: make the secrets first (README, Deployment)'; exit 2; }
  mkdir -p '$DIR/app'"
rsync -a --delete --exclude .git --exclude .claude --exclude data --exclude '__pycache__' \
  --exclude tm.toml --exclude '*.env' "$here/" "$HOST:$DIR/app/"
rsync -a "$here/tm.toml" "$HOST:$DIR/tm.toml"
ssh -o BatchMode=yes "$HOST" "cd '$DIR/app' && TM_DIR='$DIR' TM_PUBLIC_HOST='$TM_PUBLIC_HOST' \
  docker compose -p tm -f deploy/docker-compose.yml up -d --build \
  && sleep 3 && docker ps --filter name=^tm-monitor\$ --format '{{.Names}} {{.Status}}'"
