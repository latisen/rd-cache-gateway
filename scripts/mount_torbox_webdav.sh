#!/usr/bin/env bash
set -euo pipefail

WEBDAV_URL="${WEBDAV_URL:-http://192.168.30.58:8000/dav}"
MOUNT_POINT="${MOUNT_POINT:-/srv/media/mnt/torbox/webdav}"
UID_VALUE="${UID_VALUE:-1000}"
GID_VALUE="${GID_VALUE:-1000}"

if ! command -v rclone >/dev/null 2>&1; then
  echo "rclone is required. Install it first, then run this script again." >&2
  exit 1
fi

mkdir -p "$MOUNT_POINT"

if mountpoint -q "$MOUNT_POINT"; then
  echo "TorBox WebDAV is already mounted at $MOUNT_POINT"
  exit 0
fi

REMOTE=":webdav,url='${WEBDAV_URL}',vendor=other:"
DAEMON_MODE="${DAEMON_MODE:-1}"

echo "Mounting $WEBDAV_URL at $MOUNT_POINT"
RCLONE_ARGS=(
  --read-only
  --allow-other
  --dir-cache-time 10s
  --poll-interval 15s
  --vfs-cache-mode full
  --uid "$UID_VALUE"
  --gid "$GID_VALUE"
  --umask 002
)

if [ "$DAEMON_MODE" = "1" ]; then
  rclone mount "$REMOTE" "$MOUNT_POINT" --daemon "${RCLONE_ARGS[@]}"
  echo "Mounted TorBox WebDAV at $MOUNT_POINT"
else
  exec rclone mount "$REMOTE" "$MOUNT_POINT" "${RCLONE_ARGS[@]}"
fi
