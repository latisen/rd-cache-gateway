#!/usr/bin/env bash
set -euo pipefail

WEBDAV_URL="${WEBDAV_URL:-http://192.168.30.58:8000/dav}"
MOUNT_POINT="${MOUNT_POINT:-/srv/media/mnt/torbox/webdav}"
UID_VALUE="${UID_VALUE:-1000}"
GID_VALUE="${GID_VALUE:-1000}"
MOUNT_ATTEMPTS="${MOUNT_ATTEMPTS:-15}"
MOUNT_RETRY_DELAY="${MOUNT_RETRY_DELAY:-2}"

if ! command -v rclone >/dev/null 2>&1; then
  echo "rclone is required. Install it first, then run this script again." >&2
  exit 1
fi

mkdir -p "$MOUNT_POINT"

if mountpoint -q "$MOUNT_POINT"; then
  if find "$MOUNT_POINT" -mindepth 1 -maxdepth 1 | grep -q .; then
    echo "TorBox WebDAV is already mounted and populated at $MOUNT_POINT"
    exit 0
  fi
  echo "TorBox WebDAV mount exists but is empty at $MOUNT_POINT; trying to remount"
  fusermount -u "$MOUNT_POINT" >/dev/null 2>&1 || true
fi

REMOTE=":webdav,url='${WEBDAV_URL}',vendor=other:"
DAEMON_MODE="${DAEMON_MODE:-1}"

echo "Waiting for WebDAV endpoint $WEBDAV_URL"
for attempt in $(seq 1 "$MOUNT_ATTEMPTS"); do
  if rclone lsf "$REMOTE" --max-depth 1 >/dev/null 2>&1; then
    break
  fi
  echo "WebDAV endpoint not ready yet (attempt ${attempt}/${MOUNT_ATTEMPTS})"
  sleep "$MOUNT_RETRY_DELAY"
done

echo "Mounting $WEBDAV_URL at $MOUNT_POINT"
RCLONE_ARGS=(
  --read-only
  --allow-other
  --dir-cache-time 10s
  --poll-interval 15s
  --vfs-cache-mode off
  --uid "$UID_VALUE"
  --gid "$GID_VALUE"
  --umask 002
)

if [ "$DAEMON_MODE" = "1" ]; then
  rclone mount "$REMOTE" "$MOUNT_POINT" --daemon "${RCLONE_ARGS[@]}"
else
  exec rclone mount "$REMOTE" "$MOUNT_POINT" "${RCLONE_ARGS[@]}"
fi

echo "Mounted TorBox WebDAV at $MOUNT_POINT"
