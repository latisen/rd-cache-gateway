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
TARGET_DIR="$MOUNT_POINT/__all__"

has_real_fuse_mount() {
  local fstype=""
  if command -v findmnt >/dev/null 2>&1; then
    fstype="$(findmnt -T "$MOUNT_POINT" -n -o FSTYPE 2>/dev/null || true)"
  fi

  if printf '%s' "$fstype" | grep -Eq '^(fuse|fuse\..+|rclone)$'; then
    return 0
  fi

  grep -F " $MOUNT_POINT " /proc/mounts 2>/dev/null | grep -Eq 'fuse|rclone'
}

has_visible_entries() {
  [ -d "$TARGET_DIR" ] && find "$TARGET_DIR" -mindepth 1 -maxdepth 1 ! -name '.*' | grep -q .
}

if has_real_fuse_mount; then
  if has_visible_entries; then
    echo "TorBox WebDAV is already mounted and populated at $TARGET_DIR"
    exit 0
  fi
  echo "TorBox WebDAV FUSE mount is active but $TARGET_DIR is still empty; trying to remount"
  fusermount -uz "$MOUNT_POINT" >/dev/null 2>&1 || true
else
  echo "No active TorBox WebDAV FUSE mount detected at $MOUNT_POINT; mounting now"
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

  for attempt in $(seq 1 "$MOUNT_ATTEMPTS"); do
    if has_real_fuse_mount && has_visible_entries; then
      echo "Mounted TorBox WebDAV at $TARGET_DIR"
      exit 0
    fi
    echo "Waiting for usable WebDAV files at $TARGET_DIR (attempt ${attempt}/${MOUNT_ATTEMPTS})"
    sleep "$MOUNT_RETRY_DELAY"
  done

  echo "Mount command returned but no usable FUSE-backed files appeared at $TARGET_DIR" >&2
  exit 1
else
  exec rclone mount "$REMOTE" "$MOUNT_POINT" "${RCLONE_ARGS[@]}"
fi
