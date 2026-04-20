# rd-cache-gateway

A qBittorrent-compatible gateway for Sonarr/Radarr backed by TorBox or Real-Debrid cached torrent downloads.

## Debug dashboard

After deploy, a live status and log dashboard is available at:

- http://192.168.30.58:8888
- http://192.168.30.58:8000/debug/live
- http://192.168.30.58:8000/debug/logs.txt

The dashboard shows:

- active jobs
- status and RD status
- progress bar and percentage
- download speed
- seeds and peers
- live incoming/outgoing API activity

## Provider setup

For TorBox, set these environment variables or Kubernetes secrets:

- `DEBRID_PROVIDER=torbox`
- `TORBOX_API_KEY=<your TorBox API key>`
- `DEBRID_ALL_DIR=/mnt/torbox/webdav/__all__`

For Real-Debrid, keep using `RD_TOKEN` and the Real-Debrid path.

## TorBox WebDAV mount

Recommended stable setup:

- run a dedicated privileged WebDAV mounter alongside the gateway in Kubernetes
- let that mounter own the FUSE lifecycle for `/data/downloads/torbox`
- let the gateway only read `/data/downloads/torbox/__all__` and create symlinks under `/data/downloads/rd-cache-gateway/<category>/...`
- keep the shared media path backed by `/srv/media/data` so Sonarr and the gateway see the same files

This matches the Decypharr-style model better: the app does not try to repair or own the mount itself, and the mounter can be restarted independently if the FUSE session dies.

The provided deployment manifest now includes this dedicated mounter sidecar.

If you prefer a host-owned mount instead, use:

- [scripts/mount_torbox_webdav.sh](scripts/mount_torbox_webdav.sh)
- [scripts/torbox-webdav.service.example](scripts/torbox-webdav.service.example)

The gateway still exposes a debug WebDAV view at:

- `http://192.168.30.58:8000/dav`
- `http://rd-cache-gateway-internal.automation-system.svc.cluster.local:8000/dav`

## Sonarr setup

If Sonarr runs inside Kubernetes, prefer the internal service name:

- Host: rd-cache-gateway-internal.automation-system.svc.cluster.local
- Port: 8000

## Troubleshooting

Useful checks:

```bash
kubectl -n automation-system get pods -o wide
kubectl -n automation-system logs deploy/rd-cache-gateway --tail=300
kubectl -n automation-system logs deploy/rd-cache-gateway --previous --tail=300
kubectl -n automation-system describe pod -l app=rd-cache-gateway
```

If a job is stuck, open the debug dashboard and look for:

- `STAGE source not found`
- `ARR trigger scan`
- `IMPORT success`
