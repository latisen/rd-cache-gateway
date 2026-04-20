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

- mount the TorBox WebDAV catalog on the host, outside the gateway pod
- expose it to containers through the shared media path at `/srv/media/data/downloads/torbox`
- let the gateway read it at `/data/downloads/torbox/__all__`
- let the gateway create symlinks under `/data/downloads/rd-cache-gateway/<category>/...`

This avoids a circular dependency where the gateway tries to mount and consume its own WebDAV inside the same pod.

If you need a helper for the host mount, use:

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
