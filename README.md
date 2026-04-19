# rd-cache-gateway

A qBittorrent-compatible gateway for Sonarr/Radarr backed by Real-Debrid cached torrent downloads.

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
