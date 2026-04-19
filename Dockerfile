FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_HOME=/app \
    DATA_DIR=/config \
    STAGING_ROOT=/srv/media/data/downloads/rd-cache-gateway \
    SONARR_STAGING_ROOT=/data/downloads/rd-cache-gateway \
    POLL_INTERVAL=5 \
    UVICORN_HOST=0.0.0.0 \
    UVICORN_PORT=8000 \
    UVICORN_WORKERS=1 \
    ENABLE_DEBUG_UI=1 \
    DEBUG_WEB_PORT=8888 \
    APP_UID=1000 \
    APP_GID=1000

WORKDIR ${APP_HOME}

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates fuse3 rclone \
    && sed -i 's/^#user_allow_other$/user_allow_other/' /etc/fuse.conf \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN python -m pip install --upgrade pip \
    && python -m pip install -r requirements.txt

COPY app ./app
COPY scripts ./scripts

RUN groupadd --gid 1000 appuser \
    && useradd --create-home --uid 1000 --gid 1000 appuser \
    && mkdir -p /config /data/downloads/rd-cache-gateway /srv/media/data/downloads/rd-cache-gateway /mnt/torbox/webdav \
    && chown -R appuser:appuser /app /config /data /srv/media/data /mnt/torbox

USER appuser

EXPOSE 8000 8888

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:${UVICORN_PORT}/healthz || exit 1

CMD ["sh", "-c", "uvicorn app.main:app --host ${UVICORN_HOST} --port ${UVICORN_PORT} --workers ${UVICORN_WORKERS}"]
