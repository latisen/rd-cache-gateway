from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    app_name: str
    app_version: str
    data_dir: Path
    debrid_all_dir: Path
    staging_root: Path
    visible_staging_root: Path
    poll_interval: int
    import_stability_min_bytes: int
    qbit_username: str
    qbit_password: str
    debrid_provider: str
    rd_token: str | None
    sonarr_url: str | None
    sonarr_api_key: str | None
    radarr_url: str | None
    radarr_api_key: str | None
    enable_poller: bool
    enable_debug_ui: bool
    debug_web_port: int
    webdav_url: str | None
    webdav_mount_check_enabled: bool
    webdav_mount_check_delay: int
    webdav_mount_check_timeout: int

    @property
    def jobs_file(self) -> Path:
        return self.data_dir / "jobs.json"

    @property
    def qbit_save_path(self) -> str:
        return str(self.visible_staging_root)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    # Temporary safety lock: always run this gateway against TorBox only,
    # even if legacy Real-Debrid environment variables are still present.
    torbox_token = os.getenv("TORBOX_API_KEY")
    debrid_provider = "torbox"
    debrid_token = torbox_token
    default_debrid_dir = "/mnt/torbox/webdav/__all__"

    return Settings(
        app_name="rd-cache-gateway",
        app_version=os.getenv("APP_VERSION", "0.9.0"),
        data_dir=Path(os.getenv("DATA_DIR", "./data")).expanduser(),
        debrid_all_dir=Path(os.getenv("DEBRID_ALL_DIR", default_debrid_dir)).expanduser(),
        staging_root=Path(
            os.getenv("STAGING_ROOT", "/srv/media/data/downloads/rd-cache-gateway")
        ).expanduser(),
        visible_staging_root=Path(
            os.getenv("SONARR_STAGING_ROOT", "/data/downloads/rd-cache-gateway")
        ).expanduser(),
        poll_interval=max(1, int(os.getenv("POLL_INTERVAL", "5"))),
        import_stability_min_bytes=max(
            1,
            int(os.getenv("IMPORT_STABILITY_MIN_BYTES", "1048576")),
        ),
        qbit_username=os.getenv("QBIT_USERNAME", "admin"),
        qbit_password=os.getenv("QBIT_PASSWORD", "adminadmin"),
        debrid_provider=debrid_provider,
        rd_token=debrid_token,
        sonarr_url=os.getenv("SONARR_URL"),
        sonarr_api_key=os.getenv("SONARR_API_KEY"),
        radarr_url=os.getenv("RADARR_URL"),
        radarr_api_key=os.getenv("RADARR_API_KEY"),
        enable_poller=_env_bool("ENABLE_POLLER", bool(debrid_token)),
        enable_debug_ui=_env_bool("ENABLE_DEBUG_UI", True),
        debug_web_port=max(1, int(os.getenv("DEBUG_WEB_PORT", "8888"))),
        webdav_url=os.getenv("WEBDAV_URL", "http://127.0.0.1:8000/dav"),
        webdav_mount_check_enabled=_env_bool("WEBDAV_MOUNT_CHECK_ENABLED", True),
        webdav_mount_check_delay=max(0, int(os.getenv("WEBDAV_MOUNT_CHECK_DELAY", "5"))),
        webdav_mount_check_timeout=max(0, int(os.getenv("WEBDAV_MOUNT_CHECK_TIMEOUT", "25"))),
    )
