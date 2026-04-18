import importlib
from pathlib import Path

from fastapi.testclient import TestClient


MAGNET = "magnet:?xt=urn:btih:ABC123&dn=Example.Release.S01E01.1080p"
REAL_HASH = "0123456789abcdef0123456789abcdef01234567"
REAL_MAGNET = f"magnet:?xt=urn:btih:{REAL_HASH.upper()}&dn=Example.Release.S01E01.1080p"


def load_main(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("STAGING_ROOT", str(tmp_path / "staging"))
    monkeypatch.setenv("SONARR_STAGING_ROOT", str(tmp_path / "sonarr"))
    monkeypatch.setenv("DEBRID_ALL_DIR", str(tmp_path / "debrid"))
    monkeypatch.setenv("QBIT_USERNAME", "admin")
    monkeypatch.setenv("QBIT_PASSWORD", "adminadmin")

    import app.main as main

    return importlib.reload(main)


def test_healthz_and_empty_qbit_list(tmp_path, monkeypatch):
    main = load_main(tmp_path, monkeypatch)
    client = TestClient(main.app)

    health = client.get("/healthz")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    info = client.get("/api/v2/torrents/info")
    assert info.status_code == 200
    assert info.json() == []


def test_add_magnet_persists_job_in_configured_data_dir(tmp_path, monkeypatch):
    main = load_main(tmp_path, monkeypatch)

    monkeypatch.setattr(main, "rd_add_magnet", lambda magnet_uri: "rd123")
    monkeypatch.setattr(main, "rd_select_all_files", lambda torrent_id: None)
    monkeypatch.setattr(
        main,
        "fetch_rd_info_raw",
        lambda torrent_id: {
            "id": torrent_id,
            "status": "queued",
            "filename": "Example.Release.S01E01.1080p.mkv",
            "bytes": 123456789,
            "files": [],
        },
    )

    client = TestClient(main.app)
    response = client.post(
        "/api/v2/torrents/add",
        data={"urls": MAGNET, "category": "sonarr"},
    )

    assert response.status_code == 200
    assert response.text == "Ok."

    jobs_file = Path(tmp_path / "data" / "jobs.json")
    assert jobs_file.exists()
    contents = jobs_file.read_text(encoding="utf-8")
    assert "rd123" in contents

    info = client.get("/api/v2/torrents/info")
    payload = info.json()
    assert len(payload) == 1
    assert payload[0]["category"] == "sonarr"
    assert payload[0]["state"] == "queuedDL"


def test_delete_hides_job_from_qbit_listing(tmp_path, monkeypatch):
    main = load_main(tmp_path, monkeypatch)

    monkeypatch.setattr(main, "rd_add_magnet", lambda magnet_uri: "rd999")
    monkeypatch.setattr(main, "rd_select_all_files", lambda torrent_id: None)
    monkeypatch.setattr(
        main,
        "fetch_rd_info_raw",
        lambda torrent_id: {
            "id": torrent_id,
            "status": "downloaded",
            "filename": "Movie.Title.2025.1080p.mkv",
            "bytes": 456,
            "files": [],
        },
    )

    client = TestClient(main.app)
    add = client.post("/api/v2/torrents/add", data={"urls": MAGNET, "category": "radarr"})
    assert add.status_code == 200

    delete = client.post("/api/v2/torrents/delete", data={"hashes": "rd999", "deleteFiles": "true"})
    assert delete.status_code == 200

    info = client.get("/api/v2/torrents/info")
    assert info.status_code == 200
    assert info.json() == []


def test_magnet_uses_stable_infohash_for_sonarr_tracking(tmp_path, monkeypatch):
    main = load_main(tmp_path, monkeypatch)

    monkeypatch.setattr(main, "rd_add_magnet", lambda magnet_uri: "rd123")
    monkeypatch.setattr(main, "rd_select_all_files", lambda torrent_id: None)
    monkeypatch.setattr(
        main,
        "fetch_rd_info_raw",
        lambda torrent_id: {
            "id": torrent_id,
            "status": "downloaded",
            "filename": "Example.Release.S01E01.1080p.mkv",
            "bytes": 123456789,
            "files": [],
        },
    )

    client = TestClient(main.app)
    response = client.post(
        "/api/v2/torrents/add",
        data={"urls": REAL_MAGNET, "category": "sonarr"},
    )

    assert response.status_code == 200

    info = client.get("/api/v2/torrents/info")
    assert info.status_code == 200
    payload = info.json()
    assert len(payload) == 1
    assert payload[0]["hash"] == REAL_HASH
    assert payload[0]["state"] == "downloading"
    assert payload[0]["progress"] == 0.99
    assert payload[0]["label"] == "sonarr"
    assert payload[0]["content_path"].endswith("Example.Release.S01E01.1080p.mkv")

    job = main.store.get(REAL_HASH)
    assert job is not None
    assert job["rd_torrent_id"] == "rd123"


def test_cached_download_triggers_immediate_stage_attempt(tmp_path, monkeypatch):
    main = load_main(tmp_path, monkeypatch)
    called = {"count": 0}

    monkeypatch.setattr(main, "rd_add_magnet", lambda magnet_uri: "rd123")
    monkeypatch.setattr(main, "rd_select_all_files", lambda torrent_id: None)
    monkeypatch.setattr(
        main,
        "fetch_rd_info_raw",
        lambda torrent_id: {
            "id": torrent_id,
            "status": "downloaded",
            "filename": "Example.Release.S01E01.1080p.mkv",
            "bytes": 123456789,
            "files": [],
        },
    )
    monkeypatch.setattr(main.poller, "poll_once", lambda: called.__setitem__("count", called["count"] + 1))

    client = TestClient(main.app)
    response = client.post(
        "/api/v2/torrents/add",
        data={"urls": REAL_MAGNET, "category": "sonarr"},
    )

    assert response.status_code == 200
    assert called["count"] == 1



def test_poller_marks_downloaded_job_ready_for_arr_using_media_file_size(tmp_path, monkeypatch):
    main = load_main(tmp_path, monkeypatch)

    debrid_root = tmp_path / "debrid"
    debrid_root.mkdir(parents=True, exist_ok=True)
    media_file = debrid_root / "Show.Name.S01E01.1080p.mkv"
    media_file.write_bytes(b"x" * (2 * 1024 * 1024))

    main.store.replace_all(
        {
            "rd123": {
                "torrent_id": "rd123",
                "rd_torrent_id": "rd123",
                "filename": media_file.name,
                "status": "downloading",
                "category": "sonarr",
                "raw": {},
            }
        }
    )

    monkeypatch.setattr(main.rd_client, "is_configured", lambda: True)
    monkeypatch.setattr(
        main.rd_client,
        "torrent_info",
        lambda torrent_id: {
            "id": torrent_id,
            "status": "downloaded",
            "filename": media_file.name,
            "bytes": 999999,
            "files": [
                {
                    "path": f"/{media_file.name}",
                    "bytes": 2 * 1024 * 1024,
                    "selected": 1,
                }
            ],
        },
    )

    main.poller.poll_once()

    job = main.store.get("rd123")
    assert job is not None
    assert job["status"] == "ready_for_arr"
    assert job["arr_ready_reason"] == "ready"
    assert str(tmp_path / "sonarr") in job["arr_path"]
    assert media_file.name in job["arr_file_path"]
    assert "Show.Name.S01E01.1080p-rd123" in job["arr_path"]

    info = TestClient(main.app).get("/api/v2/torrents/info")
    payload = info.json()
    assert payload[0]["content_path"].startswith(str(tmp_path / "sonarr"))
    assert payload[0]["content_path"].endswith(media_file.name)


def test_poller_uses_client_hash_as_download_client_id(tmp_path, monkeypatch):
    main = load_main(tmp_path, monkeypatch)

    debrid_root = tmp_path / "debrid"
    debrid_root.mkdir(parents=True, exist_ok=True)
    media_file = debrid_root / "Show.Name.S01E01.1080p.mkv"
    media_file.write_bytes(b"x" * (2 * 1024 * 1024))

    captured: dict[str, str] = {}

    class FakeArrClient:
        def is_configured(self):
            return True

        def refresh_monitored_downloads(self):
            return {"id": 1, "name": "RefreshMonitoredDownloads"}

        def trigger_scan(self, folder, download_id):
            captured["folder"] = str(folder)
            captured["download_id"] = str(download_id)
            return {"id": 2, "name": "DownloadedEpisodesScan"}

        def get_command(self, command_id):
            return {"id": command_id, "status": "queued"}

    main.store.replace_all(
        {
            REAL_HASH: {
                "torrent_id": REAL_HASH,
                "client_hash": REAL_HASH,
                "rd_torrent_id": "rd555",
                "filename": media_file.name,
                "status": "downloading",
                "category": "sonarr",
                "raw": {},
            }
        }
    )

    monkeypatch.setattr(main.rd_client, "is_configured", lambda: True)
    monkeypatch.setattr(
        main.rd_client,
        "torrent_info",
        lambda torrent_id: {
            "id": torrent_id,
            "status": "downloaded",
            "filename": media_file.name,
            "bytes": 2 * 1024 * 1024,
            "files": [
                {
                    "path": f"/{media_file.name}",
                    "bytes": 2 * 1024 * 1024,
                    "selected": 1,
                }
            ],
        },
    )
    monkeypatch.setattr("app.poller.get_arr_client", lambda category, settings: FakeArrClient())

    main.poller.poll_once()

    job = main.store.get(REAL_HASH)
    assert job is not None
    assert job["status"] == "scan_pending"
    assert captured["download_id"] == REAL_HASH
    assert captured["folder"].startswith(str(tmp_path / "sonarr"))
    assert "Show.Name.S01E01.1080p-" in captured["folder"]
