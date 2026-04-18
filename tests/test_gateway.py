import importlib
from pathlib import Path

from fastapi.testclient import TestClient

from app.staging import find_matching_media_file


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

    debug_status = client.get("/debug/status")
    assert debug_status.status_code == 200
    assert debug_status.json()["status"] == "ok"

    debug_live = client.get("/debug/live")
    assert debug_live.status_code == 200
    assert "rd-cache-gateway live log" in debug_live.text


def test_falls_back_when_data_dir_is_not_writable(tmp_path, monkeypatch):
    locked = tmp_path / "locked-data"
    locked.mkdir()
    locked.chmod(0o555)

    monkeypatch.setenv("DATA_DIR", str(locked))
    monkeypatch.setenv("STAGING_ROOT", str(tmp_path / "staging"))
    monkeypatch.setenv("SONARR_STAGING_ROOT", str(tmp_path / "sonarr"))
    monkeypatch.setenv("DEBRID_ALL_DIR", str(tmp_path / "debrid"))
    monkeypatch.setenv("QBIT_USERNAME", "admin")
    monkeypatch.setenv("QBIT_PASSWORD", "adminadmin")
    monkeypatch.setenv("ENABLE_POLLER", "0")

    import app.main as main

    main = importlib.reload(main)
    client = TestClient(main.app)

    health = client.get("/healthz")
    assert health.status_code == 200
    assert "rd-cache-gateway-data" in str(main.store.jobs_file)


def test_add_http_url_fetches_remote_torrent_for_sonarr(tmp_path, monkeypatch):
    main = load_main(tmp_path, monkeypatch)

    monkeypatch.setattr(main, "resolve_add_url", lambda url: {"kind": "torrent_file", "content": b"dummy", "filename": "remote.torrent"})
    monkeypatch.setattr(main, "rd_add_torrent_file_bytes", lambda content, filename=None: "rdurl")
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
        data={"urls": "https://indexer.example/file.torrent", "category": "sonarr"},
    )

    assert response.status_code == 200
    assert response.text == "Ok."

    info = client.get("/api/v2/torrents/info")
    payload = info.json()
    assert len(payload) == 1
    assert payload[0]["name"] == "Example.Release.S01E01.1080p.mkv"



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


def test_cached_download_add_does_not_run_global_poll_inline(tmp_path, monkeypatch):
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
    assert called["count"] == 0



def test_find_matching_media_file_does_not_pick_wrong_show(tmp_path):
    debrid_root = tmp_path / "debrid"
    debrid_root.mkdir(parents=True, exist_ok=True)

    wrong = debrid_root / "Blue.Bloods.S03E11.1080p.WEB-DL.mkv"
    wrong.write_bytes(b"x" * 100)
    correct = debrid_root / "Below.Deck.Down.Under.S03E11.The.Shots.You.Dont.Take.1080p.WEB-DL.mkv"
    correct.write_bytes(b"x" * 100)

    info = {
        "filename": "Below Deck Down Under S03E11 The Shots You Dont Take 1080p WEB-DL.mkv",
    }

    match = find_matching_media_file(info, debrid_root)
    assert match == correct



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


def test_visible_arr_symlink_is_created_for_sonarr_import(tmp_path, monkeypatch):
    main = load_main(tmp_path, monkeypatch)

    debrid_root = tmp_path / "debrid"
    debrid_root.mkdir(parents=True, exist_ok=True)
    media_file = debrid_root / "Below.Deck.Down.Under.S03E13.Lipstick.Service.1080p.mkv"
    media_file.write_bytes(b"x" * (2 * 1024 * 1024))

    main.store.replace_all(
        {
            "rdvisible": {
                "torrent_id": "rdvisible",
                "rd_torrent_id": "rdvisible",
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

    main.poller.poll_once()

    job = main.store.get("rdvisible")
    assert job is not None
    assert Path(job["staging_path"]).is_symlink()
    assert Path(job["arr_file_path"]).is_symlink()
    assert Path(job["arr_file_path"]).resolve() == media_file.resolve()



def test_poller_disables_deleted_remote_torrents(tmp_path, monkeypatch):
    main = load_main(tmp_path, monkeypatch)

    main.store.replace_all(
        {
            "stalejob": {
                "torrent_id": "stalejob",
                "rd_torrent_id": "missing-rd-id",
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
        lambda torrent_id: (_ for _ in ()).throw(RuntimeError("RD info failed for missing-rd-id: 404 {\"error\": \"unknown_ressource\"}")),
    )

    main.poller.poll_once()

    job = main.store.get("stalejob")
    assert job is not None
    assert job["polling_disabled"] is True



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
    assert captured["download_id"] == REAL_HASH.upper()
    assert captured["folder"].startswith(str(tmp_path / "sonarr"))
    assert "Show.Name.S01E01.1080p-" in captured["folder"]
