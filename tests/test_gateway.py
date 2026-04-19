import importlib
from pathlib import Path

from fastapi.testclient import TestClient

from app.live_log import get_log_view_html, set_jobs_provider
from app.rd_client import RealDebridClient
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

    direct_live = get_log_view_html()
    assert "connecting" not in direct_live.lower()
    assert "auto-refreshes every" in direct_live.lower()

    debug_text = client.get("/debug/logs.txt")
    assert debug_text.status_code == 200


def test_live_dashboard_renders_job_stats():
    set_jobs_provider(
        lambda: {
            "job1": {
                "filename": "Example.Release.S01E01.mkv",
                "status": "downloading",
                "rd_status": "downloading",
                "raw": {
                    "progress": 55,
                    "speed": 3145728,
                    "seeders": 12,
                    "peers": 34,
                },
            }
        }
    )

    html_view = get_log_view_html()
    assert "Active jobs" in html_view
    assert "Example.Release.S01E01.mkv" in html_view
    assert "Seeds: 12" in html_view
    assert "Peers: 34" in html_view
    assert "Progress: 55%" in html_view


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


def test_ready_job_is_reported_as_completed_to_sonarr(tmp_path, monkeypatch):
    main = load_main(tmp_path, monkeypatch)

    main.store.replace_all(
        {
            REAL_HASH: {
                "torrent_id": REAL_HASH,
                "client_hash": REAL_HASH.upper(),
                "rd_torrent_id": "tb123",
                "filename": "Example.Release.S01E01.1080p.mkv",
                "status": "ready",
                "category": "sonarr",
                "raw": {"bytes": 123456789, "progress": 100},
            }
        }
    )

    client = TestClient(main.app)
    info = client.get("/api/v2/torrents/info")
    assert info.status_code == 200
    payload = info.json()
    assert len(payload) == 1
    assert payload[0]["state"] == "pausedUP"
    assert payload[0]["progress"] == 1.0

    props = client.get(f"/api/v2/torrents/properties?hash={REAL_HASH}")
    assert props.status_code == 200
    assert props.json()["progress"] == 1.0



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
    assert payload[0]["state"] == "pausedUP"
    assert payload[0]["progress"] == 1.0
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



def test_readding_same_hash_clears_stale_poller_state(tmp_path, monkeypatch):
    main = load_main(tmp_path, monkeypatch)

    main.store.replace_all(
        {
            REAL_HASH: {
                "torrent_id": REAL_HASH,
                "client_hash": REAL_HASH.upper(),
                "rd_torrent_id": "oldrd",
                "filename": "Old.Release.S01E01.mkv",
                "status": "imported",
                "polling_disabled": True,
                "arr_scan_command": {"id": 99, "status": "completed"},
                "deleted_by_client": True,
                "imported_at": "2026-01-01T00:00:00Z",
            }
        }
    )

    monkeypatch.setattr(main, "rd_add_magnet", lambda magnet_uri: "newrd")
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
        data={"urls": REAL_MAGNET, "category": "sonarr"},
    )

    assert response.status_code == 200

    job = main.store.get(REAL_HASH)
    assert job is not None
    assert job["rd_torrent_id"] == "newrd"
    assert job["status"] == "queued"
    assert job.get("polling_disabled") in (None, False)
    assert job.get("deleted_by_client") in (None, False)
    assert job.get("arr_scan_command") is None
    assert job.get("imported_at") is None


def test_provider_is_hard_locked_to_torbox(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("STAGING_ROOT", str(tmp_path / "staging"))
    monkeypatch.setenv("SONARR_STAGING_ROOT", str(tmp_path / "sonarr"))
    monkeypatch.setenv("DEBRID_ALL_DIR", str(tmp_path / "debrid"))
    monkeypatch.setenv("QBIT_USERNAME", "admin")
    monkeypatch.setenv("QBIT_PASSWORD", "adminadmin")
    monkeypatch.setenv("DEBRID_PROVIDER", "realdebrid")
    monkeypatch.setenv("RD_TOKEN", "legacy-rd-token")
    monkeypatch.setenv("TORBOX_API_KEY", "torbox-token")
    monkeypatch.setenv("ENABLE_POLLER", "0")

    import app.main as main

    main = importlib.reload(main)
    client = TestClient(main.app)

    assert main.settings.debrid_provider == "torbox"
    assert main.settings.rd_token == "torbox-token"

    status = client.get("/debug/status")
    assert status.status_code == 200
    assert status.json()["debrid_provider"] == "torbox"



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



def test_find_matching_media_file_matches_same_episode_with_season_episode_words(tmp_path):
    debrid_root = tmp_path / "debrid"
    debrid_root.mkdir(parents=True, exist_ok=True)

    correct = debrid_root / "Below Deck Down Under - Season 3 Episode 9 - Foam Sick 1080p WEB-DL.mkv"
    correct.write_bytes(b"x" * 100)

    info = {
        "filename": "Below Deck Down Under S03E09 Foam Sick 1080p AMZN WEB-DL DDP2 0 H 264-NTb",
    }

    match = find_matching_media_file(info, debrid_root)
    assert match == correct



def test_find_matching_media_file_returns_none_for_wrong_show_only(tmp_path):
    debrid_root = tmp_path / "debrid"
    debrid_root.mkdir(parents=True, exist_ok=True)

    wrong = debrid_root / "Blue.Bloods.S03E12.1080p.WEB-DL.mkv"
    wrong.write_bytes(b"x" * 100)

    info = {
        "filename": "Below Deck Down Under S03E12 Across Frenemy Lines 1080p WEB-DL.mkv",
    }

    match = find_matching_media_file(info, debrid_root)
    assert match is None



def test_find_matching_media_file_returns_none_for_wrong_episode_same_show(tmp_path):
    debrid_root = tmp_path / "debrid"
    debrid_root.mkdir(parents=True, exist_ok=True)

    wrong = debrid_root / "Below.Deck.Down.Under.S04E03.The.Boil.Over.1080p.WEB-DL.mkv"
    wrong.write_bytes(b"x" * 100)

    info = {
        "filename": "Below Deck Down Under S03E10 A Greek Tragedy 1080p WEB-DL.mkv",
    }

    match = find_matching_media_file(info, debrid_root)
    assert match is None



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



def test_poller_records_rd_failure_reason_and_stops_repolling(tmp_path, monkeypatch):
    main = load_main(tmp_path, monkeypatch)

    main.store.replace_all(
        {
            "rdfail": {
                "torrent_id": "rdfail",
                "rd_torrent_id": "rdfail",
                "filename": "Broken.Release.S01E01.mkv",
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
            "status": "magnet_error",
            "error": "not_cached_or_dead",
            "progress": 0,
            "seeders": 0,
        },
    )

    main.poller.poll_once()

    job = main.store.get("rdfail")
    assert job is not None
    assert job["status"] == "failed"
    assert job["polling_disabled"] is True
    assert "magnet_error" in (job.get("last_error") or "")
    assert "not_cached_or_dead" in (job.get("last_error") or "")



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



def test_torbox_user_endpoint_works_with_bearer_token(monkeypatch):
    class FakeResponse:
        def __init__(self, status_code, payload):
            self.status_code = status_code
            self._payload = payload
            self.text = "ok"

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"http {self.status_code}")

    def fake_get(url, headers=None, timeout=None, **kwargs):
        assert url.endswith("/v1/api/user/me")
        assert headers is not None
        assert headers["Authorization"] == "Bearer token123"
        return FakeResponse(200, {"success": True, "data": {"email": "user@example.com"}})

    monkeypatch.setattr("app.rd_client.requests.get", fake_get)

    client = RealDebridClient("token123", provider="torbox")
    user = client.user()

    assert user["email"] == "user@example.com"



def test_torbox_torrent_info_is_normalized(monkeypatch):
    class FakeResponse:
        def __init__(self, status_code, payload):
            self.status_code = status_code
            self._payload = payload
            self.text = "ok"

        def json(self):
            return self._payload

    def fake_get(url, headers=None, timeout=None, params=None, **kwargs):
        assert url.endswith("/v1/api/torrents/mylist")
        return FakeResponse(
            200,
            {
                "success": True,
                "data": [
                    {
                        "id": 42,
                        "hash": "abcdef123456",
                        "name": "Example.Release.S01E01.1080p",
                        "download_state": "cached",
                        "progress": 100,
                        "download_speed": 0,
                        "seeds": 12,
                        "peers": 7,
                        "size": 123456,
                        "files": [
                            {
                                "id": 1,
                                "short_name": "Example.Release.S01E01.1080p.mkv",
                                "size": 123456,
                            }
                        ],
                    }
                ],
            },
        )

    monkeypatch.setattr("app.rd_client.requests.get", fake_get)

    client = RealDebridClient("token123", provider="torbox")
    info = client.torrent_info("42")

    assert info["id"] == "42"
    assert info["status"] == "cached"
    assert info["filename"] == "Example.Release.S01E01.1080p"
    assert info["bytes"] == 123456
    assert info["seeders"] == 12
    assert info["peers"] == 7
    assert info["files"][0]["path"] == "Example.Release.S01E01.1080p.mkv"



def test_webdav_root_and_all_listing_for_torbox(tmp_path, monkeypatch):
    monkeypatch.setenv("DEBRID_PROVIDER", "torbox")
    main = load_main(tmp_path, monkeypatch)

    monkeypatch.setattr(
        main.rd_client,
        "list_webdav_entries",
        lambda: [
            {
                "href": "/dav/__all__/Example.Release.S01E01.1080p.mkv",
                "name": "Example.Release.S01E01.1080p.mkv",
                "is_dir": False,
                "size": 123456,
                "torrent_id": "42",
                "file_id": "1",
            }
        ],
    )

    client = TestClient(main.app)

    root = client.request("PROPFIND", "/dav", headers={"Depth": "1"})
    assert root.status_code == 207
    assert "__all__" in root.text

    listing = client.request("PROPFIND", "/dav/__all__", headers={"Depth": "1"})
    assert listing.status_code == 207
    assert "Example.Release.S01E01.1080p.mkv" in listing.text



def test_webdav_file_get_redirects_to_torbox_download(tmp_path, monkeypatch):
    monkeypatch.setenv("DEBRID_PROVIDER", "torbox")
    main = load_main(tmp_path, monkeypatch)

    monkeypatch.setattr(
        main.rd_client,
        "list_webdav_entries",
        lambda: [
            {
                "href": "/dav/__all__/Example.Release.S01E01.1080p.mkv",
                "name": "Example.Release.S01E01.1080p.mkv",
                "is_dir": False,
                "size": 123456,
                "torrent_id": "42",
                "file_id": "1",
            }
        ],
    )
    monkeypatch.setattr(main.rd_client, "get_download_url", lambda torrent_id, file_id: "https://cdn.example/file.mkv")

    client = TestClient(main.app)
    response = client.get("/dav/__all__/Example.Release.S01E01.1080p.mkv", follow_redirects=False)

    assert response.status_code in {302, 307}
    assert response.headers["location"] == "https://cdn.example/file.mkv"
