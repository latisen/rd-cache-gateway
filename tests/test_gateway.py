import importlib
from pathlib import Path

from fastapi.testclient import TestClient


MAGNET = "magnet:?xt=urn:btih:ABC123&dn=Example.Release.S01E01.1080p"


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
    assert payload[0]["state"] == "downloading"


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
