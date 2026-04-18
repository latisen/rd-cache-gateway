from __future__ import annotations

import logging
import os
import re
import time
from contextlib import asynccontextmanager
from urllib.parse import unquote, urljoin, urlparse

import requests
from fastapi import FastAPI, File, Form, HTTPException, Request, Response, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, PlainTextResponse

from app.api_qbit import (
    build_categories,
    build_preferences,
    build_qbit_torrent_list,
    extract_urls_from_add_request,
    is_magnet_link,
    magnet_display_name,
    magnet_info_hash,
    temporary_job_id_from_text,
    torrent_file_info_hash,
)
from app.config import get_settings
from app.jobs_store import JobStore
from app.live_log import LiveLogServer, get_log_view_html, get_recent_logs, install_live_log_handler
from app.models import (
    CreateJobRequest,
    CreateJobResponse,
    HealthResponse,
    JobStatusResponse,
    RDUserResponse,
    map_rd_status,
    now_utc_iso,
    safe_int,
)
from app.poller import JobPoller
from app.rd_client import RealDebridClient
from app.staging import cleanup_staging_for_job

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
install_live_log_handler()
logger = logging.getLogger(__name__)

get_settings.cache_clear()
settings = get_settings()
store = JobStore(settings.jobs_file)
rd_client = RealDebridClient(settings.rd_token)
poller = JobPoller(store=store, rd_client=rd_client, settings=settings)
live_log_server = LiveLogServer(port=settings.debug_web_port)


def qbit_ok_plain(text: str = "Ok.") -> Response:
    return Response(content=text, media_type="text/plain")


def rd_add_magnet(magnet_uri: str) -> str:
    return rd_client.add_magnet(magnet_uri)


def rd_add_torrent_file_bytes(content: bytes, filename: str | None = None) -> str:
    return rd_client.add_torrent_file(content, filename)


def rd_select_all_files(torrent_id: str) -> None:
    rd_client.select_all_files(torrent_id)


def fetch_rd_info_raw(torrent_id: str) -> dict:
    return rd_client.torrent_info(torrent_id)


def rd_delete_torrent(torrent_id: str) -> None:
    rd_client.delete_torrent(torrent_id)


def _extract_remote_filename(source_url: str, response: requests.Response) -> str:
    content_disposition = response.headers.get("Content-Disposition", "")
    match = re.search(r"filename\*=UTF-8''([^;]+)|filename=\"?([^\";]+)\"?", content_disposition, re.IGNORECASE)
    if match:
        return unquote(match.group(1) or match.group(2) or "download.torrent")
    path = urlparse(response.url or source_url).path
    name = os.path.basename(path)
    return name or "download.torrent"


def resolve_add_url(url: str) -> dict:
    if is_magnet_link(url):
        return {"kind": "magnet", "value": url}

    logger.info("ADD resolving remote url=%s", url)
    headers = {"User-Agent": settings.app_name}
    probe = requests.get(url, headers=headers, timeout=60, allow_redirects=False)
    if probe.is_redirect or probe.is_permanent_redirect:
        location = probe.headers.get("Location")
        if location:
            redirected = urljoin(url, location)
            if is_magnet_link(redirected):
                logger.info("ADD remote url redirected to magnet")
                return {"kind": "magnet", "value": redirected}
            url = redirected

    response = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
    response.raise_for_status()
    text_body = response.text.strip() if "text" in response.headers.get("Content-Type", "").lower() else ""
    if is_magnet_link(response.url):
        logger.info("ADD final remote url became magnet")
        return {"kind": "magnet", "value": response.url}
    if text_body.startswith("magnet:?"):
        logger.info("ADD remote body contained magnet")
        return {"kind": "magnet", "value": text_body}

    filename = _extract_remote_filename(url, response)
    logger.info("ADD remote url downloaded torrent filename=%s bytes=%s", filename, len(response.content))
    return {"kind": "torrent_file", "content": response.content, "filename": filename}


def _boolish(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _create_placeholder_job(name: str, category: str, source: str, seed: str, preferred_id: str | None = None) -> str:
    temp_id = (preferred_id or temporary_job_id_from_text(f"{seed}|{now_utc_iso()}"))
    client_hash = str(preferred_id or temp_id).upper()
    store.merge(
        temp_id,
        {
            "torrent_id": temp_id,
            "client_hash": client_hash,
            "filename": name,
            "saved_at": now_utc_iso(),
            "last_checked_at": now_utc_iso(),
            "rd_status": "queued",
            "raw": {"filename": name, "status": "queued", "progress": 0},
            "status": "queued",
            "category": category,
            "last_error": None,
            "source": source,
        },
    )
    return temp_id


def _record_failure(job_id: str, exc: Exception) -> None:
    store.merge(
        job_id,
        {
            "status": "failed",
            "last_checked_at": now_utc_iso(),
            "last_error": str(exc),
        },
    )


def _finalize_job(
    temp_id: str,
    rd_id: str,
    info: dict,
    category: str,
    source: str,
    *,
    client_hash: str | None = None,
) -> tuple[str, dict]:
    normalized_client_hash = str(client_hash or temp_id or rd_id).upper()
    job_id = normalized_client_hash.lower()
    if temp_id != job_id:
        store.replace_key(temp_id, job_id)
    job = store.merge(
        job_id,
        {
            "torrent_id": job_id,
            "client_hash": normalized_client_hash,
            "rd_torrent_id": rd_id,
            "filename": info.get("filename") or job_id,
            "saved_at": now_utc_iso(),
            "last_checked_at": now_utc_iso(),
            "rd_status": info.get("status"),
            "raw": info,
            "status": map_rd_status(info.get("status")),
            "category": category,
            "last_error": None,
            "source": source,
        },
    )
    return job_id, job


def _add_magnet_job(magnet_uri: str, category: str, *, raise_on_error: bool = False) -> tuple[str, dict]:
    display_name = magnet_display_name(magnet_uri)
    client_hash = magnet_info_hash(magnet_uri)
    temp_id = _create_placeholder_job(display_name, category, "magnet", magnet_uri, preferred_id=client_hash)
    logger.info("ADD start type=magnet temp_id=%s name=%s category=%s", temp_id, display_name, category)

    try:
        rd_id = rd_add_magnet(magnet_uri)
        rd_select_all_files(rd_id)
        info = fetch_rd_info_raw(rd_id)
        logger.info("ADD linked temp_id=%s rd_id=%s", temp_id, rd_id)
        job_id, job = _finalize_job(temp_id, rd_id, info, category, "magnet", client_hash=client_hash)
        return job_id, job
    except Exception as exc:
        logger.exception("ADD failed temp_id=%s", temp_id)
        _record_failure(temp_id, exc)
        if raise_on_error:
            raise
        return temp_id, store.get(temp_id) or {}


def _add_torrent_file_job(
    content: bytes,
    filename: str | None,
    category: str,
    *,
    raise_on_error: bool = False,
) -> tuple[str, dict]:
    display_name = filename or "upload.torrent"
    client_hash = torrent_file_info_hash(content)
    temp_id = _create_placeholder_job(
        display_name,
        category,
        "torrent_file",
        f"{display_name}:{len(content)}",
        preferred_id=client_hash,
    )
    logger.info("ADD start type=torrent_file temp_id=%s name=%s category=%s", temp_id, display_name, category)

    try:
        rd_id = rd_add_torrent_file_bytes(content, filename)
        rd_select_all_files(rd_id)
        info = fetch_rd_info_raw(rd_id)
        logger.info("ADD linked temp_id=%s rd_id=%s", temp_id, rd_id)
        job_id, job = _finalize_job(temp_id, rd_id, info, category, "torrent_file", client_hash=client_hash)
        return job_id, job
    except Exception as exc:
        logger.exception("ADD failed temp_id=%s", temp_id)
        _record_failure(temp_id, exc)
        if raise_on_error:
            raise
        return temp_id, store.get(temp_id) or {}


def _resolve_job(torrent_id: str) -> tuple[str, dict]:
    jobs = store.all()
    if torrent_id in jobs:
        return torrent_id, jobs[torrent_id]
    wanted = torrent_id.lower()
    for key, value in jobs.items():
        if str(key).lower() == wanted:
            return key, value
        if str(value.get("client_hash") or "").lower() == wanted:
            return key, value
        if str(value.get("rd_torrent_id") or "").lower() == wanted:
            return key, value
    raise HTTPException(status_code=404, detail="Job not found")


@asynccontextmanager
async def lifespan(_: FastAPI):
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.staging_root.mkdir(parents=True, exist_ok=True)
    if settings.enable_debug_ui:
        live_log_server.start()
    poller.start()
    yield
    poller.stop()
    if settings.enable_debug_ui:
        live_log_server.stop()


app = FastAPI(title=settings.app_name, version=settings.app_version, lifespan=lifespan)


@app.middleware("http")
async def log_http_requests(request: Request, call_next):
    path = request.url.path
    if path in {"/healthz", "/debug/logs"}:
        return await call_next(request)

    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        logger.exception("HTTP %s %s failed", request.method, path)
        raise

    elapsed_ms = (time.perf_counter() - started) * 1000
    logger.info("HTTP %s %s -> %s %.1fms", request.method, path, response.status_code, elapsed_ms)
    return response


@app.get("/debug/logs")
def debug_logs(limit: int = 300) -> dict:
    return {"entries": get_recent_logs(limit)}


@app.get("/debug/live", response_class=HTMLResponse)
def debug_live() -> str:
    return get_log_view_html("/debug/logs")


@app.get("/debug/status")
def debug_status() -> dict:
    return {
        "status": "ok",
        "jobs_file": str(store.jobs_file),
        "staging_root": str(settings.staging_root),
        "visible_staging_root": str(settings.visible_staging_root),
        "debrid_all_dir": str(settings.debrid_all_dir),
        "poller_enabled": settings.enable_poller,
        "debug_ui_enabled": settings.enable_debug_ui,
        "debug_web_port": settings.debug_web_port,
    }


@app.get("/healthz", response_model=HealthResponse)
def healthz() -> HealthResponse:
    return HealthResponse(
        status="ok",
        hostname=os.uname().nodename,
        version=settings.app_version,
    )


@app.get("/rd/test", response_model=RDUserResponse)
def rd_test() -> RDUserResponse:
    if not rd_client.is_configured():
        raise HTTPException(status_code=503, detail="RD_TOKEN is not set")
    data = rd_client.user()
    return RDUserResponse(
        status="ok",
        username=data.get("username"),
        email=data.get("email"),
        points=data.get("points"),
    )


@app.post("/jobs", response_model=CreateJobResponse)
def create_job(req: CreateJobRequest) -> CreateJobResponse:
    try:
        torrent_id, job = _add_magnet_job(req.magnet, (req.category or "sonarr").strip().lower(), raise_on_error=True)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return CreateJobResponse(
        status="accepted",
        torrent_id=torrent_id,
        rd_status=job.get("rd_status"),
        filename=job.get("filename"),
    )


@app.get("/jobs")
def list_jobs() -> dict[str, dict]:
    return store.all()


@app.get("/jobs/{torrent_id}", response_model=JobStatusResponse)
def get_job(torrent_id: str) -> JobStatusResponse:
    _, job = _resolve_job(torrent_id)
    return JobStatusResponse(
        torrent_id=job["torrent_id"],
        status=job.get("status", "queued"),
        rd_status=job.get("rd_status"),
        filename=job.get("filename"),
        saved_at=job.get("saved_at"),
        last_checked_at=job.get("last_checked_at"),
        source_path=job.get("source_path"),
        staging_path=job.get("staging_path"),
        arr_path=job.get("arr_path"),
        arr_file_path=job.get("arr_file_path"),
        raw=job.get("raw"),
        last_error=job.get("last_error"),
        imported_at=job.get("imported_at"),
    )


@app.post("/poll")
def poll_now() -> dict[str, str]:
    poller.poll_once()
    return {"status": "ok"}


@app.post("/api/v2/auth/login")
def qbit_auth_login(username: str = Form(...), password: str = Form(...)):
    if username != settings.qbit_username or password != settings.qbit_password:
        return Response(content="Fails.", status_code=403, media_type="text/plain")
    response = qbit_ok_plain("Ok.")
    response.set_cookie("SID", "rd-cache-gateway-session")
    return response


@app.post("/api/v2/auth/logout")
def qbit_auth_logout():
    response = qbit_ok_plain("Ok.")
    response.delete_cookie("SID")
    return response


@app.get("/api/v2/app/version")
def qbit_app_version():
    return Response(content="5.0.0", media_type="text/plain")


@app.get("/api/v2/app/webapiVersion")
def qbit_webapi_version():
    return Response(content="2.8.19", media_type="text/plain")


@app.get("/api/v2/app/defaultSavePath")
def qbit_default_save_path():
    return Response(content=settings.qbit_save_path, media_type="text/plain")


@app.get("/api/v2/app/preferences")
def qbit_app_preferences():
    return build_preferences(settings.qbit_save_path)


@app.get("/api/v2/torrents/categories")
def qbit_torrents_categories():
    return build_categories(settings.qbit_save_path)


@app.get("/api/v2/torrents/info")
def qbit_torrents_info(category: str | None = None):
    return build_qbit_torrent_list(store.all(), settings.qbit_save_path, category)


@app.post("/api/v2/torrents/add")
async def qbit_torrents_add(
    urls: str | None = Form(None),
    url: str | None = Form(None),
    category: str | None = Form(None),
    savepath: str | None = Form(None),
    paused: str | None = Form(None),
    skip_checking: str | None = Form(None),
    contentLayout: str | None = Form(None),
    sequentialDownload: str | None = Form(None),
    firstLastPiecePrio: str | None = Form(None),
    torrent_files: list[UploadFile] | None = File(None),
):
    del savepath, paused, skip_checking, contentLayout, sequentialDownload, firstLastPiecePrio

    job_category = (category or "sonarr").strip().lower()
    accepted_any = False
    entries = extract_urls_from_add_request(urls, url)
    logger.info("QBIT add request category=%s urls=%s files=%s", job_category, len(entries), len(torrent_files or []))

    for entry in entries:
        try:
            resolved = await run_in_threadpool(resolve_add_url, entry)
            if resolved.get("kind") == "magnet":
                await run_in_threadpool(_add_magnet_job, str(resolved["value"]), job_category)
                accepted_any = True
            elif resolved.get("kind") == "torrent_file":
                await run_in_threadpool(
                    _add_torrent_file_job,
                    bytes(resolved["content"]),
                    str(resolved.get("filename") or "remote.torrent"),
                    job_category,
                )
                accepted_any = True
        except Exception:
            logger.exception("QBIT add failed for entry=%s", entry)

    if torrent_files:
        for upload in torrent_files:
            content = await upload.read()
            if not content:
                continue
            _add_torrent_file_job(content, upload.filename, job_category)
            accepted_any = True

    if not accepted_any:
        return PlainTextResponse("Failed.", status_code=400)
    return PlainTextResponse("Ok.", status_code=200)


@app.post("/api/v2/torrents/delete")
def qbit_torrents_delete(
    hashes: str = Form(...),
    deleteFiles: str | None = Form(None),
):
    for raw_hash in hashes.split("|"):
        torrent_id = raw_hash.strip()
        if not torrent_id:
            continue
        try:
            resolved_id, job = _resolve_job(torrent_id)
        except HTTPException:
            continue

        next_status = job.get("status", "queued")
        if next_status in {"ready_for_arr", "scan_pending"}:
            next_status = "imported"

        store.merge(
            resolved_id,
            {
                "status": next_status,
                "deleted_by_client": True,
                "deleted_at": now_utc_iso(),
                "imported_at": job.get("imported_at") or (now_utc_iso() if next_status == "imported" else None),
            },
        )

        if _boolish(deleteFiles):
            cleanup_staging_for_job(resolved_id, settings.staging_root, settings.visible_staging_root)

        rd_id = job.get("rd_torrent_id") or resolved_id
        if rd_client.is_configured():
            try:
                rd_delete_torrent(str(rd_id))
            except Exception:
                logger.warning("DELETE remote cleanup failed torrent_id=%s", rd_id)

    return qbit_ok_plain("Ok.")


@app.get("/api/v2/torrents/files")
def qbit_torrents_files(hash: str):
    _, job = _resolve_job(hash)
    files = (job.get("raw") or {}).get("files") or []
    return [
        {
            "index": safe_int(item.get("id"), index),
            "name": str(item.get("path") or "").lstrip("/"),
            "size": safe_int(item.get("bytes"), 0),
            "progress": 1.0 if item.get("selected", 1) else 0.0,
            "priority": 1 if item.get("selected", 1) else 0,
            "is_seed": False,
            "piece_range": [0, 0],
            "availability": 1.0,
        }
        for index, item in enumerate(files)
    ]


@app.get("/api/v2/torrents/properties")
def qbit_torrents_properties(hash: str):
    resolved_id, job = _resolve_job(hash)
    raw = job.get("raw") or {}
    total_size = safe_int(raw.get("bytes"), 0)
    progress = 1.0 if job.get("status") in {"staged", "ready_for_arr", "scan_pending", "imported"} else 0.0
    return {
        "hash": str(job.get("client_hash") or resolved_id).lower(),
        "save_path": str(job.get("arr_path") or f"{settings.qbit_save_path}/{resolved_id}"),
        "creation_date": 0,
        "piece_size": 0,
        "comment": "",
        "total_wasted": 0,
        "total_uploaded": 0,
        "total_uploaded_session": 0,
        "total_downloaded": total_size,
        "total_downloaded_session": total_size,
        "up_limit": 0,
        "dl_limit": 0,
        "time_elapsed": 0,
        "seeding_time": 0,
        "nb_connections": 0,
        "nb_connections_limit": 0,
        "share_ratio": 0,
        "addition_date": 0,
        "completion_date": 0,
        "created_by": settings.app_name,
        "dl_speed_avg": 0,
        "up_speed_avg": 0,
        "dl_speed": safe_int(raw.get("speed"), 0),
        "up_speed": 0,
        "eta": 0 if progress == 1.0 else 8640000,
        "last_seen": 0,
        "peers": 0,
        "peers_total": 0,
        "seeds": 0,
        "seeds_total": 0,
        "progress": progress,
    }


