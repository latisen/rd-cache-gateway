from __future__ import annotations

from datetime import datetime, timezone
from email.utils import format_datetime
from urllib.parse import quote, unquote
from xml.sax.saxutils import escape

DAV_CONTENT_TYPE = 'application/xml; charset="utf-8"'


def normalize_subpath(subpath: str | None) -> str:
    return (subpath or "").strip().strip("/")


def root_entry() -> dict:
    return {
        "href": "/dav/",
        "name": "dav",
        "is_dir": True,
        "size": 0,
        "modified": _http_date_now(),
    }


def all_dir_entry() -> dict:
    return {
        "href": "/dav/__all__/",
        "name": "__all__",
        "is_dir": True,
        "size": 0,
        "modified": _http_date_now(),
    }


def _http_date_now() -> str:
    return format_datetime(datetime.now(timezone.utc), usegmt=True)


def _format_modified(value: str | None) -> str:
    if not value:
        return _http_date_now()
    try:
        return format_datetime(datetime.fromisoformat(str(value).replace("Z", "+00:00")), usegmt=True)
    except Exception:
        return str(value)


def _entry_xml(entry: dict) -> str:
    href = escape(str(entry.get("href") or "/dav/"))
    name = escape(str(entry.get("name") or ""))
    modified = escape(_format_modified(entry.get("modified")))
    if entry.get("is_dir"):
        resource_type = "<d:collection/>"
        size_xml = ""
    else:
        resource_type = ""
        size_xml = f"<d:getcontentlength>{int(entry.get('size') or 0)}</d:getcontentlength>"
    return f"""
  <d:response>
    <d:href>{href}</d:href>
    <d:propstat>
      <d:prop>
        <d:displayname>{name}</d:displayname>
        <d:resourcetype>{resource_type}</d:resourcetype>
        <d:getlastmodified>{modified}</d:getlastmodified>
        {size_xml}
      </d:prop>
      <d:status>HTTP/1.1 200 OK</d:status>
    </d:propstat>
  </d:response>"""


def build_multistatus(subpath: str | None, remote_entries: list[dict], depth: str = "1") -> str:
    normalized = normalize_subpath(subpath)
    responses: list[dict] = []

    if normalized == "":
        responses.append(root_entry())
        if depth != "0":
            responses.append(all_dir_entry())
    elif normalized == "__all__":
        responses.append(all_dir_entry())
        if depth != "0":
            responses.extend(sorted(remote_entries, key=lambda item: str(item.get("name") or "").lower()))
    else:
        entry = find_entry(normalized, remote_entries)
        if entry is not None:
            responses.append(entry)

    xml = "".join(_entry_xml(item) for item in responses)
    return "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<d:multistatus xmlns:d=\"DAV:\">" + xml + "\n</d:multistatus>"


def find_entry(subpath: str | None, remote_entries: list[dict]) -> dict | None:
    normalized = normalize_subpath(subpath)
    if normalized == "":
        return root_entry()
    if normalized == "__all__":
        return all_dir_entry()
    if not normalized.startswith("__all__/"):
        return None

    wanted_name = unquote(normalized.split("/", 1)[1])
    for entry in remote_entries:
        href = str(entry.get("href") or "")
        if str(entry.get("name") or "") == wanted_name:
            return entry
        if href.rstrip("/").endswith("/" + quote(wanted_name)):
            return entry
    return None
