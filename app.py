#!/usr/bin/env python3
"""
Amilcar Cabral Archive — Search & Tagging Web App
Run: python app.py   then open http://localhost:5000
"""

import json, re, sqlite3, threading, time
from pathlib import Path
from flask import Flask, render_template, request, jsonify, redirect, url_for
import requests

DB_PATH = Path(__file__).parent / "archive.db"
app = Flask(__name__)

# ── Google Photos URL Refresh ─────────────────────────────────────────────────
# Image URLs from Google Photos expire, so we refresh them on every cold start
# in a background thread so the app stays responsive immediately.

ALBUM_ID  = "AF1QipPF6G7g8w3a2nIMVElvLFRvoCSChj9oTCDHBipVYECS2kLi9f4qIpGKF9vi8DYVTQ"
ALBUM_KEY = "WUUtNko0alphMUFMQ3h3MTZsSHRzRDJRMzZub21B"
ALBUM_URL = f"https://photos.google.com/share/{ALBUM_ID}?key={ALBUM_KEY}"
GPHOTO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

def _parse_photos(data, start_index=1):
    photos = []
    for idx, item in enumerate(data[1]):
        photo_id = item[0]
        img_base = item[1][0]
        photos.append({
            "index_in_album": start_index + idx,
            "photo_id": photo_id,
            "image_url": img_base + "=w4096",
        })
    return photos, data[2]  # photos + next_page_token

def _fetch_next_page(session, token, sid, bl, count_so_far):
    inner = [token, None, None, None, None, None, None,
             [ALBUM_ID], None, None, None, None, None,
             None, None, None, None, None, None, None, None, [ALBUM_KEY]]
    body = json.dumps([[["x5vKt", json.dumps(inner), None, "generic"]]])
    url  = (f"https://photos.google.com/_/PhotosUi/data/batchexecute"
            f"?rpcids=x5vKt&source-path=%2Fshare%2F{ALBUM_ID}"
            f"&f.sid={sid}&bl={bl}&hl=en-US"
            f"&soc-app=165&soc-platform=1&soc-device=1&_reqid=99999&rt=c")
    resp = session.post(url,
        data={"f.req": body},
        headers={**GPHOTO_HEADERS, "content-type": "application/x-www-form-urlencoded;charset=UTF-8"},
        timeout=30)
    resp.raise_for_status()
    text = re.sub(r"^\)\]\}'\r?\n", "", resp.text)
    inner_json = None
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            outer = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(outer, list):
            continue
        for entry in outer:
            if isinstance(entry, list) and len(entry) >= 3 and entry[0] == "x5vKt":
                try:
                    inner_json = json.loads(entry[2])
                except Exception:
                    pass
                break
        if inner_json:
            break
    if not inner_json or not inner_json[1]:
        return [], None
    return _parse_photos(inner_json, start_index=count_so_far + 1)

def _fetch_all_urls():
    """Fetch fresh image URLs for every photo in the album."""
    session = requests.Session()
    resp = session.get(ALBUM_URL, headers=GPHOTO_HEADERS, timeout=30)
    resp.raise_for_status()
    html = resp.text
    m = re.search(r"AF_initDataCallback\(\{key: 'ds:1', hash: '\d+', data:([\s\S]+?), sideChannel", html)
    if not m:
        raise ValueError("Could not parse album data from Google Photos page.")
    data = json.loads(m.group(1))
    sid = (re.search(r'"FdrFJe":"(-?\d+)"', html) or type("", (), {"group": lambda s, n: ""})()).group(1)
    bl  = (re.search(r'"cfb2h":"([^"]+)"',  html) or type("", (), {"group": lambda s, n: ""})()).group(1)
    photos, next_token = _parse_photos(data, start_index=1)
    page = 2
    while next_token:
        try:
            more, next_token = _fetch_next_page(session, next_token, sid, bl, len(photos))
            if not more:
                break
            photos.extend(more)
            page += 1
            time.sleep(0.3)
        except Exception as e:
            print(f"[url-refresh] Pagination stopped at page {page}: {e}")
            break
    return photos

refresh_status = {"state": "not started", "fetched": 0, "updated": 0, "error": None}

def _refresh_urls_background():
    """Background thread: fetch fresh Google Photos URLs and update the DB."""
    global refresh_status
    refresh_status = {"state": "running", "fetched": 0, "updated": 0, "error": None}
    try:
        print("[url-refresh] Starting image URL refresh...")
        photos = _fetch_all_urls()
        refresh_status["fetched"] = len(photos)
        print(f"[url-refresh] Fetched {len(photos)} photo URLs from album, updating DB...")

        conn = sqlite3.connect(DB_PATH)
        updated = 0
        for i, p in enumerate(photos):
            rows = conn.execute(
                "UPDATE photos SET image_url=? WHERE photo_id=?",
                (p["image_url"], p["photo_id"])
            ).rowcount
            updated += rows
            # Commit every 100 rows so partial progress is saved
            if (i + 1) % 100 == 0:
                conn.commit()
                print(f"[url-refresh] Committed {i+1}/{len(photos)}...")
        conn.commit()
        conn.close()
        refresh_status["updated"] = updated
        refresh_status["state"] = "done"
        print(f"[url-refresh] Done — refreshed {updated} URLs across {len(photos)} album photos.")
    except Exception as e:
        refresh_status["state"] = "error"
        refresh_status["error"] = str(e)
        print(f"[url-refresh] Failed: {e}")

# Kick off URL refresh in the background as soon as the module loads
if DB_PATH.exists():
    threading.Thread(target=_refresh_urls_background, daemon=True).start()

# ─────────────────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ── Search ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/search")
def search():
    q           = request.args.get("q", "").strip()
    date_from   = request.args.get("date_from", "")
    date_to     = request.args.get("date_to", "")
    language    = request.args.get("language", "")
    group       = request.args.get("group", "")
    cover_only  = request.args.get("cover_only", "")
    limit       = min(int(request.args.get("limit", 50)), 200)
    offset      = int(request.args.get("offset", 0))

    conn   = get_db()
    c      = conn.cursor()
    params = []

    if q:
        # FTS search
        sql = """
            SELECT p.*, fg.group_number, fg.cover_sheet_code
            FROM photos p
            LEFT JOIN file_groups fg ON p.file_group = fg.group_number
            WHERE p.id IN (
                SELECT rowid FROM photos_fts WHERE photos_fts MATCH ?
            )
        """
        params.append(q)
    else:
        sql = """
            SELECT p.*, fg.group_number, fg.cover_sheet_code
            FROM photos p
            LEFT JOIN file_groups fg ON p.file_group = fg.group_number
            WHERE 1=1
        """

    if date_from:  sql += " AND p.doc_date >= ?"; params.append(date_from)
    if date_to:    sql += " AND p.doc_date <= ?"; params.append(date_to)
    if language:   sql += " AND p.language = ?";  params.append(language)
    if group:      sql += " AND p.file_group = ?"; params.append(int(group))
    if cover_only: sql += " AND p.is_cover_sheet = 1"

    sql += " AND p.processed = 1 ORDER BY p.index_in_album LIMIT ? OFFSET ?"
    params += [limit, offset]

    rows  = c.execute(sql, params).fetchall()
    total = c.execute("SELECT COUNT(*) FROM photos WHERE processed=1").fetchone()[0]

    results = []
    for r in rows:
        results.append({
            "id":            r["id"],
            "index":         r["index_in_album"],
            "photo_id":      r["photo_id"],
            "image_url":     r["image_url"],
            "page_url":      r["page_url"],
            "is_cover":      bool(r["is_cover_sheet"]),
            "file_group":    r["file_group"],
            "cover_code":    r["cover_sheet_code"],
            "transcription": (r["transcription"] or "")[:500],
            "translation":   (r["translation"]   or "")[:500],
            "doc_date":      r["doc_date"],
            "subject":       r["subject"],
            "language":      r["language"],
            "tags":          json.loads(r["tags"] or "[]"),
            "notes":         r["notes"] or "",
        })

    conn.close()
    return jsonify({"results": results, "total": total, "offset": offset})

# ── File Groups ───────────────────────────────────────────────────────────────

@app.route("/api/groups")
def list_groups():
    conn = get_db()
    rows = conn.execute("""
        SELECT fg.*, COUNT(p.id) as photo_count
        FROM file_groups fg
        LEFT JOIN photos p ON p.file_group = fg.group_number AND p.processed=1
        GROUP BY fg.id ORDER BY fg.group_number
    """).fetchall()
    groups = [{
        "id":           r["id"],
        "group_number": r["group_number"],
        "cover_code":   r["cover_sheet_code"],
        "title":        r["title"],
        "doc_date":     r["doc_date"],
        "subject":      r["subject"],
        "first_index":  r["first_index"],
        "last_index":   r["last_index"],
        "photo_count":  r["photo_count"],
        "tags":         json.loads(r["tags"] or "[]"),
        "notes":        r["notes"] or "",
    } for r in rows]
    conn.close()
    return jsonify(groups)

@app.route("/api/group/<int:group_num>")
def get_group(group_num):
    conn  = get_db()
    fg    = conn.execute("SELECT * FROM file_groups WHERE group_number=?", (group_num,)).fetchone()
    photos = conn.execute(
        "SELECT * FROM photos WHERE file_group=? AND processed=1 ORDER BY index_in_album",
        (group_num,)
    ).fetchall()
    if not fg:
        conn.close(); return jsonify({"error": "Group not found"}), 404

    result = {
        "group": {
            "group_number": fg["group_number"],
            "cover_code":   fg["cover_sheet_code"],
            "title":        fg["title"],
            "doc_date":     fg["doc_date"],
            "subject":      fg["subject"],
            "first_index":  fg["first_index"],
            "last_index":   fg["last_index"],
            "tags":         json.loads(fg["tags"] or "[]"),
            "notes":        fg["notes"] or "",
        },
        "photos": [{
            "id":            p["id"],
            "index":         p["index_in_album"],
            "image_url":     p["image_url"],
            "page_url":      p["page_url"],
            "is_cover":      bool(p["is_cover_sheet"]),
            "transcription": p["transcription"] or "",
            "translation":   p["translation"]   or "",
            "doc_date":      p["doc_date"],
            "subject":       p["subject"],
            "language":      p["language"],
            "tags":          json.loads(p["tags"] or "[]"),
            "notes":         p["notes"] or "",
        } for p in photos]
    }
    conn.close()
    return jsonify(result)

# ── Tagging ───────────────────────────────────────────────────────────────────

@app.route("/api/photo/<int:photo_id>/tag", methods=["POST"])
def tag_photo(photo_id):
    data   = request.json
    action = data.get("action")   # "add" or "remove"
    tag    = data.get("tag","").strip()
    if not tag: return jsonify({"error":"empty tag"}), 400

    conn = get_db()
    row  = conn.execute("SELECT tags FROM photos WHERE id=?", (photo_id,)).fetchone()
    if not row: conn.close(); return jsonify({"error":"not found"}), 404

    tags = json.loads(row["tags"] or "[]")
    if action == "add"    and tag not in tags: tags.append(tag)
    if action == "remove" and tag in tags:     tags.remove(tag)

    conn.execute("UPDATE photos SET tags=? WHERE id=?", (json.dumps(tags), photo_id))
    conn.commit(); conn.close()
    return jsonify({"tags": tags})

@app.route("/api/photo/<int:photo_id>/note", methods=["POST"])
def note_photo(photo_id):
    note = request.json.get("note","")
    conn = get_db()
    conn.execute("UPDATE photos SET notes=? WHERE id=?", (note, photo_id))
    conn.commit(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/photo/<int:photo_id>/date", methods=["POST"])
def set_date(photo_id):
    doc_date = request.json.get("doc_date","").strip() or None
    conn = get_db()
    conn.execute("UPDATE photos SET doc_date=? WHERE id=?", (doc_date, photo_id))
    conn.commit(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/group/<int:group_num>/tag", methods=["POST"])
def tag_group(group_num):
    data   = request.json
    action = data.get("action")
    tag    = data.get("tag","").strip()
    if not tag: return jsonify({"error":"empty tag"}), 400

    conn = get_db()
    row  = conn.execute("SELECT tags FROM file_groups WHERE group_number=?", (group_num,)).fetchone()
    if not row: conn.close(); return jsonify({"error":"not found"}), 404

    tags = json.loads(row["tags"] or "[]")
    if action == "add"    and tag not in tags: tags.append(tag)
    if action == "remove" and tag in tags:     tags.remove(tag)

    conn.execute("UPDATE file_groups SET tags=? WHERE group_number=?",
                 (json.dumps(tags), group_num))
    # Also tag all photos in group
    conn.execute("UPDATE photos SET tags=json_insert(tags,'$[#]',?) WHERE file_group=? AND json_type(tags)='array'",
                 (tag, group_num)) if action == "add" else None
    conn.commit(); conn.close()
    return jsonify({"tags": tags})

@app.route("/api/group/<int:group_num>/note", methods=["POST"])
def note_group(group_num):
    note = request.json.get("note","")
    conn = get_db()
    conn.execute("UPDATE file_groups SET notes=? WHERE group_number=?", (note, group_num))
    conn.commit(); conn.close()
    return jsonify({"ok": True})

# ── Refresh ───────────────────────────────────────────────────────────────────

@app.route("/api/refresh-status")
def get_refresh_status():
    return jsonify(refresh_status)

@app.route("/api/refresh", methods=["POST"])
def trigger_refresh():
    if refresh_status.get("state") == "running":
        return jsonify({"ok": False, "message": "Refresh already running"}), 409
    threading.Thread(target=_refresh_urls_background, daemon=True).start()
    return jsonify({"ok": True, "message": "Refresh started"})

# ── Stats ─────────────────────────────────────────────────────────────────────

@app.route("/api/stats")
def stats():
    conn = get_db()
    total     = conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0]
    processed = conn.execute("SELECT COUNT(*) FROM photos WHERE processed=1").fetchone()[0]
    covers    = conn.execute("SELECT COUNT(*) FROM photos WHERE is_cover_sheet=1").fetchone()[0]
    groups    = conn.execute("SELECT COUNT(*) FROM file_groups").fetchone()[0]
    langs     = conn.execute(
        "SELECT language, COUNT(*) as n FROM photos WHERE processed=1 AND language IS NOT NULL "
        "GROUP BY language ORDER BY n DESC"
    ).fetchall()
    errors    = conn.execute(
        "SELECT COUNT(*) FROM photos WHERE processed=1 AND processing_error IS NOT NULL"
    ).fetchone()[0]
    conn.close()
    return jsonify({
        "total": total, "processed": processed, "pending": total-processed,
        "covers": covers, "groups": groups, "errors": errors,
        "languages": [{"lang": r["language"], "count": r["n"]} for r in langs]
    })

if __name__ == "__main__":
    if not DB_PATH.exists():
        print("No database found. Run: python process.py")
    else:
        print("Starting Amilcar Cabral Archive search app...")
        print("Open: http://localhost:5000")
    app.run(debug=True, port=5000)
