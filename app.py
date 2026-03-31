#!/usr/bin/env python3
"""
Amilcar Cabral Archive — Search & Tagging Web App
Run: python app.py   then open http://localhost:5000
"""

import json, sqlite3
from pathlib import Path
from flask import Flask, render_template, request, jsonify, redirect, url_for

DB_PATH = Path(__file__).parent / "archive.db"
app = Flask(__name__)

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
