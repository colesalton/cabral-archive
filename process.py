#!/usr/bin/env python3
"""
Amilcar Cabral Archive — Photo Processor
Fetches all photos from the shared Google Photos album,
transcribes + translates with Claude, stores in SQLite.
Run: python process.py
"""

import re, sys, json, time, sqlite3, base64, datetime
from pathlib import Path
import requests
import anthropic

ALBUM_URL = (
    "https://photos.google.com/share/"
    "AF1QipPF6G7g8w3a2nIMVElvLFRvoCSChj9oTCDHBipVYECS2kLi9f4qIpGKF9vi8DYVTQ"
    "?key=WUUtNko0alphMUFMQ3h3MTZsSHRzRDJRMzZub21B"
)
ALBUM_ID  = "AF1QipPF6G7g8w3a2nIMVElvLFRvoCSChj9oTCDHBipVYECS2kLi9f4qIpGKF9vi8DYVTQ"
ALBUM_KEY = "WUUtNko0alphMUFMQ3h3MTZsSHRzRDJRMzZub21B"
DB_PATH   = Path(__file__).parent / "archive.db"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ── DB Setup ──────────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS photos (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            index_in_album  INTEGER UNIQUE,
            photo_id        TEXT UNIQUE,
            image_url       TEXT,
            page_url        TEXT,
            width           INTEGER,
            height          INTEGER,
            taken_at        TEXT,
            is_cover_sheet  INTEGER DEFAULT 0,
            file_group      INTEGER,
            transcription   TEXT,
            translation     TEXT,
            doc_date        TEXT,
            subject         TEXT,
            language        TEXT,
            processed       INTEGER DEFAULT 0,
            processing_error TEXT,
            tags            TEXT DEFAULT '[]',
            notes           TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS file_groups (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            group_number        INTEGER UNIQUE,
            cover_photo_index   INTEGER,
            cover_photo_id      TEXT,
            cover_sheet_code    TEXT,
            title               TEXT,
            doc_date            TEXT,
            subject             TEXT,
            first_index         INTEGER,
            last_index          INTEGER,
            tags                TEXT DEFAULT '[]',
            notes               TEXT DEFAULT ''
        );
        CREATE VIRTUAL TABLE IF NOT EXISTS photos_fts USING fts5(
            photo_id, transcription, translation, doc_date, subject, tags, notes,
            content='photos', content_rowid='id'
        );
        CREATE TRIGGER IF NOT EXISTS photos_ai AFTER INSERT ON photos BEGIN
            INSERT INTO photos_fts(rowid,photo_id,transcription,translation,doc_date,subject,tags,notes)
            VALUES(new.id,new.photo_id,new.transcription,new.translation,new.doc_date,new.subject,new.tags,new.notes);
        END;
        CREATE TRIGGER IF NOT EXISTS photos_au AFTER UPDATE ON photos BEGIN
            INSERT INTO photos_fts(photos_fts,rowid,photo_id,transcription,translation,doc_date,subject,tags,notes)
            VALUES('delete',old.id,old.photo_id,old.transcription,old.translation,old.doc_date,old.subject,old.tags,old.notes);
            INSERT INTO photos_fts(rowid,photo_id,transcription,translation,doc_date,subject,tags,notes)
            VALUES(new.id,new.photo_id,new.transcription,new.translation,new.doc_date,new.subject,new.tags,new.notes);
        END;
    """)
    conn.commit()
    return conn

# ── Photo List Fetching (with pagination) ─────────────────────────────────────

def parse_photos_from_data(data, start_index=1):
    photos = []
    for idx, item in enumerate(data[1]):
        photo_id   = item[0]
        img_base   = item[1][0]
        width      = item[1][1]
        height     = item[1][2]
        ts_ms      = item[2]
        taken_at   = (datetime.datetime.utcfromtimestamp(ts_ms/1000).isoformat()+"Z"
                      if isinstance(ts_ms,(int,float)) else None)
        photos.append({
            "index_in_album": start_index + idx,
            "photo_id":        photo_id,
            "image_url":       img_base + "=w4096",
            "page_url":        f"https://photos.google.com/share/{ALBUM_ID}/photo/{photo_id}?key={ALBUM_KEY}",
            "width":           width,
            "height":          height,
            "taken_at":        taken_at,
        })
    return photos, data[2]   # photos + next_page_token

def fetch_more_photos(session, token, sid, bl, count_so_far):
    """Call batchexecute to get next page of photos."""
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
        headers={**HEADERS, "content-type": "application/x-www-form-urlencoded;charset=UTF-8"},
        timeout=30)
    resp.raise_for_status()
    text = resp.text
    # Strip XSSI prefix (handles \r\n on Windows too)
    text = re.sub(r"^\)\]\}'\r?\n", "", text)
    # batchexecute returns multiple JSON chunks separated by newlines;
    # scan each line for the x5vKt entry
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
    return parse_photos_from_data(inner_json, start_index=count_so_far+1)

def fetch_all_photos():
    """Fetch all photos from the album, following pagination."""
    session = requests.Session()
    print("Fetching album page...")
    resp = session.get(ALBUM_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    html = resp.text

    pattern = r"AF_initDataCallback\(\{key: 'ds:1', hash: '\d+', data:([\s\S]+?), sideChannel"
    m = re.search(pattern, html)
    if not m:
        raise ValueError("Could not parse album data from page.")
    data = json.loads(m.group(1))

    # Extract session params for pagination
    sid_m = re.search(r'"FdrFJe":"(-?\d+)"', html)
    bl_m  = re.search(r'"cfb2h":"([^"]+)"', html)
    sid   = sid_m.group(1) if sid_m else ""
    bl    = bl_m.group(1)  if bl_m  else ""

    photos, next_token = parse_photos_from_data(data, start_index=1)
    print(f"  Page 1: {len(photos)} photos loaded")

    # Paginate
    page = 2
    while next_token:
        try:
            more, next_token = fetch_more_photos(session, next_token, sid, bl, len(photos))
            if not more:
                break
            photos.extend(more)
            print(f"  Page {page}: +{len(more)} photos (total {len(photos)})")
            page += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"  Pagination stopped at page {page}: {e}")
            break

    print(f"Total photos fetched: {len(photos)}")
    return photos

def upsert_photos(conn, photos):
    """Insert new photos and always refresh image_url (they expire)."""
    c = conn.cursor()
    new = 0
    for p in photos:
        r = c.execute(
            "INSERT OR IGNORE INTO photos "
            "(index_in_album,photo_id,image_url,page_url,width,height,taken_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (p["index_in_album"], p["photo_id"], p["image_url"],
             p["page_url"], p["width"], p["height"], p["taken_at"])
        )
        if r.rowcount:
            new += 1
        else:
            # Always refresh the URL so it doesn't expire between runs
            c.execute("UPDATE photos SET image_url=? WHERE photo_id=?",
                      (p["image_url"], p["photo_id"]))
    conn.commit()
    print(f"  {new} new photos added to DB ({len(photos)-new} already existed, URLs refreshed)")

# ── Claude Vision Processing ──────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert archivist specializing in Portuguese colonial-era documents and the Amilcar Cabral / PAIGC archive (Guinea-Bissau liberation movement). You transcribe handwritten and typed documents and extract structured metadata.

Respond ONLY with valid JSON (no markdown, no explanation) matching this schema exactly:
{
  "is_cover_sheet": true or false,
  "cover_sheet_code": "e.g. EV/AAC/FAE/001 or null",
  "transcription": "full verbatim text in original language",
  "translation": "full English translation (copy verbatim if already English)",
  "language": "Portuguese | French | Creole | English | other",
  "doc_date": "YYYY-MM-DD or YYYY-MM or YYYY or null",
  "subject": "1-2 sentence English description of the document's topic"
}

Cover sheet rules — a page IS a cover sheet if it:
- Has sparse text mostly in the upper portion
- Contains an archival reference code (EV/AAC/FAE/ pattern, or similar)
- Acts as a divider/label for a group of subsequent pages
- May say things like "Pasta", "Dossier", classification stamps

For typed/printed pages: transcribe ALL visible text faithfully.
For handwritten pages: do your best, mark illegible words as [illegible]."""

def process_photo_with_claude(client, image_url, index):
    # Download image (retry once on failure — URL may have just expired)
    for attempt in range(2):
        try:
            img_resp = requests.get(image_url, headers=HEADERS, timeout=60)
            img_resp.raise_for_status()
            break
        except Exception as e:
            if attempt == 1:
                return {"error": f"Download failed: {e}"}
            time.sleep(2)

    media_type = img_resp.headers.get("Content-Type","image/jpeg").split(";")[0]
    b64 = base64.standard_b64encode(img_resp.content).decode()

    # Call Claude with retry on transient errors (529 overload, 500 server error)
    for attempt in range(4):
        try:
            msg = client.messages.create(
                model="claude-opus-4-5",
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64}},
                        {"type": "text",  "text": f"Photo #{index} from Amilcar Cabral Archive. Return JSON only."}
                    ]
                }]
            )
            raw = msg.content[0].text.strip()
            # Strip markdown code fences if present
            if raw.startswith("```"): raw = re.sub(r"^```[a-z]*\n?", "", raw).rstrip("`").strip()
            return json.loads(raw)
        except json.JSONDecodeError as e:
            return {"error": f"JSON parse error: {e} | raw: {raw[:200]}"}
        except anthropic.RateLimitError:
            wait = 30 * (2 ** attempt)
            print(f"[rate limit, waiting {wait}s]", end=" ", flush=True)
            time.sleep(wait)
        except anthropic.APIStatusError as e:
            if e.status_code in (529, 500) and attempt < 3:
                wait = 15 * (2 ** attempt)
                print(f"[{e.status_code}, waiting {wait}s]", end=" ", flush=True)
                time.sleep(wait)
            else:
                return {"error": str(e)}
        except Exception as e:
            return {"error": str(e)}
    return {"error": "Max retries exceeded"}

# ── File Group Assignment ─────────────────────────────────────────────────────

def assign_file_groups(conn):
    c = conn.cursor()
    rows = c.execute(
        "SELECT id,index_in_album,photo_id,is_cover_sheet,subject,doc_date,transcription "
        "FROM photos ORDER BY index_in_album"
    ).fetchall()

    group_num   = 0
    current_grp = None

    for row in rows:
        pid, idx, photo_id, is_cover, subject, doc_date, transcription = (
            row["id"], row["index_in_album"], row["photo_id"],
            row["is_cover_sheet"], row["subject"], row["doc_date"], row["transcription"]
        )
        if is_cover:
            group_num += 1
            current_grp = group_num
            # Extract cover sheet code from transcription
            code_m = re.search(r'[A-Z]{2,}/[A-Z]{2,}[/A-Z0-9]*', transcription or "")
            code = code_m.group(0) if code_m else None
            # Close out previous group
            if group_num > 1:
                c.execute("UPDATE file_groups SET last_index=? WHERE group_number=?",
                          (idx-1, group_num-1))
            c.execute("""
                INSERT OR REPLACE INTO file_groups
                  (group_number,cover_photo_index,cover_photo_id,cover_sheet_code,title,doc_date,subject,first_index)
                VALUES (?,?,?,?,?,?,?,?)
            """, (group_num, idx, photo_id, code, subject, doc_date, subject, idx))

        if current_grp:
            c.execute("UPDATE photos SET file_group=? WHERE id=?", (current_grp, pid))

    # Close final group
    if rows and group_num > 0:
        c.execute("UPDATE file_groups SET last_index=? WHERE group_number=?",
                  (rows[-1]["index_in_album"], group_num))

    conn.commit()
    print(f"Assigned {group_num} file groups.")

# ── Main ──────────────────────────────────────────────────────────────────────

def run(start_index=1, end_index=None):
    conn   = init_db()
    client = anthropic.Anthropic()

    # Refresh photo list (also catches new photos added to album)
    print("\n[1/3] Fetching photo list from album...")
    photos = fetch_all_photos()
    upsert_photos(conn, photos)

    # Process unprocessed photos
    c = conn.cursor()
    q = "SELECT id,index_in_album,photo_id,image_url FROM photos WHERE processed=0"
    params = []
    if start_index > 1: q += " AND index_in_album>=?"; params.append(start_index)
    if end_index:        q += " AND index_in_album<=?"; params.append(end_index)
    q += " ORDER BY index_in_album"
    pending = c.execute(q, params).fetchall()
    total   = len(pending)

    print(f"\n[2/3] Processing {total} photos with Claude...\n")
    for i, row in enumerate(pending):
        db_id, idx, photo_id, image_url = row["id"], row["index_in_album"], row["photo_id"], row["image_url"]
        print(f"  [{i+1}/{total}] #{idx} ... ", end="", flush=True)

        result = process_photo_with_claude(client, image_url, idx)

        if "error" in result:
            print(f"ERROR: {result['error'][:80]}")
            # Leave processed=0 so the next run will retry
            c.execute("UPDATE photos SET processing_error=? WHERE id=?",
                      (result["error"], db_id))
        else:
            label = "COVER" if result.get("is_cover_sheet") else "page"
            print(f"{label} | {result.get('language','?')} | {result.get('doc_date','no date')}")
            c.execute("""
                UPDATE photos SET
                  processed=1, is_cover_sheet=?, transcription=?, translation=?,
                  language=?, doc_date=?, subject=?, processing_error=NULL
                WHERE id=?
            """, (
                1 if result.get("is_cover_sheet") else 0,
                result.get("transcription",""),
                result.get("translation",""),
                result.get("language",""),
                result.get("doc_date"),
                result.get("subject",""),
                db_id
            ))
        conn.commit()
        if (i+1) % 10 == 0: time.sleep(1)

    print(f"\n[3/3] Assigning file groups...")
    assign_file_groups(conn)
    conn.close()
    print("\nDone! Start the search app with: python app.py")

if __name__ == "__main__":
    start = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    end   = int(sys.argv[2]) if len(sys.argv) > 2 else None
    run(start, end)
