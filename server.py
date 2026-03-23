from flask import Flask, request, Response, jsonify, send_from_directory, redirect
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
import sqlite3
import threading
import re
import os
import time

app = Flask(__name__, static_folder="static")
CORS(app)

DB_PATH = os.environ.get("DB_PATH", "books.db")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://hebrewbooks.org/",
    "Accept-Language": "he,en;q=0.9",
}

# ─── DATABASE ────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id      INTEGER PRIMARY KEY,
            title   TEXT NOT NULL DEFAULT '',
            author  TEXT DEFAULT '',
            year    TEXT DEFAULT '',
            subject TEXT DEFAULT '',
            valid   INTEGER DEFAULT 1
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_title   ON books(title)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_author  ON books(author)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_subject ON books(subject)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crawler_state (
            key TEXT PRIMARY KEY, value TEXT
        )
    """)
    conn.commit()

    # Seed known books so app isn't empty on first run
    seed = [
        (9780,  "שולחן ערוך - אורח חיים",       "ר' יוסף קארו",            "1565","הלכה"),
        (9781,  "שולחן ערוך - יורה דעה",         "ר' יוסף קארו",            "1565","הלכה"),
        (9782,  "שולחן ערוך - חושן משפט",        "ר' יוסף קארו",            "1565","הלכה"),
        (9783,  "שולחן ערוך - אבן העזר",         "ר' יוסף קארו",            "1565","הלכה"),
        (14763, "משנה תורה",                      "הרמב\"ם",                  "1180","הלכה"),
        (9999,  "מורה נבוכים",                   "הרמב\"ם",                  "1190","פילוסופיה"),
        (8888,  "ספר המצוות",                    "הרמב\"ם",                  "1170","הלכה"),
        (3281,  "ספר החינוך",                    "ר' אהרן הלוי",             "1523","מצוות"),
        (22879, "חפץ חיים",                      "ר' ישראל מאיר קגן",        "1873","מוסר"),
        (43081, "נפש החיים",                     "ר' חיים מוולוז'ין",        "1824","מחשבה"),
        (8774,  "מסילת ישרים",                  "הרמח\"ל",                  "1740","מוסר"),
        (4902,  "ספר הכוזרי",                   "ר' יהודה הלוי",            "1140","פילוסופיה"),
        (14490, "עין יעקב",                      "ר' יעקב אבן חביב",        "1516","אגדה"),
        (11234, "תניא",                          "ר' שניאור זלמן מלאדי",    "1797","חסידות"),
        (2865,  "ספר הזוהר",                    "רשב\"י",                   "1280","קבלה"),
        (5432,  "אור החיים",                    "ר' חיים בן עטר",           "1742","פרשנות"),
        (6789,  "שפת אמת",                      "ר' יהודה אריה לייב",      "1905","חסידות"),
        (3456,  "חתם סופר - שו\"ת",             "ר' משה סופר",              "1839","שו\"ת"),
        (8901,  "אגרות משה",                    "ר' משה פיינשטיין",         "1959","שו\"ת"),
        (9012,  "יביע אומר",                    "ר' עובדיה יוסף",           "1954","שו\"ת"),
        (5678,  "ערוך השולחן",                  "ר' יחיאל מיכל עפשטיין",   "1903","הלכה"),
        (3210,  "בן איש חי",                    "ר' יוסף חיים",             "1898","הלכה"),
        (1111,  "תורה תמימה",                   "ר' ברוך הלוי עפשטיין",    "1902","פרשנות"),
        (2222,  "משך חכמה",                     "ר' מאיר שמחה מדווינסק",   "1927","פרשנות"),
        (4444,  "ליקוטי מוהר\"ן",              "ר' נחמן מברסלב",           "1808","חסידות"),
        (6666,  "עץ חיים",                      "ר' חיים ויטאל",            "1573","קבלה"),
        (2109,  "חיי אדם",                      "ר' אברהם דנציג",           "1810","הלכה"),
        (4567,  "קצות החושן",                   "ר' אריה לייב הלר",        "1788","הלכה"),
        (7890,  "נתיבות המשפט",                 "ר' יעקב לורברבוים",       "1809","הלכה"),
        (9876,  "פניני הלכה",                   "ר' אליעזר מלמד",           "1990","הלכה"),
        (6543,  "שמירת שבת כהלכתה",            "ר' יהושע נויבירט",        "1965","שבת"),
        (7643,  "פרי מגדים",                    "ר' יוסף תאומים",          "1787","הלכה"),
        (3333,  "שם משמואל",                    "ר' שמואל בורנשטיין",      "1926","חסידות"),
        (1234,  "ברכי יוסף",                    "ר' חיד\"א",                "1774","הלכה"),
        (1010,  "עקידת יצחק",                   "ר' יצחק עראמה",           "1522","פרשנות"),
        (2345,  "ציץ אליעזר",                   "ר' אליעזר וולדנברג",      "1945","שו\"ת"),
        (5555,  "ספר יצירה",                    "עתיק",                    "200", "קבלה"),
        (7777,  "תולדות יצחק",                  "ר' יצחק קארו",            "1518","פרשנות"),
        (2468,  "ספר תהילים",                   "דוד המלך",                 "900", "תנ\"ך"),
        (1357,  "הגדה של פסח",                  "שונות",                   "1000","מועדים"),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO books (id,title,author,year,subject) VALUES (?,?,?,?,?)",
        seed
    )
    conn.commit()
    conn.close()
    print(f"✅ DB ready: {DB_PATH}")

# ─── CRAWLER ─────────────────────────────────────────────────────
def get_state(key, default=None):
    conn = get_db()
    r = conn.execute("SELECT value FROM crawler_state WHERE key=?", (key,)).fetchone()
    conn.close()
    return r["value"] if r else default

def set_state(key, value):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO crawler_state(key,value) VALUES(?,?)", (key, str(value)))
    conn.commit()
    conn.close()

def book_count():
    conn = get_db()
    n = conn.execute("SELECT COUNT(*) FROM books WHERE valid=1").fetchone()[0]
    conn.close()
    return n

def fetch_book_meta(book_id):
    """Fetch title/author from HebrewBooks book page."""
    try:
        r = requests.get(
            f"https://hebrewbooks.org/{book_id}",
            headers=HEADERS, timeout=12
        )
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        # HebrewBooks page structure
        title = ""
        author = ""
        year = ""
        subject = ""

        # Try meta tags first
        og_title = soup.find("meta", property="og:title")
        if og_title:
            title = og_title.get("content", "").strip()

        # Try h1/h2
        if not title:
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text(strip=True)

        # Author from h2 or specific fields
        h2 = soup.find("h2")
        if h2:
            author = h2.get_text(strip=True)

        # Look for structured data
        for td in soup.find_all("td"):
            txt = td.get_text(strip=True)
            nxt = td.find_next_sibling("td")
            if not nxt:
                continue
            val = nxt.get_text(strip=True)
            if "מחבר" in txt or "author" in txt.lower():
                author = val
            elif "שנה" in txt or "year" in txt.lower():
                year = val
            elif "נושא" in txt or "subject" in txt.lower() or "קטגור" in txt:
                subject = val

        # Look in page title
        if not title:
            pt = soup.find("title")
            if pt:
                title = pt.get_text(strip=True).split(" - ")[0].strip()

        if not title or len(title) < 2:
            return None

        # Clean up
        title  = re.sub(r'\s+', ' ', title).strip()
        author = re.sub(r'\s+', ' ', author).strip()

        return (book_id, title, author, year, subject)
    except Exception as e:
        return None

def background_crawler():
    """Crawl HebrewBooks IDs sequentially."""
    time.sleep(3)
    print("🕷️  Crawler started")

    BATCH = 5       # fetch 5 books concurrently (sequential)
    DELAY = 0.8     # seconds between batches

    while True:
        try:
            current_id = int(get_state("last_id", 1))
            MAX_ID = 110000

            if current_id >= MAX_ID:
                print("✅ Full crawl done!")
                time.sleep(86400)
                set_state("last_id", 1)
                continue

            # Fetch a batch
            fetched = []
            for book_id in range(current_id, min(current_id + BATCH, MAX_ID)):
                # Skip if already in DB
                conn = get_db()
                exists = conn.execute(
                    "SELECT id FROM books WHERE id=?", (book_id,)
                ).fetchone()
                conn.close()
                if exists:
                    continue

                meta = fetch_book_meta(book_id)
                if meta:
                    fetched.append(meta)
                time.sleep(0.2)

            if fetched:
                conn = get_db()
                conn.executemany(
                    "INSERT OR IGNORE INTO books(id,title,author,year,subject) VALUES(?,?,?,?,?)",
                    fetched
                )
                conn.commit()
                conn.close()
                total = book_count()
                print(f"📚 IDs {current_id}-{current_id+BATCH}: +{len(fetched)} | Total: {total}")

            set_state("last_id", current_id + BATCH)
            time.sleep(DELAY)

        except Exception as e:
            print(f"Crawler error: {e}")
            time.sleep(30)

# ─── ROUTES ──────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/api/stats")
def stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM books WHERE valid=1").fetchone()[0]
    crawled = get_state("last_id", "1")
    subjects = conn.execute(
        "SELECT subject, COUNT(*) n FROM books WHERE subject!='' AND valid=1 "
        "GROUP BY subject ORDER BY n DESC LIMIT 16"
    ).fetchall()
    conn.close()
    return jsonify({
        "total":    total,
        "crawled":  int(crawled),
        "subjects": [{"name": r["subject"], "count": r["n"]} for r in subjects],
    })

@app.route("/api/featured")
def featured():
    ids = "9780,14763,3281,22879,43081,8774,4902,14490,11234,2865,5432,6789,3456,9012,5678,3210,1111,2222,4444,6666"
    conn = get_db()
    rows = conn.execute(f"SELECT * FROM books WHERE id IN ({ids}) AND valid=1").fetchall()
    conn.close()
    return jsonify({"books": [dict(r) for r in rows]})

@app.route("/api/search")
def search():
    q      = request.args.get("q","").strip()
    page   = request.args.get("page", 1, type=int)
    subj   = request.args.get("subject","")
    limit  = 30
    offset = (page-1)*limit

    conn = get_db()
    where_parts = ["valid=1"]
    params = []

    if q:
        where_parts.append("(title LIKE ? OR author LIKE ?)")
        params += [f"%{q}%", f"%{q}%"]
    if subj:
        where_parts.append("subject=?")
        params.append(subj)

    where = " AND ".join(where_parts)

    total = conn.execute(f"SELECT COUNT(*) FROM books WHERE {where}", params).fetchone()[0]
    rows  = conn.execute(
        f"SELECT * FROM books WHERE {where} ORDER BY id LIMIT ? OFFSET ?",
        params + [limit, offset]
    ).fetchall()
    conn.close()

    books = [dict(r) for r in rows]

    # If not enough local results for a query, do a live fetch
    if q and len(books) < 10:
        try:
            url = f"https://hebrewbooks.org/search?sdesc={requests.utils.quote(q)}&page=1"
            r = requests.get(url, headers=HEADERS, timeout=10)
            soup = BeautifulSoup(r.text, "html.parser")
            live = []
            existing_ids = {b["id"] for b in books}
            for row in soup.select("table tr"):
                cells = row.find_all("td")
                link = row.find("a", href=re.compile(r"^/\d+$"))
                if not link or len(cells) < 2:
                    continue
                try:
                    bid = int(re.search(r"(\d+)", link["href"]).group(1))
                    if bid in existing_ids:
                        continue
                    title  = cells[0].get_text(strip=True)
                    author = cells[1].get_text(strip=True) if len(cells)>1 else ""
                    year   = cells[2].get_text(strip=True) if len(cells)>2 else ""
                    if title:
                        live.append({"id":bid,"title":title,"author":author,"year":year,"subject":"","valid":1})
                        existing_ids.add(bid)
                        # save to DB
                        save_conn = get_db()
                        save_conn.execute(
                            "INSERT OR IGNORE INTO books(id,title,author,year,subject) VALUES(?,?,?,?,?)",
                            (bid,title,author,year,"")
                        )
                        save_conn.commit()
                        save_conn.close()
                except:
                    pass
            books = books + live
            if not total:
                total = len(books)
        except:
            pass

    return jsonify({
        "books": books,
        "total": total,
        "page":  page,
        "pages": max(1, (total+limit-1)//limit),
    })

@app.route("/api/viewer/<int:book_id>")
def viewer_url(book_id):
    """Return the HebrewBooks viewer URL for embedding."""
    return jsonify({
        "url": f"https://hebrewbooks.org/pdfpager.aspx?req={book_id}",
        "download": f"https://hebrewbooks.org/pdfs/{book_id}.pdf",
    })

# ─── INIT ────────────────────────────────────────────────────────
init_db()
t = threading.Thread(target=background_crawler, daemon=True)
t.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"🚀 http://localhost:{port}")
    app.run(debug=False, host="0.0.0.0", port=port)
