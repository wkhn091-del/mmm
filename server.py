from flask import Flask, request, Response, jsonify, send_from_directory, redirect
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
import sqlite3
import threading
import re
import os
import time
import json

app = Flask(__name__, static_folder="static")
CORS(app)

DB_PATH = os.environ.get("DB_PATH", "books.db")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://hebrewbooks.org/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "he,en;q=0.9",
}

# ─── DATABASE SETUP ──────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id      INTEGER PRIMARY KEY,
            title   TEXT,
            author  TEXT,
            year    TEXT,
            subject TEXT,
            pages   INTEGER DEFAULT 0,
            indexed INTEGER DEFAULT 0
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_title  ON books(title)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_author ON books(author)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_subject ON books(subject)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crawler_state (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()

    # Seed with known good books
    seed = [
        (9780,  "שולחן ערוך - אורח חיים",      "ר' יוסף קארו",         "1565", "הלכה"),
        (9781,  "שולחן ערוך - יורה דעה",        "ר' יוסף קארו",         "1565", "הלכה"),
        (9782,  "שולחן ערוך - חושן משפט",       "ר' יוסף קארו",         "1565", "הלכה"),
        (9783,  "שולחן ערוך - אבן העזר",        "ר' יוסף קארו",         "1565", "הלכה"),
        (14763, "משנה תורה - רמב\"ם",            "הרמב\"ם",               "1180", "הלכה"),
        (3281,  "ספר החינוך",                    "ר' אהרן הלוי",          "1523", "מצוות"),
        (22879, "חפץ חיים",                      "ר' ישראל מאיר קגן",    "1873", "מוסר"),
        (43081, "נפש החיים",                     "ר' חיים מוולוז'ין",    "1824", "מחשבה"),
        (8774,  "מסילת ישרים",                  "הרמח\"ל",               "1740", "מוסר"),
        (4902,  "כוזרי",                         "ר' יהודה הלוי",         "1140", "פילוסופיה"),
        (14490, "עין יעקב",                      "ר' יעקב אבן חביב",     "1516", "אגדה"),
        (7643,  "פרי מגדים",                     "ר' יוסף בן מאיר תאומים","1787","הלכה"),
        (2865,  "ספר הזוהר",                     "רשב\"י",                "1280", "קבלה"),
        (11234, "תניא",                          "ר' שניאור זלמן מלאדי", "1797", "חסידות"),
        (5432,  "אור החיים",                     "ר' חיים בן עטר",        "1742", "פרשנות"),
        (6789,  "שפת אמת",                       "ר' יהודה אריה לייב",   "1905", "חסידות"),
        (3456,  "חתם סופר - שו\"ת",              "ר' משה סופר",           "1839", "שו\"ת"),
        (8901,  "אגרות משה",                     "ר' משה פיינשטיין",      "1959", "שו\"ת"),
        (2345,  "ציץ אליעזר",                    "ר' אליעזר יהודה וולדנברג","1945","שו\"ת"),
        (9012,  "יביע אומר",                     "ר' עובדיה יוסף",        "1954", "שו\"ת"),
        (1234,  "ברכי יוסף",                     "ר' חיים יוסף דוד אזולאי","1774","הלכה"),
        (4567,  "קצות החושן",                    "ר' אריה לייב הלר",      "1788", "הלכה"),
        (7890,  "נתיבות המשפט",                  "ר' יעקב לורברבוים",     "1809", "הלכה"),
        (2109,  "חיי אדם",                       "ר' אברהם דנציג",        "1810", "הלכה"),
        (5678,  "ערוך השולחן",                   "ר' יחיאל מיכל עפשטיין", "1903","הלכה"),
        (3210,  "בן איש חי",                     "ר' יוסף חיים",          "1898", "הלכה"),
        (6543,  "שמירת שבת כהלכתה",             "ר' יהושע נויבירט",      "1965", "שבת"),
        (9876,  "פניני הלכה",                    "ר' אליעזר מלמד",        "1990", "הלכה"),
        (1357,  "הלכות פסח",                     "שונות",                 "2000", "מועדים"),
        (2468,  "ספר תהילים",                    "דוד המלך",               "900",  "תנ\"ך"),
        (1111,  "תורה תמימה",                    "ר' ברוך הלוי עפשטיין",  "1902", "פרשנות"),
        (2222,  "משך חכמה",                      "ר' מאיר שמחה מדווינסק", "1927", "פרשנות"),
        (3333,  "שם משמואל",                     "ר' שמואל בורנשטיין",    "1926", "חסידות"),
        (4444,  "לקוטי מוהר\"ן",                 "ר' נחמן מברסלב",        "1808", "חסידות"),
        (5555,  "ספר יצירה",                     "עתיק",                  "200",  "קבלה"),
        (6666,  "עץ חיים",                       "ר' חיים ויטאל",         "1573", "קבלה"),
        (7777,  "תולדות יצחק",                   "ר' יצחק קארו",          "1518", "פרשנות"),
        (8888,  "ספר המצוות לרמב\"ם",             "הרמב\"ם",               "1170", "הלכה"),
        (9999,  "מורה נבוכים",                   "הרמב\"ם",               "1190", "פילוסופיה"),
        (1010,  "עקידת יצחק",                    "ר' יצחק עראמה",         "1522", "פרשנות"),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO books (id, title, author, year, subject) VALUES (?,?,?,?,?)",
        seed
    )
    conn.commit()
    conn.close()
    print(f"✅ DB initialized at {DB_PATH}")

# ─── BACKGROUND CRAWLER ──────────────────────────────────────────
CATEGORIES = list(range(1, 80))   # HebrewBooks has ~70 categories
CRAWL_DELAY = 1.5                  # seconds between requests (polite)

def get_crawler_state(key, default=None):
    conn = get_db()
    row = conn.execute("SELECT value FROM crawler_state WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default

def set_crawler_state(key, value):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO crawler_state (key,value) VALUES (?,?)", (key, str(value)))
    conn.commit()
    conn.close()

def book_count():
    conn = get_db()
    n = conn.execute("SELECT COUNT(*) FROM books").fetchone()[0]
    conn.close()
    return n

def crawl_category_page(cat_id, page):
    """Scrape one page of a category from HebrewBooks."""
    url = f"https://hebrewbooks.org/category.aspx?catid={cat_id}&page={page}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")

        books = []
        # HebrewBooks category page structure
        for item in soup.select(".bookitem, .book-item, tr"):
            link = item.find("a", href=re.compile(r"^/\d+$"))
            if not link:
                continue
            book_id = int(re.search(r"(\d+)", link["href"]).group(1))
            title = link.get_text(strip=True)
            if not title or len(title) < 2:
                continue

            # Try to get author from adjacent element
            author = ""
            tds = item.find_all("td")
            if len(tds) >= 2:
                author = tds[1].get_text(strip=True)

            books.append((book_id, title, author, "", ""))

        # Also try search-style results
        for row in soup.select("table tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            link = row.find("a", href=re.compile(r"^/\d+$"))
            if not link:
                continue
            try:
                book_id = int(re.search(r"(\d+)", link["href"]).group(1))
                title  = cells[0].get_text(strip=True)
                author = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                year   = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                if title and len(title) > 1:
                    books.append((book_id, title, author, year, ""))
            except:
                pass

        # Check if there's a next page
        has_next = bool(soup.find("a", string=re.compile(r"הבא|next|›|»", re.I)))

        return books, has_next
    except Exception as e:
        print(f"Crawl error cat={cat_id} p={page}: {e}")
        return [], False

def crawl_search_page(query, page):
    """Scrape HebrewBooks search results."""
    url = f"https://hebrewbooks.org/search?sdesc={requests.utils.quote(query)}&page={page}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        books = []
        for row in soup.select("table tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            link = row.find("a", href=re.compile(r"^/\d+$"))
            if not link:
                continue
            try:
                book_id = int(re.search(r"(\d+)", link["href"]).group(1))
                title  = cells[0].get_text(strip=True)
                author = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                year   = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                if title:
                    books.append((book_id, title, author, year, ""))
            except:
                pass
        has_next = bool(soup.find("a", string=re.compile(r"הבא|next|›", re.I)))
        return books, has_next
    except:
        return [], False

def save_books(books_list):
    if not books_list:
        return
    conn = get_db()
    conn.executemany(
        "INSERT OR IGNORE INTO books (id, title, author, year, subject) VALUES (?,?,?,?,?)",
        books_list
    )
    conn.commit()
    conn.close()

def background_crawler():
    """Slowly crawl HebrewBooks and index into SQLite."""
    time.sleep(5)  # wait for app to start
    print("🕷️ Crawler started")

    while True:
        try:
            # Phase 1: crawl categories
            for cat_id in CATEGORIES:
                state_key = f"cat_{cat_id}_done"
                if get_crawler_state(state_key) == "1":
                    continue

                page = int(get_crawler_state(f"cat_{cat_id}_page", 1))
                while True:
                    books, has_next = crawl_category_page(cat_id, page)
                    if books:
                        save_books(books)
                        total = book_count()
                        print(f"📚 Cat {cat_id} p{page}: +{len(books)} books (total: {total})")

                    set_crawler_state(f"cat_{cat_id}_page", page + 1)

                    if not has_next or not books:
                        set_crawler_state(state_key, "1")
                        break

                    page += 1
                    time.sleep(CRAWL_DELAY)

            # Phase 2: search common Hebrew letters to find more books
            search_terms = [
                "שולחן", "תשובות", "פירוש", "חידושים", "ספר", "הלכות",
                "תורה", "מדרש", "אגדה", "קבלה", "חסידות", "מוסר",
                "שבת", "יום טוב", "נשים", "קידושין", "גיטין",
                "אבות", "ברכות", "תפילה", "פסח", "סוכות",
                "רמב\"ם", "רש\"י", "תוספות", "ריטב\"א", "רמב\"ן",
            ]
            for term in search_terms:
                state_key = f"search_{term}_done"
                if get_crawler_state(state_key) == "1":
                    continue
                for page in range(1, 20):
                    books, has_next = crawl_search_page(term, page)
                    if books:
                        save_books(books)
                    if not has_next or not books:
                        break
                    time.sleep(CRAWL_DELAY)
                set_crawler_state(state_key, "1")

            print(f"✅ Crawl cycle done. Total books: {book_count()}")
            time.sleep(3600)  # wait 1 hour before re-crawling

        except Exception as e:
            print(f"Crawler error: {e}")
            time.sleep(60)

# ─── SERVE FRONTEND ──────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/static/<path:path>")
def static_files(path):
    return send_from_directory("static", path)

# ─── API: STATS ──────────────────────────────────────────────────
@app.route("/api/stats")
def stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM books").fetchone()[0]
    subjects = conn.execute(
        "SELECT subject, COUNT(*) as n FROM books WHERE subject!='' GROUP BY subject ORDER BY n DESC LIMIT 20"
    ).fetchall()
    conn.close()
    return jsonify({
        "total": total,
        "subjects": [{"name": r["subject"], "count": r["n"]} for r in subjects]
    })

# ─── API: SEARCH ─────────────────────────────────────────────────
@app.route("/api/search")
def search():
    q     = request.args.get("q", "").strip()
    page  = request.args.get("page", 1, type=int)
    subj  = request.args.get("subject", "")
    limit = 24
    offset = (page - 1) * limit

    conn = get_db()

    if q:
        # Search local DB first
        like = f"%{q}%"
        if subj:
            rows = conn.execute(
                "SELECT * FROM books WHERE (title LIKE ? OR author LIKE ?) AND subject=? LIMIT ? OFFSET ?",
                (like, like, subj, limit, offset)
            ).fetchall()
            total = conn.execute(
                "SELECT COUNT(*) FROM books WHERE (title LIKE ? OR author LIKE ?) AND subject=?",
                (like, like, subj)
            ).fetchone()[0]
        else:
            rows = conn.execute(
                "SELECT * FROM books WHERE title LIKE ? OR author LIKE ? LIMIT ? OFFSET ?",
                (like, like, limit, offset)
            ).fetchall()
            total = conn.execute(
                "SELECT COUNT(*) FROM books WHERE title LIKE ? OR author LIKE ?",
                (like, like)
            ).fetchone()[0]

        local_books = [dict(r) for r in rows]
        local_ids   = {b["id"] for b in local_books}

        # If not enough local results, also query HebrewBooks live
        live_books = []
        if len(local_books) < 8:
            live_raw, _ = crawl_search_page(q, 1)
            if live_raw:
                save_books(live_raw)
                for lb in live_raw:
                    if lb[0] not in local_ids:
                        live_books.append({
                            "id": lb[0], "title": lb[1],
                            "author": lb[2], "year": lb[3], "subject": lb[4]
                        })

        books = local_books + live_books
    else:
        # Browse mode
        if subj:
            rows = conn.execute(
                "SELECT * FROM books WHERE subject=? ORDER BY id LIMIT ? OFFSET ?",
                (subj, limit, offset)
            ).fetchall()
            total = conn.execute(
                "SELECT COUNT(*) FROM books WHERE subject=?", (subj,)
            ).fetchone()[0]
        else:
            rows = conn.execute(
                "SELECT * FROM books ORDER BY id LIMIT ? OFFSET ?",
                (limit, offset)
            ).fetchall()
            total = conn.execute("SELECT COUNT(*) FROM books").fetchone()[0]
        books = [dict(r) for r in rows]

    conn.close()
    return jsonify({
        "books":   books,
        "total":   total,
        "page":    page,
        "pages":   max(1, (total + limit - 1) // limit),
        "query":   q,
    })

# ─── API: FEATURED ───────────────────────────────────────────────
@app.route("/api/featured")
def featured():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM books WHERE id IN (9780,14763,3281,22879,43081,8774,4902,14490,11234,2865,5432,6789) ORDER BY id"
    ).fetchall()
    conn.close()
    return jsonify({"books": [dict(r) for r in rows]})

# ─── API: PDF PROXY ──────────────────────────────────────────────
def find_pdf_url(book_id):
    # Try direct PDF paths
    for fmt in [
        f"https://hebrewbooks.org/pdfs/{book_id}.pdf",
        f"https://hebrewbooks.org/pdfs/{book_id:05d}.pdf",
        f"https://hebrewbooks.org/pdfs/books/{book_id}.pdf",
    ]:
        try:
            h = requests.head(fmt, headers=HEADERS, timeout=6, allow_redirects=True)
            ct = h.headers.get("Content-Type", "")
            if "pdf" in ct.lower() or h.status_code == 200:
                return fmt
        except:
            pass

    # Scrape book page
    try:
        r = requests.get(f"https://hebrewbooks.org/{book_id}", headers=HEADERS, timeout=12)
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup.find_all(["a", "iframe", "embed"]):
            src = tag.get("href") or tag.get("src") or ""
            if ".pdf" in src.lower():
                return src if src.startswith("http") else "https://hebrewbooks.org" + src
        # Look in JS
        for s in soup.find_all("script"):
            if s.string:
                m = re.search(r'["\']([^"\']*\.pdf[^"\']*)["\']', s.string)
                if m:
                    u = m.group(1)
                    return u if u.startswith("http") else "https://hebrewbooks.org" + u
    except:
        pass
    return None

@app.route("/api/pdf/<int:book_id>")
def proxy_pdf(book_id):
    try:
        url = find_pdf_url(book_id)
        if not url:
            return redirect(f"https://hebrewbooks.org/pdfpager.aspx?req={book_id}")

        r = requests.get(url, headers=HEADERS, timeout=60, stream=True)

        def gen():
            for chunk in r.iter_content(16384):
                if chunk:
                    yield chunk

        hdrs = {
            "Content-Disposition": f'inline; filename="book_{book_id}.pdf"',
            "Access-Control-Allow-Origin": "*",
        }
        if "Content-Length" in r.headers:
            hdrs["Content-Length"] = r.headers["Content-Length"]

        return Response(gen(), content_type="application/pdf", headers=hdrs)
    except Exception as e:
        return redirect(f"https://hebrewbooks.org/pdfpager.aspx?req={book_id}")

# ─── INIT ─────────────────────────────────────────────────────────
init_db()
crawler_thread = threading.Thread(target=background_crawler, daemon=True)
crawler_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"🚀 Server on http://localhost:{port}")
    app.run(debug=False, host="0.0.0.0", port=port)
