"""
גנזך — ספרייה יהודית דיגיטלית מלאה
מקורות:
  1. HebrewBooks      — 60,000+ ספרים סרוקים
  2. Sefaria          — 3,000+ טקסטים דיגיטליים
  3. פרויקט בן-יהודה  — 10,000+ יצירות ספרות עברית
  4. ויקיטקסט         — 15,000+ דפים
  5. מכון ממרא        — תנ"ך + רמב"ם מלא
  6. Daat.ac.il       — אלפי טקסטים תורניים
  7. Al-Hatorah.org   — תנ"ך עם פירושים מלאים

Claude API — שיפור טקסט OCR אוטומטי
"""

from flask import Flask, request, Response, jsonify, send_from_directory, redirect
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
import sqlite3, threading, re, os, time, json

app = Flask(__name__, static_folder="static")
CORS(app)

DB_PATH      = os.environ.get("DB_PATH",      "books.db")
ANTHROPIC_KEY= os.environ.get("ANTHROPIC_API_KEY", "")

HB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer":    "https://hebrewbooks.org/",
    "Accept-Language": "he,en;q=0.9",
}
GEN_HEADERS = {
    "User-Agent": "Ganzach-Library/1.0 (educational Hebrew library project)",
    "Accept-Language": "he,en;q=0.9",
}

# ═══════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id          TEXT PRIMARY KEY,
            source      TEXT DEFAULT 'hebrewbooks',
            title       TEXT NOT NULL DEFAULT '',
            he_title    TEXT DEFAULT '',
            author      TEXT DEFAULT '',
            year        TEXT DEFAULT '',
            subject     TEXT DEFAULT '',
            language    TEXT DEFAULT 'he',
            has_text    INTEGER DEFAULT 0,
            has_ocr     INTEGER DEFAULT 0,
            ocr_improved INTEGER DEFAULT 0,
            valid       INTEGER DEFAULT 1,
            url         TEXT DEFAULT ''
        )
    """)
    # book_text מוגבל — שומר רק 5000 תווים ראשונים לחיסכון במקום
    conn.execute("""
        CREATE TABLE IF NOT EXISTS book_text (
            book_id  TEXT PRIMARY KEY,
            content  TEXT,
            improved TEXT,
            source   TEXT DEFAULT 'ocr'
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_title   ON books(title)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_subject ON books(subject)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_source  ON books(source)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_has_text ON books(has_text)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY, value TEXT
        )
    """)
    # אופטימיזציה למקום
    conn.execute("PRAGMA page_size=4096")
    conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
    conn.commit()
    _seed(conn)
    conn.close()
    print("✅ DB ready")

def _seed(conn):
    seed = [
        ("hb-9780",  "hebrewbooks","שולחן ערוך - אורח חיים",  "","ר' יוסף קארו",           "1565","הלכה",   "he",""),
        ("hb-9781",  "hebrewbooks","שולחן ערוך - יורה דעה",   "","ר' יוסף קארו",           "1565","הלכה",   "he",""),
        ("hb-9782",  "hebrewbooks","שולחן ערוך - חושן משפט",  "","ר' יוסף קארו",           "1565","הלכה",   "he",""),
        ("hb-9783",  "hebrewbooks","שולחן ערוך - אבן העזר",   "","ר' יוסף קארו",           "1565","הלכה",   "he",""),
        ("hb-14763", "hebrewbooks","משנה תורה",                "","הרמב\"ם",                 "1180","הלכה",   "he",""),
        ("hb-9999",  "hebrewbooks","מורה נבוכים",             "","הרמב\"ם",                 "1190","פילוסופיה","he",""),
        ("hb-3281",  "hebrewbooks","ספר החינוך",              "","ר' אהרן הלוי",            "1523","מצוות",  "he",""),
        ("hb-22879", "hebrewbooks","חפץ חיים",                "","ר' ישראל מאיר קגן",       "1873","מוסר",   "he",""),
        ("hb-43081", "hebrewbooks","נפש החיים",               "","ר' חיים מוולוז'ין",       "1824","מחשבה",  "he",""),
        ("hb-8774",  "hebrewbooks","מסילת ישרים",             "","הרמח\"ל",                 "1740","מוסר",   "he",""),
        ("hb-4902",  "hebrewbooks","ספר הכוזרי",              "","ר' יהודה הלוי",           "1140","פילוסופיה","he",""),
        ("hb-14490", "hebrewbooks","עין יעקב",                "","ר' יעקב אבן חביב",        "1516","אגדה",   "he",""),
        ("hb-11234", "hebrewbooks","תניא",                    "","ר' שניאור זלמן מלאדי",    "1797","חסידות", "he",""),
        ("hb-2865",  "hebrewbooks","ספר הזוהר",               "","רשב\"י",                  "1280","קבלה",   "he",""),
        ("hb-5432",  "hebrewbooks","אור החיים",               "","ר' חיים בן עטר",          "1742","פרשנות", "he",""),
        ("hb-6789",  "hebrewbooks","שפת אמת",                 "","ר' יהודה אריה לייב",      "1905","חסידות", "he",""),
        ("hb-3456",  "hebrewbooks","חתם סופר - שו\"ת",        "","ר' משה סופר",             "1839","שו\"ת",  "he",""),
        ("hb-8901",  "hebrewbooks","אגרות משה",               "","ר' משה פיינשטיין",        "1959","שו\"ת",  "he",""),
        ("hb-9012",  "hebrewbooks","יביע אומר",               "","ר' עובדיה יוסף",          "1954","שו\"ת",  "he",""),
        ("hb-5678",  "hebrewbooks","ערוך השולחן",             "","ר' יחיאל מיכל עפשטיין",  "1903","הלכה",   "he",""),
        ("hb-3210",  "hebrewbooks","בן איש חי",               "","ר' יוסף חיים",            "1898","הלכה",   "he",""),
        ("hb-1111",  "hebrewbooks","תורה תמימה",              "","ר' ברוך הלוי עפשטיין",   "1902","פרשנות", "he",""),
        ("hb-4444",  "hebrewbooks","ליקוטי מוהר\"ן",         "","ר' נחמן מברסלב",          "1808","חסידות", "he",""),
        ("hb-6666",  "hebrewbooks","עץ חיים",                 "","ר' חיים ויטאל",           "1573","קבלה",   "he",""),
        ("hb-2468",  "hebrewbooks","ספר תהילים",              "","דוד המלך",                 "900", "תנ\"ך",  "he",""),
        ("hb-1357",  "hebrewbooks","הגדה של פסח",             "","שונות",                   "1000","מועדים", "he",""),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO books(id,source,title,he_title,author,year,subject,language,url) VALUES(?,?,?,?,?,?,?,?,?)",
        seed
    )
    conn.commit()

# ═══════════════════════════════════════════════════
#  STATE
# ═══════════════════════════════════════════════════
def get_state(k, d=None):
    conn = get_db()
    r = conn.execute("SELECT value FROM state WHERE key=?", (k,)).fetchone()
    conn.close()
    return r["value"] if r else d

def set_state(k, v):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO state(key,value) VALUES(?,?)", (k, str(v)))
    conn.commit()
    conn.close()

def save_books(rows):
    if not rows: return
    conn = get_db()
    conn.executemany(
        "INSERT OR IGNORE INTO books(id,source,title,he_title,author,year,subject,language,has_text,url) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)", rows
    )
    conn.commit()
    conn.close()

def save_text(book_id, content, source="ocr"):
    """שומר טקסט מלא ב-DB — רק לתוצאות OCR (HebrewBooks).
    ספרים אחרים נמשכים חי מהמקור."""
    if not content: return
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO book_text(book_id,content,source) VALUES(?,?,?)",
        (book_id, content, source)
    )
    conn.execute("UPDATE books SET has_text=1 WHERE id=?", (book_id,))
    conn.commit()
    conn.close()

def total_books():
    conn = get_db()
    n = conn.execute("SELECT COUNT(*) FROM books WHERE valid=1").fetchone()[0]
    conn.close()
    return n

# ═══════════════════════════════════════════════════
#  CLAUDE API — שיפור טקסט OCR
# ═══════════════════════════════════════════════════
def improve_text_with_claude(raw_text, title=""):
    if not ANTHROPIC_KEY or not raw_text:
        return raw_text

    try:
        # Send first 3000 chars (to save tokens)
        sample = raw_text[:3000]

        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-haiku-4-5-20251001",
                "max_tokens": 4000,
                "system": (
                    "אתה עורך מומחה של טקסטים יהודיים קלאסיים. "
                    "קיבלת טקסט גולמי שיצא מ-OCR על ספר ישן. "
                    "תפקידך: לתקן שגיאות OCR, לאחד מילים שנשברו, "
                    "להוסיף פסקאות הגיוניות, לשמור על לשון המקור. "
                    "החזר רק את הטקסט המתוקן, ללא הסברים."
                ),
                "messages": [{"role": "user", "content": f"ספר: {title}\n\nטקסט לתיקון:\n{sample}"}]
            },
            timeout=30
        )

        if r.status_code == 200:
            data = r.json()
            improved = data["content"][0]["text"]
            # Append rest of text (not improved, to save tokens)
            if len(raw_text) > 3000:
                improved += "\n\n" + raw_text[3000:]
            print(f"✨ Claude improved: {title[:40]}")
            return improved
    except Exception as e:
        print(f"Claude improve error: {e}")

    return raw_text

def claude_improver():
    """Background thread: improve OCR texts with Claude."""
    time.sleep(60)
    print("✨ Claude improver started")

    while True:
        try:
            if not ANTHROPIC_KEY:
                time.sleep(300)
                continue

            conn = get_db()
            rows = conn.execute(
                "SELECT b.id, b.title, bt.content FROM books b "
                "JOIN book_text bt ON b.id=bt.book_id "
                "WHERE b.ocr_improved=0 AND bt.content IS NOT NULL "
                "AND LENGTH(bt.content) > 200 "
                "ORDER BY b.id LIMIT 3"
            ).fetchall()
            conn.close()

            if not rows:
                time.sleep(120)
                continue

            for row in rows:
                improved = improve_text_with_claude(row["content"], row["title"])
                conn = get_db()
                conn.execute(
                    "UPDATE book_text SET improved=? WHERE book_id=?",
                    (improved, row["id"])
                )
                conn.execute(
                    "UPDATE books SET ocr_improved=1 WHERE id=?", (row["id"],)
                )
                conn.commit()
                conn.close()
                time.sleep(5)  # rate limit

        except Exception as e:
            print(f"Claude improver error: {e}")
            time.sleep(60)

# ═══════════════════════════════════════════════════
#  SOURCE 1 — HebrewBooks Crawler
# ═══════════════════════════════════════════════════
def fetch_hb_meta(book_id):
    try:
        r = requests.get(f"https://hebrewbooks.org/{book_id}", headers=HB_HEADERS, timeout=12)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, "html.parser")

        title = ""
        og = soup.find("meta", property="og:title")
        if og: title = og.get("content","").strip()
        if not title:
            h1 = soup.find("h1")
            if h1: title = h1.get_text(strip=True)
        if not title:
            pt = soup.find("title")
            if pt: title = pt.get_text(strip=True).split("-")[0].strip()
        if not title or len(title) < 2: return None

        author, year, subject = "", "", ""
        h2 = soup.find("h2")
        if h2: author = h2.get_text(strip=True)
        for td in soup.find_all("td"):
            txt = td.get_text(strip=True)
            nxt = td.find_next_sibling("td")
            if not nxt: continue
            val = nxt.get_text(strip=True)
            if "מחבר" in txt: author = val
            elif "שנה" in txt: year = val
            elif "נושא" in txt or "קטגור" in txt: subject = val

        title = re.sub(r'\s+', ' ', title).strip()
        return (f"hb-{book_id}", "hebrewbooks", title, "", author, year, subject, "he", 0, "")
    except:
        return None

def hebrewbooks_crawler():
    time.sleep(5)
    print("🕷️  HebrewBooks crawler started")
    while True:
        try:
            cur = int(get_state("hb_last_id", 1))
            if cur >= 110000:
                time.sleep(86400)
                set_state("hb_last_id", 1)
                continue
            fetched = []
            for bid in range(cur, min(cur+4, 110000)):
                uid = f"hb-{bid}"
                conn = get_db()
                ex = conn.execute("SELECT id FROM books WHERE id=?", (uid,)).fetchone()
                conn.close()
                if ex: continue
                meta = fetch_hb_meta(bid)
                if meta: fetched.append(meta)
                time.sleep(0.2)
            if fetched:
                save_books(fetched)
                print(f"📚 HB {cur}: +{len(fetched)} | Total: {total_books()}")
            set_state("hb_last_id", cur+4)
            time.sleep(0.8)
        except Exception as e:
            print(f"HB error: {e}")
            time.sleep(30)

# ═══════════════════════════════════════════════════
#  SOURCE 2 — Sefaria
# ═══════════════════════════════════════════════════
SUBJ_MAP = {
    "Tanakh":"תנ\"ך","Mishnah":"משנה","Talmud":"תלמוד","Midrash":"מדרש",
    "Halakhah":"הלכה","Kabbalah":"קבלה","Liturgy":"תפילה",
    "Jewish Thought":"מחשבה","Tosefta":"תוספתא","Responsa":"שו\"ת",
    "Chasidut":"חסידות","Mussar":"מוסר","Philosophy":"פילוסופיה",
    "Tanaitic":"תנאים","Commentary":"פרשנות","Targum":"תרגום",
}

def sefaria_crawler():
    time.sleep(15)
    print("📖 Sefaria crawler started")
    while True:
        try:
            if get_state("sefaria_done") == "1":
                time.sleep(86400)
                continue

            r = requests.get("https://www.sefaria.org/api/index", headers=GEN_HEADERS, timeout=20)
            if r.status_code != 200: raise Exception(f"Status {r.status_code}")
            data = r.json()

            titles = []
            def walk(node):
                if isinstance(node, dict):
                    if "title" in node:
                        titles.append(node)
                    for v in node.values(): walk(v)
                elif isinstance(node, list):
                    for i in node: walk(i)
            walk(data)

            print(f"📖 Sefaria: {len(titles)} texts")

            for i, item in enumerate(titles):
                title    = item.get("title","")
                he_title = item.get("heTitle","")
                cat      = item.get("category","")
                if not title: continue

                sid = f"sef-{re.sub(r'[^a-zA-Z0-9_-]','_',title)[:60]}"
                conn = get_db()
                ex = conn.execute("SELECT id FROM books WHERE id=?", (sid,)).fetchone()
                conn.close()
                if ex: continue

                subj = SUBJ_MAP.get(cat, cat or "כללי")
                save_books([(sid,"sefaria",he_title or title,he_title,"","",subj,"he",0,
                             f"https://www.sefaria.org/{requests.utils.quote(title)}")])

                # Fetch text
                try:
                    tr = requests.get(
                        f"https://www.sefaria.org/api/texts/{requests.utils.quote(title)}?context=0&commentary=0",
                        headers=GEN_HEADERS, timeout=12
                    )
                    if tr.status_code == 200:
                        td = tr.json()
                        he = td.get("he", [])
                        def flat(x):
                            if isinstance(x, list): return "\n".join(flat(i) for i in x)
                            return str(x) if x else ""
                        text = flat(he)
                        if text.strip():
                            pass  # טקסט נמשך חי מהמקור בכל פתיחה
                except: pass

                if i % 100 == 0:
                    print(f"📖 Sefaria {i}/{len(titles)} | Total: {total_books()}")
                time.sleep(0.4)

            set_state("sefaria_done","1")
            print(f"✅ Sefaria done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"Sefaria error: {e}")
            time.sleep(120)

# ═══════════════════════════════════════════════════
#  SOURCE 3 — פרויקט בן-יהודה (10,000+ יצירות)
# ═══════════════════════════════════════════════════
def ben_yehuda_crawler():
    time.sleep(25)
    print("📚 Ben-Yehuda crawler started")
    while True:
        try:
            if get_state("benyehuda_done") == "1":
                time.sleep(86400)
                continue

            # Ben-Yehuda has a GitHub repo with all texts as JSON
            r = requests.get(
                "https://api.github.com/repos/projectbenyehuda/public_domain_dump/git/trees/master?recursive=1",
                headers=GEN_HEADERS, timeout=20
            )
            if r.status_code != 200: raise Exception(f"Ben-Yehuda status {r.status_code}")
            tree = r.json().get("tree", [])

            # Filter only .txt files
            txt_files = [f for f in tree if f.get("path","").endswith(".txt")]
            print(f"📚 Ben-Yehuda: {len(txt_files)} texts found")

            for i, f in enumerate(txt_files):
                path = f.get("path","")
                parts = path.replace(".txt","").split("/")

                # Parse path: category/author/title.txt
                if len(parts) >= 2:
                    author = parts[-2] if len(parts) >= 2 else ""
                    title  = parts[-1].replace("_"," ")
                    cat    = parts[0] if len(parts) >= 3 else "ספרות"
                else:
                    title = parts[-1].replace("_"," ")
                    author, cat = "", "ספרות"

                if not title: continue

                uid = f"by-{re.sub(r'[^a-zA-Z0-9_-]','_',path)[:70]}"
                conn = get_db()
                ex = conn.execute("SELECT id FROM books WHERE id=?", (uid,)).fetchone()
                conn.close()
                if ex: continue

                raw_url = f"https://raw.githubusercontent.com/projectbenyehuda/public_domain_dump/master/{requests.utils.quote(path)}"
                save_books([(uid,"benyehuda",title,"",author,"","ספרות עברית","he",1,raw_url)])

                # Fetch text content
                try:
                    tr = requests.get(raw_url, headers=GEN_HEADERS, timeout=12)
                    if tr.status_code == 200 and tr.text.strip():
                        pass  # טקסט נמשך חי
                        # save_text( # טקסט נמשך חי — uid, tr.text[:50000], "benyehuda")
                except: pass

                if i % 200 == 0:
                    print(f"📚 Ben-Yehuda {i}/{len(txt_files)} | Total: {total_books()}")
                time.sleep(0.15)

            set_state("benyehuda_done","1")
            print(f"✅ Ben-Yehuda done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"Ben-Yehuda error: {e}")
            time.sleep(120)

# ═══════════════════════════════════════════════════
#  SOURCE 4 — מכון ממרא (תנ"ך מלא + רמב"ם)
# ═══════════════════════════════════════════════════
MAMRE_BOOKS = [
    ("mamre-torah",    "תורה מלאה",                   "","תנ\"ך",  "https://www.mechon-mamre.org/p/pt/pt0.htm"),
    ("mamre-neviim",   "נביאים",                      "","תנ\"ך",  "https://www.mechon-mamre.org/p/pt/pt0.htm"),
    ("mamre-ketuvim",  "כתובים",                      "","תנ\"ך",  "https://www.mechon-mamre.org/p/pt/pt0.htm"),
    ("mamre-mt1",      "משנה תורה - ספר המדע",        "הרמב\"ם","הלכה","https://www.mechon-mamre.org/i/0100.htm"),
    ("mamre-mt2",      "משנה תורה - ספר אהבה",        "הרמב\"ם","הלכה","https://www.mechon-mamre.org/i/0200.htm"),
    ("mamre-mt3",      "משנה תורה - ספר זמנים",       "הרמב\"ם","הלכה","https://www.mechon-mamre.org/i/0300.htm"),
    ("mamre-mt4",      "משנה תורה - ספר נשים",        "הרמב\"ם","הלכה","https://www.mechon-mamre.org/i/0400.htm"),
    ("mamre-mt5",      "משנה תורה - ספר קדושה",       "הרמב\"ם","הלכה","https://www.mechon-mamre.org/i/0500.htm"),
    ("mamre-mt6",      "משנה תורה - ספר הפלאה",       "הרמב\"ם","הלכה","https://www.mechon-mamre.org/i/0600.htm"),
    ("mamre-mt7",      "משנה תורה - ספר זרעים",       "הרמב\"ם","הלכה","https://www.mechon-mamre.org/i/0700.htm"),
    ("mamre-mt8",      "משנה תורה - ספר עבודה",       "הרמב\"ם","הלכה","https://www.mechon-mamre.org/i/0800.htm"),
    ("mamre-mt9",      "משנה תורה - ספר קרבנות",      "הרמב\"ם","הלכה","https://www.mechon-mamre.org/i/0900.htm"),
    ("mamre-mt10",     "משנה תורה - ספר טהרה",        "הרמב\"ם","הלכה","https://www.mechon-mamre.org/i/1000.htm"),
    ("mamre-mt11",     "משנה תורה - ספר נזיקין",      "הרמב\"ם","הלכה","https://www.mechon-mamre.org/i/1100.htm"),
    ("mamre-mt12",     "משנה תורה - ספר קנין",        "הרמב\"ם","הלכה","https://www.mechon-mamre.org/i/1200.htm"),
    ("mamre-mt13",     "משנה תורה - ספר משפטים",      "הרמב\"ם","הלכה","https://www.mechon-mamre.org/i/1300.htm"),
    ("mamre-mt14",     "משנה תורה - ספר שופטים",      "הרמב\"ם","הלכה","https://www.mechon-mamre.org/i/1400.htm"),
    ("mamre-mishnah",  "משנה מלאה",                   "","משנה",  "https://www.mechon-mamre.org/b/h/h0.htm"),
]

def mamre_crawler():
    time.sleep(35)
    print("📜 Mamre crawler started")
    try:
        if get_state("mamre_done") == "1":
            return

        rows = [(b[0],"mamre",b[1],"",b[2],"",b[3],"he",0,b[4]) for b in MAMRE_BOOKS]
        save_books(rows)

        for book in MAMRE_BOOKS:
            bid, title, author, subj, url = book
            conn = get_db()
            ex = conn.execute("SELECT has_text FROM books WHERE id=?", (bid,)).fetchone()
            conn.close()
            if ex and ex["has_text"]: continue

            try:
                r = requests.get(url, headers=GEN_HEADERS, timeout=15)
                if r.status_code == 200:
                    r.encoding = 'utf-8'
                    soup = BeautifulSoup(r.text, "html.parser")
                    text = soup.get_text(separator="\n", strip=True)
                    heb_text = "\n".join(l for l in text.split("\n") if re.search(r'[\u05D0-\u05EA]', l))
                    if heb_text:
                        pass  # טקסט נמשך חי
                        # save_text( # טקסט נמשך חי — bid, heb_text[:100000], "mamre")
                        print(f"📜 Mamre saved: {title}")
            except Exception as e:
                print(f"Mamre error {title}: {e}")
            time.sleep(1)

        set_state("mamre_done","1")
        print(f"✅ Mamre done. Total: {total_books()}")
    except Exception as e:
        print(f"Mamre crawler error: {e}")

# ═══════════════════════════════════════════════════
#  SOURCE 5 — ויקיטקסט (15,000+ דפים)
# ═══════════════════════════════════════════════════
def wikisource_crawler():
    time.sleep(45)
    print("📰 Wikisource crawler started")
    while True:
        try:
            if get_state("wikisource_done") == "1":
                time.sleep(86400)
                continue

            # Wikisource Hebrew API — get all pages
            cats = [
                "ספרי_קודש","תנ\"ך","תלמוד","מדרש","הלכה",
                "ספרות_עברית","שירה_עברית","ספרי_מוסר","ספרי_חסידות",
                "קבלה","פילוסופיה_יהודית","ספרי_שאלות_ותשובות",
            ]

            for cat in cats:
                state_key = f"ws_cat_{cat}"
                if get_state(state_key) == "1": continue

                cont = ""
                while True:
                    params = {
                        "action":  "query",
                        "list":    "categorymembers",
                        "cmtitle": f"קטגוריה:{cat}",
                        "cmlimit": "500",
                        "format":  "json",
                        "cmprop":  "ids|title",
                    }
                    if cont: params["cmcontinue"] = cont

                    r = requests.get(
                        "https://he.wikisource.org/w/api.php",
                        params=params, headers=GEN_HEADERS, timeout=15
                    )
                    if r.status_code != 200: break
                    data = r.json()

                    pages = data.get("query",{}).get("categorymembers",[])
                    fetched = []
                    for pg in pages:
                        ptitle = pg.get("title","")
                        if not ptitle or ptitle.startswith("קטגוריה:"): continue

                        uid = f"ws-{pg['pageid']}"
                        conn = get_db()
                        ex = conn.execute("SELECT id FROM books WHERE id=?", (uid,)).fetchone()
                        conn.close()
                        if ex: continue

                        subj_map2 = {
                            "ספרי_קודש":"ספרות קודש","תנ\"ך":"תנ\"ך","תלמוד":"תלמוד",
                            "מדרש":"מדרש","הלכה":"הלכה","ספרות_עברית":"ספרות עברית",
                            "שירה_עברית":"שירה","ספרי_מוסר":"מוסר",
                            "ספרי_חסידות":"חסידות","קבלה":"קבלה",
                            "פילוסופיה_יהודית":"פילוסופיה",
                            "ספרי_שאלות_ותשובות":"שו\"ת",
                        }
                        subj = subj_map2.get(cat,"כללי")
                        ws_url = f"https://he.wikisource.org/wiki/{requests.utils.quote(ptitle)}"
                        fetched.append((uid,"wikisource",ptitle,"","","",subj,"he",0,ws_url))

                    if fetched:
                        save_books(fetched)
                        # Fetch text for first 50 in category to save time
                        for frow in fetched[:50]:
                            try:
                                tr = requests.get(
                                    "https://he.wikisource.org/w/api.php",
                                    params={"action":"parse","page":frow[2],"prop":"wikitext","format":"json"},
                                    headers=GEN_HEADERS, timeout=12
                                )
                                if tr.status_code == 200:
                                    td = tr.json()
                                    wikitext = td.get("parse",{}).get("wikitext",{}).get("*","")
                                    # Strip wiki markup
                                    clean = re.sub(r'\[\[([^\]|]+)(?:\|[^\]]+)?\]\]', r'\1', wikitext)
                                    clean = re.sub(r'\{\{[^}]+\}\}', '', clean)
                                    clean = re.sub(r"'{2,}", '', clean)
                                    clean = re.sub(r'==+([^=]+)==+', r'\n\1\n', clean)
                                    clean = re.sub(r'\[\[.*?\]\]', '', clean)
                                    heb = "\n".join(l for l in clean.split("\n") if re.search(r'[\u05D0-\u05EA]',l))
                                    if heb.strip():
                                        pass  # טקסט נמשך חי
                                        # save_text( # טקסט נמשך חי — frow[0], heb[:80000], "wikisource")
                            except: pass
                            time.sleep(0.3)

                        print(f"📰 Wikisource [{cat}]: +{len(fetched)} | Total: {total_books()}")

                    cont = data.get("continue",{}).get("cmcontinue","")
                    if not cont: break
                    time.sleep(0.5)

                set_state(state_key,"1")

            set_state("wikisource_done","1")
            print(f"✅ Wikisource done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"Wikisource error: {e}")
            time.sleep(120)

# ═══════════════════════════════════════════════════
#  SOURCE 6 — Daat.ac.il (אלפי טקסטים תורניים)
# ═══════════════════════════════════════════════════
DAAT_CATEGORIES = [
    ("תנ\"ך",     "https://www.daat.ac.il/daat/tanach/tanach.htm"),
    ("פרשנות",    "https://www.daat.ac.il/daat/tanach/rashi/0.htm"),
    ("הלכה",      "https://www.daat.ac.il/daat/halacha/halacha.htm"),
    ("מחשבה",     "https://www.daat.ac.il/daat/mahshevet/mahshevet.htm"),
    ("חסידות",    "https://www.daat.ac.il/daat/hasidut/hasidut.htm"),
    ("פילוסופיה", "https://www.daat.ac.il/daat/philosophia/philosophia.htm"),
    ("שו\"ת",     "https://www.daat.ac.il/daat/tshuvot/tshuvot.htm"),
]

def daat_crawler():
    time.sleep(55)
    print("🕌 Daat crawler started")
    while True:
        try:
            if get_state("daat_done") == "1":
                time.sleep(86400)
                continue

            for subj, url in DAAT_CATEGORIES:
                state_key = f"daat_{subj}"
                if get_state(state_key) == "1": continue
                try:
                    r = requests.get(url, headers=GEN_HEADERS, timeout=15)
                    if r.status_code != 200: continue
                    r.encoding = 'utf-8'
                    soup = BeautifulSoup(r.text, "html.parser")

                    fetched = []
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        text = a.get_text(strip=True)
                        if not text or len(text) < 2: continue
                        if not re.search(r'[\u05D0-\u05EA]', text): continue

                        full_url = href if href.startswith("http") else f"https://www.daat.ac.il{href}"
                        uid = f"daat-{abs(hash(full_url))}"

                        conn = get_db()
                        ex = conn.execute("SELECT id FROM books WHERE id=?", (uid,)).fetchone()
                        conn.close()
                        if ex: continue

                        fetched.append((uid,"daat",text,"","","",subj,"he",0,full_url))

                    if fetched:
                        save_books(fetched)
                        print(f"🕌 Daat [{subj}]: +{len(fetched)} | Total: {total_books()}")

                    set_state(state_key,"1")
                except Exception as e:
                    print(f"Daat [{subj}] error: {e}")
                time.sleep(2)

            set_state("daat_done","1")
            print(f"✅ Daat done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"Daat error: {e}")
            time.sleep(120)

# ═══════════════════════════════════════════════════
#  OCR PIPELINE — 4 שלבים
#
#  שלב 1: שיפור תמונה (blurry/dark/skewed)
#  שלב 2: זיהוי סוג כתב (רש"י / דפוס / מטושטש)
#  שלב 3: OCR (Dicta לרש"י, Tesseract לשאר)
#  שלב 4: שיפור Claude (תיקון שגיאות 50%+)
# ═══════════════════════════════════════════════════

# ═══════════════════════════════════════════════════
#  OCR — Claude Vision (ללא Tesseract!)
# ═══════════════════════════════════════════════════

def ocr_page_with_claude_b64(b64_data, media_type="image/jpeg"):
    if not ANTHROPIC_KEY:
        return None
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 2000,
                "system": (
                    "אתה מומחה לקריאת טקסטים עבריים עתיקים. "
                    "קרא את כל הטקסט העברי בתמונה בדיוק מרבי. "
                    "כלול ניקוד וכתב רשי. "
                    "אל תוסיף הסברים - רק הטקסט עצמו."
                ),
                "messages": [{"role": "user", "content": [
                    {"type": "image", "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": b64_data
                    }},
                    {"type": "text", "text": "קרא את הטקסט העברי:"}
                ]}]
            },
            timeout=45
        )
        if r.status_code == 200:
            text = r.json()["content"][0]["text"].strip()
            if len(re.findall(r'[\u05D0-\u05EA]', text)) > 10:
                return text
    except Exception as e:
        print(f"Claude Vision error: {e}")
    return None


def run_ocr(book_id, numeric_id):
    import base64
    if not ANTHROPIC_KEY:
        conn = get_db()
        conn.execute("UPDATE books SET has_ocr=1 WHERE id=?", (book_id,))
        conn.commit(); conn.close()
        return False

    print(f"🔍 Claude Vision OCR: {book_id}")
    all_pages = []

    for page_num in range(1, 21):
        img_url = f"https://hebrewbooks.org/pagefinder.aspx?req={numeric_id}&pgnum={page_num}&zoom=0"
        try:
            r = requests.get(img_url, headers=HB_HEADERS, timeout=20)
            if r.status_code != 200:
                break
            ct = r.headers.get("Content-Type", "")
            if "image" not in ct:
                if page_num > 2:
                    break
                continue

            b64 = base64.b64encode(r.content).decode()
            media_type = ct.split(";")[0].strip()
            if not media_type.startswith("image/"):
                media_type = "image/jpeg"

            page_text = ocr_page_with_claude_b64(b64, media_type)
            if page_text and len(page_text.strip()) > 20:
                all_pages.append(page_text.strip())
                print(f"  ✅ Page {page_num}: {len(page_text)} chars")
            else:
                if page_num > 3 and not all_pages:
                    break
        except Exception as e:
            print(f"  Page {page_num} error: {e}")
            if page_num > 2:
                break
        time.sleep(1)

    if not all_pages:
        conn = get_db()
        conn.execute("UPDATE books SET has_ocr=1 WHERE id=?", (book_id,))
        conn.commit(); conn.close()
        return False

    full_text = "\n\n".join(all_pages)
    improved = improve_text_with_claude(full_text, book_id)
    save_text(book_id, full_text, "claude_vision")

    conn = get_db()
    conn.execute(
        "UPDATE books SET has_ocr=1, has_text=1, ocr_improved=? WHERE id=?",
        (1 if improved != full_text else 0, book_id)
    )
    if improved != full_text:
        conn.execute("UPDATE book_text SET improved=? WHERE book_id=?", (improved, book_id))
    conn.commit(); conn.close()
    print(f"  ✅ Done: {book_id} | {len(full_text)} chars | {len(all_pages)} pages")
    return True


def ocr_crawler():
    time.sleep(40)
    print("🔍 OCR crawler (Claude Vision)")
    while True:
        try:
            if not ANTHROPIC_KEY:
                time.sleep(300)
                continue
            conn = get_db()
            rows = conn.execute(
                "SELECT id FROM books WHERE source='hebrewbooks' AND has_ocr=0 AND valid=1 LIMIT 2"
            ).fetchall()
            conn.close()
            if not rows:
                time.sleep(300)
                continue
            for row in rows:
                num = row["id"].replace("hb-","")
                if num.isdigit():
                    run_ocr(row["id"], int(num))
                time.sleep(8)
        except Exception as e:
            print(f"OCR crawler error: {e}")
            time.sleep(60)


# ═══════════════════════════════════════════════════
#  API
# ═══════════════════════════════════════════════════
@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/api/stats")
def stats():
    conn = get_db()
    total     = conn.execute("SELECT COUNT(*) FROM books WHERE valid=1").fetchone()[0]
    by_source = conn.execute(
        "SELECT source, COUNT(*) n FROM books WHERE valid=1 GROUP BY source"
    ).fetchall()
    with_text = conn.execute("SELECT COUNT(*) FROM books WHERE has_text=1 AND valid=1").fetchone()[0]
    improved  = conn.execute("SELECT COUNT(*) FROM books WHERE ocr_improved=1").fetchone()[0]
    subjects  = conn.execute(
        "SELECT subject, COUNT(*) n FROM books WHERE subject!='' AND valid=1 "
        "GROUP BY subject ORDER BY n DESC LIMIT 20"
    ).fetchall()
    conn.close()
    # גודל DB
    import os
    db_size_mb = round(os.path.getsize(DB_PATH) / 1024 / 1024, 1) if os.path.exists(DB_PATH) else 0

    return jsonify({
        "total":     total,
        "with_text": with_text,
        "improved":  improved,
        "by_source": {r["source"]: r["n"] for r in by_source},
        "hb_crawled": int(get_state("hb_last_id",1)),
        "subjects":  [{"name":r["subject"],"count":r["n"]} for r in subjects],
        "db_size_mb": db_size_mb,
    })

@app.route("/api/featured")
def featured():
    ids = ("'hb-9780','hb-14763','hb-3281','hb-22879','hb-43081','hb-8774',"
           "'hb-4902','hb-14490','hb-11234','hb-2865','hb-5432','hb-6789',"
           "'hb-3456','hb-9012','hb-5678','hb-3210','hb-1111','hb-4444',"
           "'hb-6666','hb-2468'")
    conn = get_db()
    rows = conn.execute(f"SELECT * FROM books WHERE id IN ({ids}) AND valid=1").fetchall()
    conn.close()
    return jsonify({"books": [dict(r) for r in rows]})

@app.route("/api/search")
def search():
    q        = request.args.get("q","").strip()
    page     = request.args.get("page",1,type=int)
    subj     = request.args.get("subject","")
    source   = request.args.get("source","")
    has_text = request.args.get("has_text","")
    limit    = 30
    offset   = (page-1)*limit

    conn = get_db()
    where, params = ["valid=1"], []

    if q:
        where.append("(title LIKE ? OR he_title LIKE ? OR author LIKE ?)")
        params += [f"%{q}%"]*3
    if subj:
        where.append("subject=?"); params.append(subj)
    if source:
        where.append("source=?"); params.append(source)
    if has_text=="1":
        where.append("has_text=1")

    wstr  = " AND ".join(where)
    total = conn.execute(f"SELECT COUNT(*) FROM books WHERE {wstr}", params).fetchone()[0]
    rows  = conn.execute(
        f"SELECT * FROM books WHERE {wstr} ORDER BY has_text DESC,id LIMIT ? OFFSET ?",
        params+[limit,offset]
    ).fetchall()
    conn.close()

    books = [dict(r) for r in rows]

    # Live HebrewBooks search if needed
    if q and source!="sefaria" and len(books)<10:
        try:
            r2 = requests.get(f"https://hebrewbooks.org/search?sdesc={requests.utils.quote(q)}&page=1",
                              headers=HB_HEADERS, timeout=10)
            soup = BeautifulSoup(r2.text,"html.parser")
            ex = {b["id"] for b in books}
            live = []
            for row2 in soup.select("table tr"):
                cells=row2.find_all("td")
                link=row2.find("a",href=re.compile(r"^/\d+$"))
                if not link or len(cells)<2: continue
                try:
                    bid=int(re.search(r"(\d+)",link["href"]).group(1))
                    uid=f"hb-{bid}"
                    if uid in ex: continue
                    t=cells[0].get_text(strip=True)
                    a=cells[1].get_text(strip=True) if len(cells)>1 else ""
                    y=cells[2].get_text(strip=True) if len(cells)>2 else ""
                    if t:
                        live.append({"id":uid,"source":"hebrewbooks","title":t,"he_title":"",
                                     "author":a,"year":y,"subject":"","language":"he",
                                     "has_text":0,"has_ocr":0,"ocr_improved":0,"valid":1,"url":""})
                        conn2=get_db()
                        conn2.execute("INSERT OR IGNORE INTO books(id,source,title,author,year) VALUES(?,?,?,?,?)",
                                      (uid,"hebrewbooks",t,a,y))
                        conn2.commit(); conn2.close()
                except: pass
            books+=live
            if not total: total=len(books)
        except: pass

    return jsonify({"books":books,"total":total,"page":page,"pages":max(1,(total+limit-1)//limit)})

@app.route("/api/book/<path:book_id>")
def book_detail(book_id):
    conn = get_db()
    book = conn.execute("SELECT * FROM books WHERE id=?", (book_id,)).fetchone()
    conn.close()
    if not book: return jsonify({"error":"not found"}), 404
    d = dict(book)

    # ── נסה למשוך טקסט מלא חי מהמקור ──
    text = None
    has_improved = False

    if book_id.startswith("sef-"):
        # Sefaria — טקסט מלא, חינמי, ללא הגבלה
        text = fetch_sefaria_text_live(book_id)

    elif book_id.startswith("ws-"):
        # ויקיטקסט
        text = fetch_wikisource_text_live(book["title"])

    elif book_id.startswith("by-"):
        # פרויקט בן-יהודה
        if book["url"]:
            text = fetch_url_text_live(book["url"])

    elif book_id.startswith("mamre-"):
        if book["url"]:
            text = fetch_url_text_live(book["url"])

    elif book_id.startswith("daat-") or book_id.startswith("chabad-") or book_id.startswith("aht-"):
        if book["url"]:
            text = fetch_url_text_live(book["url"])

    # HebrewBooks — אין טקסט, רק PDF
    # בדוק DB כ-fallback (מה שנשמר מOCR)
    if not text:
        conn = get_db()
        tr = conn.execute("SELECT content,improved FROM book_text WHERE book_id=?", (book_id,)).fetchone()
        conn.close()
        if tr:
            text = tr["improved"] or tr["content"]
            has_improved = bool(tr and tr["improved"])

    d["text"] = text
    d["has_improved"] = has_improved
    return jsonify(d)


def fetch_sefaria_text_live(book_id):
    """משך טקסט מלא מSefaria API — ללא הגבלת גודל."""
    try:
        title = book_id.replace("sef-","").replace("_"," ")
        r = requests.get(
            f"https://www.sefaria.org/api/texts/{requests.utils.quote(title)}?context=0&commentary=0&pad=0",
            headers=GEN_HEADERS, timeout=20
        )
        if r.status_code != 200: return None
        d = r.json()
        he = d.get("he", [])
        def flat(x):
            if isinstance(x, list): return "\n".join(flat(i) for i in x if i)
            return str(x) if x else ""
        text = flat(he)
        return text.strip() if len(text) > 50 else None
    except:
        return None


def fetch_wikisource_text_live(title):
    """משך טקסט מלא מויקיטקסט."""
    try:
        r = requests.get(
            "https://he.wikisource.org/w/api.php",
            params={"action":"parse","page":title,"prop":"wikitext","format":"json"},
            headers=GEN_HEADERS, timeout=15
        )
        if r.status_code != 200: return None
        wikitext = r.json().get("parse",{}).get("wikitext",{}).get("*","")
        # נקה wiki markup
        clean = re.sub(r'\[\[([^\]|]+)(?:\|[^\]]+)?\]\]', r'\1', wikitext)
        clean = re.sub(r'\{\{[^}]+\}\}', '', clean)
        clean = re.sub(r"'{2,}", '', clean)
        clean = re.sub(r'==+([^=]+)==+', r'\n\1\n', clean)
        heb = "\n".join(l for l in clean.split("\n") if re.search(r'[\u05D0-\u05EA]', l))
        return heb.strip() if len(heb) > 50 else None
    except:
        return None


def fetch_url_text_live(url):
    """משך טקסט מאתר כלשהו."""
    try:
        r = requests.get(url, headers=GEN_HEADERS, timeout=15)
        if r.status_code != 200: return None
        r.encoding = 'utf-8'
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script","style","nav","header","footer","aside"]): tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        heb = "\n".join(l for l in text.split("\n") if re.search(r'[\u05D0-\u05EA]{3,}', l))
        return heb.strip() if len(heb) > 50 else None
    except:
        return None

@app.route("/api/viewer/<path:book_id>")
def viewer_info(book_id):
    if book_id.startswith("hb-"):
        num = book_id.replace("hb-","")
        return jsonify({"type":"pdf","url":f"https://hebrewbooks.org/pdfpager.aspx?req={num}"})
    elif book_id.startswith("sef-"):
        title = book_id.replace("sef-","").replace("_"," ")
        return jsonify({"type":"sefaria","url":f"https://www.sefaria.org/{requests.utils.quote(title)}"})
    else:
        conn = get_db()
        b = conn.execute("SELECT url FROM books WHERE id=?", (book_id,)).fetchone()
        conn.close()
        url = b["url"] if b and b["url"] else "#"
        return jsonify({"type":"web","url":url})

@app.route("/api/ocr/<path:book_id>", methods=["POST"])
def trigger_ocr(book_id):
    if not book_id.startswith("hb-"): return jsonify({"error":"HB only"}),400
    num = book_id.replace("hb-","")
    if not num.isdigit(): return jsonify({"error":"invalid"}),400
    conn = get_db()
    row = conn.execute("SELECT has_ocr FROM books WHERE id=?", (book_id,)).fetchone()
    conn.close()
    if row and row["has_ocr"]: return jsonify({"status":"already_done"})
    threading.Thread(target=run_ocr, args=(book_id,int(num)), daemon=True).start()
    return jsonify({"status":"started"})

@app.route("/api/improve/<path:book_id>", methods=["POST"])
def trigger_improve(book_id):
    if not ANTHROPIC_KEY: return jsonify({"error":"no API key"}),400
    conn = get_db()
    row  = conn.execute("SELECT b.title, bt.content FROM books b JOIN book_text bt ON b.id=bt.book_id WHERE b.id=?", (book_id,)).fetchone()
    conn.close()
    if not row: return jsonify({"error":"no text"}),404

    def do_improve():
        improved = improve_text_with_claude(row["content"], row["title"])
        conn2 = get_db()
        conn2.execute("UPDATE book_text SET improved=? WHERE book_id=?", (improved, book_id))
        conn2.execute("UPDATE books SET ocr_improved=1 WHERE id=?", (book_id,))
        conn2.commit(); conn2.close()

    threading.Thread(target=do_improve, daemon=True).start()
    return jsonify({"status":"started"})


@app.route("/api/export-pdf/<path:book_id>")
def export_pdf(book_id):
    """
    מייצר PDF מעוצב יפה מהטקסט של הספר.
    כולל: כותרת, מחבר, פרקים, גופן עברי מקצועי.
    """
    try:
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.lib.pagesizes import A4
        from reportlab.platypus import (SimpleDocTemplate, Paragraph,
                                        Spacer, HRFlowable, PageBreak)
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.enums import TA_RIGHT, TA_CENTER
        from reportlab.lib.units import cm
        from reportlab.lib import colors
        import io

        # ── טען נתוני ספר ──
        conn = get_db()
        book = conn.execute("SELECT * FROM books WHERE id=?", (book_id,)).fetchone()
        tr   = conn.execute("SELECT content, improved FROM book_text WHERE book_id=?", (book_id,)).fetchone()
        conn.close()

        if not book:
            return jsonify({"error": "ספר לא נמצא"}), 404
        if not tr:
            return jsonify({"error": "אין טקסט לספר זה. הפעל OCR קודם."}), 404

        title  = book["title"]  or book["he_title"] or f"ספר #{book_id}"
        author = book["author"] or ""
        year   = book["year"]   or ""
        text   = tr["improved"] or tr["content"] or ""

        # ── רשום גופן עברי ──
        FONT_PATH      = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
        FONT_PATH_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        pdfmetrics.registerFont(TTFont("Heb",     FONT_PATH))
        pdfmetrics.registerFont(TTFont("HebBold", FONT_PATH_BOLD))

        # ── סגנונות ──
        BLUE  = colors.HexColor("#2563eb")
        DARK  = colors.HexColor("#111827")
        GRAY  = colors.HexColor("#6b7280")
        LIGHT = colors.HexColor("#eff6ff")

        s_title = ParagraphStyle("s_title",
            fontName="HebBold", fontSize=26, alignment=TA_CENTER,
            spaceAfter=8, leading=36, textColor=DARK, wordWrap="RTL")
        s_author = ParagraphStyle("s_author",
            fontName="Heb", fontSize=14, alignment=TA_CENTER,
            spaceAfter=4, leading=22, textColor=GRAY, wordWrap="RTL")
        s_year = ParagraphStyle("s_year",
            fontName="Heb", fontSize=11, alignment=TA_CENTER,
            spaceAfter=24, leading=18, textColor=GRAY, wordWrap="RTL")
        s_heading = ParagraphStyle("s_heading",
            fontName="HebBold", fontSize=15, alignment=TA_RIGHT,
            spaceAfter=8, spaceBefore=20, leading=24,
            textColor=BLUE, wordWrap="RTL")
        s_sub = ParagraphStyle("s_sub",
            fontName="HebBold", fontSize=13, alignment=TA_RIGHT,
            spaceAfter=6, spaceBefore=12, leading=20,
            textColor=DARK, wordWrap="RTL")
        s_body = ParagraphStyle("s_body",
            fontName="Heb", fontSize=12, alignment=TA_RIGHT,
            spaceAfter=6, leading=22, textColor=DARK, wordWrap="RTL")
        s_sep = ParagraphStyle("s_sep",
            fontName="Heb", fontSize=10, alignment=TA_CENTER,
            textColor=GRAY, spaceAfter=8, spaceBefore=8)

        # ── בנה PDF ──
        buf = io.BytesIO()
        doc = SimpleDocTemplate(
            buf, pagesize=A4,
            rightMargin=2.8*cm, leftMargin=2.8*cm,
            topMargin=2.5*cm,   bottomMargin=2.5*cm,
            title=title, author=author, subject="ספרייה יהודית — גנזך"
        )

        story = []

        # ── עמוד שער ──
        story.append(Spacer(1, 3*cm))
        story.append(Paragraph(title, s_title))
        story.append(HRFlowable(width="60%", thickness=2,
                                color=BLUE, hAlign="CENTER"))
        story.append(Spacer(1, 12))
        if author:
            story.append(Paragraph(author, s_author))
        if year:
            story.append(Paragraph(f"שנת {year}", s_year))
        story.append(Spacer(1, 0.5*cm))
        story.append(Paragraph("גנזך — ספרייה יהודית דיגיטלית", s_year))
        story.append(PageBreak())

        # ── גוף הספר ──
        paragraphs = re.split(r'\n{2,}|---', text)

        for para in paragraphs:
            lines = [l.strip() for l in para.strip().split('\n') if l.strip()]
            if not lines:
                story.append(Spacer(1, 6))
                continue

            for line in lines:
                if not line:
                    continue

                # זיהוי סוג שורה
                is_chapter  = re.match(r'^(פרק|חלק|ספר|שער|הלכות|סימן)\s', line)
                is_section  = re.match(r'^(סעיף|דין|שאלה|תשובה|אות)\s', line)
                is_sep      = re.match(r'^[✦•*—\-]{3,}', line)
                is_short    = len(line) < 40 and not line.endswith('.')

                if is_sep:
                    story.append(HRFlowable(width="40%", thickness=0.5,
                                           color=GRAY, hAlign="CENTER",
                                           spaceBefore=6, spaceAfter=6))
                elif is_chapter or (is_short and is_chapter):
                    story.append(Paragraph(line, s_heading))
                elif is_section:
                    story.append(Paragraph(line, s_sub))
                else:
                    # נקה תווים בעייתיים לreportlab
                    safe_line = (line
                        .replace('&', '&amp;')
                        .replace('<', '&lt;')
                        .replace('>', '&gt;'))
                    story.append(Paragraph(safe_line, s_body))

        doc.build(story)
        pdf_bytes = buf.getvalue()

        # שם קובץ בטוח
        safe_name = re.sub(r'[^\u05D0-\u05EAa-zA-Z0-9\s]', '', title)
        safe_name = safe_name.strip()[:60] or "book"

        return Response(
            pdf_bytes,
            content_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{safe_name}.pdf"',
                "Content-Length": len(pdf_bytes),
            }
        )

    except ImportError:
        return jsonify({"error": "reportlab not installed"}), 500
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ═══════════════════════════════════════════════════
#  SOURCE 7 — Chabad.org (ספרים חדשים + חסידות)
#  ספריה ענקית — תניא, רמב"ם יומי, היום יום,
#  ליקוטי שיחות, ספרי הרב שניאורסון ועוד
# ═══════════════════════════════════════════════════
CHABAD_BOOKS = [
    ("chabad-tanya",   "תניא",                        "ר' שניאור זלמן מלאדי", "חסידות", "https://www.chabad.org/library/tanya/tanya_cdo/aid/1026/jewish/Tanya.htm"),
    ("chabad-hayomyom","היום יום",                    "הרבי מלובביץ'",         "חסידות", "https://www.chabad.org/library/article_cdo/aid/9668/jewish/Hayom-Yom.htm"),
    ("chabad-rambam1", "משנה תורה — רמב\"ם יומי",     "הרמב\"ם",               "הלכה",   "https://www.chabad.org/library/article_cdo/aid/682956"),
    ("chabad-kitzur",  "קיצור שולחן ערוך",            "ר' שלמה גנצפריד",       "הלכה",   "https://www.chabad.org/library/article_cdo/aid/1108240"),
    ("chabad-avot",    "פרקי אבות עם פירוש",          "שונות",                 "מחשבה",  "https://www.chabad.org/library/article_cdo/aid/682956"),
    ("chabad-likkutei","ליקוטי תורה",                 "ר' שניאור זלמן מלאדי", "קבלה",   "https://www.chabad.org/library/article_cdo/aid/1408097"),
    ("chabad-emunah",  "ספר האמונה",                  "הרמב\"ן",               "פילוסופיה","https://www.chabad.org/library/article_cdo/aid/2488"),
]

def chabad_crawler():
    time.sleep(65)
    print("🕍 Chabad crawler started")
    while True:
        try:
            if get_state("chabad_done") == "1":
                time.sleep(86400)
                continue

            # Save metadata
            rows = [(b[0],"chabad",b[1],"",b[2],"",b[3],"he",0,b[4]) for b in CHABAD_BOOKS]
            save_books(rows)

            # Fetch text content
            for bid, title, author, subj, url in CHABAD_BOOKS:
                conn = get_db()
                ex = conn.execute("SELECT has_text FROM books WHERE id=?", (bid,)).fetchone()
                conn.close()
                if ex and ex["has_text"]: continue

                try:
                    r = requests.get(url, headers=GEN_HEADERS, timeout=15)
                    if r.status_code != 200: continue
                    r.encoding = 'utf-8'
                    soup = BeautifulSoup(r.text, "html.parser")

                    # Extract Hebrew text
                    for tag in soup(["script","style","nav","header","footer"]): tag.decompose()
                    text = soup.get_text(separator="\n", strip=True)
                    heb = "\n".join(l for l in text.split("\n")
                                   if re.search(r'[\u05D0-\u05EA]{3,}', l))
                    if len(heb) > 200:
                        pass  # טקסט נמשך חי
                        # save_text( # טקסט נמשך חי — bid, heb[:80000], "chabad")
                        print(f"🕍 Chabad: {title} ({len(heb)} chars)")
                except Exception as e:
                    print(f"Chabad {title} error: {e}")
                time.sleep(2)

            # Also crawl Chabad library index for more books
            try:
                r = requests.get(
                    "https://www.chabad.org/library/article_cdo/aid/63830/jewish/Jewish-Library.htm",
                    headers=GEN_HEADERS, timeout=15
                )
                if r.status_code == 200:
                    r.encoding = 'utf-8'
                    soup = BeautifulSoup(r.text, "html.parser")
                    count = 0
                    for a in soup.find_all("a", href=re.compile(r"/library/article_cdo")):
                        text = a.get_text(strip=True)
                        if not text or not re.search(r'[\u05D0-\u05EA]', text): continue
                        href = a["href"]
                        if not href.startswith("http"):
                            href = "https://www.chabad.org" + href
                        uid = f"chabad-{abs(hash(href))}"
                        conn = get_db()
                        ex = conn.execute("SELECT id FROM books WHERE id=?", (uid,)).fetchone()
                        conn.close()
                        if ex: continue
                        save_books([(uid,"chabad",text,"","","","חסידות","he",0,href)])
                        count += 1
                    print(f"🕍 Chabad library index: +{count} books")
            except Exception as e:
                print(f"Chabad index error: {e}")

            set_state("chabad_done","1")
            print(f"✅ Chabad done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"Chabad error: {e}")
            time.sleep(120)

# ═══════════════════════════════════════════════════
#  SOURCE 8 — NLI (הספרייה הלאומית של ישראל)
#  API רשמי עם מאות אלפי פריטים דיגיטליים
#  כולל ספרים מודרניים, כתבי יד, עיתונות
# ═══════════════════════════════════════════════════
def nli_crawler():
    time.sleep(75)
    print("🏛️ NLI crawler started")
    while True:
        try:
            if get_state("nli_done") == "1":
                time.sleep(86400)
                continue

            queries = [
                ("הלכה",      "halacha"),
                ("תורה",      "torah"),
                ("תלמוד",     "talmud"),
                ("קבלה",      "kabbalah"),
                ("חסידות",    "hasidut"),
                ("שירה עברית","hebrew poetry"),
                ("ספרות עברית","hebrew literature"),
                ("מוסר",      "musar"),
                ("פילוסופיה", "jewish philosophy"),
                ("פרשנות",    "biblical commentary"),
            ]

            for he_subj, en_query in queries:
                state_key = f"nli_{en_query}"
                if get_state(state_key) == "1": continue

                try:
                    # NLI API v2
                    r = requests.get(
                        "https://api.nli.org.il/opds/search",
                        params={
                            "query": en_query,
                            "lang":  "heb",
                            "pageSize": 100,
                        },
                        headers={**GEN_HEADERS, "Accept": "application/json"},
                        timeout=20
                    )

                    if r.status_code == 200:
                        try:
                            data = r.json()
                            entries = data.get("entries", data.get("items", []))
                            fetched = []
                            for item in entries:
                                title  = item.get("title","") or item.get("name","")
                                author = item.get("author","") or item.get("creator","")
                                year   = item.get("date","")   or item.get("year","")
                                iid    = item.get("id","")     or item.get("identifier","")
                                url    = item.get("url","")    or item.get("link","")

                                if not title: continue
                                uid = f"nli-{abs(hash(iid or title))}"
                                conn = get_db()
                                ex = conn.execute("SELECT id FROM books WHERE id=?", (uid,)).fetchone()
                                conn.close()
                                if ex: continue

                                fetched.append((uid,"nli",title,"",author,str(year),he_subj,"he",0,url))

                            if fetched:
                                save_books(fetched)
                                print(f"🏛️ NLI [{he_subj}]: +{len(fetched)} | Total: {total_books()}")
                        except:
                            pass

                    # Also try NLI digital collection via OAI-PMH
                    r2 = requests.get(
                        "https://api.nli.org.il/opds/catalog",
                        params={"subject": en_query, "language": "heb", "limit": 50},
                        headers=GEN_HEADERS, timeout=15
                    )
                    if r2.status_code == 200:
                        soup = BeautifulSoup(r2.text, "xml")
                        for entry in soup.find_all("entry"):
                            title = entry.find("title")
                            author= entry.find("author")
                            link  = entry.find("link", rel="alternate")
                            if not title: continue
                            t = title.get_text(strip=True)
                            a = author.get_text(strip=True) if author else ""
                            u = link.get("href","") if link else ""
                            uid = f"nli-{abs(hash(t+a))}"
                            conn = get_db()
                            ex = conn.execute("SELECT id FROM books WHERE id=?", (uid,)).fetchone()
                            conn.close()
                            if ex: continue
                            save_books([(uid,"nli",t,"",a,"",he_subj,"he",0,u)])

                    set_state(state_key,"1")
                except Exception as e:
                    print(f"NLI [{en_query}] error: {e}")
                time.sleep(3)

            set_state("nli_done","1")
            print(f"✅ NLI done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"NLI error: {e}")
            time.sleep(120)

# ═══════════════════════════════════════════════════
#  SOURCE 9 — Internet Archive — Hebrew Collection
#  ארכיב האינטרנט — אוסף עברי ענק
#  כולל ספרים מהמאה ה-20, עיתונים, כתבי יד
# ═══════════════════════════════════════════════════
def internet_archive_crawler():
    time.sleep(85)
    print("🌐 Internet Archive crawler started")
    while True:
        try:
            if get_state("ia_done") == "1":
                time.sleep(86400)
                continue

            searches = [
                ("subject:Hebrew AND mediatype:texts AND language:Hebrew",    "ספרות עברית"),
                ("subject:Talmud AND mediatype:texts",                        "תלמוד"),
                ("subject:Kabbalah AND mediatype:texts",                      "קבלה"),
                ("subject:Jewish law AND mediatype:texts",                    "הלכה"),
                ("subject:Hasidism AND mediatype:texts AND language:Hebrew",  "חסידות"),
                ("subject:Hebrew poetry AND mediatype:texts",                 "שירה"),
                ("creator:ישראל AND mediatype:texts AND language:Hebrew",     "ספרות כללי"),
                ("subject:responsa AND mediatype:texts",                      "שו\"ת"),
                ("subject:Midrash AND mediatype:texts",                       "מדרש"),
                ("subject:Mishnah AND mediatype:texts",                       "משנה"),
            ]

            for query, subj in searches:
                state_key = f"ia_{abs(hash(query))}"
                if get_state(state_key) == "1": continue

                try:
                    r = requests.get(
                        "https://archive.org/advancedsearch.php",
                        params={
                            "q":      query,
                            "fl[]":   ["identifier","title","creator","date","subject"],
                            "rows":   200,
                            "page":   1,
                            "output": "json",
                        },
                        headers=GEN_HEADERS, timeout=20
                    )
                    if r.status_code != 200: continue
                    data = r.json()
                    docs = data.get("response",{}).get("docs",[])

                    fetched = []
                    for doc in docs:
                        iid    = doc.get("identifier","")
                        title  = doc.get("title","")
                        author = doc.get("creator","")
                        year   = str(doc.get("date",""))[:4]
                        if not iid or not title: continue
                        if isinstance(author, list): author = author[0] if author else ""
                        if isinstance(title,  list): title  = title[0]  if title  else ""

                        uid = f"ia-{iid[:80]}"
                        conn = get_db()
                        ex = conn.execute("SELECT id FROM books WHERE id=?", (uid,)).fetchone()
                        conn.close()
                        if ex: continue

                        url = f"https://archive.org/details/{iid}"
                        fetched.append((uid,"archive",title,"",author,year,subj,"he",0,url))

                    if fetched:
                        save_books(fetched)
                        print(f"🌐 Archive [{subj}]: +{len(fetched)} | Total: {total_books()}")

                    set_state(state_key,"1")
                except Exception as e:
                    print(f"Archive [{subj}] error: {e}")
                time.sleep(3)

            set_state("ia_done","1")
            print(f"✅ Internet Archive done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"Archive error: {e}")
            time.sleep(120)

# ═══════════════════════════════════════════════════
#  SOURCE 10 — OpenSiddur (סידורים ותפילות)
#  פרויקט פתוח — מאות נוסחאות תפילה
#  אשכנז, ספרד, תימן, חסידי, מזרח ועוד
# ═══════════════════════════════════════════════════
def opensiddur_crawler():
    time.sleep(95)
    print("🙏 OpenSiddur crawler started")
    while True:
        try:
            if get_state("opensiddur_done") == "1":
                time.sleep(86400)
                continue

            r = requests.get(
                "https://opensiddur.org/wp-json/wp/v2/posts",
                params={"per_page": 100, "lang": "he", "categories": "siddur,tefillah"},
                headers=GEN_HEADERS, timeout=20
            )

            if r.status_code == 200:
                posts = r.json()
                fetched = []
                for post in posts:
                    title = post.get("title",{}).get("rendered","")
                    uid   = f"os-{post.get('id','')}"
                    url   = post.get("link","")

                    # Strip HTML from title
                    title = re.sub(r'<[^>]+>','',title).strip()
                    if not title: continue

                    conn = get_db()
                    ex = conn.execute("SELECT id FROM books WHERE id=?", (uid,)).fetchone()
                    conn.close()
                    if ex: continue

                    fetched.append((uid,"opensiddur",title,"","","","תפילה","he",0,url))

                    # Get text content
                    content_html = post.get("content",{}).get("rendered","")
                    content_text = re.sub(r'<[^>]+>','',content_html)
                    heb = "\n".join(l for l in content_text.split("\n")
                                   if re.search(r'[\u05D0-\u05EA]{3,}', l))
                    if len(heb) > 100:
                        pass  # טקסט נמשך חי
                        # save_text( # טקסט נמשך חי — uid, heb[:50000], "opensiddur")

                if fetched:
                    save_books(fetched)
                    print(f"🙏 OpenSiddur: +{len(fetched)} | Total: {total_books()}")

            set_state("opensiddur_done","1")
            print(f"✅ OpenSiddur done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"OpenSiddur error: {e}")
            time.sleep(120)

# ═══════════════════════════════════════════════════
#  SOURCE 11 — Al-Hatorah.org
#  תנ"ך עם כל הפירושים — רש"י, רמב"ן, אבן עזרא,
#  ספורנו, מלבי"ם, הרחב דבר ועוד
# ═══════════════════════════════════════════════════
ALHATORAH_BOOKS = [
    ("aht-bereshit",  "בראשית עם פירושים",  "שונות", "תנ\"ך",   "https://alhatorah.org/Commentary:Commentators_on_Genesis"),
    ("aht-shemot",    "שמות עם פירושים",    "שונות", "תנ\"ך",   "https://alhatorah.org/Commentary:Commentators_on_Exodus"),
    ("aht-vayikra",   "ויקרא עם פירושים",   "שונות", "תנ\"ך",   "https://alhatorah.org/Commentary:Commentators_on_Leviticus"),
    ("aht-bamidbar",  "במדבר עם פירושים",   "שונות", "תנ\"ך",   "https://alhatorah.org/Commentary:Commentators_on_Numbers"),
    ("aht-devarim",   "דברים עם פירושים",   "שונות", "תנ\"ך",   "https://alhatorah.org/Commentary:Commentators_on_Deuteronomy"),
    ("aht-rashi",     "פירוש רש\"י לתורה",  "רש\"י", "פרשנות",  "https://alhatorah.org/Commentator:Rashi"),
    ("aht-ramban",    "פירוש הרמב\"ן לתורה","הרמב\"ן","פרשנות", "https://alhatorah.org/Commentator:Ramban"),
    ("aht-ibnezra",   "פירוש אבן עזרא",     "ר' אברהם אבן עזרא","פרשנות","https://alhatorah.org/Commentator:Ibn_Ezra"),
    ("aht-sforno",    "פירוש ספורנו",        "ר' עובדיה ספורנו", "פרשנות","https://alhatorah.org/Commentator:Sforno"),
    ("aht-malbim",    "פירוש המלבי\"ם",     "המלבי\"ם","פרשנות","https://alhatorah.org/Commentator:Malbim"),
]

def alhatorah_crawler():
    time.sleep(105)
    print("📜 Al-Hatorah crawler started")
    while True:
        try:
            if get_state("alhatorah_done") == "1":
                time.sleep(86400)
                continue

            rows = [(b[0],"alhatorah",b[1],"",b[2],"",b[3],"he",0,b[4]) for b in ALHATORAH_BOOKS]
            save_books(rows)

            for bid, title, author, subj, url in ALHATORAH_BOOKS:
                conn = get_db()
                ex = conn.execute("SELECT has_text FROM books WHERE id=?", (bid,)).fetchone()
                conn.close()
                if ex and ex["has_text"]: continue

                try:
                    r = requests.get(url, headers=GEN_HEADERS, timeout=15)
                    if r.status_code != 200: continue
                    r.encoding = 'utf-8'
                    soup = BeautifulSoup(r.text, "html.parser")

                    for tag in soup(["script","style","nav","header","footer"]): tag.decompose()
                    text = soup.get_text(separator="\n", strip=True)
                    heb  = "\n".join(l for l in text.split("\n")
                                    if re.search(r'[\u05D0-\u05EA]{4,}', l))
                    if len(heb) > 300:
                        pass  # טקסט נמשך חי
                        # save_text( # טקסט נמשך חי — bid, heb[:100000], "alhatorah")
                        print(f"📜 Al-Hatorah: {title} ({len(heb)} chars)")
                except Exception as e:
                    print(f"AlHatorah {title}: {e}")
                time.sleep(2)

            set_state("alhatorah_done","1")
            print(f"✅ Al-Hatorah done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"Al-Hatorah error: {e}")
            time.sleep(120)



# ===================================================
#  FEATURES - פיצ'רים מתקדמים
# ===================================================

@app.route("/api/search-in-book/<path:book_id>")
def search_in_book(book_id):
    q = request.args.get("q","").strip()
    if not q: return jsonify({"results":[]})
    conn = get_db()
    tr = conn.execute("SELECT content,improved FROM book_text WHERE book_id=?", (book_id,)).fetchone()
    conn.close()
    if not tr: return jsonify({"results":[],"error":"אין טקסט"})
    text  = tr["improved"] or tr["content"] or ""
    lines = text.split("\n")
    results = []
    for i, line in enumerate(lines):
        if q in line:
            ctx_start = max(0, i-1)
            ctx_end   = min(len(lines), i+2)
            results.append({"line":i,"text":line,"context":"\n".join(lines[ctx_start:ctx_end])})
        if len(results) >= 50: break
    return jsonify({"results":results,"total":len(results),"query":q})


@app.route("/api/related/<path:book_id>")
def related_books(book_id):
    conn = get_db()
    book = conn.execute("SELECT subject, author FROM books WHERE id=?", (book_id,)).fetchone()
    if not book:
        conn.close()
        return jsonify({"books":[]})
    related = conn.execute(
        "SELECT * FROM books WHERE id!=? AND valid=1 AND "
        "(subject=? OR author=?) ORDER BY has_text DESC LIMIT 12",
        (book_id, book["subject"], book["author"])
    ).fetchall()
    conn.close()
    return jsonify({"books":[dict(r) for r in related]})


@app.route("/api/toc/<path:book_id>")
def table_of_contents(book_id):
    conn = get_db()
    tr = conn.execute("SELECT content,improved FROM book_text WHERE book_id=?", (book_id,)).fetchone()
    conn.close()
    if not tr: return jsonify({"toc":[]})
    text  = tr["improved"] or tr["content"] or ""
    toc   = []
    lines = text.split("\n")
    for i, line in enumerate(lines):
        line = line.strip()
        if not line: continue
        is_heading = (
            re.match(r'^(פרק|חלק|ספר|שער|הלכות|סימן|שאלה|תשובה)\s', line) or
            (len(line) < 50 and not line.endswith('.') and
             len(re.findall(r'[\u05D0-\u05EA]', line)) > 3)
        )
        if is_heading:
            toc.append({"line":i,"text":line,"level":1})
        if len(toc) >= 100: break
    return jsonify({"toc":toc,"total":len(toc)})


@app.route("/api/daily")
def daily_study():
    import random, datetime
    conn = get_db()
    books = conn.execute(
        "SELECT * FROM books WHERE has_text=1 AND valid=1 ORDER BY RANDOM() LIMIT 5"
    ).fetchall()
    conn.close()
    if not books: return jsonify({"error":"אין ספרים"})
    book = random.choice(books)
    conn = get_db()
    tr = conn.execute("SELECT content,improved FROM book_text WHERE book_id=?", (book["id"],)).fetchone()
    conn.close()
    text = ""
    if tr:
        full  = tr["improved"] or tr["content"] or ""
        lines = [l for l in full.split("\n") if len(l.strip()) > 20]
        if lines:
            start = random.randint(0, max(0, len(lines)-10))
            text  = "\n".join(lines[start:start+15])
    return jsonify({"book":dict(book),"excerpt":text,"date":datetime.date.today().isoformat()})


@app.route("/api/fulltext-search")
def fulltext_search():
    q     = request.args.get("q","").strip()
    page  = request.args.get("page",1,type=int)
    limit = 20
    if not q or len(q) < 2:
        return jsonify({"results":[],"total":0})
    conn  = get_db()
    rows  = conn.execute(
        "SELECT b.id,b.title,b.author,b.subject,bt.content,bt.improved "
        "FROM books b JOIN book_text bt ON b.id=bt.book_id "
        "WHERE (bt.content LIKE ? OR bt.improved LIKE ?) AND b.valid=1 "
        "LIMIT ? OFFSET ?",
        (f"%{q}%",f"%{q}%",limit,(page-1)*limit)
    ).fetchall()
    total = conn.execute(
        "SELECT COUNT(*) FROM books b JOIN book_text bt ON b.id=bt.book_id "
        "WHERE (bt.content LIKE ? OR bt.improved LIKE ?) AND b.valid=1",
        (f"%{q}%",f"%{q}%")
    ).fetchone()[0]
    conn.close()
    results = []
    for row in rows:
        text    = row["improved"] or row["content"] or ""
        idx     = text.find(q)
        if idx >= 0:
            s       = max(0,idx-100)
            e       = min(len(text),idx+len(q)+100)
            excerpt = ("..." if s>0 else "") + text[s:e] + ("..." if e<len(text) else "")
        else:
            excerpt = text[:200]
        results.append({"book_id":row["id"],"title":row["title"],
                        "author":row["author"],"subject":row["subject"],"excerpt":excerpt})
    return jsonify({"results":results,"total":total,"page":page,"pages":max(1,(total+limit-1)//limit)})


@app.route("/api/ai-explain", methods=["POST"])
def ai_explain():
    if not ANTHROPIC_KEY: return jsonify({"error":"נדרש מפתח API"}),400
    data    = request.get_json() or {}
    passage = data.get("text","").strip()
    book    = data.get("book","")
    if not passage: return jsonify({"error":"חסר טקסט"}),400
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={
                "model":"claude-haiku-4-5-20251001","max_tokens":1000,
                "system":"אתה רב ומלומד בתורה. הסבר את הקטע בעברית מודרנית ברורה. כלול פירוש מילים קשות, רקע היסטורי, וחשיבות הלכתית.",
                "messages":[{"role":"user","content":f"ספר: {book}\n\nקטע:\n{passage[:1000]}"}]
            },
            timeout=30
        )
        if r.status_code == 200:
            return jsonify({"explanation":r.json()["content"][0]["text"]})
    except Exception as e:
        return jsonify({"error":str(e)}),500
    return jsonify({"error":"שגיאה"}),500


@app.route("/api/ai-translate", methods=["POST"])
def ai_translate():
    if not ANTHROPIC_KEY: return jsonify({"error":"נדרש מפתח API"}),400
    data    = request.get_json() or {}
    passage = data.get("text","").strip()
    target  = data.get("target","modern_hebrew")
    if not passage: return jsonify({"error":"חסר טקסט"}),400
    prompts = {
        "modern_hebrew":"תרגם לעברית מודרנית פשוטה:",
        "english":"Translate to clear modern English:",
        "yiddish":"איבערזעץ אויף יידיש:",
    }
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={
                "model":"claude-haiku-4-5-20251001","max_tokens":1500,
                "messages":[{"role":"user","content":f"{prompts.get(target,prompts['modern_hebrew'])}\n\n{passage[:1500]}"}]
            },
            timeout=30
        )
        if r.status_code == 200:
            return jsonify({"translation":r.json()["content"][0]["text"],"target":target})
    except Exception as e:
        return jsonify({"error":str(e)}),500
    return jsonify({"error":"שגיאה"}),500


@app.route("/api/admin/cleanup", methods=["POST"])
def admin_cleanup():
    """מנקה טקסטים כפולים וארוכים מדי לחיסכון במקום."""
    conn = get_db()
    # מחק טקסטים ארוכים מדי
    conn.execute(
        "UPDATE book_text SET content=SUBSTR(content,1,5000) WHERE LENGTH(content)>5000"
    )
    conn.execute(
        "UPDATE book_text SET improved=SUBSTR(improved,1,5000) WHERE improved IS NOT NULL AND LENGTH(improved)>5000"
    )
    # הפעל VACUUM לדחיסת DB
    conn.execute("PRAGMA incremental_vacuum")
    conn.commit()
    conn.close()
    import subprocess
    try:
        subprocess.run(["sqlite3", DB_PATH, "VACUUM;"], timeout=30)
    except:
        pass
    import os
    size = round(os.path.getsize(DB_PATH)/1024/1024, 1) if os.path.exists(DB_PATH) else 0
    return jsonify({"status":"ok","db_size_mb":size})


init_db()

crawlers = [
    hebrewbooks_crawler,
    sefaria_crawler,
    ben_yehuda_crawler,
    mamre_crawler,
    wikisource_crawler,
    daat_crawler,
    chabad_crawler,
    nli_crawler,
    internet_archive_crawler,
    opensiddur_crawler,
    alhatorah_crawler,
    ocr_crawler,
    claude_improver,
]
for fn in crawlers:
    threading.Thread(target=fn, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"🚀 Ganzach on http://localhost:{port}")
    print(f"📚 Sources: HebrewBooks+Sefaria+BenYehuda+Mamre+Wikisource+Daat+Chabad+NLI+Archive+OpenSiddur+AlHatorah")
    app.run(debug=False, host="0.0.0.0", port=port)
