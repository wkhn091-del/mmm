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
                            save_text(sid, text, "sefaria")
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
                        save_text(uid, tr.text[:50000], "benyehuda")
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
                        save_text(bid, heb_text[:100000], "mamre")
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
                                        save_text(frow[0], heb[:80000], "wikisource")
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

# ── שלב 1: שיפור תמונה ──────────────────────────────
def enhance_image(img):
    """
    פונקציה חכמה לשיפור תמונה לפני OCR.
    מטפלת ב: מטושטש, כהה, נטוי, רעש, ניגוד נמוך.
    """
    import numpy as np
    from PIL import ImageFilter, ImageEnhance, ImageOps

    # המר ל-grayscale
    img = img.convert("L")
    arr = np.array(img)

    # ── זיהוי בעיות אוטומטי ──
    mean_brightness = arr.mean()
    std_contrast    = arr.std()
    is_dark    = mean_brightness < 100
    is_bright  = mean_brightness > 200
    is_low_contrast = std_contrast < 40
    is_blurry  = _measure_blur(arr) < 50

    print(f"    📊 brightness={mean_brightness:.0f} contrast={std_contrast:.0f} "
          f"blur={'yes' if is_blurry else 'no'} dark={'yes' if is_dark else 'no'}")

    # ── תיקון הטיה (deskew) ──
    img = _deskew(img)
    arr = np.array(img)

    # ── נורמליזציה של בהירות ──
    if is_dark or is_bright:
        img = ImageOps.autocontrast(img, cutoff=2)

    # ── שיפור ניגוד ──
    if is_low_contrast:
        # CLAHE-like: חלק לאזורים ושפר כל אחד
        img = _local_contrast(img)
    else:
        enhancer = ImageEnhance.Contrast(img)
        img = enhancer.enhance(1.8)

    # ── הסרת רעש ──
    img = img.filter(ImageFilter.MedianFilter(size=3))

    # ── חידוד לOCR ──
    if is_blurry:
        # חידוד חזק לדפים מטושטשים
        img = img.filter(ImageFilter.UnsharpMask(radius=2, percent=180, threshold=2))
        img = img.filter(ImageFilter.SHARPEN)
        img = img.filter(ImageFilter.SHARPEN)
    else:
        img = img.filter(ImageFilter.SHARPEN)

    # ── Binarization (שחור-לבן נקי) ──
    # Otsu threshold
    arr2 = np.array(img)
    threshold = _otsu_threshold(arr2)
    binary = (arr2 > threshold).astype(np.uint8) * 255
    from PIL import Image as PILImage
    img = PILImage.fromarray(binary)

    # Scale up for better OCR
    w, h = img.size
    img = img.resize((w*2, h*2), PILImage.LANCZOS)

    return img

def _measure_blur(arr):
    """Laplacian variance — מדד חדות."""
    import numpy as np
    kernel = np.array([[0,-1,0],[-1,4,-1],[0,-1,0]], dtype=np.float32)
    from scipy.signal import convolve2d
    try:
        conv = convolve2d(arr.astype(np.float32), kernel, mode='valid')
        return conv.var()
    except:
        return 100  # assume OK if scipy not available

def _otsu_threshold(arr):
    """חישוב סף אוטומטי לבינריזציה."""
    import numpy as np
    hist, bins = np.histogram(arr.flatten(), 256, [0,256])
    hist = hist.astype(float)
    total = hist.sum()
    best, best_thresh = 0, 128
    cumsum = cumvar = 0
    for t in range(256):
        cumsum += hist[t]
        if cumsum == 0: continue
        if cumsum == total: break
        cumvar += t * hist[t]
        mean_b = cumvar / cumsum
        mean_f = (cumvar + sum(i*hist[i] for i in range(t+1,256))) / (total - cumsum + 1e-9)
        w_b = cumsum / total
        w_f = 1 - w_b
        between = w_b * w_f * (mean_b - mean_f) ** 2
        if between > best:
            best = between
            best_thresh = t
    return best_thresh

def _deskew(img):
    """תיקון הטיה של עמוד."""
    try:
        import numpy as np
        from PIL import Image as PILImage
        arr = np.array(img)
        # Simple horizontal projection to detect skew
        angles = range(-5, 6)
        best_angle, best_score = 0, 0
        for angle in angles:
            rotated = img.rotate(angle, expand=False, fillcolor=255)
            arr_r = np.array(rotated)
            # Score = variance of row sums (text lines = high variance)
            row_sums = arr_r.sum(axis=1)
            score = row_sums.var()
            if score > best_score:
                best_score = score
                best_angle = angle
        if best_angle != 0:
            img = img.rotate(best_angle, expand=False, fillcolor=255)
            print(f"    📐 Deskewed: {best_angle}°")
    except:
        pass
    return img

def _local_contrast(img):
    """שיפור ניגוד מקומי לדפים בעייתיים."""
    try:
        import numpy as np
        from PIL import Image as PILImage
        arr = np.array(img, dtype=np.float32)
        h, w = arr.shape
        tile = 64  # tile size
        result = arr.copy()
        for y in range(0, h, tile):
            for x in range(0, w, tile):
                patch = arr[y:y+tile, x:x+tile]
                mn, mx = patch.min(), patch.max()
                if mx > mn:
                    result[y:y+tile, x:x+tile] = (patch - mn) / (mx - mn) * 255
        return PILImage.fromarray(result.astype(np.uint8))
    except:
        return img

# ── שלב 2: זיהוי סוג כתב ────────────────────────────
def detect_script_type(img):
    """
    מנסה לזהות אם הדף מכיל כתב רש"י או דפוס רגיל.
    רש"י = קווים עגולים ומסולסלים, דפוס = זוויתי.
    """
    try:
        import numpy as np
        arr = np.array(img.convert("L"))
        # בינריזציה פשוטה
        binary = arr < 128
        # מדידת עקמומיות — רש"י יש לו יותר עקמומיות
        from PIL import ImageFilter
        edges = img.filter(ImageFilter.FIND_EDGES)
        edge_arr = np.array(edges.convert("L"))
        curvature = edge_arr.mean()
        return "rashi" if curvature > 15 else "print"
    except:
        return "print"

# ── שלב 3א: Dicta OCR (לכתב רש"י ועברית ישנה) ────────
def ocr_with_dicta(img_bytes, title=""):
    """
    Dicta OCR API מהאוניברסיטה העברית.
    מיועד לעברית קלאסית, כתב רש"י, ניקוד עתיק.
    """
    try:
        import base64
        img_b64 = base64.b64encode(img_bytes).decode()

        r = requests.post(
            "https://ocr.dicta.org.il/api/ocr",
            json={
                "img": img_b64,
                "lang": "heb",
                "mode": "historical",   # מצב היסטורי — מזהה רש"י וניקוד
            },
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        if r.status_code == 200:
            data = r.json()
            text = data.get("text", "") or data.get("result", "")
            if text and len(text.strip()) > 20:
                print(f"    ✅ Dicta OCR success ({len(text)} chars)")
                return text
    except Exception as e:
        print(f"    ⚠️ Dicta error: {e}")
    return None

# ── שלב 3ב: Tesseract מתקדם ──────────────────────────
def ocr_with_tesseract(img, script_type="print"):
    """
    Tesseract עם הגדרות שונות לפי סוג הכתב.
    """
    try:
        import pytesseract

        configs = {
            "print": "--psm 6 --oem 3 -c tessedit_char_whitelist=אבגדהוזחטיכלמנסעפצקרשתךםןףץ ",
            "rashi": "--psm 6 --oem 3",
            "column":"--psm 4 --oem 3",  # עמודות (גמרא)
        }
        cfg = configs.get(script_type, configs["print"])
        text = pytesseract.image_to_string(img, lang="heb", config=cfg)

        # מדוד איכות — כמה % אותיות עבריות
        heb_chars = len(re.findall(r'[\u05D0-\u05EA]', text))
        total_chars = len(re.sub(r'\s', '', text))
        quality = heb_chars / max(total_chars, 1)

        print(f"    📊 Tesseract quality: {quality:.0%} ({heb_chars} heb chars)")
        return text, quality

    except ImportError:
        return "", 0
    except Exception as e:
        print(f"    ⚠️ Tesseract error: {e}")
        return "", 0

# ── שלב 4: Claude — תיקון שגיאות כבדות (50%+) ────────
def claude_fix_heavy(raw_text, title="", quality=1.0):
    """
    שולח לClaude טקסט עם שגיאות כבדות לתיקון מעמיק.
    משמש כשאיכות OCR < 60%.
    """
    if not ANTHROPIC_KEY or not raw_text:
        return raw_text

    try:
        sample = raw_text[:4000]

        # הוראות שונות לפי רמת השגיאות
        if quality < 0.3:
            system_prompt = (
                "אתה מומחה לשחזור כתבי יד ודפוסים עבריים עתיקים. "
                "קיבלת טקסט עם שגיאות OCR חמורות מאוד (מעל 70%). "
                "עליך: לזהות מילים מוכרות מתוך הרעש, לשחזר ניסוחים הלכתיים/תלמודיים, "
                "להשלים ביטויים חסרים לפי הקשר. "
                "השתמש בידע שלך על לשון הקודש. "
                "סמן קטעים שלא הצלחת לשחזר ב[?]. "
                "החזר רק את הטקסט המשוחזר."
            )
        elif quality < 0.6:
            system_prompt = (
                "אתה עורך מומחה של טקסטים יהודיים קלאסיים. "
                "קיבלת טקסט עם שגיאות OCR רבות (50-70%). "
                "תקן: אותיות הפוכות (ר↔ד, ו↔ז, ה↔ח), "
                "מילים שנשברו, קיצורים שבורים (ז ל → ז\"ל), "
                "עמודות שהתערבבו. שמור על לשון המקור. "
                "החזר רק את הטקסט המתוקן."
            )
        else:
            system_prompt = (
                "אתה עורך טקסטים יהודיים. תקן שגיאות OCR קלות, "
                "אחד מילים שנשברו, הוסף פסקאות. שמור לשון המקור. "
                "החזר רק את הטקסט המתוקן."
            )

        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 4096,
                "system": system_prompt,
                "messages": [{
                    "role": "user",
                    "content": f"ספר: {title}\nאיכות OCR: {quality:.0%}\n\nטקסט:\n{sample}"
                }]
            },
            timeout=40
        )

        if r.status_code == 200:
            improved = r.json()["content"][0]["text"]
            # Append un-improved rest
            if len(raw_text) > 4000:
                improved += "\n\n" + raw_text[4000:]
            print(f"    ✨ Claude fixed (quality was {quality:.0%})")
            return improved

    except Exception as e:
        print(f"    ⚠️ Claude fix error: {e}")

    return raw_text

# ── MAIN OCR RUNNER ──────────────────────────────────
def run_ocr(book_id, numeric_id):
    """
    Pipeline מלא:
    PDF → תמונות → שיפור תמונה → זיהוי כתב →
    Dicta (רש"י) / Tesseract (שאר) → Claude (אם צריך)
    """
    try:
        from pdf2image import convert_from_bytes
        from PIL import Image as PILImage
        import io

        print(f"🔍 OCR starting: {book_id}")

        # הורד PDF
        pdf_url = f"https://hebrewbooks.org/pdfs/{numeric_id}.pdf"
        r = requests.get(pdf_url, headers=HB_HEADERS, timeout=60, stream=True)
        if r.status_code != 200:
            print(f"  ❌ PDF not found: {pdf_url}")
            return False
        pdf_bytes = r.content
        if len(pdf_bytes) < 1000:
            return False

        # PDF → תמונות (300 DPI לאיכות טובה)
        try:
            images = convert_from_bytes(pdf_bytes, dpi=300, first_page=1, last_page=30)
        except Exception as e:
            print(f"  ❌ PDF convert error: {e}")
            return False

        print(f"  📄 {len(images)} pages")

        all_pages   = []
        total_quality = 0
        use_dicta   = False

        for page_num, img in enumerate(images):
            print(f"  📄 Page {page_num+1}/{len(images)}")

            # שלב 1: שיפור תמונה
            enhanced = enhance_image(img)

            # שלב 2: זיהוי סוג כתב (על דף ראשון בלבד)
            if page_num == 0:
                script_type = detect_script_type(enhanced)
                use_dicta   = (script_type == "rashi")
                print(f"  📝 Script type: {script_type}")

            # שלב 3: OCR
            page_text = None
            quality   = 0

            # נסה Dicta קודם לרש"י
            if use_dicta:
                img_bytes_io = io.BytesIO()
                enhanced.save(img_bytes_io, format="PNG")
                page_text = ocr_with_dicta(img_bytes_io.getvalue())
                if page_text:
                    quality = len(re.findall(r'[\u05D0-\u05EA]', page_text)) / max(len(page_text.replace(' ','')), 1)

            # Tesseract כ-fallback (או לדפוס רגיל)
            if not page_text or quality < 0.4:
                cfg = "column" if len(images) > 50 else ("rashi" if use_dicta else "print")
                ts_text, ts_quality = ocr_with_tesseract(enhanced, cfg)

                # קח את הטוב יותר
                if ts_quality > quality:
                    page_text = ts_text
                    quality   = ts_quality

            if page_text and page_text.strip():
                all_pages.append((page_text, quality))
                total_quality += quality

        if not all_pages:
            print(f"  ❌ No text extracted")
            return False

        # חשב איכות ממוצעת
        avg_quality = total_quality / len(all_pages)
        print(f"  📊 Average quality: {avg_quality:.0%}")

        # שרשר טקסט גולמי
        raw_text = "\n\n---\n\n".join(t for t, q in all_pages)

        # ניקוי בסיסי
        raw_text = re.sub(r'[^\u05D0-\u05EA\u05B0-\u05C7\s\.\,\:\;\!\?\"\'\(\)\-\n0-9]', '', raw_text)
        raw_text = re.sub(r'\n{4,}', '\n\n', raw_text)
        raw_text = re.sub(r' {2,}', ' ', raw_text)

        # שלב 4: Claude — אם איכות נמוכה
        final_text = raw_text
        book_quality_flag = "good"

        if avg_quality < 0.6 and ANTHROPIC_KEY:
            print(f"  ✨ Quality {avg_quality:.0%} — sending to Claude for heavy fix")
            final_text = claude_fix_heavy(raw_text, book_id, avg_quality)
            book_quality_flag = "claude_fixed"
        elif avg_quality < 0.8 and ANTHROPIC_KEY:
            print(f"  ✨ Quality {avg_quality:.0%} — sending to Claude for light fix")
            final_text = improve_text_with_claude(raw_text, book_id)
            book_quality_flag = "claude_improved"

        # שמור לDB
        save_text(book_id, final_text, f"ocr_{book_quality_flag}")
        conn = get_db()
        conn.execute(
            "UPDATE books SET has_ocr=1, ocr_improved=? WHERE id=?",
            (1 if book_quality_flag != "good" else 0, book_id)
        )
        conn.commit()
        conn.close()

        print(f"  ✅ OCR done: {book_id} | quality={avg_quality:.0%} | {len(final_text)} chars | {book_quality_flag}")
        return True

    except ImportError as e:
        print(f"  ❌ Missing library: {e}")
        return False
    except Exception as e:
        print(f"  ❌ OCR error {book_id}: {e}")
        return False

def ocr_crawler():
    time.sleep(40)
    print("🔍 OCR crawler started")
    while True:
        try:
            conn = get_db()
            rows = conn.execute(
                "SELECT id FROM books WHERE source='hebrewbooks' AND has_ocr=0 AND valid=1 LIMIT 3"
            ).fetchall()
            conn.close()
            if not rows:
                time.sleep(300)
                continue
            for row in rows:
                num = row["id"].replace("hb-","")
                if num.isdigit():
                    run_ocr(row["id"], int(num))
                time.sleep(5)
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
    return jsonify({
        "total":     total,
        "with_text": with_text,
        "improved":  improved,
        "by_source": {r["source"]: r["n"] for r in by_source},
        "hb_crawled": int(get_state("hb_last_id",1)),
        "subjects":  [{"name":r["subject"],"count":r["n"]} for r in subjects],
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
    tr   = conn.execute("SELECT content,improved FROM book_text WHERE book_id=?", (book_id,)).fetchone()
    conn.close()
    if not book: return jsonify({"error":"not found"}),404
    d = dict(book)
    if tr:
        # Prefer Claude-improved text if available
        d["text"] = tr["improved"] or tr["content"]
        d["has_improved"] = bool(tr["improved"])
    else:
        d["text"] = None
        d["has_improved"] = False
    return jsonify(d)

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
                        save_text(bid, heb[:80000], "chabad")
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
                        save_text(uid, heb[:50000], "opensiddur")

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
                        save_text(bid, heb[:100000], "alhatorah")
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
