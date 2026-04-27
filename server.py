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

from flask import Flask, request, Response, jsonify, send_from_directory
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
import threading, re, os, time, json
from pymongo.errors import DuplicateKeyError
from .db_mongo import *

app = Flask(__name__, static_folder="static")
CORS(app)

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

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
#  CLAUDE API — שיפור טקסט OCR
# ═══════════════════════════════════════════════════
def improve_text_with_claude(raw_text, title=""):
    if not ANTHROPIC_KEY or not raw_text:
        return raw_text
    try:
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
            improved = r.json()["content"][0]["text"]
            if len(raw_text) > 3000:
                improved += "\n\n" + raw_text[3000:]
            print(f"✨ Claude improved: {title[:40]}")
            return improved
    except Exception as e:
        print(f"Claude improve error: {e}")
    return raw_text


def claude_improver():
    time.sleep(60)
    print("✨ Claude improver started")
    while True:
        try:
            if not ANTHROPIC_KEY:
                time.sleep(300)
                continue

            db = get_db()
            pipeline = [
                {"$match": {"ocr_improved": 0, "valid": 1}},
                {"$lookup": {"from": "book_text", "localField": "id", "foreignField": "book_id", "as": "bt"}},
                {"$unwind": "$bt"},
                {"$match": {"bt.content": {"$exists": True, "$ne": None}}},
                {"$project": {"id": 1, "title": 1, "content": "$bt.content", "_id": 0}},
                {"$limit": 3}
            ]
            rows = list(db["books"].aggregate(pipeline))

            if not rows:
                time.sleep(120)
                continue

            for row in rows:
                improved = improve_text_with_claude(row["content"], row.get("title", ""))
                db["book_text"].update_one(
                    {"book_id": row["id"]},
                    {"$set": {"improved": improved}},
                    upsert=True
                )
                db["books"].update_one({"id": row["id"]}, {"$set": {"ocr_improved": 1}})
                time.sleep(5)

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
        if og: title = og.get("content", "").strip()
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
            db = get_db()
            for bid in range(cur, min(cur + 4, 110000)):
                uid = f"hb-{bid}"
                if db["books"].find_one({"id": uid}, {"_id": 1}):
                    continue
                meta = fetch_hb_meta(bid)
                if meta: fetched.append(meta)
                time.sleep(0.2)
            if fetched:
                save_books(fetched)
                print(f"📚 HB {cur}: +{len(fetched)} | Total: {total_books()}")
            set_state("hb_last_id", cur + 4)
            time.sleep(0.8)
        except Exception as e:
            print(f"HB error: {e}")
            time.sleep(30)


# ═══════════════════════════════════════════════════
#  SOURCE 2 — Sefaria
# ═══════════════════════════════════════════════════
SUBJ_MAP = {
    "Tanakh": "תנ\"ך", "Mishnah": "משנה", "Talmud": "תלמוד", "Midrash": "מדרש",
    "Halakhah": "הלכה", "Kabbalah": "קבלה", "Liturgy": "תפילה",
    "Jewish Thought": "מחשבה", "Tosefta": "תוספתא", "Responsa": "שו\"ת",
    "Chasidut": "חסידות", "Mussar": "מוסר", "Philosophy": "פילוסופיה",
    "Tanaitic": "תנאים", "Commentary": "פרשנות", "Targum": "תרגום",
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
                    if "title" in node: titles.append(node)
                    for v in node.values(): walk(v)
                elif isinstance(node, list):
                    for i in node: walk(i)
            walk(data)

            print(f"📖 Sefaria: {len(titles)} texts")
            db = get_db()

            for i, item in enumerate(titles):
                title    = item.get("title", "")
                he_title = item.get("heTitle", "")
                cat      = item.get("category", "")
                if not title: continue

                sid = f"sef-{re.sub(r'[^a-zA-Z0-9_-]', '_', title)[:60]}"
                if db["books"].find_one({"id": sid}, {"_id": 1}):
                    continue

                subj = SUBJ_MAP.get(cat, cat or "כללי")
                save_books([(sid, "sefaria", he_title or title, he_title, "", "", subj, "he", 0,
                             f"https://www.sefaria.org/{requests.utils.quote(title)}")])

                if i % 100 == 0:
                    print(f"📖 Sefaria {i}/{len(titles)} | Total: {total_books()}")
                time.sleep(0.4)

            set_state("sefaria_done", "1")
            print(f"✅ Sefaria done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"Sefaria error: {e}")
            time.sleep(120)


# ═══════════════════════════════════════════════════
#  SOURCE 3 — פרויקט בן-יהודה
# ═══════════════════════════════════════════════════
def ben_yehuda_crawler():
    time.sleep(25)
    print("📚 Ben-Yehuda crawler started")
    while True:
        try:
            if get_state("benyehuda_done") == "1":
                time.sleep(86400)
                continue

            r = requests.get(
                "https://api.github.com/repos/projectbenyehuda/public_domain_dump/git/trees/master?recursive=1",
                headers=GEN_HEADERS, timeout=20
            )
            if r.status_code != 200: raise Exception(f"Ben-Yehuda status {r.status_code}")
            tree = r.json().get("tree", [])
            txt_files = [f for f in tree if f.get("path", "").endswith(".txt")]
            print(f"📚 Ben-Yehuda: {len(txt_files)} texts found")

            db = get_db()
            for i, f in enumerate(txt_files):
                path  = f.get("path", "")
                parts = path.replace(".txt", "").split("/")
                if len(parts) >= 2:
                    author = parts[-2] if len(parts) >= 2 else ""
                    title  = parts[-1].replace("_", " ")
                else:
                    title = parts[-1].replace("_", " ")
                    author = ""
                if not title: continue

                uid = f"by-{re.sub(r'[^a-zA-Z0-9_-]', '_', path)[:70]}"
                if db["books"].find_one({"id": uid}, {"_id": 1}):
                    continue

                raw_url = f"https://raw.githubusercontent.com/projectbenyehuda/public_domain_dump/master/{requests.utils.quote(path)}"
                save_books([(uid, "benyehuda", title, "", author, "", "ספרות עברית", "he", 1, raw_url)])

                if i % 200 == 0:
                    print(f"📚 Ben-Yehuda {i}/{len(txt_files)} | Total: {total_books()}")
                time.sleep(0.15)

            set_state("benyehuda_done", "1")
            print(f"✅ Ben-Yehuda done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"Ben-Yehuda error: {e}")
            time.sleep(120)


# ═══════════════════════════════════════════════════
#  SOURCE 4 — מכון ממרא
# ═══════════════════════════════════════════════════
MAMRE_BOOKS = [
    ("mamre-torah",   "תורה מלאה",                "", "תנ\"ך",  "https://www.mechon-mamre.org/p/pt/pt0.htm"),
    ("mamre-neviim",  "נביאים",                   "", "תנ\"ך",  "https://www.mechon-mamre.org/p/pt/pt0.htm"),
    ("mamre-ketuvim", "כתובים",                   "", "תנ\"ך",  "https://www.mechon-mamre.org/p/pt/pt0.htm"),
    ("mamre-mt1",     "משנה תורה - ספר המדע",     "הרמב\"ם", "הלכה", "https://www.mechon-mamre.org/i/0100.htm"),
    ("mamre-mt2",     "משנה תורה - ספר אהבה",     "הרמב\"ם", "הלכה", "https://www.mechon-mamre.org/i/0200.htm"),
    ("mamre-mt3",     "משנה תורה - ספר זמנים",    "הרמב\"ם", "הלכה", "https://www.mechon-mamre.org/i/0300.htm"),
    ("mamre-mt4",     "משנה תורה - ספר נשים",     "הרמב\"ם", "הלכה", "https://www.mechon-mamre.org/i/0400.htm"),
    ("mamre-mt5",     "משנה תורה - ספר קדושה",    "הרמב\"ם", "הלכה", "https://www.mechon-mamre.org/i/0500.htm"),
    ("mamre-mt6",     "משנה תורה - ספר הפלאה",    "הרמב\"ם", "הלכה", "https://www.mechon-mamre.org/i/0600.htm"),
    ("mamre-mt7",     "משנה תורה - ספר זרעים",    "הרמב\"ם", "הלכה", "https://www.mechon-mamre.org/i/0700.htm"),
    ("mamre-mt8",     "משנה תורה - ספר עבודה",    "הרמב\"ם", "הלכה", "https://www.mechon-mamre.org/i/0800.htm"),
    ("mamre-mt9",     "משנה תורה - ספר קרבנות",   "הרמב\"ם", "הלכה", "https://www.mechon-mamre.org/i/0900.htm"),
    ("mamre-mt10",    "משנה תורה - ספר טהרה",     "הרמב\"ם", "הלכה", "https://www.mechon-mamre.org/i/1000.htm"),
    ("mamre-mt11",    "משנה תורה - ספר נזיקין",   "הרמב\"ם", "הלכה", "https://www.mechon-mamre.org/i/1100.htm"),
    ("mamre-mt12",    "משנה תורה - ספר קנין",     "הרמב\"ם", "הלכה", "https://www.mechon-mamre.org/i/1200.htm"),
    ("mamre-mt13",    "משנה תורה - ספר משפטים",   "הרמב\"ם", "הלכה", "https://www.mechon-mamre.org/i/1300.htm"),
    ("mamre-mt14",    "משנה תורה - ספר שופטים",   "הרמב\"ם", "הלכה", "https://www.mechon-mamre.org/i/1400.htm"),
    ("mamre-mishnah", "משנה מלאה",                "", "משנה",  "https://www.mechon-mamre.org/b/h/h0.htm"),
]


def mamre_crawler():
    time.sleep(35)
    print("📜 Mamre crawler started")
    try:
        if get_state("mamre_done") == "1":
            return
        rows = [(b[0], "mamre", b[1], "", b[2], "", b[3], "he", 0, b[4]) for b in MAMRE_BOOKS]
        save_books(rows)
        set_state("mamre_done", "1")
        print(f"✅ Mamre done. Total: {total_books()}")
    except Exception as e:
        print(f"Mamre crawler error: {e}")


# ═══════════════════════════════════════════════════
#  SOURCE 5 — ויקיטקסט
# ═══════════════════════════════════════════════════
def wikisource_crawler():
    time.sleep(45)
    print("📰 Wikisource crawler started")
    while True:
        try:
            if get_state("wikisource_done") == "1":
                time.sleep(86400)
                continue

            cats = [
                "ספרי_קודש", "תנ\"ך", "תלמוד", "מדרש", "הלכה",
                "ספרות_עברית", "שירה_עברית", "ספרי_מוסר", "ספרי_חסידות",
                "קבלה", "פילוסופיה_יהודית", "ספרי_שאלות_ותשובות",
            ]
            db = get_db()

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

                    r = requests.get("https://he.wikisource.org/w/api.php",
                                     params=params, headers=GEN_HEADERS, timeout=15)
                    if r.status_code != 200: break
                    data = r.json()

                    pages = data.get("query", {}).get("categorymembers", [])
                    fetched = []
                    subj_map2 = {
                        "ספרי_קודש": "ספרות קודש", "תנ\"ך": "תנ\"ך", "תלמוד": "תלמוד",
                        "מדרש": "מדרש", "הלכה": "הלכה", "ספרות_עברית": "ספרות עברית",
                        "שירה_עברית": "שירה", "ספרי_מוסר": "מוסר",
                        "ספרי_חסידות": "חסידות", "קבלה": "קבלה",
                        "פילוסופיה_יהודית": "פילוסופיה", "ספרי_שאלות_ותשובות": "שו\"ת",
                    }
                    for pg in pages:
                        ptitle = pg.get("title", "")
                        if not ptitle or ptitle.startswith("קטגוריה:"): continue
                        uid = f"ws-{pg['pageid']}"
                        if db["books"].find_one({"id": uid}, {"_id": 1}):
                            continue
                        subj   = subj_map2.get(cat, "כללי")
                        ws_url = f"https://he.wikisource.org/wiki/{requests.utils.quote(ptitle)}"
                        fetched.append((uid, "wikisource", ptitle, "", "", "", subj, "he", 0, ws_url))

                    if fetched:
                        save_books(fetched)
                        print(f"📰 Wikisource [{cat}]: +{len(fetched)} | Total: {total_books()}")

                    cont = data.get("continue", {}).get("cmcontinue", "")
                    if not cont: break
                    time.sleep(0.5)

                set_state(state_key, "1")

            set_state("wikisource_done", "1")
            print(f"✅ Wikisource done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"Wikisource error: {e}")
            time.sleep(120)


# ═══════════════════════════════════════════════════
#  SOURCE 6 — Daat.ac.il
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

            db = get_db()
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
                        if db["books"].find_one({"id": uid}, {"_id": 1}): continue
                        fetched.append((uid, "daat", text, "", "", "", subj, "he", 0, full_url))
                    if fetched:
                        save_books(fetched)
                        print(f"🕌 Daat [{subj}]: +{len(fetched)} | Total: {total_books()}")
                    set_state(state_key, "1")
                except Exception as e:
                    print(f"Daat [{subj}] error: {e}")
                time.sleep(2)

            set_state("daat_done", "1")
            print(f"✅ Daat done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"Daat error: {e}")
            time.sleep(120)


# ═══════════════════════════════════════════════════
#  OCR — Claude Vision
# ═══════════════════════════════════════════════════
def ocr_page_with_claude_b64(b64_data, media_type="image/jpeg"):
    if not ANTHROPIC_KEY: return None
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
    db = get_db()
    if not ANTHROPIC_KEY:
        db["books"].update_one({"id": book_id}, {"$set": {"has_ocr": 1}})
        return False

    print(f"🔍 Claude Vision OCR: {book_id}")
    all_pages = []

    for page_num in range(1, 11):
        img_url = f"https://hebrewbooks.org/pagefinder.aspx?req={numeric_id}&pgnum={page_num}&zoom=0"
        try:
            r = requests.get(img_url, headers=HB_HEADERS, timeout=15)
            if r.status_code != 200:
                if page_num == 1:
                    print(f"  ❌ Image not accessible for {book_id}")
                break
            ct = r.headers.get("Content-Type", "")
            if "image" not in ct:
                if page_num > 2: break
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
                if page_num > 3 and not all_pages: break
        except Exception as e:
            print(f"  Page {page_num} error: {e}")
            if page_num > 2: break
        time.sleep(1)

    if not all_pages:
        db["books"].update_one({"id": book_id}, {"$set": {"has_ocr": 1}})
        return False

    full_text = "\n\n".join(all_pages)
    improved  = improve_text_with_claude(full_text, book_id)
    save_text(book_id, full_text, "claude_vision")

    db["books"].update_one({"id": book_id}, {"$set": {
        "has_ocr": 1, "has_text": 1,
        "ocr_improved": 1 if improved != full_text else 0
    }})
    if improved != full_text:
        db["book_text"].update_one(
            {"book_id": book_id},
            {"$set": {"improved": improved}},
            upsert=True
        )
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
            db   = get_db()
            rows = list(db["books"].find(
                {"source": "hebrewbooks", "has_ocr": 0, "valid": 1},
                {"id": 1, "_id": 0}
            ).limit(2))
            if not rows:
                time.sleep(300)
                continue
            for row in rows:
                num = row["id"].replace("hb-", "")
                if num.isdigit():
                    run_ocr(row["id"], int(num))
                time.sleep(8)
        except Exception as e:
            print(f"OCR crawler error: {e}")
            time.sleep(60)


# ═══════════════════════════════════════════════════
#  SOURCE 7 — Chabad.org
# ═══════════════════════════════════════════════════
CHABAD_BOOKS = [
    ("chabad-tanya",    "תניא",               "ר' שניאור זלמן מלאדי", "חסידות",    "https://www.chabad.org/library/tanya/tanya_cdo/aid/1026/jewish/Tanya.htm"),
    ("chabad-hayomyom", "היום יום",           "הרבי מלובביץ'",        "חסידות",    "https://www.chabad.org/library/article_cdo/aid/9668/jewish/Hayom-Yom.htm"),
    ("chabad-rambam1",  "משנה תורה — רמב\"ם יומי", "הרמב\"ם",        "הלכה",      "https://www.chabad.org/library/article_cdo/aid/682956"),
    ("chabad-kitzur",   "קיצור שולחן ערוך",  "ר' שלמה גנצפריד",      "הלכה",      "https://www.chabad.org/library/article_cdo/aid/1108240"),
    ("chabad-avot",     "פרקי אבות עם פירוש","שונות",                 "מחשבה",     "https://www.chabad.org/library/article_cdo/aid/682956"),
    ("chabad-likkutei", "ליקוטי תורה",        "ר' שניאור זלמן מלאדי", "קבלה",      "https://www.chabad.org/library/article_cdo/aid/1408097"),
    ("chabad-emunah",   "ספר האמונה",         "הרמב\"ן",               "פילוסופיה", "https://www.chabad.org/library/article_cdo/aid/2488"),
]


def chabad_crawler():
    time.sleep(65)
    print("🕍 Chabad crawler started")
    while True:
        try:
            if get_state("chabad_done") == "1":
                time.sleep(86400)
                continue

            rows = [(b[0], "chabad", b[1], "", b[2], "", b[3], "he", 0, b[4]) for b in CHABAD_BOOKS]
            save_books(rows)

            db = get_db()
            try:
                r = requests.get(
                    "https://www.chabad.org/library/article_cdo/aid/63830/jewish/Jewish-Library.htm",
                    headers=GEN_HEADERS, timeout=15
                )
                if r.status_code == 200:
                    r.encoding = 'utf-8'
                    soup  = BeautifulSoup(r.text, "html.parser")
                    count = 0
                    for a in soup.find_all("a", href=re.compile(r"/library/article_cdo")):
                        text = a.get_text(strip=True)
                        if not text or not re.search(r'[\u05D0-\u05EA]', text): continue
                        href = a["href"]
                        if not href.startswith("http"):
                            href = "https://www.chabad.org" + href
                        uid = f"chabad-{abs(hash(href))}"
                        if db["books"].find_one({"id": uid}, {"_id": 1}): continue
                        save_books([(uid, "chabad", text, "", "", "", "חסידות", "he", 0, href)])
                        count += 1
                    print(f"🕍 Chabad library index: +{count} books")
            except Exception as e:
                print(f"Chabad index error: {e}")

            set_state("chabad_done", "1")
            print(f"✅ Chabad done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"Chabad error: {e}")
            time.sleep(120)


# ═══════════════════════════════════════════════════
#  SOURCE 8 — NLI
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
                ("הלכה", "halacha"), ("תורה", "torah"), ("תלמוד", "talmud"),
                ("קבלה", "kabbalah"), ("חסידות", "hasidut"),
                ("שירה עברית", "hebrew poetry"), ("ספרות עברית", "hebrew literature"),
                ("מוסר", "musar"), ("פילוסופיה", "jewish philosophy"),
                ("פרשנות", "biblical commentary"),
            ]
            db = get_db()

            for he_subj, en_query in queries:
                state_key = f"nli_{en_query}"
                if get_state(state_key) == "1": continue
                try:
                    r = requests.get(
                        "https://api.nli.org.il/opds/search",
                        params={"query": en_query, "lang": "heb", "pageSize": 100},
                        headers={**GEN_HEADERS, "Accept": "application/json"},
                        timeout=20
                    )
                    if r.status_code == 200:
                        try:
                            data    = r.json()
                            entries = data.get("entries", data.get("items", []))
                            fetched = []
                            for item in entries:
                                title  = item.get("title", "") or item.get("name", "")
                                author = item.get("author", "") or item.get("creator", "")
                                year   = item.get("date", "")   or item.get("year", "")
                                iid    = item.get("id", "")     or item.get("identifier", "")
                                url    = item.get("url", "")    or item.get("link", "")
                                if not title: continue
                                uid = f"nli-{abs(hash(iid or title))}"
                                if db["books"].find_one({"id": uid}, {"_id": 1}): continue
                                fetched.append((uid, "nli", title, "", author, str(year), he_subj, "he", 0, url))
                            if fetched:
                                save_books(fetched)
                                print(f"🏛️ NLI [{he_subj}]: +{len(fetched)} | Total: {total_books()}")
                        except:
                            pass
                    set_state(state_key, "1")
                except Exception as e:
                    print(f"NLI [{en_query}] error: {e}")
                time.sleep(3)

            set_state("nli_done", "1")
            print(f"✅ NLI done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"NLI error: {e}")
            time.sleep(120)


# ═══════════════════════════════════════════════════
#  SOURCE 9 — Internet Archive
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
                ("subject:Hebrew AND mediatype:texts AND language:Hebrew", "ספרות עברית"),
                ("subject:Talmud AND mediatype:texts", "תלמוד"),
                ("subject:Kabbalah AND mediatype:texts", "קבלה"),
                ("subject:Jewish law AND mediatype:texts", "הלכה"),
                ("subject:Hasidism AND mediatype:texts AND language:Hebrew", "חסידות"),
                ("subject:responsa AND mediatype:texts", "שו\"ת"),
                ("subject:Midrash AND mediatype:texts", "מדרש"),
            ]
            db = get_db()

            for query, subj in searches:
                state_key = f"ia_{abs(hash(query))}"
                if get_state(state_key) == "1": continue
                try:
                    r = requests.get(
                        "https://archive.org/advancedsearch.php",
                        params={"q": query, "fl[]": ["identifier", "title", "creator", "date"],
                                "rows": 200, "page": 1, "output": "json"},
                        headers=GEN_HEADERS, timeout=20
                    )
                    if r.status_code != 200: continue
                    docs    = r.json().get("response", {}).get("docs", [])
                    fetched = []
                    for doc in docs:
                        iid    = doc.get("identifier", "")
                        title  = doc.get("title", "")
                        author = doc.get("creator", "")
                        year   = str(doc.get("date", ""))[:4]
                        if not iid or not title: continue
                        if isinstance(author, list): author = author[0] if author else ""
                        if isinstance(title,  list): title  = title[0]  if title  else ""
                        uid = f"ia-{iid[:80]}"
                        if db["books"].find_one({"id": uid}, {"_id": 1}): continue
                        fetched.append((uid, "archive", title, "", author, year, subj, "he", 0,
                                        f"https://archive.org/details/{iid}"))
                    if fetched:
                        save_books(fetched)
                        print(f"🌐 Archive [{subj}]: +{len(fetched)} | Total: {total_books()}")
                    set_state(state_key, "1")
                except Exception as e:
                    print(f"Archive [{subj}] error: {e}")
                time.sleep(3)

            set_state("ia_done", "1")
            print(f"✅ Internet Archive done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"Archive error: {e}")
            time.sleep(120)


# ═══════════════════════════════════════════════════
#  SOURCE 10 — OpenSiddur
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
                params={"per_page": 100, "lang": "he"},
                headers=GEN_HEADERS, timeout=20
            )
            if r.status_code == 200:
                posts   = r.json()
                fetched = []
                db      = get_db()
                for post in posts:
                    title = post.get("title", {}).get("rendered", "")
                    uid   = f"os-{post.get('id', '')}"
                    url   = post.get("link", "")
                    title = re.sub(r'<[^>]+>', '', title).strip()
                    if not title: continue
                    if db["books"].find_one({"id": uid}, {"_id": 1}): continue
                    fetched.append((uid, "opensiddur", title, "", "", "", "תפילה", "he", 0, url))
                if fetched:
                    save_books(fetched)
                    print(f"🙏 OpenSiddur: +{len(fetched)} | Total: {total_books()}")

            set_state("opensiddur_done", "1")
            print(f"✅ OpenSiddur done. Total: {total_books()}")
            time.sleep(86400)

        except Exception as e:
            print(f"OpenSiddur error: {e}")
            time.sleep(120)


# ═══════════════════════════════════════════════════
#  SOURCE 11 — Al-Hatorah.org
# ═══════════════════════════════════════════════════
ALHATORAH_BOOKS = [
    ("aht-bereshit", "בראשית עם פירושים",   "שונות",               "תנ\"ך",    "https://alhatorah.org/Commentary:Commentators_on_Genesis"),
    ("aht-shemot",   "שמות עם פירושים",     "שונות",               "תנ\"ך",    "https://alhatorah.org/Commentary:Commentators_on_Exodus"),
    ("aht-vayikra",  "ויקרא עם פירושים",    "שונות",               "תנ\"ך",    "https://alhatorah.org/Commentary:Commentators_on_Leviticus"),
    ("aht-bamidbar", "במדבר עם פירושים",    "שונות",               "תנ\"ך",    "https://alhatorah.org/Commentary:Commentators_on_Numbers"),
    ("aht-devarim",  "דברים עם פירושים",    "שונות",               "תנ\"ך",    "https://alhatorah.org/Commentary:Commentators_on_Deuteronomy"),
    ("aht-rashi",    "פירוש רש\"י לתורה",   "רש\"י",               "פרשנות",   "https://alhatorah.org/Commentator:Rashi"),
    ("aht-ramban",   "פירוש הרמב\"ן לתורה", "הרמב\"ן",             "פרשנות",   "https://alhatorah.org/Commentator:Ramban"),
    ("aht-ibnezra",  "פירוש אבן עזרא",      "ר' אברהם אבן עזרא",  "פרשנות",   "https://alhatorah.org/Commentator:Ibn_Ezra"),
    ("aht-sforno",   "פירוש ספורנו",         "ר' עובדיה ספורנו",   "פרשנות",   "https://alhatorah.org/Commentator:Sforno"),
    ("aht-malbim",   "פירוש המלבי\"ם",      "המלבי\"ם",            "פרשנות",   "https://alhatorah.org/Commentator:Malbim"),
]


def alhatorah_crawler():
    time.sleep(105)
    print("📜 Al-Hatorah crawler started")
    while True:
        try:
            if get_state("alhatorah_done") == "1":
                time.sleep(86400)
                continue
            rows = [(b[0], "alhatorah", b[1], "", b[2], "", b[3], "he", 0, b[4]) for b in ALHATORAH_BOOKS]
            save_books(rows)
            set_state("alhatorah_done", "1")
            print(f"✅ Al-Hatorah done. Total: {total_books()}")
            time.sleep(86400)
        except Exception as e:
            print(f"Al-Hatorah error: {e}")
            time.sleep(120)


# ═══════════════════════════════════════════════════
#  LIVE TEXT FETCHERS
# ═══════════════════════════════════════════════════
def fetch_sefaria_text_live(book_id):
    try:
        title = book_id.replace("sef-", "").replace("_", " ")
        r = requests.get(
            f"https://www.sefaria.org/api/texts/{requests.utils.quote(title)}?context=0&commentary=0&pad=0",
            headers=GEN_HEADERS, timeout=20
        )
        if r.status_code != 200: return None
        d  = r.json()
        he = d.get("he", [])
        def flat(x):
            if isinstance(x, list): return "\n".join(flat(i) for i in x if i)
            return str(x) if x else ""
        text = flat(he)
        return text.strip() if len(text) > 50 else None
    except:
        return None


def fetch_wikisource_text_live(title):
    try:
        r = requests.get(
            "https://he.wikisource.org/w/api.php",
            params={"action": "parse", "page": title, "prop": "wikitext", "format": "json"},
            headers=GEN_HEADERS, timeout=15
        )
        if r.status_code != 200: return None
        wikitext = r.json().get("parse", {}).get("wikitext", {}).get("*", "")
        clean = re.sub(r'\[\[([^\]|]+)(?:\|[^\]]+)?\]\]', r'\1', wikitext)
        clean = re.sub(r'\{\{[^}]+\}\}', '', clean)
        clean = re.sub(r"'{2,}", '', clean)
        clean = re.sub(r'==+([^=]+)==+', r'\n\1\n', clean)
        heb = "\n".join(l for l in clean.split("\n") if re.search(r'[\u05D0-\u05EA]', l))
        return heb.strip() if len(heb) > 50 else None
    except:
        return None


def fetch_url_text_live(url):
    try:
        r = requests.get(url, headers=GEN_HEADERS, timeout=15)
        if r.status_code != 200: return None
        r.encoding = 'utf-8'
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script", "style", "nav", "header", "footer", "aside"]): tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        heb  = "\n".join(l for l in text.split("\n") if re.search(r'[\u05D0-\u05EA]{3,}', l))
        return heb.strip() if len(heb) > 50 else None
    except:
        return None


# ═══════════════════════════════════════════════════
#  API ROUTES
# ═══════════════════════════════════════════════════
@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/stats")
def stats():
    db = get_db()
    total     = db["books"].count_documents({"valid": 1})
    with_text = db["books"].count_documents({"has_text": 1, "valid": 1})
    improved  = db["books"].count_documents({"ocr_improved": 1})

    by_source_agg = db["books"].aggregate([
        {"$match": {"valid": 1}},
        {"$group": {"_id": "$source", "count": {"$sum": 1}}}
    ])
    by_source = {r["_id"]: r["count"] for r in by_source_agg}

    subjects_agg = db["books"].aggregate([
        {"$match": {"valid": 1, "subject": {"$ne": ""}}},
        {"$group": {"_id": "$subject", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 20}
    ])
    subjects = [{"name": r["_id"], "count": r["count"]} for r in subjects_agg]

    return jsonify({
        "total":      total,
        "with_text":  with_text,
        "improved":   improved,
        "by_source":  by_source,
        "hb_crawled": int(get_state("hb_last_id", 1)),
        "subjects":   subjects,
        "db_size_mb": 0,
    })


@app.route("/api/featured")
def featured():
    rows = list(get_db()["books"].find(
        {"valid": 1, "title": {"$ne": ""}},
        {"_id": 0}
    ).limit(20))
    return jsonify({"books": rows})


@app.route("/api/search")
def search():
    q        = request.args.get("q", "").strip()
    page     = request.args.get("page", 1, type=int)
    subj     = request.args.get("subject", "")
    source   = request.args.get("source", "")
    has_text = request.args.get("has_text", "")
    limit    = 30
    skip     = (page - 1) * limit

    db    = get_db()
    query = {"valid": 1}

    if q:
        query["$or"] = [
            {"title":    {"$regex": q, "$options": "i"}},
            {"he_title": {"$regex": q, "$options": "i"}},
            {"author":   {"$regex": q, "$options": "i"}},
        ]
    if subj:     query["subject"] = subj
    if source:   query["source"]  = source
    if has_text == "1": query["has_text"] = 1

    total = db["books"].count_documents(query)
    rows  = list(db["books"].find(query, {"_id": 0})
                 .sort([("has_text", -1)])
                 .skip(skip).limit(limit))

    # Live HebrewBooks search fallback
    if q and source != "sefaria" and len(rows) < 10:
        try:
            r2   = requests.get(
                f"https://hebrewbooks.org/search?sdesc={requests.utils.quote(q)}&page=1",
                headers=HB_HEADERS, timeout=10
            )
            soup = BeautifulSoup(r2.text, "html.parser")
            ex   = {b["id"] for b in rows}
            live = []
            for row2 in soup.select("table tr"):
                cells = row2.find_all("td")
                link  = row2.find("a", href=re.compile(r"^/\d+$"))
                if not link or len(cells) < 2: continue
                try:
                    bid = int(re.search(r"(\d+)", link["href"]).group(1))
                    uid = f"hb-{bid}"
                    if uid in ex: continue
                    t = cells[0].get_text(strip=True)
                    a = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    y = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                    if t:
                        live.append({"id": uid, "source": "hebrewbooks", "title": t,
                                     "he_title": "", "author": a, "year": y,
                                     "subject": "", "language": "he",
                                     "has_text": 0, "has_ocr": 0,
                                     "ocr_improved": 0, "valid": 1, "url": ""})
                        save_books([(uid, "hebrewbooks", t, "", a, y, "", "he", 0, "")])
                except: pass
            rows  += live
            if not total: total = len(rows)
        except: pass

    return jsonify({"books": rows, "total": total, "page": page,
                    "pages": max(1, (total + limit - 1) // limit)})


@app.route("/api/book/<path:book_id>")
def book_detail(book_id):
    db   = get_db()
    book = db["books"].find_one({"id": book_id}, {"_id": 0})
    if not book: return jsonify({"error": "not found"}), 404
    d = dict(book)

    text         = None
    has_improved = False

    if book_id.startswith("sef-"):
        text = fetch_sefaria_text_live(book_id)
    elif book_id.startswith("ws-"):
        text = fetch_wikisource_text_live(book.get("title", ""))
    elif book_id.startswith("by-"):
        if book.get("url"): text = fetch_url_text_live(book["url"])
    elif book_id.startswith("mamre-"):
        if book.get("url"): text = fetch_url_text_live(book["url"])
    elif book_id.startswith(("daat-", "chabad-", "aht-")):
        if book.get("url"): text = fetch_url_text_live(book["url"])

    if not text:
        tr = db["book_text"].find_one({"book_id": book_id}, {"_id": 0})
        if tr:
            text         = tr.get("improved") or tr.get("content")
            has_improved = bool(tr.get("improved"))

    d["text"]         = text
    d["has_improved"] = has_improved
    return jsonify(d)


@app.route("/api/viewer/<path:book_id>")
def viewer_info(book_id):
    if book_id.startswith("hb-"):
        num = book_id.replace("hb-", "")
        return jsonify({"type": "pdf", "url": f"https://hebrewbooks.org/pdfpager.aspx?req={num}"})
    elif book_id.startswith("sef-"):
        title = book_id.replace("sef-", "").replace("_", " ")
        return jsonify({"type": "sefaria", "url": f"https://www.sefaria.org/{requests.utils.quote(title)}"})
    else:
        b   = get_db()["books"].find_one({"id": book_id}, {"_id": 0, "url": 1})
        url = b.get("url", "#") if b else "#"
        return jsonify({"type": "web", "url": url})


@app.route("/api/ocr/<path:book_id>", methods=["POST"])
def trigger_ocr(book_id):
    if not book_id.startswith("hb-"): return jsonify({"error": "HB only"}), 400
    num = book_id.replace("hb-", "")
    if not num.isdigit(): return jsonify({"error": "invalid"}), 400
    row = get_db()["books"].find_one({"id": book_id}, {"_id": 0, "has_ocr": 1})
    if row and row.get("has_ocr"): return jsonify({"status": "already_done"})
    threading.Thread(target=run_ocr, args=(book_id, int(num)), daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/improve/<path:book_id>", methods=["POST"])
def trigger_improve(book_id):
    if not ANTHROPIC_KEY: return jsonify({"error": "no API key"}), 400
    db   = get_db()
    book = db["books"].find_one({"id": book_id}, {"_id": 0, "title": 1})
    tr   = db["book_text"].find_one({"book_id": book_id}, {"_id": 0, "content": 1})
    if not book or not tr: return jsonify({"error": "no text"}), 404

    def do_improve():
        improved = improve_text_with_claude(tr.get("content", ""), book.get("title", ""))
        db2 = get_db()
        db2["book_text"].update_one({"book_id": book_id}, {"$set": {"improved": improved}}, upsert=True)
        db2["books"].update_one({"id": book_id}, {"$set": {"ocr_improved": 1}})

    threading.Thread(target=do_improve, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/export-pdf/<path:book_id>")
def export_pdf(book_id):
    try:
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.lib.pagesizes import A4
        from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, HRFlowable, PageBreak)
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.enums import TA_RIGHT, TA_CENTER
        from reportlab.lib.units import cm
        from reportlab.lib import colors
        import io

        db   = get_db()
        book = db["books"].find_one({"id": book_id}, {"_id": 0})
        tr   = db["book_text"].find_one({"book_id": book_id}, {"_id": 0})

        if not book: return jsonify({"error": "ספר לא נמצא"}), 404
        if not tr:   return jsonify({"error": "אין טקסט לספר זה"}), 404

        title  = book.get("title") or book.get("he_title") or f"ספר #{book_id}"
        author = book.get("author", "")
        year   = book.get("year", "")
        text   = tr.get("improved") or tr.get("content") or ""

        FONT_PATH      = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
        FONT_PATH_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        pdfmetrics.registerFont(TTFont("Heb",     FONT_PATH))
        pdfmetrics.registerFont(TTFont("HebBold", FONT_PATH_BOLD))

        BLUE = colors.HexColor("#2563eb")
        DARK = colors.HexColor("#111827")
        GRAY = colors.HexColor("#6b7280")

        s_title  = ParagraphStyle("s_title",  fontName="HebBold", fontSize=26, alignment=TA_CENTER, spaceAfter=8,  leading=36, textColor=DARK, wordWrap="RTL")
        s_author = ParagraphStyle("s_author", fontName="Heb",     fontSize=14, alignment=TA_CENTER, spaceAfter=4,  leading=22, textColor=GRAY, wordWrap="RTL")
        s_year   = ParagraphStyle("s_year",   fontName="Heb",     fontSize=11, alignment=TA_CENTER, spaceAfter=24, leading=18, textColor=GRAY, wordWrap="RTL")
        s_heading= ParagraphStyle("s_heading",fontName="HebBold", fontSize=15, alignment=TA_RIGHT,  spaceAfter=8,  spaceBefore=20, leading=24, textColor=BLUE, wordWrap="RTL")
        s_sub    = ParagraphStyle("s_sub",    fontName="HebBold", fontSize=13, alignment=TA_RIGHT,  spaceAfter=6,  spaceBefore=12, leading=20, textColor=DARK, wordWrap="RTL")
        s_body   = ParagraphStyle("s_body",   fontName="Heb",     fontSize=12, alignment=TA_RIGHT,  spaceAfter=6,  leading=22,    textColor=DARK, wordWrap="RTL")

        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=A4,
                                rightMargin=2.8*cm, leftMargin=2.8*cm,
                                topMargin=2.5*cm,   bottomMargin=2.5*cm,
                                title=title, author=author)
        story = []
        story.append(Spacer(1, 3*cm))
        story.append(Paragraph(title, s_title))
        story.append(HRFlowable(width="60%", thickness=2, color=BLUE, hAlign="CENTER"))
        story.append(Spacer(1, 12))
        if author: story.append(Paragraph(author, s_author))
        if year:   story.append(Paragraph(f"שנת {year}", s_year))
        story.append(Paragraph("גנזך — ספרייה יהודית דיגיטלית", s_year))
        story.append(PageBreak())

        for para in re.split(r'\n{2,}|---', text):
            for line in [l.strip() for l in para.strip().split('\n') if l.strip()]:
                if re.match(r'^(פרק|חלק|ספר|שער|הלכות|סימן)\s', line):
                    story.append(Paragraph(line, s_heading))
                elif re.match(r'^(סעיף|דין|שאלה|תשובה|אות)\s', line):
                    story.append(Paragraph(line, s_sub))
                elif re.match(r'^[✦•*—\-]{3,}', line):
                    story.append(HRFlowable(width="40%", thickness=0.5, color=GRAY, hAlign="CENTER"))
                else:
                    safe = line.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                    story.append(Paragraph(safe, s_body))

        doc.build(story)
        pdf_bytes = buf.getvalue()
        safe_name = re.sub(r'[^\u05D0-\u05EAa-zA-Z0-9\s]', '', title).strip()[:60] or "book"

        return Response(pdf_bytes, content_type="application/pdf",
                        headers={"Content-Disposition": f'attachment; filename="{safe_name}.pdf"'})

    except ImportError:
        return jsonify({"error": "reportlab not installed"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/search-in-book/<path:book_id>")
def search_in_book(book_id):
    q = request.args.get("q", "").strip()
    if not q: return jsonify({"results": []})

    db   = get_db()
    tr   = db["book_text"].find_one({"book_id": book_id}, {"_id": 0})
    book = db["books"].find_one({"id": book_id}, {"_id": 0})
    text = (tr.get("improved") or tr.get("content", "")) if tr else ""

    if not text and book:
        if book_id.startswith("sef-"):       text = fetch_sefaria_text_live(book_id) or ""
        elif book_id.startswith("ws-"):      text = fetch_wikisource_text_live(book.get("title", "")) or ""
        elif book.get("url"):                text = fetch_url_text_live(book["url"]) or ""

    if not text: return jsonify({"results": [], "error": "אין טקסט לספר זה"})

    lines, results = text.split("\n"), []
    for i, line in enumerate(lines):
        if q in line:
            ctx = "\n".join(lines[max(0, i-1):min(len(lines), i+2)])
            results.append({"line": i, "text": line, "context": ctx})
        if len(results) >= 50: break
    return jsonify({"results": results, "total": len(results), "query": q})


@app.route("/api/related/<path:book_id>")
def related_books(book_id):
    db   = get_db()
    book = db["books"].find_one({"id": book_id}, {"_id": 0, "subject": 1, "author": 1})
    if not book: return jsonify({"books": []})

    related = list(db["books"].find(
        {"id": {"$ne": book_id}, "valid": 1,
         "$or": [{"subject": book.get("subject", "")}, {"author": book.get("author", "")}]},
        {"_id": 0}
    ).sort([("has_text", -1)]).limit(12))
    return jsonify({"books": related})


@app.route("/api/toc/<path:book_id>")
def table_of_contents(book_id):
    db   = get_db()
    tr   = db["book_text"].find_one({"book_id": book_id}, {"_id": 0})
    book = db["books"].find_one({"id": book_id}, {"_id": 0})
    text = (tr.get("improved") or tr.get("content", "")) if tr else ""

    if not text and book:
        if book_id.startswith("sef-"):  text = fetch_sefaria_text_live(book_id) or ""
        elif book_id.startswith("ws-"): text = fetch_wikisource_text_live(book.get("title", "")) or ""
        elif book.get("url"):           text = fetch_url_text_live(book["url"]) or ""

    if not text: return jsonify({"toc": []})

    toc = []
    for i, line in enumerate([l.strip() for l in text.split("\n")]):
        if not line: continue
        if (re.match(r'^(פרק|חלק|ספר|שער|הלכות|סימן|שאלה|תשובה)\s', line) or
                (len(line) < 50 and not line.endswith('.') and len(re.findall(r'[\u05D0-\u05EA]', line)) > 3)):
            toc.append({"line": i, "text": line, "level": 1})
        if len(toc) >= 100: break
    return jsonify({"toc": toc, "total": len(toc)})


@app.route("/api/daily")
def daily_study():
    import random, datetime
    db    = get_db()
    books = list(db["books"].aggregate([
        {"$match": {"has_text": 1, "valid": 1}},
        {"$sample": {"size": 5}}
    ]))
    if not books: return jsonify({"error": "אין ספרים"})
    book = random.choice(books)
    tr   = db["book_text"].find_one({"book_id": book["id"]}, {"_id": 0})
    text = ""
    if tr:
        full  = tr.get("improved") or tr.get("content") or ""
        lines = [l for l in full.split("\n") if len(l.strip()) > 20]
        if lines:
            start = random.randint(0, max(0, len(lines) - 10))
            text  = "\n".join(lines[start:start + 15])
    book.pop("_id", None)
    return jsonify({"book": book, "excerpt": text, "date": datetime.date.today().isoformat()})


@app.route("/api/fulltext-search")
def fulltext_search():
    q     = request.args.get("q", "").strip()
    page  = request.args.get("page", 1, type=int)
    limit = 20
    if not q or len(q) < 2: return jsonify({"results": [], "total": 0})

    db      = get_db()
    pattern = {"$regex": re.escape(q), "$options": "i"}
    match_q = {"$or": [{"content": pattern}, {"improved": pattern}]}

    pipeline = [
        {"$match": match_q},
        {"$lookup": {"from": "books", "localField": "book_id", "foreignField": "id", "as": "book_info"}},
        {"$unwind": "$book_info"},
        {"$match": {"book_info.valid": 1}},
        {"$project": {"book_id": 1, "content": 1, "improved": 1,
                      "title": "$book_info.title", "author": "$book_info.author",
                      "subject": "$book_info.subject", "_id": 0}},
        {"$skip": (page - 1) * limit},
        {"$limit": limit}
    ]
    rows  = list(db["book_text"].aggregate(pipeline))
    total_pipeline = [
        {"$match": match_q},
        {"$lookup": {"from": "books", "localField": "book_id", "foreignField": "id", "as": "book_info"}},
        {"$unwind": "$book_info"},
        {"$match": {"book_info.valid": 1}},
        {"$count": "total"}
    ]
    tc    = list(db["book_text"].aggregate(total_pipeline))
    total = tc[0]["total"] if tc else 0

    results = []
    for row in rows:
        text    = row.get("improved") or row.get("content") or ""
        idx     = text.find(q)
        if idx >= 0:
            s       = max(0, idx - 100)
            e       = min(len(text), idx + len(q) + 100)
            excerpt = ("..." if s > 0 else "") + text[s:e] + ("..." if e < len(text) else "")
        else:
            excerpt = text[:200]
        results.append({"book_id": row.get("book_id"), "title": row.get("title"),
                         "author": row.get("author"), "subject": row.get("subject"),
                         "excerpt": excerpt})
    return jsonify({"results": results, "total": total, "page": page,
                    "pages": max(1, (total + limit - 1) // limit)})


@app.route("/api/ai-explain", methods=["POST"])
def ai_explain():
    if not ANTHROPIC_KEY: return jsonify({"error": "חסר ANTHROPIC_API_KEY"}), 400
    data    = request.get_json() or {}
    passage = data.get("text", "").strip()
    book    = data.get("book", "")
    if not passage: return jsonify({"error": "חסר טקסט"}), 400
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 1000,
                  "system": "אתה רב ומלומד בתורה. הסבר את הקטע בעברית מודרנית ברורה.",
                  "messages": [{"role": "user", "content": f"ספר: {book}\n\nקטע:\n{passage[:1000]}"}]},
            timeout=45
        )
        d = resp.json()
        if resp.status_code == 200 and d.get("content"):
            return jsonify({"explanation": d["content"][0]["text"]})
        return jsonify({"error": d.get("error", {}).get("message", "שגיאה")}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ai-translate", methods=["POST"])
def ai_translate():
    if not ANTHROPIC_KEY: return jsonify({"error": "נדרש מפתח API"}), 400
    data    = request.get_json() or {}
    passage = data.get("text", "").strip()
    target  = data.get("target", "modern_hebrew")
    if not passage: return jsonify({"error": "חסר טקסט"}), 400
    prompts = {
        "modern_hebrew": "תרגם לעברית מודרנית פשוטה:",
        "english":       "Translate to clear modern English:",
        "yiddish":       "איבערזעץ אויף יידיש:",
    }
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 1500,
                  "messages": [{"role": "user",
                                "content": f"{prompts.get(target, prompts['modern_hebrew'])}\n\n{passage[:1500]}"}]},
            timeout=30
        )
        if r.status_code == 200:
            return jsonify({"translation": r.json()["content"][0]["text"], "target": target})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"error": "שגיאה"}), 500


@app.route("/api/admin/cleanup", methods=["POST"])
def admin_cleanup():
    # MongoDB Atlas — אין בעיית דיסק
    return jsonify({"status": "ok", "db_size_mb": 0,
                    "message": "MongoDB Atlas — no disk cleanup needed"})


@app.route("/api/export-books")
def export_books():
    data   = list(get_db()["books"].find({"valid": 1}, {"_id": 0}))
    output = json.dumps(data, ensure_ascii=False, indent=2)
    return Response(output, mimetype="application/json",
                    headers={"Content-Disposition": "attachment; filename=ganzach_books_backup.json"})


@app.route("/api/import-books", methods=["POST"])
def import_books():
    try:
        data = request.get_json()
        if not data or not isinstance(data, list):
            return jsonify({"error": "נתונים לא תקינים"}), 400
        db       = get_db()
        imported = 0
        skipped  = 0
        for book in data:
            try:
                db["books"].insert_one({
                    "id":           book.get("id", ""),
                    "source":       book.get("source", "hebrewbooks"),
                    "title":        book.get("title", ""),
                    "he_title":     book.get("he_title", ""),
                    "author":       book.get("author", ""),
                    "year":         book.get("year", ""),
                    "subject":      book.get("subject", ""),
                    "language":     book.get("language", "he"),
                    "has_text":     book.get("has_text", 0),
                    "has_ocr":      book.get("has_ocr", 0),
                    "ocr_improved": book.get("ocr_improved", 0),
                    "valid":        book.get("valid", 1),
                    "url":          book.get("url", ""),
                })
                imported += 1
            except DuplicateKeyError:
                skipped += 1
            except:
                skipped += 1
        return jsonify({"status": "ok", "imported": imported,
                        "skipped": skipped, "total": len(data)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════
#  START
# ═══════════════════════════════════════════════════
init_db()

crawlers = [
    hebrewbooks_crawler,
    sefaria_crawler,
    mamre_crawler,
    daat_crawler,
    chabad_crawler,
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
    app.run(debug=False, host="0.0.0.0", port=port)
