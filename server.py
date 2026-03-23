from flask import Flask, request, Response, jsonify, send_from_directory
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
import re
import os

app = Flask(__name__, static_folder="static")
CORS(app)

# ─── SERVE FRONTEND ─────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/<path:path>")
def static_files(path):
    return send_from_directory("static", path)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://hebrewbooks.org/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ─── חיפוש ספרים ───────────────────────────────────────────────
@app.route("/api/search")
def search():
    query = request.args.get("q", "")
    page  = request.args.get("page", 1, type=int)

    url = f"https://hebrewbooks.org/search?sdesc={requests.utils.quote(query)}&page={page}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        books = []
        for row in soup.select("table.results tr"):
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            link = row.find("a", href=re.compile(r"/\d+"))
            if not link:
                continue

            book_id = re.search(r"/(\d+)", link["href"])
            if not book_id:
                continue

            title  = cells[0].get_text(strip=True) if cells[0] else ""
            author = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            year   = cells[2].get_text(strip=True) if len(cells) > 2 else ""

            if title:
                books.append({
                    "id":     book_id.group(1),
                    "title":  title,
                    "author": author,
                    "year":   year,
                })

        return jsonify({"books": books, "query": query, "page": page})

    except Exception as e:
        return jsonify({"error": str(e), "books": []}), 500


# ─── רשימת ספרים לפי קטגוריה ────────────────────────────────────
@app.route("/api/category/<int:cat_id>")
def category(cat_id):
    page = request.args.get("page", 1, type=int)
    url  = f"https://hebrewbooks.org/category.aspx?catid={cat_id}&page={page}"
    try:
        r    = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        books = []
        for link in soup.select("a[href*='/']"):
            m = re.match(r"^/(\d+)$", link.get("href", ""))
            if m:
                books.append({
                    "id":    m.group(1),
                    "title": link.get_text(strip=True),
                })

        return jsonify({"books": books, "category": cat_id})
    except Exception as e:
        return jsonify({"error": str(e), "books": []}), 500


# ─── מידע על ספר ────────────────────────────────────────────────
@app.route("/api/book/<int:book_id>")
def book_info(book_id):
    url = f"https://hebrewbooks.org/{book_id}"
    try:
        r    = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        title  = soup.find("h1")
        author = soup.find("h2")
        desc   = soup.find("div", class_="bookdesc")

        return jsonify({
            "id":          book_id,
            "title":       title.get_text(strip=True)  if title  else f"ספר #{book_id}",
            "author":      author.get_text(strip=True) if author else "",
            "description": desc.get_text(strip=True)   if desc   else "",
            "pdf_url":     f"/api/pdf/{book_id}",
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── PROXY ל-PDF ─────────────────────────────────────────────────
@app.route("/api/pdf/<int:book_id>")
def proxy_pdf(book_id):
    """מוריד PDF מ-HebrewBooks ומגיש אותו כאילו הוא מגיע מהשרת שלנו."""
    pdf_url = f"https://hebrewbooks.org/pdfpager.aspx?req={book_id}&pgnum=1"

    try:
        r = requests.get(pdf_url, headers=HEADERS, timeout=30, stream=True)

        # HebrewBooks מחזיר לפעמים HTML עם redirect ל-PDF אמיתי
        if "application/pdf" not in r.headers.get("Content-Type", ""):
            soup = BeautifulSoup(r.text, "html.parser")
            iframe = soup.find("iframe", src=re.compile(r"\.pdf", re.I))
            if iframe:
                real_url = iframe["src"]
                if not real_url.startswith("http"):
                    real_url = "https://hebrewbooks.org" + real_url
                r = requests.get(real_url, headers=HEADERS, timeout=30, stream=True)

        def generate():
            for chunk in r.iter_content(chunk_size=8192):
                yield chunk

        return Response(
            generate(),
            content_type="application/pdf",
            headers={
                "Content-Disposition": f'inline; filename="book_{book_id}.pdf"',
                "Access-Control-Allow-Origin": "*",
            },
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── ספרים פופולריים (hardcoded IDs ידועים) ─────────────────────
@app.route("/api/featured")
def featured():
    featured_books = [
        {"id": "9780",  "title": "שולחן ערוך - אורח חיים",    "author": "ר' יוסף קארו",    "category": "הלכה"},
        {"id": "14763", "title": "משנה תורה - רמב\"ם",         "author": "הרמב\"ם",          "category": "הלכה"},
        {"id": "3281",  "title": "ספר החינוך",                  "author": "ר' אהרן הלוי",    "category": "מצוות"},
        {"id": "22879", "title": "חפץ חיים",                    "author": "ר' ישראל מאיר קגן", "category": "מוסר"},
        {"id": "43081", "title": "נפש החיים",                   "author": "ר' חיים מוולוז'ין", "category": "מחשבה"},
        {"id": "8774",  "title": "מסילת ישרים",                 "author": "הרמח\"ל",          "category": "מוסר"},
        {"id": "4902",  "title": "כוזרי",                       "author": "ר' יהודה הלוי",   "category": "פילוסופיה"},
        {"id": "14490", "title": "עין יעקב",                    "author": "ר' יעקב אבן חביב", "category": "אגדה"},
    ]
    return jsonify({"books": featured_books})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"🚀 שרת עולה על http://localhost:{port}")
    app.run(debug=False, host="0.0.0.0", port=port)
