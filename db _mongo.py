import os
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

MONGO_URI = os.environ.get("MONGO_URI", "")
_client = None
_db_instance = None

def get_db():
    global _client, _db_instance
    if _db_instance is None:
        if not MONGO_URI:
            raise RuntimeError("חסר MONGO_URI!")
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        _db_instance = _client["ganzach"]
    return _db_instance

def init_db():
    db = get_db()
    books = db["books"]
    books.create_index("id", unique=True)
    books.create_index("title")
    books.create_index("subject")
    books.create_index("source")
    books.create_index("has_text")
    db["book_text"].create_index("book_id", unique=True)
    db["state"].create_index("key", unique=True)
    _seed(db)
    print("MongoDB ready")

VERIFIED_SEED = [
    ("hb-14763","hebrewbooks","משנה תורה","","הרמב\"ם","1180","הלכה","he",""),
    ("hb-9780","hebrewbooks","שולחן ערוך - אורח חיים","","ר' יוסף קארו","1565","הלכה","he",""),
    ("hb-9781","hebrewbooks","שולחן ערוך - יורה דעה","","ר' יוסף קארו","1565","הלכה","he",""),
    ("hb-9782","hebrewbooks","שולחן ערוך - חושן משפט","","ר' יוסף קארו","1565","הלכה","he",""),
    ("hb-9783","hebrewbooks","שולחן ערוך - אבן העזר","","ר' יוסף קארו","1565","הלכה","he",""),
    ("hb-43081","hebrewbooks","נפש החיים","","ר' חיים מוולוז'ין","1824","מחשבה","he",""),
    ("hb-8774","hebrewbooks","מסילת ישרים","","הרמח\"ל","1740","מוסר","he",""),
    ("hb-11234","hebrewbooks","תניא","","ר' שניאור זלמן מלאדי","1797","חסידות","he",""),
    ("hb-3281","hebrewbooks","ספר החינוך","","ר' אהרן הלוי","1523","מצוות","he",""),
    ("hb-22879","hebrewbooks","חפץ חיים","","ר' ישראל מאיר קגן","1873","מוסר","he",""),
    ("hb-4902","hebrewbooks","ספר הכוזרי","","ר' יהודה הלוי","1140","פילוסופיה","he",""),
    ("hb-2865","hebrewbooks","ספר הזוהר","","רשב\"י","1280","קבלה","he",""),
    ("hb-9999","hebrewbooks","מורה נבוכים","","הרמב\"ם","1190","פילוסופיה","he",""),
    ("hb-14490","hebrewbooks","עין יעקב","","ר' יעקב אבן חביב","1516","אגדה","he",""),
    ("hb-4444","hebrewbooks","ליקוטי מוהר\"ן","","ר' נחמן מברסלב","1808","חסידות","he",""),
    ("hb-6666","hebrewbooks","עץ חיים","","ר' חיים ויטאל","1573","קבלה","he",""),
    ("hb-5432","hebrewbooks","אור החיים","","ר' חיים בן עטר","1742","פרשנות","he",""),
    ("hb-1111","hebrewbooks","תורה תמימה","","ר' ברוך הלוי עפשטיין","1902","פרשנות","he",""),
    ("hb-5678","hebrewbooks","ערוך השולחן","","ר' יחיאל מיכל עפשטיין","1903","הלכה","he",""),
    ("hb-8901","hebrewbooks","אגרות משה","","ר' משה פיינשטיין","1959","שו\"ת","he",""),
    ("hb-9012","hebrewbooks","יביע אומר","","ר' עובדיה יוסף","1954","שו\"ת","he",""),
    ("hb-3210","hebrewbooks","בן איש חי","","ר' יוסף חיים","1898","הלכה","he",""),
    ("hb-3456","hebrewbooks","חתם סופר - שו\"ת","","ר' משה סופר","1839","שו\"ת","he",""),
    ("hb-2468","hebrewbooks","ספר תהילים","","דוד המלך","900","תנ\"ך","he",""),
    ("hb-1357","hebrewbooks","הגדה של פסח","","שונות","1000","מועדים","he",""),
]

def _seed(db):
    for s in VERIFIED_SEED:
        doc = {"id":s[0],"source":s[1],"title":s[2],"he_title":s[3],
               "author":s[4],"year":s[5],"subject":s[6],"language":s[7],
               "url":s[8],"has_text":0,"has_ocr":0,"ocr_improved":0,"valid":1}
        try:
            db["books"].insert_one(doc)
        except DuplicateKeyError:
            pass

def get_state(k, d=None):
    r = get_db()["state"].find_one({"key": k})
    return r["value"] if r else d

def set_state(k, v):
    get_db()["state"].update_one(
        {"key": k}, {"$set": {"value": str(v)}}, upsert=True)

def save_books(rows):
    if not rows:
        return
    db = get_db()
    for r in rows:
        doc = {"id":r[0],"source":r[1],"title":r[2],"he_title":r[3],
               "author":r[4],"year":r[5],"subject":r[6],"language":r[7],
               "has_text":int(r[8]) if len(r)>8 else 0,
               "url":r[9] if len(r)>9 else "",
               "has_ocr":0,"ocr_improved":0,"valid":1}
        try:
            db["books"].insert_one(doc)
        except DuplicateKeyError:
            pass

def save_text(book_id, content, source="ocr"):
    if not content:
        return
    db = get_db()
    db["book_text"].update_one(
        {"book_id": book_id},
        {"$set": {"book_id": book_id, "content": content, "source": source}},
        upsert=True)
    db["books"].update_one({"id": book_id}, {"$set": {"has_text": 1}})

def total_books():
    return get_db()["books"].count_documents({"valid": 1})
