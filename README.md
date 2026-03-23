# 📚 אוצר הספרים — ספרייה יהודית דיגיטלית

## פריסה ב-Railway (חינמי)

### שלב 1 — GitHub
1. צור חשבון GitHub: https://github.com
2. צור repository חדש בשם `otzar-hasefarim`
3. העלה את כל הקבצים

```bash
git init
git add .
git commit -m "first commit"
git remote add origin https://github.com/YOUR_USERNAME/otzar-hasefarim.git
git push -u origin main
```

### שלב 2 — Railway
1. כנס ל: https://railway.app
2. לחץ "Start a New Project"
3. בחר "Deploy from GitHub repo"
4. בחר את ה-repo שיצרת
5. Railway מזהה Flask אוטומטית ✅
6. לחץ "Deploy" — האתר עולה תוך ~2 דקות!

### שלב 3 — קבל קישור
Railway יתן לך URL כמו: `https://otzar-hasefarim.up.railway.app`

---

## הרצה מקומית

```bash
pip install -r requirements.txt
python server.py
# פתח http://localhost:5000
```

## מבנה הקבצים
```
├── server.py          # Flask backend + proxy
├── static/
│   └── index.html     # הממשק הגרפי
├── requirements.txt   # תלויות Python
├── Procfile           # הוראות הרצה ל-Railway
└── runtime.txt        # גרסת Python
```
