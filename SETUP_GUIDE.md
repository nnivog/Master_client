# NEPSE Tunnel v31 — Complete Cloud Setup Guide
### Deploy to Railway (Free) · Access on iPhone & Web

---

## WHAT'S IN THIS PACKAGE

```
nepse_tunnel_cloud/
├── app.py               ← Main Flask server  [PATCHED]
├── portfolio.py         ← Portfolio & DB management  [PATCHED]
├── cache.py             ← Broker floorsheet cache  [PATCHED]
├── data_fetcher.py      ← Web scraper (unchanged)
├── analysis.py          ← Technical analysis (unchanged)
├── ns_fetcher.py        ← NepalStock fetcher (unchanged)
├── static/
│   ├── index.html       ← Full UI (unchanged)
│   └── sample_portfolio_import.csv
├── Procfile             ← Tells Railway how to start the app
├── runtime.txt          ← Python 3.11.9
├── requirements.txt     ← Python dependencies (added gunicorn)
├── railway.json         ← Railway auto-config
└── .gitignore           ← Excludes .db files from git
```

### What was patched and why:
| File | Change |
|------|--------|
| `app.py` | Added `if __name__ == "__main__"` with `PORT` env var |
| `portfolio.py` | DB path reads `NEPSE_DATA_DIR` env → persistent volume |
| `cache.py` | Cache path reads `NEPSE_DATA_DIR` env → persistent volume |
| `requirements.txt` | Added `gunicorn`, removed `pyinstaller` |

---

## STEP 1 — Create a GitHub Account (skip if you have one)

1. Go to **https://github.com** → click **Sign up**
2. Enter email, password, username → verify email
3. Choose the **Free** plan

---

## STEP 2 — Create a Private GitHub Repository

1. On GitHub, click the **+** button (top right) → **New repository**
2. Repository name: `nepse-tunnel`
3. Set to **Private** (important — your portfolio data connects here)
4. ✅ Check **"Add a README file"**
5. Click **Create repository**

---

## STEP 3 — Upload Your Files to GitHub

### Option A — Web Upload (easiest, no Git needed):

1. Open your new repository on GitHub
2. Click **Add file** → **Upload files**
3. Drag and drop ALL files from this package:
   - `app.py`, `portfolio.py`, `cache.py`, `data_fetcher.py`
   - `analysis.py`, `ns_fetcher.py`
   - `requirements.txt`, `Procfile`, `runtime.txt`, `railway.json`, `.gitignore`
4. Also upload the `static/` folder:
   - Click **Add file** → **Upload files** again
   - Drag `static/index.html` and `static/sample_portfolio_import.csv`
   - In the commit box, type path: `static/index.html`
   - GitHub will auto-create the folder
5. Commit message: `Deploy NEPSE Tunnel v31`
6. Click **Commit changes**

### Option B — Git command line (if you have Git installed):
```bash
git clone https://github.com/YOUR_USERNAME/nepse-tunnel.git
cd nepse-tunnel
# Copy all files from this package into this folder
git add .
git commit -m "Deploy NEPSE Tunnel v31"
git push
```

---

## STEP 4 — Create Railway Account

1. Go to **https://railway.app**
2. Click **Login** → **Login with GitHub**
3. Authorize Railway to access your GitHub
4. You're in — Railway gives you **$5 free credit/month**
   (enough for a small always-on app)

---

## STEP 5 — Deploy on Railway

1. In Railway dashboard, click **New Project**
2. Click **Deploy from GitHub repo**
3. Find and select your `nepse-tunnel` repository
4. Railway automatically detects Python and starts building
5. Wait 2–4 minutes for the build to complete
6. You'll see green **Active** status when done

---

## STEP 6 — Add Persistent Storage (CRITICAL for portfolio data)

Without this step, your portfolio data is lost every time Railway restarts the app.

1. In your Railway project, click **+ New** (top right)
2. Select **Volume**
3. Set:
   - **Mount Path:** `/data`
   - **Size:** 1 GB (free)
4. Click **Add**
5. Railway automatically links the volume to your app

---

## STEP 7 — Set Environment Variables

1. Click on your **app service** (not the volume)
2. Click the **Variables** tab
3. Click **New Variable** and add:

| Variable Name | Value |
|--------------|-------|
| `NEPSE_DATA_DIR` | `/data` |
| `FLASK_DEBUG` | `0` |

4. Click **Add** after each one
5. Railway automatically redeploys when you add variables (~1 minute)

---

## STEP 8 — Get Your Public URL

1. Click on your app service
2. Go to **Settings** tab
3. Under **Networking**, click **Generate Domain**
4. You get a URL like: `https://nepse-tunnel-production.up.railway.app`

**That's your app — open it in any browser, anywhere!**

---

## STEP 9 — Open on iPhone (Safari)

1. Open your Railway URL in **Safari** on iPhone
2. The full NEPSE Tunnel interface loads

### Add to Home Screen (makes it feel like a native app):
1. Tap the **Share** button (box with upward arrow, bottom of Safari)
2. Scroll down → tap **Add to Home Screen**
3. Name: `NEPSE Tunnel`
4. Tap **Add**

Now it appears on your home screen, opens full-screen with no browser bar,
exactly like a native iOS app.

---

## STEP 10 — Import Your Existing Portfolio Data (optional)

If you have an existing `portfolio.db` from your Windows app:

1. Open your Railway URL
2. Go to **Portfolio** tab
3. Use **Import Transactions (CSV)** with the sample format, OR
4. Use **Import JSON** if you exported from the desktop app first

---

## DAILY USE

Just open: `https://your-app.up.railway.app` (or tap home screen icon on iPhone)

The app:
- Scrapes live NEPSE data on demand
- Stores your portfolio permanently in `/data/portfolio.db`
- Caches broker floorsheets in `/data/cache/`
- Never loses data across restarts

---

## TROUBLESHOOTING

### App won't start / build fails
- Check Railway **Logs** tab for the error
- Most common: a missing dependency → add it to `requirements.txt`

### "Internal Server Error" on first load
- Check Logs → usually a scraping timeout on first cold start
- Refresh the page — second load is always faster

### Portfolio data disappeared
- You likely forgot Step 6 (persistent volume) or Step 7 (env variable)
- Re-do Steps 6 & 7, then re-import your data

### Scraping returns empty results
- NEPSE sites may be down or blocking the server IP
- Try Railway region: **Settings → Region → Asia (Singapore)**
  (closer to Nepal, less likely to be blocked)

### Free tier ran out
- Railway gives $5/month. A 2-worker gunicorn app costs ~$3–4/month
- Upgrade to Railway Hobby ($5/month flat) for guaranteed uptime
- Or switch to **Render.com** free tier (750 hrs/month, same setup process)

---

## RENDER.COM ALTERNATIVE (also free)

If you prefer Render:
1. Go to **https://render.com** → New → **Web Service**
2. Connect your GitHub repo
3. Build Command: `pip install -r requirements.txt`
4. Start Command: `gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120`
5. Under **Advanced** → **Add Disk**: mount path `/data`, size 1 GB
6. Environment: `NEPSE_DATA_DIR=/data`

⚠️ Render free tier **spins down after 15 min idle** — first load takes ~30 seconds.
Railway does NOT spin down, making it better for this app.

---

## BACKING UP YOUR DATA

Your portfolio DB lives at `/data/portfolio.db` on the Railway server.
To back it up:

1. In the app, go to **Portfolio** → **Export (JSON)**
2. Download the JSON file to your phone/PC
3. Re-import it anytime with **Import (JSON)**

Do this weekly if you actively trade.

---

*NEPSE Tunnel v31 — Cloud Edition*
