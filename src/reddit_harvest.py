# src/reddit_harvest.py
# Purpose: Search Reddit posts (by subreddit + keywords) and save matches with proof links.

import os, csv, time, re
from datetime import datetime, timedelta, timezone

# ---- Dependencies: PRAW (installed later by GitHub Actions) ----
import praw

# Load keywords and subreddits from config files
with open("config/keywords.txt", encoding="utf-8") as f:
    KW = [k.strip() for k in f if k.strip()]
with open("config/subreddits.txt", encoding="utf-8") as f:
    SUBS = [s.strip() for s in f if s.strip()]

KW_RE = re.compile("|".join([re.escape(k) for k in KW]), re.I)

# Reddit API credentials (you'll add these next step in GitHub Secrets)
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT", "steno-leads/1.0"),
)

# Only look back 14 days to keep results fresh
cutoff = int((datetime.now(timezone.utc) - timedelta(days=14)).timestamp())

rows = []
for sub in SUBS:
    sr = reddit.subreddit(sub)
    for kw in KW:
        # Search posts (titles + selftext)
        for post in sr.search(kw, sort="new", time_filter="month", limit=50):
            if post.created_utc < cutoff:
                continue

            # Try to pull an “evidence” snippet
            body = (post.title or "") + "\n" + (post.selftext or "")
            m = KW_RE.search(body)
            evidence = (m.group(0) if m else "")[:200]

            # Compose a row
            rows.append({
                "platform": "reddit",
                "subreddit": sub,
                "url": "https://www.reddit.com" + post.permalink,
                "author_handle": f"u/{post.author.name}" if post.author else "",
                "title": (post.title or "")[:200],
                "excerpt": (post.selftext or "").replace("\n", " ")[:300],
                "evidence_quote": evidence,
                "score": post.score,
                "created_utc": int(post.created_utc),
            })
            time.sleep(0.2)  # be gentle with the API

# Ensure data/ exists and write CSV
os.makedirs("data", exist_ok=True)
out_path = "data/leads_raw.csv"
with open(out_path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [
        "platform","subreddit","url","author_handle","title","excerpt","evidence_quote","score","created_utc"
    ])
    w.writeheader()
    if rows:
        w.writerows(rows)

print(f"Wrote {len(rows)} Reddit rows to {out_path}")
