# src/reddit_harvest.py
# Purpose: Search Reddit posts across a list of subreddits using your keywords
# and write a CSV of potential leads with proof links. Skips invalid/private subs.

import os, csv, time, re
from datetime import datetime, timedelta, timezone

# ---- Dependencies (installed by GitHub Actions): praw ----
import praw
from prawcore.exceptions import Redirect, NotFound, Forbidden

LOOKBACK_DAYS = 14         # only consider recent posts
SEARCH_LIMIT_PER_COMBO = 50  # per (subreddit, keyword) combo
SLEEP_BETWEEN_CALLS = 0.2  # be gentle with the API

# --- Load config files ---
def load_lines(path):
    with open(path, encoding="utf-8") as f:
        lines = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
    return lines

KW = load_lines("config/keywords.txt")
SUBS = load_lines("config/subreddits.txt")

# Regex to find any keyword in text (used to pull an evidence snippet)
KW_RE = re.compile("|".join([re.escape(k) for k in KW]), re.I)

# --- Reddit API (values are injected via GitHub Secrets) ---
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT", "steno-leads/1.0"),
)

cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).timestamp())

rows = []
skipped = 0

for sub in SUBS:
    try:
        sr = reddit.subreddit(sub)

        # Force a fetch now; raises if the subreddit is invalid/private/banned
        _ = sr.id

        for kw in KW:
            # Search recent posts in this subreddit for this keyword
            for post in sr.search(kw, sort="new", time_filter="month", limit=SEARCH_LIMIT_PER_COMBO):
                if post.created_utc < cutoff_ts:
                    continue

                title = (post.title or "")
                body = (post.selftext or "")
                combined = f"{title}\n{body}"

                # Try to capture a tiny "evidence" fragment (the matched keyword/phrase)
                m = KW_RE.search(combined)
                evidence = (m.group(0) if m else "")[:200]

                rows.append({
                    "platform": "reddit",
                    "subreddit": sub,
                    "url": "https://www.reddit.com" + post.permalink,
                    "author_handle": f"u/{post.author.name}" if post.author else "",
                    "title": title[:200],
                    "excerpt": body.replace("\n", " ")[:300],
                    "evidence_quote": evidence,
                    "score": post.score,
                    "created_utc": int(post.created_utc),
                })

                time.sleep(SLEEP_BETWEEN_CALLS)

    except (Redirect, NotFound, Forbidden):
        print(f"[skip] r/{sub} is invalid, private, or banned")
        skipped += 1
        continue
    except Exception as e:
        print(f"[skip] r/{sub} unexpected error: {e}")
        skipped += 1
        continue

# --- Write CSV (always create the file, even if empty) ---
os.makedirs("data", exist_ok=True)
out_path = "data/leads_raw.csv"

fieldnames = [
    "platform","subreddit","url","author_handle","title",
    "excerpt","evidence_quote","score","created_utc"
]

with open(out_path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    for r in rows:
        w.writerow(r)

print(f"Subreddits processed: {len(SUBS)} (skipped: {skipped})")
print(f"Wrote {len(rows)} Reddit rows to {out_path}")
