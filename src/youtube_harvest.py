# src/youtube_harvest.py
# Purpose: Search YouTube for your keywords, pull recent videos + channel stats,
# and APPEND rows to data/leads_raw.csv using the same columns as Reddit.

import os, csv, re
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build

LOOKBACK_DAYS = 14
MAX_RESULTS_PER_QUERY = 25

# Load keywords
with open("config/keywords.txt", encoding="utf-8") as f:
    KW = [k.strip() for k in f if k.strip()]

YT = build("youtube", "v3", developerKey=os.getenv("YT_API_KEY"))

published_after = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).isoformat().replace("+00:00","Z")

def fetch_for_query(q):
    # search videos by keyword
    res = YT.search().list(
        q=q, part="id,snippet", type="video", order="date",
        publishedAfter=published_after, maxResults=MAX_RESULTS_PER_QUERY
    ).execute()

    items = res.get("items", [])
    if not items: return []

    video_ids = [it["id"]["videoId"] for it in items if it["id"]["kind"]=="youtube#video"]
    chan_ids  = list({it["snippet"]["channelId"] for it in items})

    # channel stats
    chan_map = {}
    if chan_ids:
        c = YT.channels().list(id=",".join(chan_ids), part="snippet,statistics").execute()
        chan_map = {ci["id"]: ci for ci in c.get("items", [])}

    rows = []
    for it in items:
        vid = it["id"]["videoId"]
        sn  = it["snippet"]
        ch  = chan_map.get(sn["channelId"], {})
        ch_sn = ch.get("snippet", {})
        ch_stats = ch.get("statistics", {})

        desc = (sn.get("description") or "")
        # try to capture keyword as "evidence"
        m = re.search("|".join([re.escape(k) for k in KW]), desc, re.I)
        evidence = (m.group(0) if m else "")[:200]

        # convert publishedAt to epoch seconds
        ts = int(datetime.fromisoformat(sn["publishedAt"].replace("Z","+00:00")).timestamp())

        rows.append({
            "platform": "youtube",
            "subreddit": sn.get("channelTitle",""),     # reuse column to hold channel title
            "url": f"https://www.youtube.com/watch?v={vid}",
            "author_handle": sn.get("channelTitle",""),
            "title": sn.get("title","")[:200],
            "excerpt": desc.replace("\n"," ")[:300],
            "evidence_quote": evidence,
            "score": ch_stats.get("subscriberCount",""),
            "created_utc": ts,
        })
    return rows

all_rows = []
for q in KW:
    all_rows += fetch_for_query(q)

# ensure data folder
os.makedirs("data", exist_ok=True)
dst = "data/leads_raw.csv"

# append or create
fieldnames = [
    "platform","subreddit","url","author_handle","title",
    "excerpt","evidence_quote","score","created_utc"
]

write_header = not os.path.exists(dst)
with open(dst, "a", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    if write_header:
        w.writeheader()
    for r in all_rows:
        w.writerow(r)

print(f"YouTube rows added: {len(all_rows)} to {dst}")
