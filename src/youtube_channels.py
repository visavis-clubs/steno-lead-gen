# src/youtube_channels.py
# Search YouTube CHANNELS (not videos) for “teacher/offer” signals.
# For each matched channel, collect subs + channel description + average views on last 10 uploads.
# Append rows to data/leads_raw.csv with the same columns your pipeline expects.

import os, csv, re, math
from datetime import datetime, timezone
from googleapiclient.discovery import build

DST = "data/leads_raw.csv"
LOOKBACK_UPLOADS = 10         # how many recent uploads to gauge average views
MAX_CHANNELS_PER_QUERY = 15   # per phrase
MIN_SUBS = 10000              # floor to avoid tiny channels (tune later)

# Load phrases
def load_lines(path):
    with open(path, encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]

TEACHER_SIG = load_lines("config/teacher_signals.txt")

# Build “channel intent” queries by combining verticals with offer/authority words
VERTICALS = [
    "sales","public speaking","marketing","copywriting","entrepreneurship",
    "real estate","fitness","nutrition","productivity","mindset","leadership","life coaching","career coaching"
]
OFFERS = [
    "coaching","program","bootcamp","masterclass","workshop","academy","mentorship","framework","method","blueprint"
]
BASE_QUERIES = sorted({f"{v} {o}" for v in VERTICALS for o in OFFERS})

YT = build("youtube","v3",developerKey=os.getenv("YT_API_KEY"))

def search_channels(q):
    res = YT.search().list(
        q=q, part="id,snippet", type="channel", maxResults=50, order="relevance"
    ).execute()
    ch_ids = [it["id"]["channelId"] for it in res.get("items", [])][:MAX_CHANNELS_PER_QUERY]
    return ch_ids

def get_channel_details(ids):
    if not ids: return []
    res = YT.channels().list(
        id=",".join(ids), part="snippet,statistics,contentDetails"
    ).execute()
    return res.get("items", [])

def get_last_upload_ids(uploads_playlist_id, n=LOOKBACK_UPLOADS):
    vids=[]
    if not uploads_playlist_id: return vids
    res = YT.playlistItems().list(
        playlistId=uploads_playlist_id, part="contentDetails", maxResults=n
    ).execute()
    for it in res.get("items", []):
        vids.append(it["contentDetails"]["videoId"])
    return vids

def get_video_stats(ids):
    if not ids: return {}
    res = YT.videos().list(id=",".join(ids), part="statistics").execute()
    out={}
    for it in res.get("items", []):
        out[it["id"]] = int(it.get("statistics", {}).get("viewCount", "0") or 0)
    return out

def matches_teacher_signals(text):
    t = (text or "").lower()
    # require at least one "offer/CTA/platform/authority" token
    return any(sig.lower() in t for sig in TEACHER_SIG)

def append_rows(rows):
    os.makedirs("data", exist_ok=True)
    write_header = not os.path.exists(DST)
    fields = ["platform","subreddit","url","author_handle","title",
              "excerpt","evidence_quote","score","created_utc"]
    with open(DST, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            w.writeheader()
        for r in rows:
            w.writerow(r)

all_rows=[]
seen=set()

for q in BASE_QUERIES:
    try:
        ch_ids = search_channels(q)
        details = get_channel_details(ch_ids)
        for ch in details:
            cid = ch["id"]
            if cid in seen: 
                continue
            seen.add(cid)

            sn = ch.get("snippet", {})
            stats = ch.get("statistics", {})
            subs = int(stats.get("subscriberCount","0") or 0)
            desc = sn.get("description","")
            title = sn.get("title","")

            # quick filters
            if subs < MIN_SUBS:
                continue
            if not matches_teacher_signals(desc + " " + title):
                continue

            uploads = ch.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
            vid_ids = get_last_upload_ids(uploads, LOOKBACK_UPLOADS)
            vstats = get_video_stats(vid_ids)
            avg_views = int(round(sum(vstats.values())/max(len(vstats),1))) if vstats else 0

            # evidence = the specific phrase that matched (best-effort)
            ev = ""
            for sig in TEACHER_SIG:
                if sig.lower() in (desc + " " + title).lower():
                    ev = sig
                    break

            # Save one row per channel (timestamp now, so freshness can sort)
            all_rows.append({
                "platform": "youtube",
                "subreddit": title,                                # reuse this column for channel title
                "url": f"https://www.youtube.com/channel/{cid}",
                "author_handle": title,
                "title": f"Channel: {title} (avg_views:{avg_views})",
                "excerpt": desc[:300],
                "evidence_quote": ev,
                "score": str(subs),                                # subscribers
                "created_utc": int(datetime.now(timezone.utc).timestamp()),
            })
    except Exception:
        continue

append_rows(all_rows)
print(f"Channel rows added: {len(all_rows)} → {DST}")
