# src/youtube_channels.py
# Channel-first hunter focused on business/life/sales/ETA. Skips blocklisted niches (e.g., fitness).
# Filters by: allowlist terms in channel title/description, NO blocklist terms,
# subs >= MIN_SUBS, avg views on last uploads >= MIN_AVG_VIEWS.

import os, csv, re
from datetime import datetime, timezone
from googleapiclient.discovery import build

DST = "data/leads_raw.csv"
LOOKBACK_UPLOADS = 10
MAX_CHANNELS_PER_QUERY = 15
MIN_SUBS = 20000           # raise/lower to taste
MIN_AVG_VIEWS = 5000       # helps avoid low-engagement channels

def load_lines(path):
    with open(path, encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]

TEACHER_SIG = load_lines("config/teacher_signals.txt")
ALLOW = [t.lower() for t in load_lines("config/verticals_allow.txt")]
BLOCK = [t.lower() for t in load_lines("config/verticals_block.txt")]

# Offer/authority words to combine with allowlist for discovery
OFFERS = [
    "coaching","coach","consulting","mentorship",
    "program","bootcamp","masterclass","workshop","academy",
    "framework","method","blueprint","playbook","roadmap","system",
    "webinar","challenge","cohort","membership"
]

# Build discovery queries like "life coaching program", "sales coaching framework", etc.
BASE_QUERIES = sorted({f"{v} {o}" for v in ALLOW for o in OFFERS})

YT = build("youtube","v3",developerKey=os.getenv("YT_API_KEY"))

def search_channels(q):
    res = YT.search().list(
        q=q, part="id,snippet", type="channel", maxResults=50, order="relevance"
    ).execute()
    return [it["id"]["channelId"] for it in res.get("items", [])][:MAX_CHANNELS_PER_QUERY]

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

def text_has_any(text, terms):
    t = (text or "").lower()
    return any(term in t for term in terms)

def matched_term(text, terms):
    t = (text or "").lower()
    for term in terms:
        if term in t:
            return term
    return ""

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
        for chunk_start in range(0, len(ch_ids), 50):
            details = get_channel_details(ch_ids[chunk_start:chunk_start+50])
            for ch in details:
                cid = ch["id"]
                if cid in seen: 
                    continue
                seen.add(cid)

                sn = ch.get("snippet", {})
                stats = ch.get("statistics", {})
                subs = int(stats.get("subscriberCount","0") or 0)
                title = sn.get("title","")
                desc = sn.get("description","")
                text = f"{title}\n{desc}".lower()

                # filters
                if subs < MIN_SUBS: 
                    continue
                if text_has_any(text, BLOCK):
                    continue
                if not text_has_any(text, ALLOW):
                    continue
                if not text_has_any(text, TEACHER_SIG):  # must look like a teacher/offer channel
                    continue

                uploads = ch.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
                vid_ids = get_last_upload_ids(uploads, LOOKBACK_UPLOADS)
                vstats = get_video_stats(vid_ids)
                avg_views = int(round(sum(vstats.values())/max(len(vstats),1))) if vstats else 0
                if avg_views < MIN_AVG_VIEWS:
                    continue

                ev_allow = matched_term(f"{title} {desc}", ALLOW)
                ev_sig   = matched_term(f"{title} {desc}", TEACHER_SIG)
                ev = ev_allow or ev_sig

                all_rows.append({
                    "platform": "youtube",
                    "subreddit": title,                                # channel title
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
print(f"Channel rows added (focused): {len(all_rows)} → {DST}")
