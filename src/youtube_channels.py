# Quota-friendly YouTube CHANNEL hunter focused on business/life/sales/ETA.
# Limits: samples a fixed number of discovery queries per run (YT_MAX_QUERIES env, default 20).
# Filters by: allowlist terms, NOT blocklist terms, subs >= MIN_SUBS, avg views >= MIN_AVG_VIEWS.

import os, csv, re, random
from datetime import datetime, timezone
from googleapiclient.discovery import build

DST = "data/leads_raw.csv"
LOOKBACK_UPLOADS = 10
MAX_CHANNELS_PER_QUERY = 8
MIN_SUBS = 20000
MIN_AVG_VIEWS = 5000
MAX_QUERIES_PER_RUN = int(os.getenv("YT_MAX_QUERIES", "20"))

def load_lines(path):
    with open(path, encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]

TEACHER_SIG = [t.lower() for t in load_lines("config/teacher_signals.txt")]
ALLOW = [t.lower() for t in load_lines("config/verticals_allow.txt")]
BLOCK = [t.lower() for t in load_lines("config/verticals_block.txt")]

OFFERS = ["coaching","coach","consulting","mentorship","program","bootcamp","masterclass",
          "workshop","academy","framework","method","blueprint","playbook","roadmap","system",
          "webinar","challenge","cohort","membership"]

BASE_QUERIES = sorted({f"{v} {o}" for v in ALLOW for o in OFFERS})

# rotate queries daily (deterministic)
random.seed(datetime.utcnow().date().toordinal())
random.shuffle(BASE_QUERIES)
QUERY_BATCH = BASE_QUERIES[:MAX_QUERIES_PER_RUN]

YT = build("youtube","v3",developerKey=os.getenv("YT_API_KEY"))

def search_channels(q):
    res = YT.search().list(q=q, part="id,snippet", type="channel", maxResults=50, order="relevance").execute()
    return [it["id"]["channelId"] for it in res.get("items", [])][:MAX_CHANNELS_PER_QUERY]

def get_channel_details(ids):
    if not ids: return []
    res = YT.channels().list(id=",".join(ids), part="snippet,statistics,contentDetails").execute()
    return res.get("items", [])

def get_last_upload_ids(uploads_playlist_id, n=LOOKBACK_UPLOADS):
    vids=[]; 
    if not uploads_playlist_id: return vids
    res = YT.playlistItems().list(playlistId=uploads_playlist_id, part="contentDetails", maxResults=n).execute()
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
        if term in t: return term
    return ""

def append_rows(rows):
    os.makedirs("data", exist_ok=True)
    write_header = not os.path.exists(DST)
    fields = ["platform","subreddit","url","author_handle","title","excerpt","evidence_quote","score","created_utc"]
    with open(DST, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if write_header: w.writeheader()
        for r in rows: w.writerow(r)

all_rows=[]; seen=set()

for q in QUERY_BATCH:
    try:
        ch_ids = search_channels(q)
        details = get_channel_details(ch_ids)
        for ch in details:
            cid = ch["id"]
            if cid in seen: continue
            seen.add(cid)

            sn = ch.get("snippet", {}); stats = ch.get("statistics", {})
            subs = int(stats.get("subscriberCount","0") or 0)
            title = sn.get("title",""); desc = sn.get("description","")
            text = f"{title}\n{desc}".lower()

            if subs < MIN_SUBS: continue
            if text_has_any(text, BLOCK): continue
            if not text_has_any(text, ALLOW): continue
            if not text_has_any(text, TEACHER_SIG): continue

            uploads = ch.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
            vid_ids = get_last_upload_ids(uploads, LOOKBACK_UPLOADS)
            vstats = get_video_stats(vid_ids)
            avg_views = int(round(sum(vstats.values())/max(len(vstats),1))) if vstats else 0
            if avg_views < MIN_AVG_VIEWS: continue

            ev = matched_term(f"{title} {desc}", ALLOW) or matched_term(f"{title} {desc}", TEACHER_SIG)

            all_rows.append({
                "platform": "youtube",
                "subreddit": title,
                "url": f"https://www.youtube.com/channel/{cid}",
                "author_handle": title,
                "title": f"Channel: {title} (avg_views:{avg_views})",
                "excerpt": desc[:300],
                "evidence_quote": ev,
                "score": str(subs),
                "created_utc": int(datetime.now(timezone.utc).timestamp()),
            })
    except Exception:
        # If the key is out of quota mid-run, we just stop adding rows.
        break

append_rows(all_rows)
print(f"Channel rows added (focused, quota-limited): {len(all_rows)} → {DST}")
