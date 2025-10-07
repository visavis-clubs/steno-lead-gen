# src/youtube_harvest.py
# High-volume YouTube *video* harvester (business/life/sales coaches).
# Builds queries from allowlisted verticals + offer/authority words.
# Paginates and respects a daily quota budget via env vars.

import os, csv, re, random
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build

DST = "data/leads_raw.csv"

# ---------- Tunables via ENV ----------
LOOKBACK_DAYS = int(os.getenv("YT_SEARCH_WINDOW_DAYS", "30"))   # how far back to search
MAX_QUERIES    = int(os.getenv("YT_MAX_SEARCH_QUERIES", "80"))  # # of search queries per run
MAX_PAGES      = int(os.getenv("YT_MAX_PAGES_PER_QUERY", "1"))  # pages per query (1 page = 50 videos)
MIN_SUBS       = int(os.getenv("YT_MIN_SUBS", "10000"))         # min channel subs
DEBUG          = os.getenv("DEBUG_YT", "0") == "1"

# ---------- Helpers ----------
def load_lines(path):
    with open(path, encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]

ALLOW   = [t.lower() for t in load_lines("config/verticals_allow.txt")]   # what we want
BLOCK   = [t.lower() for t in load_lines("config/verticals_block.txt")]   # what we don't want
TEACHER = [t.lower() for t in load_lines("config/teacher_signals.txt")]   # program/offer words

OFFERS = ["coaching","coach","consulting","consultant","mentorship","program","bootcamp",
          "masterclass","workshop","academy","framework","method","blueprint","playbook",
          "roadmap","system","webinar","challenge","cohort","membership","course"]

# Build discovery queries like "life coaching program", "sales coaching framework", etc.
BASE_QUERIES = sorted({f"{v} {o}" for v in ALLOW for o in OFFERS})

# Rotate / sample queries deterministically per day
random.seed(datetime.utcnow().date().toordinal())
random.shuffle(BASE_QUERIES)
QUERY_BATCH = BASE_QUERIES[:MAX_QUERIES]

YT = build("youtube", "v3", developerKey=os.getenv("YT_API_KEY"))
published_after = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).isoformat().replace("+00:00","Z")

URL_RE = re.compile(r"https?://[^\s)\]]+")

def any_in(text, terms):
    t = (text or "").lower()
    return any(term in t for term in terms)

def fetch_pages_for_query(q, max_pages=1):
    """Yield lists of search results (each up to 50 videos)."""
    next_token = None
    for _ in range(max_pages):
        res = YT.search().list(
            q=q, part="id,snippet", type="video", order="date",
            maxResults=50, publishedAfter=published_after, pageToken=next_token
        ).execute()
        items = res.get("items", [])
        if not items:
            break
        yield items
        next_token = res.get("nextPageToken")
        if not next_token:
            break

def fetch_video_and_channel(items):
    """Return rows with video + channel stats."""
    video_ids = [it["id"]["videoId"] for it in items if it["id"]["kind"]=="youtube#video"]
    if not video_ids:
        return []

    vres = YT.videos().list(id=",".join(video_ids), part="snippet,statistics").execute()
    vids = vres.get("items", [])
    chan_ids = list({v["snippet"]["channelId"] for v in vids})

    cmap = {}
    if chan_ids:
        cres = YT.channels().list(id=",".join(chan_ids), part="snippet,statistics").execute()
        cmap = {c["id"]: c for c in cres.get("items", [])}

    rows=[]
    for v in vids:
        vsn = v.get("snippet", {})
        vstats = v.get("statistics", {})
        cid = vsn.get("channelId","")
        ch = cmap.get(cid, {})
        ch_sn = ch.get("snippet", {})
        ch_stats = ch.get("statistics", {})

        subs = int(ch_stats.get("subscriberCount","0") or 0)
        ch_title = ch_sn.get("title","")
        ch_desc  = ch_sn.get("description","")
        v_title  = vsn.get("title","")
        v_desc   = vsn.get("description","")

        # Basic filtering
        blob = f"{ch_title}\n{ch_desc}\n{v_title}\n{v_desc}".lower()
        if subs < MIN_SUBS:
            if DEBUG: print(f"[skip subs<{MIN_SUBS}] {ch_title}")
            continue
        if any_in(blob, BLOCK):
            if DEBUG: print(f"[skip blocklist] {ch_title}")
            continue
        # Require vertical intent + teacher/offer signal somewhere in channel or video
        if not any_in(blob, ALLOW):
            if DEBUG: print(f"[skip no_vertical] {ch_title}")
            continue
        if not (any_in(blob, TEACHER) or any_in(blob, OFFERS)):
            if DEBUG: print(f"[skip no_teacher/offer] {ch_title}")
            continue

        # Evidence term (best-effort)
        evid = ""
        for terms in (ALLOW, TEACHER, OFFERS):
            for t in terms:
                if t in blob:
                    evid = t; break
            if evid: break

        # Convert publish time to epoch seconds
        ts = int(datetime.fromisoformat(vsn["publishedAt"].replace("Z","+00:00")).timestamp())

        rows.append({
            "platform": "youtube",
            "subreddit": ch_title,  # reuse column for channel title
            "url": f"https://www.youtube.com/watch?v={v['id']}",
            "author_handle": ch_title,
            "title": v_title[:200],
            "excerpt": (v_desc or "").replace("\n"," ")[:300],
            "evidence_quote": evid,
            "score": str(subs),  # subscribers
            "created_utc": ts,
        })
    return rows

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

all_rows=[]; seen_vid=set()
for q in QUERY_BATCH:
    try:
        if DEBUG: print(f"[query] {q}")
        page_num=0
        for items in fetch_pages_for_query(q, MAX_PAGES):
            page_num += 1
            rows = fetch_video_and_channel(items)
            # de-dupe by video id
            dedup=[]
            for r in rows:
                vid = r["url"].split("v=")[-1]
                if vid in seen_vid: continue
                seen_vid.add(vid)
                dedup.append(r)
            all_rows += dedup
            if DEBUG: print(f"  page {page_num}: {len(dedup)} rows")
    except Exception as e:
        if DEBUG: print(f"[query error] {q}: {e}")
        # most likely quota; stop gracefully
        break

append_rows(all_rows)
print(f"YouTube video rows added: {len(all_rows)} → {DST}")
