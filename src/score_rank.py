# src/score_rank.py
# Normalize, score, dedupe, and rank leads.
# Input:  data/leads_enriched.csv
# Output: data/leads_ready.csv
#
# Scoring components:
# - intent_score (1–5) from text signals
# - audience_bucket (YT subs / Reddit upvotes)
# - contactability_score (email/website present)
# - freshness_score (recent posts higher)
# - vertical_bonus (+1 if matches your allowlisted verticals)

import csv
import re
from urllib.parse import urlparse
from datetime import datetime, timezone
import os

IN_PATH  = "data/leads_enriched.csv"
OUT_PATH = "data/leads_ready.csv"

# ---------- helpers ----------
def read_csv(path):
    rows=[]
    with open(path, newline="", encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows

def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def domain_of(url):
    if not url:
        return ""
    try:
        netloc = urlparse(url).netloc.lower()
        return netloc[4:] if netloc.startswith("www.") else netloc
    except Exception:
        return ""

def to_int(x, default=0):
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return default

# ---------- load vertical allowlist (for bonus) ----------
def load_allow_terms():
    path = "config/verticals_allow.txt"
    try:
        with open(path, encoding="utf-8") as f:
            return [ln.strip().lower() for ln in f if ln.strip() and not ln.strip().startswith("#")]
    except Exception:
        return []
ALLOW_TERMS = load_allow_terms()

# ---------- scoring rules ----------
DIRECT = [
    "ai twin","ai of me","digital twin","chatbot of me","ai clone","clone my voice","voice clone",
    "personal ai","my ai assistant","ai of myself"
]
PAIN = [
    "scale q&a","answering the same questions","too many dms","24/7 answers","automate q&a",
    "community q&a","knowledge base of my content","course support bot"
]
PLATFORM_CUES = [
    "kajabi","skool","mighty networks","mighty","webflow","wordpress","teachable","thinkific",
    "circle.so","discord","slack","cohort","community platform","course platform","support bot","chatbot"
]

def intent_score(text):
    t = (text or "").lower()
    if any(p in t for p in DIRECT):  return 5
    if any(p in t for p in PAIN):    return 4
    if any(p in t for p in PLATFORM_CUES) and ("bot" in t or "assistant" in t or "automation" in t or "chatbot" in t):
        return 3
    if any(p in t for p in PLATFORM_CUES):  return 2
    return 1

def audience_bucket(platform, raw_score):
    s = to_int(raw_score, 0)
    if (platform or "").lower() == "youtube":
        if s >= 100_000: return 3
        if s >= 10_000:  return 2
        if s >= 1_000:   return 1
        return 0
    # reddit upvotes
    if s >= 50:  return 2
    if s >= 10:  return 1
    return 0

def contactability_score(email, website, contact_url):
    if (email or "").strip(): return 2
    if (contact_url or website): return 1
    return 0

def recency_score(created_utc):
    try:
        ts = to_int(created_utc, 0)
        if ts <= 0: return 0
        days = (datetime.now(timezone.utc) - datetime.fromtimestamp(ts, tz=timezone.utc)).days
        if days <= 3:  return 2
        if days <= 7:  return 1
        return 0
    except Exception:
        return 0

def build_text_index(row):
    return " ".join([
        row.get("title",""), row.get("excerpt",""), row.get("evidence_quote","")
    ])

def make_prospect_name(row):
    h = (row.get("author_handle") or "").strip()
    if h.startswith("u/"): return h[2:]
    return h

def dedupe_key(row):
    email = (row.get("email") or "").strip().lower()
    if email: return ("email", email)
    dom = domain_of(row.get("website",""))
    if dom: return ("domain", dom)
    return ("handle", (row.get("platform",""), row.get("author_handle","")))

# ---------- main ----------
if not os.path.exists(IN_PATH):
    # create empty output with headers if needed
    write_csv(OUT_PATH, [], [
      "platform","prospect_name","company",
      "audience_type","audience_metric",
      "intent_score","contactability_score","freshness_score","total_score",
      "website","email","contact_url",
      "evidence_url","evidence_quote","created_date",
      "source_title","source_excerpt","author_handle"
    ])
    print(f"No {IN_PATH} found. Wrote empty {OUT_PATH}.")
    raise SystemExit(0)

rows = read_csv(IN_PATH)
scored = []

for r in rows:
    platform = (r.get("platform") or "").lower()
    aud_type  = "yt_subscribers" if platform == "youtube" else "reddit_upvotes"
    aud_raw   = r.get("score","") or "0"
    aud_bucket = audience_bucket(platform, aud_raw)

    text = build_text_index(r)
    i_score = intent_score(text)
    c_score = contactability_score(r.get("email",""), r.get("website",""), r.get("contact_url",""))
    f_score = recency_score(r.get("created_utc","0"))

    # vertical bonus (+1) if allowlisted term appears in evidence/title/excerpt
    v_text = f"{r.get('evidence_quote','')} {r.get('title','')} {r.get('excerpt','')}".lower()
    vertical_bonus = 1 if any(term in v_text for term in ALLOW_TERMS) else 0

    total = i_score*2 + aud_bucket + c_score + f_score + vertical_bonus

    prospect = make_prospect_name(r)
    created_iso = ""
    try:
        ts = to_int(r.get("created_utc","0"), 0)
        if ts: created_iso = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        created_iso = ""

    scored.append({
        "platform": r.get("platform",""),
        "prospect_name": prospect,
        "company": r.get("company",""),
        "audience_type": aud_type,
        "audience_metric": aud_raw,
        "intent_score": i_score,
        "contactability_score": c_score,
        "freshness_score": f_score,
        "total_score": total,
        "website": r.get("website",""),
        "email": r.get("email",""),
        "contact_url": r.get("contact_url",""),
        "evidence_url": r.get("url",""),
        "evidence_quote": r.get("evidence_quote",""),
        "created_date": created_iso,
        "source_title": r.get("title",""),
        "source_excerpt": (r.get("excerpt","") or "")[:300],
        "author_handle": r.get("author_handle",""),
    })

# Sort best-first
scored.sort(key=lambda x: x["total_score"], reverse=True)

# Dedupe keeping highest-scored
seen=set(); deduped=[]
for row in scored:
    key = dedupe_key(row)
    if key in seen:
        continue
    seen.add(key)
    deduped.append(row)

FIELDS = [
  "platform","prospect_name","company",
  "audience_type","audience_metric",
  "intent_score","contactability_score","freshness_score","total_score",
  "website","email","contact_url",
  "evidence_url","evidence_quote","created_date",
  "source_title","source_excerpt","author_handle"
]

write_csv(OUT_PATH, deduped, FIELDS)
print(f"Ranked {len(deduped)} unique leads → {OUT_PATH}")
