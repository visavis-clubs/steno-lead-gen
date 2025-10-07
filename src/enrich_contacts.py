# src/enrich_contacts.py
# Enrich leads (YouTube + Reddit) with website/contact URLs and public emails.
# Improvements:
# - Scans BOTH channel/video descriptions for external links (already in "excerpt")
# - Follows link-in-bio hubs (linktr.ee, beacons.ai, solo.to, withkoji, carrd.co)
# - Crawls common pages: /contact, /about, /press, /media, /speaking, /partner(s), /work-with-me, /book, /privacy, /terms
# - Extracts mailto: and plain emails; ranks best mailbox (speaking/press/partnerships > founder > info/support)
# - Gentle timeouts; resilient to failures

import os, csv, re, time, requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import praw

RAW_PATH = "data/leads_raw.csv"
ENRICHED_PATH = "data/leads_enriched.csv"

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
URL_RE = re.compile(r"https?://[^\s)\]]+")
HUB_DOMAINS = ("linktr.ee", "beacons.ai", "solo.to", "withkoji", "koji.to", "carrd.co", "tap.bio", "shor.by")

# Reddit client for grabbing full selftext when needed
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT", "steno-leads/1.0"),
)

def safe_get(url, timeout=10):
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0"})
        if r.status_code < 400:
            return r.text
    except Exception:
        return ""
    return ""

def extract_urls(text):
    if not text: return []
    urls = [u.rstrip(").,]") for u in URL_RE.findall(text)]
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            out.append(u); seen.add(u)
    return out

def extract_emails(html):
    if not html: return set()
    emails = set(EMAIL_RE.findall(html))
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        if a["href"].lower().startswith("mailto:"):
            emails.add(a["href"][7:])
    return set(e.strip() for e in emails if not e.lower().endswith("@example.com"))

def page_title(html):
    try:
        soup = BeautifulSoup(html, "html.parser")
        if soup.title and soup.title.text:
            return soup.title.text.strip()[:200]
    except Exception:
        pass
    return ""

def add_trailing_slash(u):  # help urljoin behave
    return u if u.endswith("/") else u + "/"

COMMON_PATHS = [
    "contact","contact-us","about","about-us","press","media","speaking","speaker","partners","partnerships",
    "work-with-me","workwithme","book","booking","sponsor","sponsorship","privacy","privacy-policy","terms","support","help"
]

PREFER_ORDER = [
    "speaking","press","media","partnership","sponsor","booking",
    "ceo","founder","hello","team","contact","info","support"
]

def rank_email(em):
    e = em.lower()
    for i,kw in enumerate(PREFER_ORDER):
        if kw in e.split("@")[0]:
            return i  # lower is better
    return len(PREFER_ORDER) + (0 if not e.endswith("@gmail.com") else 1)

def hub_expand(url):
    """Fetch a link-in-bio page and return any outbound links it lists."""
    html = safe_get(url)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    links=[]
    for a in soup.find_all("a", href=True):
        href=a["href"]
        if href.startswith("mailto:"): continue
        if href.startswith("#"): continue
        if "reddit.com" in href: continue
        links.append(href)
    return links[:10]  # cap

def guess_candidate_pages(base_url):
    base = add_trailing_slash(base_url)
    return [urljoin(base, p) for p in COMMON_PATHS]

def domain(url):
    try:
        n = urlparse(url).netloc.lower()
        return n[4:] if n.startswith("www.") else n
    except Exception:
        return ""

def collect_from_site(site_url):
    """Visit homepage + common pages, return (emails, best_title, contact_url_if_any)"""
    emails=set(); title=""; contact_url=""
    html = safe_get(site_url)
    if html:
        title = page_title(html) or title
        emails |= extract_emails(html)
    for p in guess_candidate_pages(site_url):
        h = safe_get(p)
        if not h: continue
        emails |= extract_emails(h)
        if not contact_url and ("contact" in p or "speaking" in p or "press" in p) and EMAIL_RE.search(h):
            contact_url = p
        time.sleep(0.3)  # be polite
    return emails, title, contact_url

def reddit_links(permalink):
    m = re.search(r"/comments/([a-z0-9]+)/", permalink)
    if not m: return []
    sid = m.group(1)
    try:
        s = reddit.submission(id=sid)
        links=[]
        if getattr(s,"is_self",True) is False and s.url:
            links.append(s.url)
        links += extract_urls(getattr(s,"selftext",""))
        return links
    except Exception:
        return []

# ---- Read raw rows ----
if not os.path.exists(RAW_PATH):
    # produce empty enriched file if nothing to read
    with open(ENRICHED_PATH,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=[
            "platform","subreddit","url","author_handle","title","excerpt","evidence_quote",
            "score","created_utc","website","contact_url","email","company"
        ])
        w.writeheader()
    print("No leads_raw.csv found; wrote empty leads_enriched.csv")
    raise SystemExit(0)

rows=[]
with open(RAW_PATH, newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f): rows.append(r)

out_rows=[]
for r in rows:
    website=""; contact_url=""; company=""; best_email=""
    emails=set()
    # collect candidate links
    candidates=[]
    if (r.get("platform") or "").lower()=="reddit":
        candidates += reddit_links(r.get("url",""))
    else:
        # For YouTube rows we stored channel/video description in 'excerpt'
        candidates += extract_urls(r.get("excerpt",""))

    # Expand link-in-bio hubs
    expanded=[]
    for u in candidates:
        d = domain(u)
        if d.endswith(HUB_DOMAINS):
            expanded += hub_expand(u)
    candidates += expanded

    # Prefer non-social sites
    candidates = [u for u in candidates if not any(s in u for s in ["twitter.com","x.com","instagram.com","tiktok.com"])]
    # keep unique
    seen=set(); uniq=[]
    for u in candidates:
        if u not in seen:
            uniq.append(u); seen.add(u)
    candidates = uniq[:5]  # cap per lead

    # Visit candidates
    for i,link in enumerate(candidates):
        html = safe_get(link)
        if not html: continue
        if not website and domain(link):
            website = link
        company = company or page_title(html)
        emails |= extract_emails(html)

        # If this looks like a hub/landing page to a real site, pull common pages there too
        if domain(link) not in HUB_DOMAINS:
            e2, title2, c_url = collect_from_site(link)
            emails |= e2
            company = company or title2
            contact_url = contact_url or c_url
        time.sleep(0.3)

    # If nothing yet but we have a domain, brute-try common pages on root
    if not emails and website:
        e3, t3, c3 = collect_from_site(website)
        emails |= e3; company = company or t3; contact_url = contact_url or c3

    # Choose best email
    if emails:
        best_email = sorted(emails, key=rank_email)[0]

    out = dict(r)
    out.update({
        "website": website,
        "contact_url": contact_url,
        "email": best_email,
        "company": company or r.get("subreddit","") or r.get("author_handle",""),
    })
    out_rows.append(out)

# ---- write ----
os.makedirs("data", exist_ok=True)
with open(ENRICHED_PATH, "w", newline="", encoding="utf-8") as f:
    fieldnames = [
        "platform","subreddit","url","author_handle","title","excerpt",
        "evidence_quote","score","created_utc","website","contact_url","email","company"
    ]
    w=csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader(); w.writerows(out_rows)

print(f"Enriched {len(out_rows)} rows → {ENRICHED_PATH}")
