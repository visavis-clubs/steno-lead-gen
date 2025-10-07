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
        if getattr(s,"is_sel_
