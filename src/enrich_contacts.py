# src/enrich_contacts.py
# Enrich leads (Reddit + YouTube) with website + public emails.
# - Reddit: load submission, extract any external links, crawl + /contact
# - YouTube: extract URLs from the description we stored in 'excerpt', crawl + /contact

import os, csv, re, requests
from bs4 import BeautifulSoup
import praw
from urllib.parse import urljoin

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
URL_RE = re.compile(r"https?://[^\s)\]]+")

# Reddit client
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT", "steno-leads/1.0"),
)

def extract_post_id(permalink: str):
    m = re.search(r"/comments/([a-z0-9]+)/", permalink)
    return m.group(1) if m else None

def find_urls(text: str):
    if not text: return []
    urls = URL_RE.findall(text)
    urls = [u.rstrip(").,]") for u in urls if "reddit.com" not in u and "redd.it" not in u]
    # unique order
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            out.append(u); seen.add(u)
    return out

def fetch_page(url: str):
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")
        title = soup.title.text.strip() if soup.title else ""
        emails = set(EMAIL_RE.findall(r.text))
        for a in soup.find_all("a", href=True):
            if a["href"].lower().startswith("mailto:"):
                emails.add(a["href"][7:])
        return title, sorted(emails)
    except Exception:
        return "", []

def try_contact_page(base_url: str):
    try:
        contact_url = urljoin(base_url if base_url.endswith("/") else base_url + "/", "contact")
        r = requests.get(contact_url, timeout=10, headers={"User-Agent":"Mozilla/5.0"})
        if r.status_code < 400:
            emails = set(EMAIL_RE.findall(r.text))
            return contact_url, sorted(emails)
    except Exception:
        pass
    return "", []

raw_path = "data/leads_raw.csv"
enriched_path = "data/leads_enriched.csv"
os.makedirs("data", exist_ok=True)

if not os.path.exists(raw_path):
    with open(enriched_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "platform","subreddit","url","author_handle","title","excerpt",
            "evidence_quote","score","created_utc","website","contact_url","email","company"
        ])
        w.writeheader()
    print("No leads_raw.csv found; wrote empty leads_enriched.csv")
    raise SystemExit(0)

rows = []
with open(raw_path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for r in reader:
        rows.append(r)

out_rows = []
for r in rows:
    website = ""
    contact_url = ""
    email_list = []
    company = ""

    links = []

    if r.get("platform") == "reddit":
        # Load full Reddit submission to find outbound links
        post_id = extract_post_id(r.get("url",""))
        try:
            if post_id:
                subm = reddit.submission(id=post_id)
                if getattr(subm, "is_self", True) is False and subm.url:
                    links.append(subm.url)
                links.extend(find_urls(getattr(subm, "selftext", "")))
        except Exception:
            pass
    else:
        # Non-Reddit (e.g., YouTube): scan the 'excerpt' (we stored video description there)
        links.extend(find_urls(r.get("excerpt","")))

    # clean + cap
    seen = set(); cleaned = []
    for u in links:
        if "reddit.com" in u or "redd.it" in u:
            continue
        if u not in seen:
            cleaned.append(u); seen.add(u)
    links = cleaned[:2]

    # Visit up to 2 links
    for idx, link in enumerate(links):
        title, emails = fetch_page(link)
        if title and not company:
            company = title[:200]
        if emails:
            email_list.extend(emails)
        if not website:
            website = link
        if idx == 0:
            c_url, c_emails = try_contact_page(link)
            if c_url:
                contact_url = c_url
            if c_emails:
                email_list.extend(c_emails)

    email_list = sorted(set(email_list))

    out = dict(r)
    out.update({
        "website": website,
        "contact_url": contact_url,
        "email": ";".join(email_list[:3]),
        "company": company,
    })
    out_rows.append(out)

with open(enriched_path, "w", newline="", encoding="utf-8") as f:
    fieldnames = [
        "platform","subreddit","url","author_handle","title","excerpt",
        "evidence_quote","score","created_utc","website","contact_url","email","company"
    ]
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(out_rows)

print(f"Enriched {len(out_rows)} rows → {enriched_path}")
