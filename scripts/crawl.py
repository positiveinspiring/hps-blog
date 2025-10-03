# scripts/crawl.py
import os
import re
import json
import time
import random
import urllib.parse
import requests
from bs4 import BeautifulSoup
from readability import Document
from markdownify import markdownify as md

# ---- Config from secrets/env ----
START_URL = os.getenv("START_URL")                  # e.g., https://highprobabilityselling.blog/
ARCHIVE_URL = os.getenv("ARCHIVE_URL", "")          # optional
ALLOWED_DOMAINS = os.getenv("ALLOWED_DOMAINS", "")  # e.g., highprobabilityselling.blog
MAX_PAGES = int(os.getenv("MAX_PAGES", "2000"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.0"))

OUT_DIR = "docs"
POSTS_DIR = os.path.join(OUT_DIR, "posts")
STATE_FILE = os.path.join(OUT_DIR, "index.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; HPSMirrorBot/1.0; +https://github.com/<your-username>/<your-repo>)"
}

os.makedirs(POSTS_DIR, exist_ok=True)

# ---- helpers ----
def norm_url(base, url):
    return urllib.parse.urljoin(base, url.split("#")[0].strip())

def in_scope(url):
    if not ALLOWED_DOMAINS:
        return True
    host = urllib.parse.urlparse(url).netloc.lower()
    domains = [d.strip().lower() for d in ALLOWED_DOMAINS.split(",") if d.strip()]
    return any(host.endswith(d) for d in domains)

def fetch(url, tries=4):
    backoff = 1.0
    for attempt in range(1, tries + 1):
        try:
            time.sleep(REQUEST_DELAY + random.uniform(0, 0.6))
            r = requests.get(url, headers=HEADERS, timeout=40)
            if r.status_code in (429, 503):
                raise requests.HTTPError(f"Retryable status {r.status_code}")
            r.raise_for_status()
            return r.text
        except Exception as e:
            if attempt == tries:
                print(f"[FAIL] {url} after {tries} tries: {e}")
                raise
            print(f"[RETRY {attempt}/{tries}] {url}: {e}")
            time.sleep(backoff)
            backoff *= 2

def discover_links(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    for a in soup.find_all("a", href=True):
        u = norm_url(base_url, a["href"])
        if in_scope(u):
            links.add(u)
    return links

def looks_like_post(url):
    path = urllib.parse.urlparse(url).path.lower()
    if any(seg in path for seg in ["/tag/", "/category/", "/author/", "/page/", "/wp-json/"]):
        return False
    if path.endswith((".xml", ".rss", ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico")):
        return False
    return path.count("/") >= 2 and len(path) > 1

def extract_post(html, url):
    doc = Document(html)
    title = (doc.short_title() or url).strip()
    content_html = doc.summary()
    soup = BeautifulSoup(content_html, "html.parser")
    for tag in soup(["script", "style", "noscript", "nav", "header", "footer", "form"]):
        tag.decompose()
    content_html = str(soup)

    # try to find a publish date in meta
    published = ""
    full = BeautifulSoup(html, "html.parser")
    meta_candidates = [
        {"name": "article:published_time"},
        {"name": "og:updated_time"},
        {"name": "date"},
        {"property": "article:published_time"},
        {"itemprop": "datePublished"},
    ]
    for attrs in meta_candidates:
        m = full.find("meta", attrs=attrs)
        if m and (m.get("content") or m.get("value")):
            published = (m.get("content") or m.get("value")).strip()
            break

    content_md = md(content_html, strip=["img"])
    return title, content_md, published

def slugify(s):
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return re.sub(r"-+", "-", s).strip("-")

def write_post(title, content_md, url, published):
    # prepend date when detectable
    date_part = ""
    m = re.search(r"(20\\d{2}-\\d{2}-\\d{2})", published or "")
    if m:
        date_part = m.group(1) + "-"
    slug = date_part + slugify(title or "untitled")
    path_md = os.path.join(POSTS_DIR, f"{slug}.md")

    safe_title = (title or "Untitled").replace('"', "'")

    fm = [
        "---",
        f'title: "{safe_title}"',
        f"source_url: {url}",
        f"published: {published or ''}",
        "---",
        "",
    ]
    with open(path_md, "w", encoding="utf-8") as f:
        f.write("\n".join(fm) + content_md.strip() + "\n")
    return {"title": title, "url": f"posts/{slug}.md", "source_url": url, "published": published or ""}

# ---- main crawl ----
def crawl():
    assert START_URL, "START_URL is required"

    # define start FIRST
    start = START_URL if START_URL.endswith("/") else START_URL + "/"

    # init crawl state
    seen = set([start])
    queue = [start]

    # --- multi-archive seeds (comma-separated in ARCHIVE_URL) ---
    if ARCHIVE_URL:
        archives = [a.strip() for a in ARCHIVE_URL.split(",") if a.strip()]
        for raw in archives:
            archive = norm_url(start, raw)
            if archive and archive not in seen:
                seen.add(archive)
                queue.append(archive)
        print(f"Seeded archives: {archives}")
    # -------------------------------------------------------------



    post_urls = set()
    pages_crawled = 0

    while queue and pages_crawled < MAX_PAGES:
        url = queue.pop(0)
        try:
            html = fetch(url)
        except Exception as e:
            print("Fetch error:", url, e)
            continue

        pages_crawled += 1
        links = discover_links(html, url)
        for u in links:
            if u not in seen and in_scope(u):
                seen.add(u)
                if not re.search(r"\.(png|jpg|jpeg|gif|svg|pdf|zip|mp4|mp3|rss|xml)(\?.*)?$", u, re.I):
                    queue.append(u)
            if looks_like_post(u):
                post_urls.add(u)

    print(f"Discovered {len(post_urls)} candidate posts out of {pages_crawled} pages crawled.")

    items = []
    for purl in sorted(post_urls):
        try:
            html = fetch(purl)
            title, content_md, published = extract_post(html, purl)
            item = write_post(title, content_md, purl, published)
            items.append(item)
        except Exception as e:
            print("Post error:", purl, e)

    # newest first by published, then title
    def sort_key(x):
        return (x.get("published") or "", x.get("title") or "")
    items_sorted = sorted(items, key=sort_key, reverse=True)

    os.makedirs(OUT_DIR, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(items_sorted, f, ensure_ascii=False, indent=2)

    with open(os.path.join(OUT_DIR, "index.md"), "w", encoding="utf-8") as f:
        f.write("# HPS Blog Mirror\n\n")
        f.write("_Auto-synced by crawler. Newest first._\n\n")
        for it in items_sorted:
            title = it["title"]
            url = it["url"]
            pub = it["published"]
            src = it["source_url"]
            f.write(f"- **[{title}]({url})**  \n  {pub} · [Source]({src})\n")

if __name__ == "__main__":
    crawl()
