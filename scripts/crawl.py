# scripts/crawl.py
import os
import re
import json
import time
import random
from urllib.parse import urlparse, urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup
from readability import Document
from markdownify import markdownify as md

# ----- Config from env/secrets -----
START_URL = os.getenv("START_URL")                  # e.g., https://highprobabilityselling.blog/
ARCHIVE_URL = os.getenv("ARCHIVE_URL", "")          # optional; comma-separated author/archive URLs
MAX_PAGES = int(os.getenv("MAX_PAGES", "2000"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.0"))

OUT_DIR = "docs"
POSTS_DIR = os.path.join(OUT_DIR, "posts")
STATE_FILE = os.path.join(OUT_DIR, "index.json")

# ----- Site scope & filters -----
BASE_HOST = urlparse(START_URL or "").netloc.lower()

SKIP_HOSTS = {
    "linkedin.com", "www.linkedin.com",
    "facebook.com", "www.facebook.com",
    "x.com", "twitter.com", "t.co",
    "instagram.com", "www.instagram.com",
    "youtube.com", "www.youtube.com",
    "medium.com", "pinterest.com", "www.pinterest.com"
}

# paths that are not content
SKIP_PATH_KEYWORDS = [
    "/tag/", "/wp-json/", "/feed", "/feeds",
    "/login", "/log-in", "/signin", "/sign-in", "/signup", "/join",
    "/user-agreement", "/privacy", "/cookie", "/cookies", "/legal", "/terms"
]

# Optional: constrain posts to specific prefixes if you know them (leave empty tuple to disable)
ALLOWED_PATH_PREFIXES = tuple()  # e.g., ("/blog/", "/posts/", "/202")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; HPSMirrorBot/1.0; +https://github.com/<your-username>/<your-repo>)"
}

os.makedirs(POSTS_DIR, exist_ok=True)

# ---------- Helpers ----------
def norm_url(base, url):
    return urljoin(base, (url or "").split("#")[0].strip())

def clean_url(u: str) -> str:
    """Strip query & fragment; normalize trailing slash (except for root)."""
    s = urlsplit(u)
    s = s._replace(query="", fragment="")
    cleaned = urlunsplit(s)
    if cleaned.endswith("/") and len(urlsplit(cleaned).path) > 1:
        cleaned = cleaned[:-1]
    return cleaned

def in_scope(url):
    """Allow relative or same-site links only (same host or subdomain)."""
    u = urlparse(url or "")
    host = (u.netloc or "").lower()
    if host == "" or host == BASE_HOST or (BASE_HOST and host.endswith("." + BASE_HOST)):
        return True
    return False

def fetch(url, tries=4):
    """Polite fetch with jitter + retries/backoff for 429/503/timeout."""
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

def is_listing(url: str) -> bool:
    """Author/archive/category/blog index pages that lead to posts."""
    p = urlparse(url).path.lower()
    return any(k in p for k in ["/author/", "/archive", "/category/", "/categories/", "/blog/", "/posts/"])

def discover_links(html, base_url):
    """
    Collect only same-site content links; strip queries; drop social/legal/auth/etc.
    """
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    for a in soup.find_all("a", href=True):
        href = (a["href"] or "").strip()
        if href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:") or href.startswith("tel:"):
            continue

        u = norm_url(base_url, href)
        if not in_scope(u):
            continue

        u = clean_url(u)
        pu = urlparse(u)

        if pu.netloc.lower() in SKIP_HOSTS:
            continue

        path = pu.path.lower()
        if any(k in path for k in SKIP_PATH_KEYWORDS):
            continue

        links.add(u)
    return links

def extract_listing_post_links(html, base_url):
    """
    From author/archive/category pages, aggressively pull likely post links.
    """
    soup = BeautifulSoup(html, "html.parser")
    hrefs = set()

    # common patterns on many blogs
    selectors = [
        "article a[rel='bookmark']",
        "article h2 a",
        "h2.entry-title a",
        "h3.entry-title a",
        ".post a[rel='bookmark']",
    ]
    for sel in selectors:
        for a in soup.select(sel):
            if a and a.get("href"):
                hrefs.add(norm_url(base_url, a["href"]))

    # fallback: any link inside an article
    for a in soup.select("article a[href]"):
        hrefs.add(norm_url(base_url, a["href"]))

    clean = set()
    for u in hrefs:
        if in_scope(u):
            u = clean_url(u)
            path = urlparse(u).path.lower()
            if not any(k in path for k in SKIP_PATH_KEYWORDS):
                clean.add(u)
    return clean

def looks_like_post(url):
    """
    Heuristic: treat any non-root page as a post candidate unless it’s a listing or binary.
    If ALLOWED_PATH_PREFIXES is set, require posts to start with one of those prefixes.
    """
    path = urlparse(url).path.lower()

    if ALLOWED_PATH_PREFIXES:
        if not path.startswith(ALLOWED_PATH_PREFIXES):
            return False

    # Exclude non-post sections
    if any(seg in path for seg in ["/tag/", "/author/", "/page/", "/wp-json/", "/category/"]):
        return False

    if path.endswith((
        ".xml", ".rss", ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico", ".webp", ".zip", ".mp4", ".mp3"
    )):
        return False

    return len(path.strip("/")) > 0

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

    content_md = md(content_html, strip=["img"])  # set strip=[] to keep images
    return title, content_md, published

def slugify(s):
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return re.sub(r"-+", "-", s).strip("-")

def write_post(title, content_md, url, published):
    date_part = ""
    m = re.search(r"(20\d{2}-\d{2}-\d{2})", (published or ""))
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

# ---------- Main ----------
def crawl():
    assert START_URL, "START_URL is required"

    start = START_URL if START_URL.endswith("/") else START_URL + "/"
    print("BASE_HOST:", BASE_HOST)

    seen = set([start])
    queue = [start]

    # Multi-archive seeds via comma-separated ARCHIVE_URL
    if ARCHIVE_URL:
        archives = [a.strip() for a in ARCHIVE_URL.split(",") if a.strip()]
        for raw in archives:
            archive = norm_url(start, raw)
            if archive and archive not in seen:
                seen.add(archive)
                queue.append(archive)
        print("Seeded archives:", archives)

    print("Initial queue size:", len(queue))

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

        # If this is a listing page, explicitly extract post links
        if is_listing(url):
            listing_links = extract_listing_post_links(html, url)
            for lu in listing_links:
                if looks_like_post(lu):
                    post_urls.add(lu)
                if lu not in seen and in_scope(lu) and (is_listing(lu) or looks_like_post(lu)):
                    seen.add(lu)
                    queue.append(lu)

        # Generic link discovery + classification
        links = discover_links(html, url)
        for u in links:
            is_post = looks_like_post(u)
            is_list = is_listing(u)

            if is_post:
                post_urls.add(u)

            # Only enqueue homepage, listing pages, or actual posts
            if u not in seen and in_scope(u):
                if u == start or is_list or is_post:
                    seen.add(u)
                    queue.append(u)

        if pages_crawled % 50 == 0:
            print(f"Progress: crawled={pages_crawled}, queue={len(queue)}, posts_found={len(post_urls)}")

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

    # newest first by published (then title)
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
            u = it["url"]
            pub = it["published"]
            src = it["source_url"]
            f.write(f"- **[{title}]({u})**  \n  {pub} · [Source]({src})\n")

if __name__ == "__main__":
    crawl()
