# scripts/crawl.py
import os, re, json, time, hashlib, datetime, urllib.parse
import requests
from bs4 import BeautifulSoup
from readability import Document
from markdownify import markdownify as md

# --- Config via env (set these in GitHub Actions Secrets) ---
START_URL = os.getenv("START_URL")           # e.g. https://highprobabilityselling.blog/
ARCHIVE_URL = os.getenv("ARCHIVE_URL", "")   # optional: explicit archive page
ALLOWED_DOMAINS = os.getenv("ALLOWED_DOMAINS", "")  # comma-separated, e.g. highprobabilityselling.blog
MAX_PAGES = int(os.getenv("MAX_PAGES", "2000"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.8"))  # politeness delay (sec)

OUT_DIR = "docs"
POSTS_DIR = os.path.join(OUT_DIR, "posts")
STATE_FILE = os.path.join(OUT_DIR, "index.json")   # public index of mirrored posts

HEADERS = {"User-Agent": "hps-blog-mirror/1.0 (+github actions)"}

os.makedirs(POSTS_DIR, exist_ok=True)

def norm_url(base, url):
    return urllib.parse.urljoin(base, url.split("#")[0].strip())

def in_scope(url):
    if not ALLOWED_DOMAINS: return True
    host = urllib.parse.urlparse(url).netloc.lower()
    domains = [d.strip().lower() for d in ALLOWED_DOMAINS.split(",") if d.strip()]
    return any(host.endswith(d) for d in domains)

def fetch(url):
    time.sleep(REQUEST_DELAY)
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def discover_links(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    for a in soup.find_all("a", href=True):
        u = norm_url(base_url, a["href"])
        if in_scope(u):
            links.add(u)
    return links

def looks_like_post(url):
    # Heuristics for post URLs (adjust as needed):
    # - has date-like path or multiple segments
    path = urllib.parse.urlparse(url).path
    if path.count("/") >= 2 and len(path) > 1 and not path.endswith((".xml", ".rss", ".pdf", ".jpg", ".png", ".gif", ".svg")):
        # exclude index/ category/ tag/ pages:
        if any(seg in path.lower() for seg in ["/tag/", "/category/", "/author/", "/page/"]):
            return False
        return True
    return False

def extract_post(html, url):
    doc = Document(html)
    title = (doc.short_title() or url).strip()
    content_html = doc.summary()
    soup = BeautifulSoup(content_html, "html.parser")
    # Remove scripts/styles/nav
    for tag in soup(["script","style","noscript","nav","header","footer","form"]): tag.decompose()
    content_html = str(soup)

    # Attempt to detect published date from meta tags
    published = ""
    full = BeautifulSoup(html, "html.parser")
    meta_candidates = [
        {"name":"article:published_time"},
        {"name":"og:updated_time"},
        {"name":"date"},
        {"property":"article:published_time"},
        {"itemprop":"datePublished"}
    ]
    for attrs in meta_candidates:
        m = full.find("meta", attrs=attrs)
        if m and (m.get("content") or m.get("value")):
            published = (m.get("content") or m.get("value")).strip()
            break

    content_md = md(content_html, strip=["img"])  # omit images for compactness; change if needed
    return title, content_md, published

def slugify(s):
    s = s.lower().strip()
    s = re.sub(r'[^a-z0-9]+', '-', s)
    return re.sub(r'-+', '-', s).strip('-')

def write_post(title, content_md, url, published):
    date_part = ""
    m = re.search(r'(20\\d{2}-\\d{2}-\\d{2})', published or "")
    if m: date_part = m.group(1) + "-"
    slug = date_part + slugify(title or "untitled")
    path_md = os.path.join(POSTS_DIR, f"{slug}.md")

    # ---- fix: avoid backslashes in f-string expressions ----
    safe_title = (title or "Untitled").replace('"', "'")  # swap double quotes for single
    # --------------------------------------------------------

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


def crawl():
    assert START_URL, "START_URL is required"
    start = START_URL if START_URL.endswith("/") else START_URL + "/"

    seen, queue = set([start]), [start]
    if ARCHIVE_URL:
        archive = norm_url(start, ARCHIVE_URL)
        if archive not in seen:
            seen.add(archive); queue.append(archive)

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
                # Only enqueue HTML pages (skip files)
                if not re.search(r'\\.(png|jpg|jpeg|gif|svg|pdf|zip|mp4|mp3|rss|xml)(\\?.*)?$', u, re.I):
                    queue.append(u)
            if looks_like_post(u):
                post_urls.add(u)

    print(f\"Discovered {len(post_urls)} candidate posts out of {pages_crawled} pages crawled.\")

    items = []
    for purl in sorted(post_urls):
        try:
            html = fetch(purl)
            title, content_md, published = extract_post(html, purl)
            item = write_post(title, content_md, purl, published)
            items.append(item)
        except Exception as e:
            print(\"Post error:\", purl, e)

    # Sort newest first by published (fallback title)
    def sort_key(x):
        return (x.get(\"published\") or \"\", x.get(\"title\") or \"\")
    items_sorted = sorted(items, key=sort_key, reverse=True)

    # Public index
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(STATE_FILE, \"w\", encoding=\"utf-8\") as f:
        json.dump(items_sorted, f, ensure_ascii=False, indent=2)

    # Simple homepage
    with open(os.path.join(OUT_DIR, \"index.md\"), \"w\", encoding=\"utf-8\") as f:
        f.write(\"# HPS Blog Mirror\\n\\n\")
        f.write(\"_Auto-synced by crawler. Newest first._\\n\\n\")
        for it in items_sorted:
            title = it[\"title\"]; url = it[\"url\"]; pub = it[\"published\"]; src = it[\"source_url\"]
            f.write(f\"- **[{title}]({url})**  \\n  {pub} · [Source]({src})\\n\")

if __name__ == \"__main__\":
    crawl()
