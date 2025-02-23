import streamlit as st
import pandas as pd
import re
import asyncio
import nest_asyncio
import aiohttp
import orjson
import gc
import logging
import requests

from datetime import datetime
from typing import List, Dict, Tuple, Set, Optional
from collections import deque
from urllib.parse import urlparse, urlunparse, urljoin

from bs4 import BeautifulSoup

# For retry logic
from tenacity import retry, stop_after_attempt, wait_exponential

# For sitemap parsing
import xml.etree.ElementTree as ET

nest_asyncio.apply()

# --------------------------
# Logging Configuration
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='url_checker.log'
)

# --------------------------
# Constants / Defaults
# --------------------------
DEFAULT_TIMEOUT = 15
DEFAULT_CHUNK_SIZE = 100
DEFAULT_MAX_URLS = 25000
MAX_REDIRECTS = 5
DEFAULT_USER_AGENT = "custom_adidas_seo_x3423/1.0"

# Predefined user agents
USER_AGENTS = {
    "Googlebot Desktop": (
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    ),
    "Googlebot Mobile": (
        "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Mobile Safari/537.36 "
        "(compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    ),
    "Chrome Desktop": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.5481.100 Safari/537.36"
    ),
    "Chrome Mobile": (
        "Mozilla/5.0 (Linux; Android 10; Pixel 3) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.5481.100 Mobile Safari/537.36"
    ),
    "Custom Adidas SEO Bot": DEFAULT_USER_AGENT,
}


# --------------------------
# Helper: Remove any #fragment
# --------------------------
def normalize_url(url: str) -> str:
    """
    Remove #fragment so that URLs differing only by anchor are treated the same.
    """
    parsed = urlparse(url)
    return urlunparse(parsed._replace(fragment=""))


# -------------------------------------------------------------------
# URL Checker Class
# -------------------------------------------------------------------
class URLChecker:
    """
    A class that checks various SEO and technical aspects of URLs.
    Used in BFS or chunked processing.
    """

    def __init__(self, user_agent: str, concurrency: int = 10, timeout: int = 15):
        self.user_agent = user_agent
        self.max_concurrency = concurrency
        self.timeout_duration = timeout
        self.robots_cache = {}  # base_url -> robots.txt content or None
        self.connector = None
        self.session = None
        self.semaphore = None

    async def setup(self):
        """
        Prepare an aiohttp session with concurrency limits.
        """
        self.connector = aiohttp.TCPConnector(
            limit=self.max_concurrency,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            force_close=False
        )
        timeout = aiohttp.ClientTimeout(
            total=None,
            connect=self.timeout_duration,
            sock_read=self.timeout_duration
        )
        self.session = aiohttp.ClientSession(
            connector=self.connector,
            timeout=timeout,
            json_serialize=orjson.dumps
        )
        self.semaphore = asyncio.Semaphore(self.max_concurrency)

    async def close(self):
        """Close the aiohttp session."""
        if self.session:
            await self.session.close()

    # -------------------------
    # Robots.txt check
    # -------------------------
    async def check_robots_txt(self, url: str) -> Tuple[bool, str]:
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        path = parsed.path

        if base_url not in self.robots_cache:
            robots_url = f"{base_url}/robots.txt"
            try:
                headers = {"User-Agent": self.user_agent}
                async with self.session.get(robots_url, ssl=False, headers=headers) as resp:
                    if resp.status == 200:
                        content = await resp.text()
                        self.robots_cache[base_url] = content
                    else:
                        self.robots_cache[base_url] = None
            except:
                self.robots_cache[base_url] = None

        robots_content = self.robots_cache.get(base_url)
        if not robots_content:
            # No robots.txt => assume allowed
            return True, "N/A"

        # Parse robots
        return self.parse_robots(robots_content, path)

    def parse_robots(self, robots_content: str, path: str) -> Tuple[bool, str]:
        """
        Very basic parse: look for user-agent: * or matching agent lines,
        then check Disallow. Ignore 'Allow:' lines for simplicity.
        """
        lines = robots_content.splitlines()
        is_relevant_section = False
        block_rule = "N/A"
        path_lower = path.lower()
        ua_lower = self.user_agent.lower()

        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split(':', 1)
            if len(parts) < 2:
                continue

            field, value = parts[0].lower(), parts[1].strip().lower()

            if field == "user-agent":
                if value == '*' or ua_lower in value:
                    is_relevant_section = True
                else:
                    is_relevant_section = False

            elif field == "disallow" and is_relevant_section:
                dis_path = value
                if dis_path and path_lower.startswith(dis_path):
                    block_rule = f"Disallow: {dis_path}"
                    return False, block_rule

        return True, "N/A"

    # -------------------------
    # Main Fetch & Parse
    # -------------------------
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=10))
    async def fetch_and_parse(self, url: str, known_sitemap_date: str = "") -> Dict:
        """
        Fetch a URL (no auto-redirect), follow up to MAX_REDIRECTS manually.
        known_sitemap_date: lastmod from sitemap if available
        Return a dict with SEO data & last-modified info.
        """
        url = normalize_url(url)
        headers = {"User-Agent": self.user_agent}

        async with self.semaphore:
            try:
                # Check robots
                is_allowed, block_rule = await self.check_robots_txt(url)

                async with self.session.get(url, headers=headers, ssl=False, allow_redirects=False) as resp:
                    initial_status = resp.status
                    initial_type = self.get_status_type(initial_status)
                    location = resp.headers.get("Location")
                    final_url = str(resp.url)

                    # Follow redirect chain if needed
                    if initial_status in (301, 302, 307, 308) and location:
                        final_url, final_status, final_html, final_headers = await self.follow_redirect_chain(url, headers)
                        if isinstance(final_status, int) and final_status == 200 and final_html:
                            return await self.parse_html(
                                original_url=url,
                                current_url=final_url,
                                html_content=final_html,
                                status_code=final_status,
                                headers=final_headers,
                                is_allowed=is_allowed,
                                block_rule=block_rule,
                                initial_status=initial_status,
                                initial_type=initial_type,
                                known_sitemap_date=known_sitemap_date
                            )
                        else:
                            return self.build_non200_result(
                                original_url=url,
                                final_url=final_url,
                                final_status=final_status,
                                is_allowed=is_allowed,
                                block_rule=block_rule,
                                initial_status=initial_status,
                                initial_type=initial_type,
                                known_sitemap_date=known_sitemap_date
                            )
                    else:
                        # Not a redirect => parse if 200 & HTML
                        if initial_status == 200 and self.is_html_response(resp):
                            html_content = await resp.text(encoding='utf-8', errors='replace')
                            return await self.parse_html(
                                original_url=url,
                                current_url=final_url,
                                html_content=html_content,
                                status_code=initial_status,
                                headers=resp.headers,
                                is_allowed=is_allowed,
                                block_rule=block_rule,
                                initial_status=initial_status,
                                initial_type=initial_type,
                                known_sitemap_date=known_sitemap_date
                            )
                        else:
                            return self.build_non200_result(
                                original_url=url,
                                final_url=final_url,
                                final_status=initial_status,
                                is_allowed=is_allowed,
                                block_rule=block_rule,
                                initial_status=initial_status,
                                initial_type=initial_type,
                                known_sitemap_date=known_sitemap_date
                            )
            except asyncio.TimeoutError:
                return self.create_error_response(url, "Timeout", "Request timed out", False, known_sitemap_date)
            except Exception as e:
                return self.create_error_response(url, "Error", str(e), False, known_sitemap_date)

    async def follow_redirect_chain(self, start_url: str, headers: Dict) -> Tuple[str, int, str, Dict]:
        """
        Follow up to MAX_REDIRECTS. Return (final_url, final_status, final_html, final_headers).
        """
        current_url = normalize_url(start_url)
        html_content = None
        final_headers = {}

        for _ in range(MAX_REDIRECTS):
            async with self.session.get(current_url, headers=headers, ssl=False, allow_redirects=False) as resp:
                status = resp.status
                final_headers = resp.headers
                if self.is_html_response(resp):
                    html_content = await resp.text(encoding='utf-8', errors='replace')
                else:
                    html_content = None

                if status in (301, 302, 307, 308):
                    loc = resp.headers.get("Location")
                    if not loc:
                        return current_url, status, html_content, final_headers
                    current_url = normalize_url(urljoin(current_url, loc))
                else:
                    return current_url, status, html_content, final_headers

        return current_url, "Redirect Loop", html_content, final_headers

    async def parse_html(
        self,
        original_url: str,
        current_url: str,
        html_content: str,
        status_code: int,
        headers: Dict,
        is_allowed: bool,
        block_rule: str,
        initial_status: int,
        initial_type: str,
        known_sitemap_date: str
    ) -> Dict:
        soup = BeautifulSoup(html_content, "lxml")

        title = self.get_title(soup)
        meta_desc = self.get_meta_description(soup)
        h1_tags = soup.find_all("h1")
        h1_count = len(h1_tags)
        first_h1_text = h1_tags[0].get_text(strip=True) if h1_count > 0 else ""
        html_lang = self.get_html_lang(soup)

        canonical_url = self.get_canonical_url(soup)
        meta_robots = self.get_robots_meta(soup)
        x_robots_tag = headers.get("X-Robots-Tag", "")

        combined_robots = f"{meta_robots.lower()} {x_robots_tag.lower()}"
        has_noindex = "noindex" in combined_robots

        # Evaluate indexability
        canonical_matches = (not canonical_url) or (canonical_url == original_url)
        is_indexable = (status_code == 200 and is_allowed and not has_noindex and canonical_matches)

        reason_parts = []
        if status_code != 200:
            reason_parts.append("Non-200 status code")
        if not is_allowed:
            reason_parts.append("Blocked by robots.txt")
        if has_noindex:
            reason_parts.append("Noindex directive")
        if not canonical_matches:
            reason_parts.append("Canonical != Original URL")
        if not reason_parts:
            reason_parts.append("Page is indexable")

        # Last-Modified from HTTP header
        last_modified_header = headers.get("Last-Modified", "")

        data = {
            "Original_URL": original_url,
            "Initial_Status_Code": initial_status,
            "Initial_Status_Type": initial_type,
            "Final_URL": current_url,
            "Final_Status_Code": status_code,
            "Final_Status_Type": self.get_status_type(status_code),
            "Is_Blocked_by_Robots": "Yes" if not is_allowed else "No",
            "Robots_Block_Rule": block_rule,
            "Title": title,
            "Meta_Description": meta_desc,
            "H1_Text": first_h1_text,
            "H1_Count": h1_count,
            "Canonical_URL": canonical_url or "",
            "Meta_Robots": meta_robots,
            "X_Robots_Tag": x_robots_tag,
            "HTML_Lang": html_lang,
            "Is_Indexable": "Yes" if is_indexable else "No",
            "Indexability_Reason": "; ".join(reason_parts),
            "HTTP_Last_Modified": last_modified_header,
            "Sitemap_LastMod": known_sitemap_date,  # if available
            "Timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        return data

    def build_non200_result(
        self,
        original_url: str,
        final_url: str,
        final_status,
        is_allowed: bool,
        block_rule: str,
        initial_status: int,
        initial_type: str,
        known_sitemap_date: str
    ) -> Dict:
        status_type = (
            self.get_status_type(final_status)
            if isinstance(final_status, int)
            else str(final_status)
        )
        reason_parts = []
        if final_status != 200:
            reason_parts.append("Non-200 status code")
        if not is_allowed:
            reason_parts.append("Blocked by robots.txt")
        if not reason_parts:
            reason_parts.append("Page is indexable")

        return {
            "Original_URL": original_url,
            "Initial_Status_Code": initial_status,
            "Initial_Status_Type": initial_type,
            "Final_URL": final_url or "N/A",
            "Final_Status_Code": final_status,
            "Final_Status_Type": status_type,
            "Is_Blocked_by_Robots": "Yes" if not is_allowed else "No",
            "Robots_Block_Rule": block_rule,
            "Title": "",
            "Meta_Description": "",
            "H1_Text": "",
            "H1_Count": 0,
            "Canonical_URL": "",
            "Meta_Robots": "",
            "X_Robots_Tag": "",
            "HTML_Lang": "",
            "Is_Indexable": "No",
            "Indexability_Reason": "; ".join(reason_parts),
            "HTTP_Last_Modified": "",
            "Sitemap_LastMod": known_sitemap_date,
            "Timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

    def create_error_response(self, url: str, code: str, message: str, is_allowed: bool, known_sitemap_date: str) -> Dict:
        reason_parts = []
        if code != "200":
            reason_parts.append("Non-200 status code")
        if not is_allowed:
            reason_parts.append("Blocked by robots.txt")
        if not reason_parts:
            reason_parts.append("Page is indexable")

        return {
            "Original_URL": url,
            "Initial_Status_Code": code,
            "Initial_Status_Type": message,
            "Final_URL": "N/A",
            "Final_Status_Code": code,
            "Final_Status_Type": message,
            "Is_Blocked_by_Robots": "Yes" if not is_allowed else "No",
            "Robots_Block_Rule": "N/A",
            "Title": "",
            "Meta_Description": "",
            "H1_Text": "",
            "H1_Count": 0,
            "Canonical_URL": "",
            "Meta_Robots": "",
            "X_Robots_Tag": "",
            "HTML_Lang": "",
            "Is_Indexable": "No",
            "Indexability_Reason": "; ".join(reason_parts),
            "HTTP_Last_Modified": "",
            "Sitemap_LastMod": known_sitemap_date,
            "Timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

    # --------------------------------------------
    # Helpers
    # --------------------------------------------
    @staticmethod
    def is_html_response(resp: aiohttp.ClientResponse) -> bool:
        ctype = resp.content_type
        return ctype and ctype.startswith(("text/html", "application/xhtml+xml"))

    @staticmethod
    def get_status_type(status) -> str:
        if not isinstance(status, int):
            return str(status)
        codes = {
            200: "OK",
            301: "Permanent Redirect",
            302: "Temporary Redirect",
            307: "Temporary Redirect",
            308: "Permanent Redirect",
            404: "Not Found",
            403: "Forbidden",
            500: "Internal Server Error",
            503: "Service Unavailable"
        }
        return codes.get(status, f"Status Code {status}")

    @staticmethod
    def get_title(soup: BeautifulSoup) -> str:
        tag = soup.find("title")
        return tag.get_text(strip=True) if tag else ""

    @staticmethod
    def get_meta_description(soup: BeautifulSoup) -> str:
        tag = soup.find("meta", {"name": "description"})
        if tag and tag.has_attr("content"):
            return tag["content"]
        return ""

    @staticmethod
    def get_canonical_url(soup: BeautifulSoup) -> str:
        link = soup.find("link", {"rel": "canonical"})
        return link["href"] if link and link.has_attr("href") else ""

    @staticmethod
    def get_robots_meta(soup: BeautifulSoup) -> str:
        tag = soup.find("meta", {"name": "robots"})
        if tag and tag.has_attr("content"):
            return tag["content"]
        return ""

    @staticmethod
    def get_html_lang(soup: BeautifulSoup) -> str:
        html_tag = soup.find("html")
        if html_tag and html_tag.has_attr("lang"):
            return html_tag["lang"]
        return ""


# -----------------------------
# BFS Crawling (with optional depth)
# -----------------------------
async def bfs_crawl(
    seed_urls: List[Tuple[str, str]],  # list of (url, lastmod) pairs
    checker: URLChecker,
    max_depth: Optional[int] = None,
    max_urls: int = DEFAULT_MAX_URLS,
    show_partial_callback=None
) -> List[Dict]:
    """
    Perform BFS from multiple seed URLs (each with an optional known sitemap lastmod).
    If max_depth is None or 0 => unlimited depth.
    """
    visited: Set[str] = set()
    queue = deque()
    results = []

    # seed_urls is a list of (url, lastmod).
    # We'll parse domain from the first seed (assuming single domain BFS),
    # or skip domain-checking if you prefer multi-domain BFS.
    if not seed_urls:
        return []

    # Let's assume we do domain-limited BFS based on the first seed:
    first_domain = urlparse(seed_urls[0][0]).netloc.lower()

    # Populate queue with all seeds at depth=0
    for (u, lm) in seed_urls:
        queue.append((normalize_url(u.strip()), lm, 0))

    await checker.setup()

    while queue and len(visited) < max_urls:
        url, known_sitemap_date, depth = queue.popleft()

        if url in visited:
            continue
        visited.add(url)

        try:
            # BFS uses the known sitemap lastmod if present
            row = await checker.fetch_and_parse(url, known_sitemap_date=known_sitemap_date)
            results.append(row)

            # Only discover new links if we haven't hit the depth limit
            if max_depth is None or max_depth == 0 or depth < max_depth:
                discovered_links = await discover_links(url, checker.session, checker.user_agent)
                for link in discovered_links:
                    link_norm = normalize_url(link)
                    # domain-limited BFS
                    if urlparse(link_norm).netloc.lower() == first_domain:
                        if link_norm not in visited:
                            queue.append((link_norm, "", depth + 1))

        except Exception as e:
            logging.error(f"BFS error on {url}: {e}")

        # Partial UI update
        if show_partial_callback and len(results) % 10 == 0:
            show_partial_callback(results, len(visited), max_urls)

    await checker.close()
    return results

async def discover_links(url: str, session: aiohttp.ClientSession, user_agent: str) -> List[str]:
    """
    Minimal fetch to parse <a href> for BFS link discovery.
    """
    headers = {"User-Agent": user_agent}
    url = normalize_url(url)
    links = []

    try:
        async with session.get(url, headers=headers, ssl=False, allow_redirects=False) as resp:
            if resp.status == 200 and resp.content_type and resp.content_type.startswith("text/html"):
                html = await resp.text(errors="replace")
                soup = BeautifulSoup(html, "lxml")
                for tag in soup.find_all("a", href=True):
                    abs_link = urljoin(url, tag["href"])
                    links.append(abs_link)
    except:
        pass

    return links


# -----------------------------
# Chunked Processing (Non-BFS)
# -----------------------------
async def process_urls_chunked(
    url_pairs: List[Tuple[str, str]],
    checker: URLChecker,
    show_partial_callback=None
) -> List[Dict]:
    """
    url_pairs: list of (url, lastmod from sitemap if any)
    """
    results = []
    total = len(url_pairs)
    # Deduplicate by URL
    seen_urls = set()
    normalized_pairs = []
    for (u, lm) in url_pairs:
        nu = normalize_url(u.strip())
        if nu not in seen_urls:
            seen_urls.add(nu)
            normalized_pairs.append((nu, lm))

    chunks = [normalized_pairs[i : i + DEFAULT_CHUNK_SIZE] for i in range(0, len(normalized_pairs), DEFAULT_CHUNK_SIZE)]
    processed = 0

    await checker.setup()

    for chunk in chunks:
        tasks = [
            checker.fetch_and_parse(url=u, known_sitemap_date=lm)
            for (u, lm) in chunk
        ]
        chunk_results = await asyncio.gather(*tasks, return_exceptions=True)
        valid = [r for r in chunk_results if isinstance(r, dict)]
        results.extend(valid)
        processed += len(chunk)

        if show_partial_callback:
            show_partial_callback(results, processed, total)

        gc.collect()

    await checker.close()
    return results


# -----------------------------
# Sitemap Parsing
# -----------------------------
def parse_sitemap(url: str) -> List[Tuple[str, str]]:
    """
    Fetch the sitemap URL, parse <loc> and <lastmod>, return list of (loc, lastmod).
    If you have a <sitemapindex>, you'd need recursion. This example just handles <urlset>.
    """
    results = []
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            root = ET.fromstring(resp.text)
            # For each <url>, parse <loc> and <lastmod>
            for url_tag in root.findall(".//{*}url"):
                loc = url_tag.find("./{*}loc")
                lastmod = url_tag.find("./{*}lastmod")
                if loc is not None and loc.text:
                    loc_text = loc.text.strip()
                    lastmod_text = lastmod.text.strip() if (lastmod is not None and lastmod.text) else ""
                    results.append((loc_text, lastmod_text))
    except:
        pass
    return results


# -----------------------------
# Streamlit UI
# -----------------------------
def main():
    st.title("Async SEO Crawler (BFS or Chunked)")

    # --- Sidebar or Main Options ---
    st.subheader("Configuration")
    concurrency = st.slider("Concurrency (parallel requests)", 1, 200, 10)

    # User-Agent
    ua_options = list(USER_AGENTS.keys()) + ["Custom"]
    chosen_ua = st.selectbox("Select User Agent", ua_options, index=0)
    if chosen_ua == "Custom":
        custom_ua = st.text_input("Enter Custom User Agent", "")
        user_agent = custom_ua.strip() if custom_ua.strip() else USER_AGENTS["Custom Adidas SEO Bot"]
    else:
        user_agent = USER_AGENTS[chosen_ua]

    # BFS or Not
    do_bfs = st.checkbox("Enable BFS (Site Crawl)?", value=False)

    bfs_depth = 0
    include_sitemaps_in_bfs = False
    if do_bfs:
        st.markdown("**BFS Options**")
        bfs_depth = st.number_input("Max BFS Depth (0 = unlimited)", min_value=0, max_value=9999, value=0)
        include_sitemaps_in_bfs = st.checkbox("Include Sitemap URLs in BFS?")

    # Input: Paste or File
    st.subheader("Enter URLs or Load from File")
    input_method = st.selectbox("Input Method", ["Paste", "Upload File"])
    raw_urls = []
    if input_method == "Paste":
        text_input = st.text_area("Paste URLs (space or line-separated)", "")
        if text_input.strip():
            raw_urls = re.split(r"\s+", text_input.strip())
    else:
        uploaded = st.file_uploader("Upload a .txt or .csv with URLs", type=["txt", "csv"])
        if uploaded:
            content = uploaded.read().decode("utf-8", errors="replace")
            raw_urls = re.split(r"\s+", content.strip())

    if 'input_urls' not in st.session_state:
        st.session_state['input_urls'] = []

    if raw_urls:
        # Convert raw strings to (url, lastmod="") pairs
        new_pairs = [(u.strip(), "") for u in raw_urls if u.strip()]
        # Deduplicate & store in session
        st.session_state['input_urls'] = list(set(st.session_state['input_urls'] + new_pairs))

    st.write(f"Total input URLs (current session): {len(st.session_state['input_urls'])}")

    # Sitemaps
    st.subheader("Sitemap")
    sitemap_url = st.text_input("Optional: Enter a Sitemap URL to parse", "")
    if 'sitemap_urls' not in st.session_state:
        st.session_state['sitemap_urls'] = []

    if st.button("Fetch Sitemap"):
        if sitemap_url.strip():
            sm_pairs = parse_sitemap(sitemap_url.strip())
            # add to session
            # sm_pairs is list of (loc, lastmod)
            st.session_state['sitemap_urls'] = list(set(st.session_state['sitemap_urls'] + sm_pairs))
            st.write(f"Fetched {len(sm_pairs)} URLs from sitemap.")
        else:
            st.warning("Please enter a sitemap URL first.")

    st.write(f"Total sitemap URLs (current session): {len(st.session_state['sitemap_urls'])}")

    # Combine user-provided + sitemap pairs (for chunked mode if BFS not used)
    combined_pairs = st.session_state['input_urls'] + st.session_state['sitemap_urls']
    if len(combined_pairs) > DEFAULT_MAX_URLS:
        combined_pairs = combined_pairs[:DEFAULT_MAX_URLS]
    st.write(f"Combined total (cap 25k): {len(combined_pairs)}")

    st.subheader("Run Crawl/Checks")

    run_button = st.button("Run Checks")

    if run_button:
        if do_bfs:
            # BFS mode
            # 1) We need at least one "seed" URL for BFS
            if not combined_pairs:
                st.warning("No seed URL found. Please provide at least one URL or a sitemap entry.")
                return

            # We'll pick the FIRST url in combined_pairs as the main BFS seed if the user wants single-seed BFS
            # or we can BFS from all combined if we want multi-seed BFS. Let's do multi-seed BFS:
            # If user wants "Include Sitemaps in BFS," we keep them in BFS seeds as well.
            # Otherwise, we exclude the sitemap pairs if user unchecks.
            # But we'll keep it simple: if "Include Sitemaps in BFS" is false, only BFS from input_urls.
            # If it's true, BFS from everything.
            # BFS depth 0 => unlimited
            if include_sitemaps_in_bfs:
                seeds_for_bfs = combined_pairs
            else:
                seeds_for_bfs = st.session_state['input_urls']  # user input only

            if not seeds_for_bfs:
                st.warning("No BFS seeds available after excluding sitemaps. Please add some URLs.")
                return

            # We'll do BFS from all seeds (multi-seed BFS).
            # BFS only crawls within the domain of the first seed, as coded in bfs_crawl.
            # If seeds are from multiple domains, we'd need more advanced code to do multi-domain BFS.
            # But we'll keep the single-domain approach for simplicity.
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Single placeholder to keep updating
            table_placeholder = st.empty()
            progress_bar = st.progress(0)
            status_text = st.empty()

            def show_partial_data(res_list, done_count, total_count):
                pct = int((done_count / total_count) * 100) if total_count else 100
                progress_bar.progress(min(pct, 100))
                status_text.write(f"Processed {done_count} of {total_count} URLs...")
                temp_df = pd.DataFrame(res_list)
                table_placeholder.dataframe(temp_df, use_container_width=True)

            checker = URLChecker(user_agent=user_agent, concurrency=concurrency, timeout=DEFAULT_TIMEOUT)

            # BFS Depth logic: if bfs_depth=0 => None
            final_bfs_depth = None if bfs_depth == 0 else bfs_depth

            final_results = loop.run_until_complete(
                bfs_crawl(
                    seed_urls=seeds_for_bfs,       # (url, lastmod)
                    checker=checker,
                    max_depth=final_bfs_depth,
                    max_urls=DEFAULT_MAX_URLS,
                    show_partial_callback=lambda r, c, t: show_partial_data(r, c, t)
                )
            )

            loop.close()
            progress_bar.empty()
            status_text.write("BFS Completed!")

            if not final_results:
                st.warning("No results found.")
                return

            df = pd.DataFrame(final_results)
            st.subheader("Final Results")
            st.dataframe(df, use_container_width=True)

            csv_data = df.to_csv(index=False).encode("utf-8")
            now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            st.download_button(
                label="Download CSV",
                data=csv_data,
                file_name=f"url_check_results_{now_str}.csv",
                mime="text/csv"
            )

            show_summary(df)

        else:
            # Chunked mode
            if not combined_pairs:
                st.warning("No URLs to process. Please add some URLs or a sitemap.")
                return

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            table_placeholder = st.empty()
            progress_bar = st.progress(0)
            status_text = st.empty()

            def show_partial_data(res_list, done_count, total_count):
                pct = int((done_count / total_count) * 100) if total_count else 100
                progress_bar.progress(min(pct, 100))
                status_text.write(f"Processed {done_count} of {total_count} URLs...")
                temp_df = pd.DataFrame(res_list)
                table_placeholder.dataframe(temp_df, use_container_width=True)

            checker = URLChecker(user_agent=user_agent, concurrency=concurrency, timeout=DEFAULT_TIMEOUT)

            final_results = loop.run_until_complete(
                process_urls_chunked(
                    combined_pairs,
                    checker,
                    show_partial_callback=lambda r, c, t: show_partial_data(r, c, t)
                )
            )

            loop.close()
            progress_bar.empty()
            status_text.write("Checks Completed!")

            if not final_results:
                st.warning("No results found.")
                return

            df = pd.DataFrame(final_results)
            st.subheader("Final Results")
            st.dataframe(df, use_container_width=True)

            csv_data = df.to_csv(index=False).encode("utf-8")
            now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            st.download_button(
                label="Download CSV",
                data=csv_data,
                file_name=f"url_check_results_{now_str}.csv",
                mime="text/csv"
            )

            show_summary(df)


def show_summary(df: pd.DataFrame):
    st.subheader("Summary (Optional)")

    st.write("**Initial Status Code Distribution**")
    code_counts = df["Initial_Status_Code"].value_counts(dropna=False)
    for code, count in code_counts.items():
        st.write(f"{code}: {count}")

    st.write("**Final Status Code Distribution**")
    final_code_counts = df["Final_Status_Code"].value_counts(dropna=False)
    for code, count in final_code_counts.items():
        st.write(f"{code}: {count}")

    st.write("**Blocked by Robots.txt?**")
    block_counts = df["Is_Blocked_by_Robots"].value_counts(dropna=False)
    for val, count in block_counts.items():
        st.write(f"{val}: {count}")

    st.write("**Indexable?**")
    indexable_counts = df["Is_Indexable"].value_counts(dropna=False)
    for val, count in indexable_counts.items():
        st.write(f"{val}: {count}")


if __name__ == "__main__":
    main()
