import streamlit as st
import pandas as pd
import re
import asyncio
import nest_asyncio
import aiohttp
import orjson
import gc
import logging

from datetime import datetime
from typing import List, Dict, Tuple, Set
from collections import deque
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup

# Tenacity for retry logic
from tenacity import retry, stop_after_attempt, wait_exponential

# For sitemap parsing
import xml.etree.ElementTree as ET

# Apply nest_asyncio so we can run async code in Streamlit
nest_asyncio.apply()

# --------------------------
# Logging Configuration (Optional)
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='url_checker.log'
)

# --------------------------
# Constants (default values)
# --------------------------
DEFAULT_TIMEOUT = 15
DEFAULT_CHUNK_SIZE = 100
DEFAULT_MAX_URLS = 25000  # BFS or overall cap
DEFAULT_MAX_DEPTH = 3     # BFS depth
MAX_REDIRECTS = 5

# We'll fill these from user inputs in the UI
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

# -------------------------------------------------------------------
# URL Checker Class: For Final SEO Checks (Including robots.txt check)
# -------------------------------------------------------------------
class URLChecker:
    """
    A class that checks various SEO and technical aspects of URLs.
    Used in BFS or in simple chunked processing.
    """

    def __init__(self, user_agent: str, concurrency: int = 10, timeout: int = 15):
        self.user_agent = user_agent
        self.max_concurrency = concurrency
        self.timeout_duration = timeout
        self.robots_cache = {}  # base_url -> robots content or True if missing
        self.connector = None
        self.session = None
        self.semaphore = None

    async def setup(self):
        """
        Prepare an aiohttp session with concurrency limits and timeouts.
        """
        self.connector = aiohttp.TCPConnector(
            limit=self.max_concurrency,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            force_close=False
        )
        # Connect & read timeouts
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
        """
        Returns (is_allowed, block_rule).
        If robots.txt is unreachable or not found -> assume allowed, block_rule = 'N/A'.
        """
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        path = parsed.path

        # If we have not cached this domain, attempt to fetch & parse
        if base_url not in self.robots_cache:
            robots_url = f"{base_url}/robots.txt"
            try:
                headers = {"User-Agent": self.user_agent}
                async with self.session.get(robots_url, ssl=False, headers=headers) as resp:
                    if resp.status == 200:
                        text_content = await resp.text()
                        self.robots_cache[base_url] = text_content
                    else:
                        self.robots_cache[base_url] = None
            except:
                self.robots_cache[base_url] = None

        robots_content = self.robots_cache.get(base_url)
        if not robots_content:
            # No robots.txt or failed fetch => assume allowed
            return True, "N/A"

        # Check if path is disallowed under relevant user-agent
        is_allowed, block_rule = self.parse_robots(robots_content, path)
        return is_allowed, block_rule

    def parse_robots(self, robots_content: str, path: str) -> Tuple[bool, str]:
        """
        Basic parse of robots.txt. We look for user-agent: * or custom user agent lines,
        then check Disallow. If multiple disallows match, we return the first.
        We do NOT parse 'Allow:' lines. It's simplistic but meets your requirement.
        """
        lines = robots_content.splitlines()
        is_relevant_section = False
        block_rule = "N/A"
        path_lower = path.lower()

        # Lower-case user agent for matching
        user_agent_lower = self.user_agent.lower()

        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split(':', 1)
            if len(parts) < 2:
                continue

            field, value = parts[0].lower(), parts[1].strip().lower()

            # Identify user agent section
            if field == "user-agent":
                # Match if it's '*' or contains our custom user agent substring
                if value == '*' or (user_agent_lower in value):
                    is_relevant_section = True
                else:
                    is_relevant_section = False

            elif field == "disallow" and is_relevant_section:
                dis_path = value  # e.g. "/some-path"
                if dis_path and path_lower.startswith(dis_path):
                    # Return first match
                    block_rule = f"Disallow: {dis_path}"
                    return False, block_rule

        # If no matching disallow found => allowed
        return True, "N/A"

    # -------------------------
    # Main Check (similar to old check_url)
    # -------------------------
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=10))
    async def fetch_and_parse(self, url: str) -> Dict:
        """
        Fetch a single URL without auto-redirect, then if needed follow up to MAX_REDIRECTS.
        Return a dictionary with all SEO data.
        """
        headers = {"User-Agent": self.user_agent}

        async with self.semaphore:
            # 1) Perform initial GET (no redirects)
            try:
                is_allowed, block_rule = await self.check_robots_txt(url)

                async with self.session.get(url, headers=headers, ssl=False, allow_redirects=False) as resp:
                    initial_status = resp.status
                    initial_type = self.get_status_type(initial_status)
                    final_url = str(resp.url)
                    location = resp.headers.get("Location")

                    # If redirect, follow chain
                    if initial_status in (301, 302, 307, 308) and location:
                        (
                            redirected_url,
                            redirected_status,
                            redirected_html,
                            redirected_headers
                        ) = await self.follow_redirect_chain(url, headers)
                        # Parse final result if it's 200 text/html
                        if isinstance(redirected_status, int) and redirected_status == 200:
                            final_data = await self.parse_html(
                                original_url=url,
                                current_url=redirected_url,
                                html_content=redirected_html,
                                status_code=redirected_status,
                                headers=redirected_headers,
                                is_allowed=is_allowed,
                                block_rule=block_rule,
                                initial_status=initial_status
                            )
                            # Then override the columns for initial vs. redirected
                            final_data["Redirected_URL"] = redirected_url
                            final_data["Redirected_Status_Code"] = redirected_status
                            final_data["Redirected_Status_Type"] = self.get_status_type(redirected_status)
                            final_data["Initial_Status_Code"] = initial_status
                            final_data["Initial_Status_Type"] = initial_type
                            return final_data
                        else:
                            # Not 200 or not HTML => no parsing
                            return self.build_non200_result(
                                original_url=url,
                                final_url=redirected_url if isinstance(redirected_url, str) else "N/A",
                                final_status=redirected_status,
                                is_allowed=is_allowed,
                                block_rule=block_rule,
                                initial_status=initial_status,
                                initial_type=initial_type
                            )
                    else:
                        # No redirect => parse if 200 HTML
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
                                initial_status=initial_status
                            )
                        else:
                            # Non-200 or not HTML => partial result
                            return self.build_non200_result(
                                original_url=url,
                                final_url=final_url,
                                final_status=initial_status,
                                is_allowed=is_allowed,
                                block_rule=block_rule,
                                initial_status=initial_status,
                                initial_type=initial_type
                            )
            except asyncio.TimeoutError:
                return self.create_error_response(
                    url=url, code="Timeout", message="Request timed out", is_allowed=False
                )
            except Exception as e:
                return self.create_error_response(
                    url=url, code="Error", message=str(e), is_allowed=False
                )

    async def follow_redirect_chain(self, start_url: str, headers: Dict) -> Tuple[str, int, str, Dict]:
        """
        Follow up to MAX_REDIRECTS. Return (final_url, final_status, html_content, final_headers).
        If loop or excessive redirect, final_status = "Redirect Loop".
        """
        current_url = start_url
        html_content = None
        final_headers = {}

        for _ in range(MAX_REDIRECTS):
            async with self.session.get(
                current_url, headers=headers, ssl=False, allow_redirects=False
            ) as resp:
                status = resp.status
                final_headers = resp.headers
                if self.is_html_response(resp):
                    html_content = await resp.text(encoding='utf-8', errors='replace')
                else:
                    html_content = None

                if status in (301, 302, 307, 308):
                    loc = resp.headers.get("Location")
                    if not loc:
                        return (current_url, status, html_content, final_headers)
                    current_url = urljoin(current_url, loc)
                else:
                    return (current_url, status, html_content, final_headers)

        # If we exceed max redirects
        return (current_url, "Redirect Loop", html_content, final_headers)

    async def parse_html(
        self,
        original_url: str,
        current_url: str,
        html_content: str,
        status_code: int,
        headers: Dict,
        is_allowed: bool,
        block_rule: str,
        initial_status: int
    ) -> Dict:
        """
        Parse the final HTML for SEO data (title, meta desc, etc.). Return a row dict.
        We also gather link <a href> if BFS calls us.
        """
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

        # Check if noindex
        combined_robots = f"{meta_robots.lower()} {x_robots_tag.lower()}"
        has_noindex = "noindex" in combined_robots

        # Evaluate indexability
        # If final status is 200, not blocked, not noindex, canonical matches, ...
        canonical_matches = (canonical_url == "" or canonical_url == original_url)
        is_indexable = (
            status_code == 200
            and is_allowed
            and not has_noindex
            and canonical_matches
        )

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

        # Build final data row
        data = {
            "Original_URL": original_url,
            # We'll fill in "Initial_Status_Code"/"Type" after we return if needed
            "Initial_Status_Code": initial_status,
            "Initial_Status_Type": self.get_status_type(initial_status),
            "Redirected_URL": "N/A",
            "Redirected_Status_Code": "N/A",
            "Redirected_Status_Type": "N/A",
            "Is_Blocked_by_Robots": "Yes" if not is_allowed else "No",
            "Robots_Block_Rule": block_rule,
            "Title": title,
            "Meta_Description": meta_desc,
            "H1_Text": first_h1_text,
            "H1_Count": h1_count,
            "Canonical_URL": canonical_url if canonical_url else "",
            "Meta_Robots": meta_robots,
            "X_Robots_Tag": x_robots_tag,
            "HTML_Lang": html_lang,
            "Is_Indexable": "Yes" if is_indexable else "No",
            "Indexability_Reason": "; ".join(reason_parts),
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
        initial_type: str
    ) -> Dict:
        """
        Build a result row for non-200 or non-HTML responses, including redirect loops, 404, etc.
        final_status might be an int or "Redirect Loop".
        """
        # If final_status is an int, get a string label
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
            "Redirected_URL": final_url if final_url else "N/A",
            "Redirected_Status_Code": final_status,
            "Redirected_Status_Type": status_type,
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
            "Timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

    def create_error_response(self, url: str, code: str, message: str, is_allowed: bool) -> Dict:
        """
        For exceptions or timeouts.
        """
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
            "Redirected_URL": "N/A",
            "Redirected_Status_Code": "N/A",
            "Redirected_Status_Type": "N/A",
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
        """
        Convert numeric codes to a short label. If unknown or a string, just return it.
        """
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
# BFS Crawling (depth=3, ignoring robots for link extraction)
# -----------------------------
async def bfs_crawl(
    seed_url: str,
    checker: URLChecker,
    max_depth: int = 3,
    max_urls: int = DEFAULT_MAX_URLS,
    show_partial_callback=None
) -> List[Dict]:
    """
    Perform a BFS up to depth=3 from the single seed URL.
    We store final SEO results (similar to check_url).
    - We do not skip links if robots.txt blocks them, because user wants to ignore robots while crawling.
    - We do, however, show in results if it's blocked from indexing (the check_url logic).
    """
    visited: Set[str] = set()
    queue = deque()
    results = []

    # Normalize seed
    seed_parsed = urlparse(seed_url)
    allowed_domain = seed_parsed.netloc.lower()
    seed_url = seed_url.strip()
    queue.append((seed_url, 0))

    await checker.setup()

    while queue:
        # If we reached max_urls, stop
        if len(visited) >= max_urls:
            break

        url, depth = queue.popleft()

        if url in visited:
            continue
        visited.add(url)

        try:
            # Use the same SEO fetch logic to get on-page data.
            row = await checker.fetch_and_parse(url)
            results.append(row)

            # If depth < max_depth, parse out links from the final HTML content
            # But we only have final HTML if it's 200 text/html in the BFS method...
            # We'll replicate a simplified approach: we can re-fetch if needed, but let's
            # do it more efficiently by hooking into parse_html. For simplicity, let's do
            # a second minimal fetch if it's 200 text/html. Or store the HTML from fetch_and_parse?
            # For clarity, let's do a simpler approach: if final status is 200 and we have
            # a "Redirected_URL" = "N/A" or "Redirected_Status_Code" = 200 (meaning no final redirect),
            # we re-fetch to parse links. (Because fetch_and_parse doesn't keep the HTML.)
            #
            # Alternatively, we can modify fetch_and_parse to return the HTML for BFS usage.
            # Let's do that to avoid double fetching. We'll just do a small tweak to parse_html
            # to also return links discovered.

            # We'll do a separate method: `fetch_and_discover_links`.
            # This is simpler if we want BFS. For now, let's do a quick hack:
            if depth < max_depth:
                discovered_links = await discover_links(url, checker.session, checker.user_agent)
                for link in discovered_links:
                    # same domain?
                    link_parsed = urlparse(link)
                    if link_parsed.netloc.lower() == allowed_domain:
                        if link not in visited and len(visited) < max_urls:
                            queue.append((link, depth + 1))

        except Exception as e:
            logging.error(f"BFS error on {url}: {e}")

        # Show partial results after every few steps
        if show_partial_callback and len(results) % 10 == 0:
            show_partial_callback(results, len(visited), max_urls)

    await checker.close()
    return results

async def discover_links(url: str, session: aiohttp.ClientSession, user_agent: str) -> List[str]:
    """
    Minimal fetch to get HTML and extract <a href> links. We do not follow redirects here.
    We do not do SEO checks. This is purely to discover new URLs for BFS.
    """
    headers = {"User-Agent": user_agent}
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
# Helper: Process URLs in Non-BFS Mode (Chunked)
# -----------------------------
async def process_urls_chunked(urls: List[str], checker: URLChecker, show_partial_callback=None) -> List[Dict]:
    results = []
    total = len(urls)
    chunks = [urls[i : i + DEFAULT_CHUNK_SIZE] for i in range(0, total, DEFAULT_CHUNK_SIZE)]
    processed = 0

    await checker.setup()

    for chunk in chunks:
        tasks = [checker.fetch_and_parse(u) for u in chunk]
        chunk_results = await asyncio.gather(*tasks, return_exceptions=True)
        # Filter out exceptions
        valid = [r for r in chunk_results if isinstance(r, dict)]
        results.extend(valid)
        processed += len(chunk)
        if show_partial_callback:
            show_partial_callback(results, processed, total)
        gc.collect()

    await checker.close()
    return results

# -----------------------------
# SITEMAP Parsing
# -----------------------------
def parse_sitemap(url: str) -> List[str]:
    """
    Fetch the given sitemap URL, parse out <loc> tags, return list of URLs.
    If it's a sitemapindex, we won't recursively fetch sub-sitemaps (for simplicity).
    """
    import requests
    out = []
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            root = ET.fromstring(resp.text)
            # We assume <urlset> structure
            # If you want to handle <sitemapindex>, you'd parse further, etc.
            # For now, just look for <loc> in any namespace.
            for loc_tag in root.findall(".//{*}loc"):
                if loc_tag.text:
                    out.append(loc_tag.text.strip())
    except:
        pass
    return out

# -----------------------------
# Main Streamlit UI
# -----------------------------
def main():
    st.title("Async URL Checker with Optional BFS Crawl")
    st.write("Check technical/SEO elements of URLs, optionally crawl up to 3 levels deep.")

    # --- Concurrency Slider ---
    concurrency = st.slider("URL Speed /s", min_value=1, max_value=200, value=10)

    # --- User-Agent Dropdown + Custom Input ---
    ua_options = list(USER_AGENTS.keys()) + ["Custom"]
    chosen_ua = st.selectbox("Select User Agent", ua_options, index=0)
    if chosen_ua == "Custom":
        custom_ua = st.text_input("Enter Custom User Agent", "")
        user_agent = custom_ua.strip() if custom_ua.strip() else USER_AGENTS["Custom Adidas SEO Bot"]
    else:
        user_agent = USER_AGENTS[chosen_ua]

    # --- Paste / Upload URLs ---
    input_method = st.selectbox("URL Input Method", ["Paste", "Upload File"])
    raw_urls = []
    if input_method == "Paste":
        text_input = st.text_area("Paste URLs (one per line or space-separated)")
        if text_input.strip():
            raw_urls = re.split(r"\s+", text_input.strip())
    else:
        uploaded = st.file_uploader("Upload a .txt or .csv with URLs", type=["txt", "csv"])
        if uploaded:
            content = uploaded.read().decode("utf-8", errors="replace")
            raw_urls = re.split(r"\s+", content.strip())

    # --- Sitemap Input ---
    sitemap_url = st.text_input("Optional: Enter a Sitemap URL to parse")
    sitemap_urls = []
    if sitemap_url.strip():
        st.write("Click the button to fetch and parse the sitemap.")
        if st.button("Fetch Sitemap"):
            sitemap_urls = parse_sitemap(sitemap_url.strip())
            st.write(f"Fetched {len(sitemap_urls)} URLs from sitemap.")

    # Combine user-provided + sitemap
    combined_urls = raw_urls + sitemap_urls
    combined_urls = [u.strip() for u in combined_urls if u.strip()]
    # Deduplicate and keep order
    seen = set()
    final_list = []
    for u in combined_urls:
        if u not in seen:
            seen.add(u)
            final_list.append(u)

    # Cap at 25k
    if len(final_list) > DEFAULT_MAX_URLS:
        final_list = final_list[:DEFAULT_MAX_URLS]
    st.write(f"Total URLs (after dedup & cap at 25k): {len(final_list)}")

    # --- BFS Option ---
    do_bfs = st.checkbox("Start Crawl")
    bfs_seed_url = ""
    if do_bfs:
        bfs_seed_url = st.text_input("Enter a single Seed URL for BFS (we'll ignore robots.txt for crawling):")
        st.write("We will discover new internal links up to 3 clicks from this seed, ignoring robots.txt during BFS.")
        st.write("The final set of discovered URLs will include your seed plus anything found. "
                 "Capped at 25k total pages.")

    # --- RUN CHECKS Button ---
    if st.button("Run Checks"):
        if do_bfs and not bfs_seed_url.strip():
            st.warning("You selected BFS, but no Seed URL was provided.")
            return

        # Prepare event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # We'll store results here
        final_results = []
        progress_bar = st.progress(0)
        status_text = st.empty()

        def show_partial_data(res_list, done_count, total_count):
            """Callback to update partial results in the UI."""
            # For BFS, total_count = max_urls (25k). For chunked, total_count = actual length of list.
            pct = int((done_count / total_count) * 100) if total_count else 0
            progress_bar.progress(min(pct, 100))
            status_text.write(f"Processed {done_count} of {total_count} URLs")

            # Show partial dataframe
            temp_df = pd.DataFrame(res_list)
            st.dataframe(temp_df, use_container_width=True)

        # If BFS is chosen, we do BFS from the seed URL, ignoring the final_list for BFS seeds
        # but we still might want to process final_list as well? 
        # The user said "if person tick then it will get the additional tab to enter the seed url, 
        # so that url can't put multiple seed input for crawl." 
        #
        # We'll interpret that BFS is an alternative approach. 
        # But if you want BFS + final_list combined, you can do that. 
        # For simplicity, let's do BFS *or* final_list:

        checker = URLChecker(user_agent=user_agent, concurrency=concurrency, timeout=DEFAULT_TIMEOUT)

        if do_bfs:
            # BFS approach
            async def run_bfs():
                # BFS uses progress approach: we consider total_count = DEFAULT_MAX_URLS for the bar
                res = await bfs_crawl(
                    seed_url=bfs_seed_url.strip(),
                    checker=checker,
                    max_depth=DEFAULT_MAX_DEPTH,
                    max_urls=DEFAULT_MAX_URLS,
                    show_partial_callback=lambda r, c, t: show_partial_data(r, c, t)
                )
                return res

            final_results = loop.run_until_complete(run_bfs())
        else:
            # No BFS => just process final_list in chunks
            async def run_normal():
                res = await process_urls_chunked(
                    final_list,
                    checker,
                    show_partial_callback=lambda r, c, t: show_partial_data(r, c, t)
                )
                return res

            final_results = loop.run_until_complete(run_normal())

        loop.close()
        progress_bar.empty()
        status_text.text("Done.")

        if not final_results:
            st.warning("No results found.")
            return

        df = pd.DataFrame(final_results)
        st.subheader("Final Results")
        st.dataframe(df, use_container_width=True)

        # Provide CSV download
        csv_data = df.to_csv(index=False).encode("utf-8")
        now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        st.download_button(
            label="Download CSV",
            data=csv_data,
            file_name=f"url_check_results_{now_str}.csv",
            mime="text/csv"
        )

        # Optional summary
        show_summary(df)

def show_summary(df: pd.DataFrame):
    st.subheader("Summary (Optional)")
    st.write("**Status Code Distribution (Initial Status)**")
    code_counts = df["Initial_Status_Code"].value_counts()
    for code, count in code_counts.items():
        st.write(f"{code}: {count}")

    st.write("**Redirected Status Distribution**")
    redirected_code_counts = df["Redirected_Status_Code"].value_counts()
    for code, count in redirected_code_counts.items():
        st.write(f"{code}: {count}")

    block_counts = df["Is_Blocked_by_Robots"].value_counts()
    st.write("**Blocked by Robots.txt?**")
    for val, count in block_counts.items():
        st.write(f"{val}: {count}")

    # Indexable analysis
    indexable_counts = df["Is_Indexable"].value_counts()
    st.write("**Indexable?**")
    for val, count in indexable_counts.items():
        st.write(f"{val}: {count}")

    # 200 OK final or initial
    # etc. More stats as needed.

if __name__ == "__main__":
    main()
