import streamlit as st
import pandas as pd
import re
import asyncio
import nest_asyncio
import aiohttp
import orjson
import gc
import logging
import requests  # for simple sitemap fetch, or replace with advertools if desired

from datetime import datetime
from typing import List, Dict, Tuple, Set
from collections import deque
from urllib.parse import urlparse, urljoin, urlunparse

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

# --------------------------------------------
# Helper to Remove Fragments
# --------------------------------------------
def normalize_url(url: str) -> str:
    """
    Remove any #fragment part so that URLs differing only by fragment
    (e.g. /page#MainContent, /page#AnotherAnchor) are treated as the same.
    """
    parsed = urlparse(url)
    # Rebuild parsed URL but remove the fragment
    return urlunparse(parsed._replace(fragment=""))

# -------------------------------------------------------------------
# URL Checker Class
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
        self.robots_cache = {}  # base_url -> robots.txt content or None if missing
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
    # Main Fetch & Parse
    # -------------------------
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=10))
    async def fetch_and_parse(self, url: str) -> Dict:
        """
        Fetch a single URL (no auto-redirect), then follow up to MAX_REDIRECTS manually.
        Return a dictionary with all SEO data, showing both initial and final statuses.
        """
        headers = {"User-Agent": self.user_agent}

        # Always remove fragment from the URL to avoid multiple requests to the same resource
        url = normalize_url(url)

        async with self.semaphore:
            try:
                # Check robots before we even request
                is_allowed, block_rule = await self.check_robots_txt(url)

                async with self.session.get(url, headers=headers, ssl=False, allow_redirects=False) as resp:
                    initial_status = resp.status
                    initial_type = self.get_status_type(initial_status)
                    location = resp.headers.get("Location")
                    final_url = str(resp.url)  # The URL we ended up with for this response

                    # If it's a redirect, follow the chain
                    if initial_status in (301, 302, 307, 308) and location:
                        (
                            final_url,
                            final_status,
                            final_html,
                            final_headers
                        ) = await self.follow_redirect_chain(url, headers)
                        # If final_status is 200 => parse HTML
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
                            )
                        else:
                            # Non-200 or error
                            return self.build_non200_result(
                                original_url=url,
                                final_url=final_url if isinstance(final_url, str) else "N/A",
                                final_status=final_status,
                                is_allowed=is_allowed,
                                block_rule=block_rule,
                                initial_status=initial_status,
                                initial_type=initial_type
                            )
                    else:
                        # Not a redirect => parse if 200 and HTML
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
                                initial_type=initial_type
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
        Follow up to MAX_REDIRECTS. Return (final_url, final_status, final_html, final_headers).
        If loop or excessive redirect, final_status = "Redirect Loop".
        """
        current_url = normalize_url(start_url)
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
                    # remove fragment from the next location
                    loc_no_fragment = normalize_url(urljoin(current_url, loc))
                    current_url = loc_no_fragment
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
        initial_status: int,
        initial_type: str
    ) -> Dict:
        """
        Parse the final HTML for SEO data (title, meta desc, etc.).
        Return a row dict with both initial and final statuses shown.
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
        canonical_matches = (not canonical_url) or (canonical_url == original_url)
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
            reason_parts.append("Canonical is pointing to other URL")
        if not reason_parts:
            reason_parts.append("Page is indexable")

        data = {
            "Original_URL": original_url,
            "Initial_Status_Code": initial_status,
            "Initial_Status_Type": initial_type,
            "Final_URL": current_url,
            "Redirect_Status": status_code,
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
        final_status might be int or 'Redirect Loop'.
        """
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
            "Redirect_Status": final_status,
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
            "Timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

    def create_error_response(self, url: str, code: str, message: str, is_allowed: bool) -> Dict:
        """
        Handle exceptions or timeouts as special rows.
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
            "Final_URL": "N/A",
            "Redirect_Status": code,
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
# BFS Crawling
# -----------------------------
async def bfs_crawl(
    seed_url: str,
    checker: URLChecker,
    max_depth: int = 3,
    max_urls: int = DEFAULT_MAX_URLS,
    show_partial_callback=None
) -> List[Dict]:
    """
    Perform a BFS up to depth = 3 from the single seed URL.
    We do not skip links if robots.txt blocks them, because user wants to ignore robots
    for BFS link *discovery*. But final checks are still done via checker.fetch_and_parse.
    """
    visited: Set[str] = set()
    queue = deque()
    results = []

    # Normalize seed_url by removing fragment
    seed_url = normalize_url(seed_url.strip())
    seed_parsed = urlparse(seed_url)
    allowed_domain = seed_parsed.netloc.lower()

    queue.append((seed_url, 0))
    await checker.setup()

    while queue:
        if len(visited) >= max_urls:
            break

        url, depth = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        try:
            row = await checker.fetch_and_parse(url)
            results.append(row)

            # If depth < max_depth, discover new links from this page
            if depth < max_depth:
                discovered_links = await discover_links(url, checker.session, checker.user_agent)
                for link in discovered_links:
                    # remove fragment from discovered link
                    link_normalized = normalize_url(link)
                    link_parsed = urlparse(link_normalized)

                    # same domain?
                    if link_parsed.netloc.lower() == allowed_domain:
                        if link_normalized not in visited and len(visited) < max_urls:
                            queue.append((link_normalized, depth + 1))

        except Exception as e:
            logging.error(f"BFS error on {url}: {e}")

        # Show partial results every 10 pages, for example
        if show_partial_callback and len(results) % 10 == 0:
            show_partial_callback(results, len(visited), max_urls)

    await checker.close()
    return results

async def discover_links(url: str, session: aiohttp.ClientSession, user_agent: str) -> List[str]:
    """
    Minimal fetch for BFS link discovery.
    We do not follow redirects here, nor do advanced checks.
    We'll parse the HTML and extract <a href>.
    """
    headers = {"User-Agent": user_agent}
    links = []

    # Make sure we remove fragment from "url" so we don't fetch duplicates
    url = normalize_url(url)

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
# Process URLs (Non-BFS Mode)
# -----------------------------
async def process_urls_chunked(urls: List[str], checker: URLChecker, show_partial_callback=None) -> List[Dict]:
    results = []
    total = len(urls)

    # Normalize (remove fragments) from all input URLs to match Screaming Frog
    normalized_urls = [normalize_url(u.strip()) for u in urls if u.strip()]
    # Deduplicate after normalization
    normalized_urls = list(dict.fromkeys(normalized_urls))

    chunks = [normalized_urls[i : i + DEFAULT_CHUNK_SIZE] for i in range(0, len(normalized_urls), DEFAULT_CHUNK_SIZE)]
    processed = 0

    await checker.setup()

    for chunk in chunks:
        tasks = [checker.fetch_and_parse(u) for u in chunk]
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
def parse_sitemap(url: str) -> List[str]:
    """
    Fetch the given sitemap URL, parse <loc> tags, return list of URLs.
    For simplicity, this only looks for <loc> under any namespace.
    If you need to handle sitemap indexes with multiple <sitemap> entries, expand accordingly.
    """
    out = []
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            root = ET.fromstring(resp.text)
            # Look for <loc> in any namespace
            for loc_tag in root.findall(".//{*}loc"):
                if loc_tag.text:
                    # Also remove fragment from each sitemap URL
                    out.append(normalize_url(loc_tag.text.strip()))
    except:
        pass
    return out

# -----------------------------
# Main Streamlit UI
# -----------------------------
def main():
    st.title("Dentsu-Crawler")

    concurrency = st.slider("URl/s", 1, 200, 10)

    # User-Agent selection
    ua_options = list(USER_AGENTS.keys()) + ["Custom"]
    chosen_ua = st.selectbox("Select User Agent", ua_options, index=0)
    if chosen_ua == "Custom":
        custom_ua = st.text_input("Enter Custom User Agent", "")
        user_agent = custom_ua.strip() if custom_ua.strip() else USER_AGENTS["Custom Adidas SEO Bot"]
    else:
        user_agent = USER_AGENTS[chosen_ua]

    # Input method
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

    # We store user-provided URLs in session state so we don't lose them after fetching sitemaps
    if 'input_urls' not in st.session_state:
        st.session_state['input_urls'] = []

    # Update session state if user has provided new input
    if raw_urls:
        # Add to input_urls (deduplicate later)
        st.session_state['input_urls'] = list(set(st.session_state['input_urls'] + raw_urls))

    # Sitemaps
    sitemap_url = st.text_input("Optional: Enter a Sitemap URL to parse")
    if 'sitemap_urls' not in st.session_state:
        st.session_state['sitemap_urls'] = []

    if st.button("Fetch Sitemap"):
        if sitemap_url.strip():
            fetched_sitemap_urls = parse_sitemap(sitemap_url.strip())
            st.session_state['sitemap_urls'] = list(set(st.session_state['sitemap_urls'] + fetched_sitemap_urls))
            st.write(f"Fetched {len(fetched_sitemap_urls)} URLs from sitemap.")
        else:
            st.warning("Please enter a sitemap URL before clicking 'Fetch Sitemap'.")

    # Combine user-provided + sitemap
    combined_urls = st.session_state['input_urls'] + st.session_state['sitemap_urls']
    combined_urls = list(dict.fromkeys([u.strip() for u in combined_urls if u.strip()]))
    if len(combined_urls) > DEFAULT_MAX_URLS:
        combined_urls = combined_urls[:DEFAULT_MAX_URLS]
    st.write(f"Total URLs (Max Limit 25k URLs): {len(combined_urls)}")

    # BFS
    do_bfs = st.checkbox("Spider")
    bfs_seed_url = ""
    if do_bfs:
        bfs_seed_url = st.text_input("Enter URL")

    if st.button("Run Checks"):
        if do_bfs and not bfs_seed_url.strip():
            st.warning("You selected BFS but did not provide a seed URL.")
            return

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        progress_bar = st.progress(0)
        status_text = st.empty()

        def show_partial_data(res_list, done_count, total_count):
            pct = int((done_count / total_count) * 100) if total_count else 0
            progress_bar.progress(min(pct, 100))
            status_text.write(f"Processed {done_count} of {total_count} URLs so far.")
            temp_df = pd.DataFrame(res_list)
            st.dataframe(temp_df.tail(10), use_container_width=True)

        checker = URLChecker(user_agent=user_agent, concurrency=concurrency, timeout=DEFAULT_TIMEOUT)

        final_results = []
        if do_bfs:
            # BFS approach (seed URL); ignoring the manually entered combined_urls
            async def run_bfs():
                return await bfs_crawl(
                    seed_url=bfs_seed_url.strip(),
                    checker=checker,
                    max_depth=DEFAULT_MAX_DEPTH,
                    max_urls=DEFAULT_MAX_URLS,
                    show_partial_callback=lambda r, c, t: show_partial_data(r, c, t)
                )
            final_results = loop.run_until_complete(run_bfs())

        else:
            # Normal chunked approach with combined_urls
            async def run_normal():
                return await process_urls_chunked(
                    combined_urls,
                    checker,
                    show_partial_callback=lambda r, c, t: show_partial_data(r, c, t)
                )
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
    st.write("**Initial Status Code Distribution**")
    code_counts = df["Initial_Status_Code"].value_counts()
    for code, count in code_counts.items():
        st.write(f"{code}: {count}")

    st.write("**Final Status Code Distribution**")
    final_code_counts = df["Redirect_Status"].value_counts()
    for code, count in final_code_counts.items():
        st.write(f"{code}: {count}")

    block_counts = df["Is_Blocked_by_Robots"].value_counts()
    st.write("**Blocked by Robots.txt?**")
    for val, count in block_counts.items():
        st.write(f"{val}: {count}")

    indexable_counts = df["Is_Indexable"].value_counts()
    st.write("**Indexable?**")
    for val, count in indexable_counts.items():
        st.write(f"{val}: {count}")


if __name__ == "__main__":
    main()
