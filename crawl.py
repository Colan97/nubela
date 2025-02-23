import streamlit as st
import pandas as pd
import re
import asyncio
import nest_asyncio
import aiohttp
import orjson
import logging
import requests

from typing import List, Dict, Tuple, Set, Optional
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from datetime import datetime
from collections import deque

nest_asyncio.apply()

# --------------------------
# Logging Config (Optional)
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='url_checker.log'
)

# --------------------------
# Constants
# --------------------------
DEFAULT_TIMEOUT = 15
DEFAULT_MAX_URLS = 25000
MAX_REDIRECTS = 5
DEFAULT_USER_AGENT = "custom_adidas_seo_x3423/1.0"

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


# ----------------------------------------
# Helpers for URL Normalization & Scope
# ----------------------------------------
def normalize_url(url: str) -> str:
    """
    Remove #fragment, strip whitespace, ensure consistent format.
    """
    url = url.strip()
    # parse => remove fragment => rejoin
    parsed = urlparse(url)
    normalized = parsed._replace(fragment="")
    return normalized.geturl()


def in_scope(base_url: str, test_url: str, scope_mode: str) -> bool:
    """
    Return True if 'test_url' is within the chosen scope relative to 'base_url'.
    scope_mode can be:
      - "Exact URL Only"
      - "In Subfolder"
      - "Same Subdomain"
      - "All Subdomains"
    This is a simplistic approach; adapt as needed.
    """
    base_parsed = urlparse(base_url)
    test_parsed = urlparse(test_url)

    base_scheme = base_parsed.scheme
    base_netloc = base_parsed.netloc.lower()
    base_path = base_parsed.path
    test_netloc = test_parsed.netloc.lower()
    test_path = test_parsed.path

    # Must match scheme?
    if test_parsed.scheme != base_scheme:
        return False

    if scope_mode == "Exact URL Only":
        # Means the test URL must be exactly the same as base
        return (test_url == base_url)

    elif scope_mode == "In Subfolder":
        # The netloc must match exactly (same domain/subdomain)
        # And test_url's path must start with base_path
        # e.g. base:  https://example.com/folder
        # test: https://example.com/folder/anything
        if test_netloc != base_netloc:
            return False
        return test_path.startswith(base_path)

    elif scope_mode == "Same Subdomain":
        # netloc must match exactly, ignoring path
        return (test_netloc == base_netloc)

    elif scope_mode == "All Subdomains":
        # check if test_netloc ends with base_netloc's "root domain"
        # e.g. base_netloc: store.example.com => root: example.com
        # let's do a naive approach: split base_netloc by '.', then check if test_netloc endswith
        # or you can handle TLD special rules. We'll keep it simple:
        parts = base_netloc.split('.')
        if len(parts) <= 1:
            # fallback, just use same domain check
            return (test_netloc == base_netloc)
        # remove subdomain part
        root_domain = '.'.join(parts[-2:])  # e.g. "example.com"
        return test_netloc.endswith(root_domain)

    return False


def regex_filter(url: str, include_pattern: Optional[str], exclude_pattern: Optional[str]) -> bool:
    """
    Return True if 'url' passes the include/exclude regex filters.
    - If include_pattern is set, the URL must match it.
    - If exclude_pattern is set, the URL must NOT match it.
    """
    if include_pattern:
        if not re.search(include_pattern, url):
            return False
    if exclude_pattern:
        if re.search(exclude_pattern, url):
            return False
    return True


# ----------------------------------------
# URL Checker Class
# ----------------------------------------
class URLChecker:
    def __init__(self, user_agent: str, concurrency: int = 10, timeout: int = 15,
                 respect_robots: bool = True):
        self.user_agent = user_agent
        self.max_concurrency = concurrency
        self.timeout_duration = timeout
        self.respect_robots = respect_robots
        self.robots_cache = {}  # domain => content or None
        self.connector = None
        self.session = None
        self.semaphore = None

    async def setup(self):
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
        if self.session:
            await self.session.close()

    async def check_robots_txt(self, url: str) -> bool:
        """
        Return True if allowed, False if disallowed by robots.txt
        If self.respect_robots is False => always return True.
        """
        if not self.respect_robots:
            return True
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        path = parsed.path

        if base not in self.robots_cache:
            # fetch
            robots_url = base + "/robots.txt"
            try:
                headers = {"User-Agent": self.user_agent}
                async with self.session.get(robots_url, ssl=False, headers=headers) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        self.robots_cache[base] = text
                    else:
                        self.robots_cache[base] = None
            except:
                self.robots_cache[base] = None

        content = self.robots_cache.get(base)
        if not content:
            return True  # no robots => assume allowed

        return self.parse_robots(content, path)

    def parse_robots(self, content: str, path: str) -> bool:
        """
        Minimal parse of 'disallow' lines for 'User-agent: *' or our custom user agent.
        We do not parse 'Allow:' lines. Return True if allowed, False if disallowed.
        """
        lines = content.splitlines()
        active = False
        agent_lower = self.user_agent.lower()
        path_lower = path.lower()

        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split(':', 1)
            if len(parts) < 2:
                continue
            key, val = parts[0].lower(), parts[1].strip().lower()

            if key == "user-agent":
                # if val == '*' or agent_lower in val => relevant
                if val == '*' or agent_lower in val:
                    active = True
                else:
                    active = False
            elif key == "disallow" and active:
                if val and path_lower.startswith(val):
                    return False  # disallowed
        return True  # if not disallowed => allowed

    @staticmethod
    def is_html(resp: aiohttp.ClientResponse) -> bool:
        ctype = resp.content_type
        return ctype and ctype.startswith("text/html")

    async def fetch_and_parse(self, url: str) -> Dict:
        """
        Returns a dict with SEO data. If error => returns partial info.
        """
        url = normalize_url(url)

        # CAUSE OF ARROWINVALID ERROR:
        # Storing 'Initial_Status_Code' & 'Final_Status_Code' as strings to avoid
        # "ArrowInvalid: Could not convert 'Timeout' with type str: tried to convert to int64"
        # You can unify them as int if you handle "Timeout"/"Error" differently.
        data = {
            "Original_URL": url,
            "Initial_Status_Code": "",
            "Initial_Status_Type": "",
            "Final_URL": "",
            "Final_Status_Code": "",
            "Final_Status_Type": "",
            "Title": "",
            "Meta_Description": "",
            "H1_Text": "",
            "H1_Count": 0,
            "Canonical_URL": "",
            "Meta_Robots": "",
            "X_Robots_Tag": "",
            "HTML_Lang": "",
            "Is_Blocked_by_Robots": "",
            "Robots_Block_Rule": "",
            "Is_Indexable": "No",
            "Indexability_Reason": "",
            "HTTP_Last_Modified": "",
            "Sitemap_LastMod": "",  # not used in this example, but kept for consistency
            "Timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

        # 1) Robots check
        allowed = await self.check_robots_txt(url)
        data["Is_Blocked_by_Robots"] = "No" if allowed else "Yes"
        if not allowed:
            data["Robots_Block_Rule"] = "Disallow"
            data["Indexability_Reason"] = "Blocked by robots.txt"
            data["Final_URL"] = url
            data["Final_Status_Code"] = "N/A"
            data["Final_Status_Type"] = "Robots Block"
            return data

        # 2) Attempt fetch
        headers = {"User-Agent": self.user_agent}
        async with self.semaphore:
            try:
                async with self.session.get(url, headers=headers, ssl=False, allow_redirects=False) as resp:
                    initial_status_str = str(resp.status)
                    data["Initial_Status_Code"] = initial_status_str
                    data["Initial_Status_Type"] = self.status_type_label(resp.status)
                    data["Final_URL"] = str(resp.url)
                    location = resp.headers.get("Location")

                    if resp.status in (301, 302, 307, 308) and location:
                        # follow redirect chain
                        return await self.follow_redirect_chain(url, location, data, headers)
                    else:
                        # no redirect => parse if 200 HTML
                        if resp.status == 200 and self.is_html(resp):
                            content = await resp.text(errors='replace')
                            return self.parse_html_content(
                                data, content, resp.headers, resp.status, is_allowed=True
                            )
                        else:
                            # Non-200 or not HTML => partial
                            data["Final_Status_Code"] = initial_status_str
                            data["Final_Status_Type"] = data["Initial_Status_Type"]
                            data["Indexability_Reason"] = "Non-200 or Non-HTML"
                            return data

            except asyncio.TimeoutError:
                data["Initial_Status_Code"] = "Timeout"
                data["Initial_Status_Type"] = "Request Timeout"
                data["Final_URL"] = url
                data["Final_Status_Code"] = "Timeout"
                data["Final_Status_Type"] = "Request Timeout"
                data["Indexability_Reason"] = "Timeout"
                return data
            except Exception as e:
                data["Initial_Status_Code"] = "Error"
                data["Initial_Status_Type"] = str(e)
                data["Final_URL"] = url
                data["Final_Status_Code"] = "Error"
                data["Final_Status_Type"] = str(e)
                data["Indexability_Reason"] = "Exception"
                return data

    async def follow_redirect_chain(self, origin_url: str, location: str, data: Dict, headers: Dict) -> Dict:
        """
        Follow up to MAX_REDIRECTS manually, returning final result dict.
        """
        current_url = origin_url
        for _ in range(MAX_REDIRECTS):
            # Next redirect target
            next_url = urljoin(current_url, location)
            next_url = normalize_url(next_url)
            try:
                async with self.session.get(next_url, headers=headers, ssl=False, allow_redirects=False) as resp:
                    status_str = str(resp.status)
                    data["Final_URL"] = str(resp.url)
                    data["Final_Status_Code"] = status_str
                    data["Final_Status_Type"] = self.status_type_label(resp.status)

                    if resp.status in (301, 302, 307, 308):
                        location = resp.headers.get("Location")
                        if not location:
                            data["Indexability_Reason"] = "Redirect without location"
                            return data
                        current_url = next_url
                        continue
                    else:
                        # parse if 200 HTML
                        if resp.status == 200 and self.is_html(resp):
                            content = await resp.text(errors='replace')
                            return self.parse_html_content(
                                data, content, resp.headers, resp.status, is_allowed=True
                            )
                        else:
                            data["Indexability_Reason"] = "Non-200 or Non-HTML after redirect"
                            return data
            except asyncio.TimeoutError:
                data["Final_Status_Code"] = "Timeout"
                data["Final_Status_Type"] = "Request Timeout"
                data["Indexability_Reason"] = "Timeout during redirect"
                return data
            except Exception as e:
                data["Final_Status_Code"] = "Error"
                data["Final_Status_Type"] = str(e)
                data["Indexability_Reason"] = "Exception in redirect chain"
                return data

        data["Indexability_Reason"] = "Redirect Loop or Exceeding Max Redirects"
        data["Final_Status_Code"] = "Redirect Loop"
        data["Final_Status_Type"] = "Redirect Loop"
        return data

    def parse_html_content(self, data: Dict, html: str, resp_headers: aiohttp.typedefs.LooseHeaders,
                           status: int, is_allowed: bool) -> Dict:
        soup = BeautifulSoup(html, "lxml")

        # Basic SEO elements
        title_tag = soup.find("title")
        data["Title"] = title_tag.get_text(strip=True) if title_tag else ""

        desc_tag = soup.find("meta", attrs={"name": "description"})
        if desc_tag and desc_tag.has_attr("content"):
            data["Meta_Description"] = desc_tag["content"]

        h1_tags = soup.find_all("h1")
        data["H1_Count"] = len(h1_tags)
        data["H1_Text"] = h1_tags[0].get_text(strip=True) if len(h1_tags) > 0 else ""

        html_tag = soup.find("html")
        if html_tag and html_tag.has_attr("lang"):
            data["HTML_Lang"] = html_tag["lang"]

        canon_tag = soup.find("link", attrs={"rel": "canonical"})
        data["Canonical_URL"] = canon_tag["href"] if (canon_tag and canon_tag.has_attr("href")) else ""

        meta_robots = soup.find("meta", attrs={"name": "robots"})
        if meta_robots and meta_robots.has_attr("content"):
            data["Meta_Robots"] = meta_robots["content"]

        x_robots = resp_headers.get("X-Robots-Tag", "")
        data["X_Robots_Tag"] = x_robots

        # last modified
        data["HTTP_Last_Modified"] = resp_headers.get("Last-Modified", "")

        # Evaluate indexability
        combined = f"{data['Meta_Robots'].lower()} {x_robots.lower()}"
        if "noindex" in combined:
            data["Is_Indexable"] = "No"
            data["Indexability_Reason"] = "Noindex directive"
        elif not is_allowed:
            data["Is_Indexable"] = "No"
            data["Indexability_Reason"] = "Blocked by robots.txt"
        elif status != 200:
            data["Is_Indexable"] = "No"
            data["Indexability_Reason"] = "Non-200"
        else:
            data["Is_Indexable"] = "Yes"
            data["Indexability_Reason"] = "Page is indexable"

        return data

    def status_type_label(self, status: int) -> str:
        mapping = {
            200: "OK",
            301: "Permanent Redirect",
            302: "Temporary Redirect",
            307: "Temporary Redirect",
            308: "Permanent Redirect",
            404: "Not Found",
            403: "Forbidden",
            500: "Server Error",
            503: "Service Unavailable",
        }
        return mapping.get(status, f"Status {status}")


# ------------------------
# LAYER-BASED BFS CRAWL
# ------------------------
async def layer_bfs_crawl(
    seed_urls: List[str],
    checker: URLChecker,
    max_urls: int,
    scope_mode: str,
    include_regex: Optional[str],
    exclude_regex: Optional[str],
    show_partial_callback=None
) -> List[Dict]:
    """
    Layer-based BFS:
    1) Start with 'seed_urls' as the first layer.
    2) Fetch them in parallel, discover links, filter them by scope + regex.
    3) Next iteration => newly discovered links become the next layer.
    4) Stop when no new URLs or we reach max_urls.

    No BFS depth => effectively unlimited depth until we run out of URLs or hit max_urls.
    """
    visited: Set[str] = set()
    current_layer = set(normalize_url(u) for u in seed_urls if u.strip())
    results = []

    await checker.setup()

    while current_layer and len(visited) < max_urls:
        # 1) Prepare tasks for this layer
        layer_list = list(current_layer)
        current_layer.clear()

        # 2) BFS concurrency: gather all fetches in parallel
        tasks = [checker.fetch_and_parse(u) for u in layer_list]
        layer_results = await asyncio.gather(*tasks, return_exceptions=True)

        # 3) Append to overall results
        valid_rows = [r for r in layer_results if isinstance(r, dict)]
        results.extend(valid_rows)

        # 4) Mark visited
        for u in layer_list:
            visited.add(u)
        
        # 5) For each successful fetch, discover links => next layer
        #    but only if we haven't exceeded max_urls
        next_layer = set()
        for i, row in enumerate(valid_rows):
            final_url = row["Final_URL"] or row["Original_URL"]
            try:
                # discover links only if final status 200 & is_html, etc.
                # but let's do a minimal approach: we always do discover_links
                discovered = await discover_links(final_url, checker.session, checker.user_agent)
                base_seed = seed_urls[0]  # reference for scope
                for link in discovered:
                    link = normalize_url(link)
                    # Check scope
                    if not in_scope(base_seed, link, scope_mode):
                        continue
                    # Check include/exclude
                    if not regex_filter(link, include_regex, exclude_regex):
                        continue
                    if link not in visited and len(visited) + len(next_layer) < max_urls:
                        next_layer.add(link)
            except:
                continue
        
        current_layer = next_layer

        # partial UI update
        if show_partial_callback:
            show_partial_callback(results, len(visited), max_urls)

    await checker.close()
    return results


# ---------------
# Discover Links
# ---------------
async def discover_links(url: str, session: aiohttp.ClientSession, user_agent: str) -> List[str]:
    """
    Minimal fetch to get raw HTML and parse <a href>.
    """
    headers = {"User-Agent": user_agent}
    out = []
    try:
        async with session.get(url, headers=headers, ssl=False, allow_redirects=False) as resp:
            if resp.status == 200 and resp.content_type and resp.content_type.startswith("text/html"):
                html = await resp.text(errors='replace')
                soup = BeautifulSoup(html, "lxml")
                for a in soup.find_all("a", href=True):
                    abs_link = urljoin(url, a["href"])
                    out.append(abs_link)
    except:
        pass
    return out

# ---------------
# Sitemap
# ---------------
def parse_sitemap(url: str) -> List[str]:
    """
    Very simple: parse <loc> from a single-level sitemap.
    """
    out = []
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(resp.text)
            for loc_tag in root.findall(".//{*}loc"):
                if loc_tag.text:
                    out.append(loc_tag.text.strip())
    except:
        pass
    return out


# ---------------
# Streamlit UI
# ---------------
def main():
    st.set_page_config(layout="wide")
    st.title("Advanced URL Crawler")

    # --- Sidebar configuration ---
    st.sidebar.header("Configuration")

    concurrency = st.sidebar.slider("Concurrency", 1, 200, 10)
    user_agent_option = st.sidebar.selectbox("User Agent", list(USER_AGENTS.keys()))
    user_agent = USER_AGENTS[user_agent_option]
    respect_robots = st.sidebar.checkbox("Respect robots.txt", value=True)

    # Crawl Scope
    scope_mode = st.sidebar.radio(
        "Crawl Scope",
        ["Exact URL Only", "In Subfolder", "Same Subdomain", "All Subdomains"],
        index=2
    )

    # BFS is the only mode (layer-based), so no chunk mode or BFS depth.

    # Include/Exclude filters (Regex)
    st.sidebar.subheader("BFS Regex Filters")
    include_pattern = st.sidebar.text_input("Include Pattern (optional, regex)", "")
    exclude_pattern = st.sidebar.text_input("Exclude Pattern (optional, regex)", "")

    # --- Main area for input method ---
    st.header("Input URLs")
    input_method = st.radio("Input method", ["Direct Input", "Sitemap"], horizontal=True)

    urls = []
    if input_method == "Direct Input":
        st.write("Enter URLs (one per line)")
        text_input = st.text_area("", "")
        if text_input.strip():
            lines = text_input.strip().splitlines()
            urls = [u.strip() for u in lines if u.strip()]
    else:
        # Sitemaps
        st.write("Enter Sitemap URL")
        sitemap_val = st.text_input("Sitemap URL", "")
        if st.button("Fetch Sitemap"):
            if sitemap_val.strip():
                sm_urls = parse_sitemap(sitemap_val.strip())
                st.write(f"Fetched {len(sm_urls)} URLs from sitemap.")
                urls = sm_urls
            else:
                st.warning("Please enter a sitemap URL first.")

    # Start crawl button
    if st.button("Start Crawl"):
        if not urls:
            st.warning("No URLs to crawl. Please provide something above.")
            return

        # We'll do BFS on everything from 'urls' as seeds
        # NOTE: BFS is unlimited depth by default => no BFS depth.

        # Setup partial results placeholders
        progress_placeholder = st.empty()
        table_placeholder = st.empty()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        def show_partial_data(res_list, done_count, total_count):
            # partial updates
            pct = int((done_count / total_count) * 100) if total_count else 100
            progress_placeholder.progress(min(pct, 100))
            temp_df = pd.DataFrame(res_list)
            table_placeholder.dataframe(temp_df.tail(10), use_container_width=True)

        checker = URLChecker(
            user_agent=user_agent,
            concurrency=concurrency,
            timeout=DEFAULT_TIMEOUT,
            respect_robots=respect_robots
        )

        final_results = loop.run_until_complete(
            layer_bfs_crawl(
                seed_urls=urls,
                checker=checker,
                max_urls=DEFAULT_MAX_URLS,
                scope_mode=scope_mode,
                include_regex=include_pattern,
                exclude_regex=exclude_pattern,
                show_partial_callback=show_partial_data
            )
        )
        loop.close()
        progress_placeholder.empty()

        if not final_results:
            st.warning("No results found or BFS ended with no data.")
            return

        df = pd.DataFrame(final_results)

        # Display final
        st.subheader("Crawl Results")
        st.dataframe(df, use_container_width=True)

        # CSV download
        csv_data = df.to_csv(index=False).encode("utf-8")
        now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        st.download_button(
            label="Download CSV",
            data=csv_data,
            file_name=f"crawl_results_{now_str}.csv",
            mime="text/csv"
        )

        # Summaries
        show_summary(df)


def show_summary(df: pd.DataFrame):
    st.subheader("Summary")
    st.write("**Initial Status Code Distribution**")
    init_counts = df["Initial_Status_Code"].value_counts(dropna=False)
    for code, cnt in init_counts.items():
        st.write(f"{code}: {cnt}")

    st.write("**Final Status Code Distribution**")
    final_counts = df["Final_Status_Code"].value_counts(dropna=False)
    for code, cnt in final_counts.items():
        st.write(f"{code}: {cnt}")

    st.write("**Blocked by Robots.txt?**")
    block_counts = df["Is_Blocked_by_Robots"].value_counts(dropna=False)
    for val, cnt in block_counts.items():
        st.write(f"{val}: {cnt}")

    st.write("**Indexable?**")
    idx_counts = df["Is_Indexable"].value_counts(dropna=False)
    for val, cnt in idx_counts.items():
        st.write(f"{val}: {cnt}")


if __name__ == "__main__":
    main()
