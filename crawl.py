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

nest_asyncio.apply()

# -----------------------
# Logging (Optional)
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='url_checker.log'
)

# -----------------------
# Constants
# -----------------------
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


# -----------------------
# Normalization & Scope
# -----------------------
def normalize_url(url: str) -> str:
    url = url.strip()
    parsed = urlparse(url)
    # remove fragment
    norm = parsed._replace(fragment="")
    return norm.geturl()

def in_scope(base_url: str, test_url: str, scope_mode: str) -> bool:
    """
    Simple scope check:
      - "Exact URL Only"
      - "In Subfolder"
      - "Same Subdomain"
      - "All Subdomains"
    """
    base_parsed = urlparse(base_url)
    test_parsed = urlparse(test_url)

    # If scheme differs, skip
    if test_parsed.scheme != base_parsed.scheme:
        return False

    base_netloc = base_parsed.netloc.lower()
    test_netloc = test_parsed.netloc.lower()

    if scope_mode == "Exact URL Only":
        return (test_url == base_url)

    elif scope_mode == "In Subfolder":
        # same netloc, and test_path starts with base_path
        if test_netloc != base_netloc:
            return False
        return test_parsed.path.startswith(base_parsed.path)

    elif scope_mode == "Same Subdomain":
        return (test_netloc == base_netloc)

    elif scope_mode == "All Subdomains":
        # naive approach: check if test_netloc ends with the base domain's root
        parts = base_netloc.split('.')
        if len(parts) <= 1:
            # fallback for single-level TLD
            return (test_netloc == base_netloc)
        # e.g. store.example.com => root: example.com
        root_domain = '.'.join(parts[-2:])
        return test_netloc.endswith(root_domain)

    return False

def regex_filter(url: str, include_pattern: str, exclude_pattern: str) -> bool:
    """
    If include_pattern is given, the URL must match it.
    If exclude_pattern is given, the URL must NOT match it.
    """
    if include_pattern:
        if not re.search(include_pattern, url):
            return False
    if exclude_pattern:
        if re.search(exclude_pattern, url):
            return False
    return True


# -----------------------
# URL Checker
# -----------------------
class URLChecker:
    def __init__(self, user_agent: str, concurrency: int, timeout: int, respect_robots: bool):
        self.user_agent = user_agent
        self.max_concurrency = concurrency
        self.timeout = timeout
        self.respect_robots = respect_robots
        self.robots_cache = {}
        self.connector = None
        self.session = None

    async def setup(self):
        self.connector = aiohttp.TCPConnector(
            limit=self.max_concurrency,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            force_close=False
        )
        aio_timeout = aiohttp.ClientTimeout(
            total=None,
            connect=self.timeout,
            sock_read=self.timeout
        )
        self.session = aiohttp.ClientSession(
            connector=self.connector,
            timeout=aio_timeout,
            json_serialize=orjson.dumps
        )

    async def close(self):
        if self.session:
            await self.session.close()

    async def check_robots(self, url: str) -> bool:
        """
        Return True if allowed, False if disallowed. If not respecting robots => True.
        """
        if not self.respect_robots:
            return True

        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        path_lower = parsed.path.lower()

        if base not in self.robots_cache:
            # fetch robots.txt
            rob_url = base + "/robots.txt"
            try:
                headers = {"User-Agent": self.user_agent}
                async with self.session.get(rob_url, ssl=False, headers=headers) as resp:
                    if resp.status == 200:
                        txt = await resp.text()
                        self.robots_cache[base] = txt
                    else:
                        self.robots_cache[base] = None
            except:
                self.robots_cache[base] = None

        content = self.robots_cache.get(base)
        if not content:
            return True  # no robots => allowed

        return self.parse_robots(content, path_lower)

    def parse_robots(self, text: str, path_lower: str) -> bool:
        lines = text.splitlines()
        user_agent_lower = self.user_agent.lower()
        active = False

        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split(':', 1)
            if len(parts) < 2:
                continue

            key, val = parts[0].lower(), parts[1].strip().lower()
            if key == "user-agent":
                if val == '*' or user_agent_lower in val:
                    active = True
                else:
                    active = False
            elif key == "disallow" and active:
                if val and path_lower.startswith(val):
                    return False
        return True

    def status_label(self, code: int) -> str:
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
        return mapping.get(code, f"Status {code}")

    async def fetch_and_parse(self, url: str) -> Dict:
        """
        Return a row of data for the URL.
        # CAUSE OF ARROWINVALID ERROR:
        #   We store status codes as strings so "Timeout"/"Error" won't crash PyArrow.
        """
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
            "Timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

        allowed = await self.check_robots(url)
        data["Is_Blocked_by_Robots"] = "No" if allowed else "Yes"
        if not allowed:
            data["Robots_Block_Rule"] = "Disallow"
            data["Indexability_Reason"] = "Blocked by robots.txt"
            data["Final_URL"] = url
            data["Final_Status_Code"] = "N/A"
            data["Final_Status_Type"] = "Robots Block"
            return data

        headers = {"User-Agent": self.user_agent}
        try:
            async with self.session.get(url, headers=headers, ssl=False, allow_redirects=False) as resp:
                init_str = str(resp.status)
                data["Initial_Status_Code"] = init_str
                data["Initial_Status_Type"] = self.status_label(resp.status)
                data["Final_URL"] = str(resp.url)

                loc = resp.headers.get("Location")
                if resp.status in (301, 302, 307, 308) and loc:
                    return await self.follow_redirect_chain(url, loc, data, headers)
                else:
                    if resp.status == 200 and resp.content_type and resp.content_type.startswith("text/html"):
                        text_html = await resp.text(errors='replace')
                        return self.parse_html_content(data, text_html, resp.headers, resp.status, True)
                    else:
                        data["Final_Status_Code"] = init_str
                        data["Final_Status_Type"] = data["Initial_Status_Type"]
                        data["Indexability_Reason"] = "Non-200 or non-HTML"
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

    async def follow_redirect_chain(self, orig_url: str, location: str, data: Dict, headers: Dict) -> Dict:
        current_url = orig_url
        for _ in range(MAX_REDIRECTS):
            next_url = urljoin(current_url, location)
            next_url = normalize_url(next_url)
            try:
                async with self.session.get(next_url, headers=headers, ssl=False, allow_redirects=False) as r2:
                    stat_str = str(r2.status)
                    data["Final_URL"] = str(r2.url)
                    data["Final_Status_Code"] = stat_str
                    data["Final_Status_Type"] = self.status_label(r2.status)

                    if r2.status in (301, 302, 307, 308):
                        loc2 = r2.headers.get("Location")
                        if not loc2:
                            data["Indexability_Reason"] = "Redirect with no location"
                            return data
                        current_url = next_url
                        location = loc2
                        continue
                    else:
                        if r2.status == 200 and r2.content_type and r2.content_type.startswith("text/html"):
                            html = await r2.text(errors='replace')
                            return self.parse_html_content(data, html, r2.headers, r2.status, True)
                        else:
                            data["Indexability_Reason"] = "Non-200 or non-HTML after redirect"
                            return data
            except asyncio.TimeoutError:
                data["Final_Status_Code"] = "Timeout"
                data["Final_Status_Type"] = "Request Timeout"
                data["Indexability_Reason"] = "Timeout in redirect chain"
                return data
            except Exception as e:
                data["Final_Status_Code"] = "Error"
                data["Final_Status_Type"] = str(e)
                data["Indexability_Reason"] = "Exception in redirect chain"
                return data

        data["Indexability_Reason"] = "Redirect Loop or Exceeded"
        data["Final_Status_Code"] = "Redirect Loop"
        data["Final_Status_Type"] = "Redirect Loop"
        return data

    def parse_html_content(self, data: Dict, html: str, headers: Dict, status: int, is_allowed: bool) -> Dict:
        soup = BeautifulSoup(html, "lxml")

        title = soup.find("title")
        data["Title"] = title.get_text(strip=True) if title else ""

        desc = soup.find("meta", attrs={"name": "description"})
        if desc and desc.has_attr("content"):
            data["Meta_Description"] = desc["content"]

        h1_tags = soup.find_all("h1")
        data["H1_Count"] = len(h1_tags)
        data["H1_Text"] = h1_tags[0].get_text(strip=True) if h1_tags else ""

        canon = soup.find("link", attrs={"rel": "canonical"})
        data["Canonical_URL"] = canon["href"] if canon and canon.has_attr("href") else ""

        m_robots = soup.find("meta", attrs={"name": "robots"})
        if m_robots and m_robots.has_attr("content"):
            data["Meta_Robots"] = m_robots["content"]
        x_robots = headers.get("X-Robots-Tag", "")
        data["X_Robots_Tag"] = x_robots

        html_tag = soup.find("html")
        if html_tag and html_tag.has_attr("lang"):
            data["HTML_Lang"] = html_tag["lang"]

        data["HTTP_Last_Modified"] = headers.get("Last-Modified", "")

        # evaluate indexability
        combined = f"{data['Meta_Robots'].lower()} {x_robots.lower()}"
        if "noindex" in combined:
            data["Is_Indexable"] = "No"
            data["Indexability_Reason"] = "Noindex directive"
        elif status != 200:
            data["Is_Indexable"] = "No"
            data["Indexability_Reason"] = f"Status {status}"
        elif not is_allowed:
            data["Is_Indexable"] = "No"
            data["Indexability_Reason"] = "Blocked by robots.txt"
        else:
            data["Is_Indexable"] = "Yes"
            data["Indexability_Reason"] = "Page is indexable"

        return data


# ---------------------------
# BFS: Layer-Based
# ---------------------------
async def layer_bfs(
    seed_urls: List[str],
    checker: URLChecker,
    scope_mode: str,
    include_regex: str,
    exclude_regex: str,
    max_urls: int,
    show_partial_callback=None
) -> List[Dict]:
    """
    BFS with unlimited depth. We gather all current-layer URLs, fetch them
    in parallel, discover links, check scope & regex, then move on to next layer.
    """
    visited: Set[str] = set()
    current_layer = set(normalize_url(u) for u in seed_urls if u.strip())
    results = []

    await checker.setup()

    while current_layer and len(visited) < max_urls:
        layer_list = list(current_layer)
        current_layer.clear()

        tasks = [checker.fetch_and_parse(u) for u in layer_list]
        layer_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Add to results
        valid_rows = [r for r in layer_results if isinstance(r, dict)]
        results.extend(valid_rows)

        # Mark visited
        for u in layer_list:
            visited.add(u)

        # If we still have capacity, discover next-layer links
        next_layer = set()
        for row in valid_rows:
            final_url = row["Final_URL"] or row["Original_URL"]
            # Attempt discover if it's 200 + HTML etc
            # We'll do a minimal approach: always discover links
            discovered = await discover_links(final_url, checker.session, checker.user_agent)
            # We'll apply scope & regex to discovered
            base_seed = seed_urls[0]  # reference for scope
            for link in discovered:
                link = normalize_url(link)
                if not in_scope(base_seed, link, scope_mode):
                    continue
                if not regex_filter(link, include_regex, exclude_regex):
                    continue
                if link not in visited and len(visited) + len(next_layer) < max_urls:
                    next_layer.add(link)

        current_layer = next_layer

        if show_partial_callback:
            show_partial_callback(results, len(visited), max_urls)

    await checker.close()
    return results


# ---------------------------
# Chunk (Sitemap-Only) Mode
# ---------------------------
async def process_urls_chunked(urls: List[str], checker: URLChecker, show_partial_callback=None) -> List[Dict]:
    results = []
    total = len(urls)
    seen = set()
    final_list = []

    for u in urls:
        nu = normalize_url(u)
        if nu not in seen:
            seen.add(nu)
            final_list.append(nu)

    await checker.setup()

    chunk_size = 100
    processed = 0
    for i in range(0, len(final_list), chunk_size):
        chunk = final_list[i : i + chunk_size]
        tasks = [checker.fetch_and_parse(u) for u in chunk]
        chunk_results = await asyncio.gather(*tasks, return_exceptions=True)
        valid_rows = [r for r in chunk_results if isinstance(r, dict)]
        results.extend(valid_rows)
        processed += len(chunk)
        if show_partial_callback:
            show_partial_callback(results, processed, total)

    await checker.close()
    return results


# ---------------------------
# Link Discovery
# ---------------------------
async def discover_links(url: str, session: aiohttp.ClientSession, user_agent: str) -> List[str]:
    out = []
    headers = {"User-Agent": user_agent}
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


# ---------------------------
# Sitemap Parse
# ---------------------------
def parse_sitemap(url: str) -> List[str]:
    out = []
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(r.text)
            for loc_tag in root.findall(".//{*}loc"):
                if loc_tag.text:
                    out.append(loc_tag.text.strip())
    except:
        pass
    return out


# ---------------------------
# Streamlit UI
# ---------------------------
def main():
    st.set_page_config(layout="wide")
    st.title("Advanced Crawler: BFS + Sitemap Options")

    # Sidebar
    st.sidebar.header("Configuration")
    concurrency = st.sidebar.slider("Concurrency", 1, 200, 10)
    ua_keys = list(USER_AGENTS.keys())
    chosen_ua = st.sidebar.selectbox("User Agent", ua_keys)
    user_agent = USER_AGENTS[chosen_ua]
    respect_robots = st.sidebar.checkbox("Respect robots.txt", value=True)

    scope_mode = st.sidebar.radio(
        "Crawl Scope",
        ["Exact URL Only", "In Subfolder", "Same Subdomain", "All Subdomains"],
        index=2
    )

    # BFS or not BFS
    do_bfs = st.sidebar.checkbox("Enable BFS Mode", value=True)

    # If BFS -> show regex filters
    include_pattern = ""
    exclude_pattern = ""
    if do_bfs:
        st.sidebar.subheader("BFS Regex Filters")
        include_pattern = st.sidebar.text_input("Include Regex (optional)", "")
        exclude_pattern = st.sidebar.text_input("Exclude Regex (optional)", "")

    # Main area
    st.header("Input URLs")
    st.write("You can provide direct URLs, or optionally a sitemap (or both).")

    # 1) Direct input
    st.subheader("Direct Input")
    text_input = st.text_area("Enter URLs (one per line)")

    # 2) Sitemap
    st.subheader("Sitemap Input")
    sitemap_url = st.text_input("Sitemap URL")
    # We'll store the parsed sitemap in session
    if 'sitemap_urls' not in st.session_state:
        st.session_state['sitemap_urls'] = []

    if st.button("Fetch Sitemap"):
        if sitemap_url.strip():
            sm = parse_sitemap(sitemap_url.strip())
            st.session_state['sitemap_urls'] = sm
            st.write(f"Fetched {len(sm)} URLs from sitemap.")
        else:
            st.warning("Please provide a sitemap URL first.")

    if not st.session_state['sitemap_urls']:
        st.write("No sitemap URLs yet. Please fetch a sitemap if desired.")
    else:
        st.write(f"Sitemap has {len(st.session_state['sitemap_urls'])} URLs loaded.")

    # Combine user input + sitemap if BFS and user wants
    # If BFS, we treat them as BFS seeds.
    # If not BFS, we do chunk-based for only the sitemap if user doesn't provide direct input.
    start_button = st.button("Start Crawl")

    if start_button:
        # gather direct input
        direct_list = []
        if text_input.strip():
            direct_list = [line.strip() for line in text_input.splitlines() if line.strip()]

        # gather sitemap
        sm_list = st.session_state['sitemap_urls']

        # BFS mode
        if do_bfs:
            # BFS seeds = direct input + sitemap
            seeds = direct_list + sm_list
            if not seeds:
                st.warning("No BFS seeds found (no direct input + no sitemap).")
                return

            # partial results
            progress_ph = st.empty()
            table_ph = st.empty()

            def show_partial_data(res_list, done_count, total_count):
                pct = int((done_count / total_count) * 100) if total_count else 100
                progress_ph.progress(min(pct, 100))
                tmp_df = pd.DataFrame(res_list)
                table_ph.dataframe(tmp_df.tail(10), use_container_width=True)

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            checker = URLChecker(user_agent, concurrency, DEFAULT_TIMEOUT, respect_robots)
            results = loop.run_until_complete(
                layer_bfs(
                    seed_urls=seeds,
                    checker=checker,
                    scope_mode=scope_mode,
                    include_regex=include_pattern,
                    exclude_regex=exclude_pattern,
                    max_urls=DEFAULT_MAX_URLS,
                    show_partial_callback=show_partial_data
                )
            )
            loop.close()
            progress_ph.empty()

            if not results:
                st.warning("No results from BFS.")
                return

            df = pd.DataFrame(results)
            st.subheader("Final BFS Results")
            st.dataframe(df, use_container_width=True)

            # CSV
            csv_data = df.to_csv(index=False).encode("utf-8")
            now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            st.download_button(
                label="Download CSV",
                data=csv_data,
                file_name=f"bfs_results_{now_str}.csv",
                mime="text/csv"
            )
            show_summary(df)

        else:
            # Not BFS => chunk mode
            # If user gave direct input => chunk those
            # If user gave sitemap => chunk that
            # We can combine or we do separate? Let's combine them to one list
            all_urls = direct_list + sm_list
            if not all_urls:
                st.warning("No URLs to crawl. Provide direct input or a sitemap.")
                return

            progress_ph = st.empty()
            table_ph = st.empty()

            def show_partial_data(res_list, done_count, total_count):
                pct = int((done_count / total_count) * 100) if total_count else 100
                progress_ph.progress(min(pct, 100))
                tmp_df = pd.DataFrame(res_list)
                table_ph.dataframe(tmp_df.tail(10), use_container_width=True)

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            checker = URLChecker(user_agent, concurrency, DEFAULT_TIMEOUT, respect_robots)
            results = loop.run_until_complete(
                process_urls_chunked(
                    all_urls,
                    checker,
                    show_partial_callback=show_partial_data
                )
            )
            loop.close()
            progress_ph.empty()

            if not results:
                st.warning("No results found in chunk mode.")
                return

            df = pd.DataFrame(results)
            st.subheader("Chunk Results")
            st.dataframe(df, use_container_width=True)

            csv_data = df.to_csv(index=False).encode("utf-8")
            now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            st.download_button(
                label="Download CSV",
                data=csv_data,
                file_name=f"chunk_results_{now_str}.csv",
                mime="text/csv"
            )

            show_summary(df)


def show_summary(df: pd.DataFrame):
    st.subheader("Summary")

    st.write("**Initial Status Code Distribution**")
    icounts = df["Initial_Status_Code"].value_counts(dropna=False)
    for code, cnt in icounts.items():
        st.write(f"{code}: {cnt}")

    st.write("**Final Status Code Distribution**")
    fcounts = df["Final_Status_Code"].value_counts(dropna=False)
    for code, cnt in fcounts.items():
        st.write(f"{code}: {cnt}")

    st.write("**Blocked by Robots.txt?**")
    block_counts = df["Is_Blocked_by_Robots"].value_counts(dropna=False)
    for val, cnt in block_counts.items():
        st.write(f"{val}: {cnt}")

    st.write("**Indexable?**")
    index_counts = df["Is_Indexable"].value_counts(dropna=False)
    for val, cnt in index_counts.items():
        st.write(f"{val}: {cnt}")


if __name__ == "__main__":
    main()
