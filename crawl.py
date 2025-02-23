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
from asyncio import as_completed

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
# Basic Helpers
# ----------------------------------------
def normalize_url(url: str) -> str:
    """
    Remove #fragment, strip whitespace, ensure consistent format.
    """
    url = url.strip()
    parsed = urlparse(url)
    # drop fragment
    normalized = parsed._replace(fragment="")
    return normalized.geturl()

def in_same_domain(base_url: str, test_url: str) -> bool:
    """
    Example: Restrict BFS to exactly the same domain (netloc).
    You can expand to subdomain logic, subfolder logic, etc. if desired.
    """
    base_netloc = urlparse(base_url).netloc.lower()
    test_netloc = urlparse(test_url).netloc.lower()
    return (test_netloc == base_netloc)

# Optionally add a regex filter, if you like:
def pass_regex_filters(url: str, include_pattern: Optional[str], exclude_pattern: Optional[str]) -> bool:
    if include_pattern:
        if not re.search(include_pattern, url):
            return False
    if exclude_pattern:
        if re.search(exclude_pattern, url):
            return False
    return True

# ----------------------------------------
# URLChecker
# ----------------------------------------
class URLChecker:
    def __init__(self, user_agent: str, concurrency: int = 10, timeout: int = 15):
        self.user_agent = user_agent
        self.max_concurrency = concurrency
        self.timeout_duration = timeout
        self.robots_cache = {}
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

    @staticmethod
    def is_html(resp: aiohttp.ClientResponse) -> bool:
        ctype = resp.content_type
        return ctype and ctype.startswith("text/html")

    async def fetch_and_parse(self, url: str) -> Dict:
        """
        Returns a dict with SEO data. 
        HTTP Status fields are stored as strings to avoid ArrowInvalid 
        when mixing int & str (e.g., 'Timeout').
        """
        url = normalize_url(url)
        # initialize row
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
            "Is_Blocked_by_Robots": "No",
            "Robots_Block_Rule": "",
            "Is_Indexable": "No",
            "Indexability_Reason": "",
            "HTTP_Last_Modified": "",
            "Timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

        headers = {"User-Agent": self.user_agent}
        async with self.semaphore:
            try:
                async with self.session.get(url, headers=headers, ssl=False, allow_redirects=False) as resp:
                    init_str = str(resp.status)
                    data["Initial_Status_Code"] = init_str
                    data["Initial_Status_Type"] = self.status_type_label(resp.status)
                    data["Final_URL"] = str(resp.url)

                    if resp.status in (301, 302, 307, 308) and resp.headers.get("Location"):
                        return await self.handle_redirect_chain(url, data, headers)
                    else:
                        if resp.status == 200 and self.is_html(resp):
                            content = await resp.text(errors='replace')
                            return self.parse_html_content(
                                data, content, resp.headers, str(resp.status)
                            )
                        else:
                            # partial
                            data["Final_Status_Code"] = init_str
                            data["Final_Status_Type"] = data["Initial_Status_Type"]
                            data["Indexability_Reason"] = "Non-200 or Non-HTML"
                            return data
            except asyncio.TimeoutError:
                data["Initial_Status_Code"] = "Timeout"
                data["Initial_Status_Type"] = "Timeout"
                data["Final_Status_Code"] = "Timeout"
                data["Final_Status_Type"] = "Timeout"
                data["Indexability_Reason"] = "Timeout"
                return data
            except Exception as e:
                err_str = str(e)
                data["Initial_Status_Code"] = "Error"
                data["Initial_Status_Type"] = err_str
                data["Final_Status_Code"] = "Error"
                data["Final_Status_Type"] = err_str
                data["Indexability_Reason"] = "Exception"
                return data

    async def handle_redirect_chain(self, origin_url: str, data: Dict, headers: Dict) -> Dict:
        """
        Follow up to MAX_REDIRECTS manually.
        """
        current_url = origin_url
        for _ in range(MAX_REDIRECTS):
            try:
                async with self.session.get(current_url, headers=headers, ssl=False, allow_redirects=False) as resp:
                    st_str = str(resp.status)
                    data["Final_URL"] = str(resp.url)
                    data["Final_Status_Code"] = st_str
                    data["Final_Status_Type"] = self.status_type_label(resp.status)

                    loc = resp.headers.get("Location")
                    if resp.status in (301, 302, 307, 308) and loc:
                        current_url = urljoin(current_url, loc)
                        current_url = normalize_url(current_url)
                        continue
                    else:
                        # parse if 200 HTML
                        if resp.status == 200 and self.is_html(resp):
                            content = await resp.text(errors='replace')
                            return self.parse_html_content(data, content, resp.headers, st_str)
                        else:
                            data["Indexability_Reason"] = "Non-200 or Non-HTML in redirect chain"
                            return data
            except asyncio.TimeoutError:
                data["Final_Status_Code"] = "Timeout"
                data["Final_Status_Type"] = "Timeout"
                data["Indexability_Reason"] = "Timeout in redirect chain"
                return data
            except Exception as e:
                data["Final_Status_Code"] = "Error"
                data["Final_Status_Type"] = str(e)
                data["Indexability_Reason"] = "Exception in redirect chain"
                return data

        data["Final_Status_Code"] = "Redirect Loop"
        data["Final_Status_Type"] = "Redirect Loop"
        data["Indexability_Reason"] = "Max Redirects Exceeded"
        return data

    def parse_html_content(self, data: Dict, html: str, headers: aiohttp.typedefs.LooseHeaders,
                           final_status_str: str) -> Dict:
        soup = BeautifulSoup(html, "lxml")

        t = soup.find("title")
        data["Title"] = t.get_text(strip=True) if t else ""

        desc_tag = soup.find("meta", attrs={"name": "description"})
        if desc_tag and desc_tag.has_attr("content"):
            data["Meta_Description"] = desc_tag["content"]

        h1_tags = soup.find_all("h1")
        data["H1_Count"] = len(h1_tags)
        data["H1_Text"] = h1_tags[0].get_text(strip=True) if h1_tags else ""

        canon_tag = soup.find("link", attrs={"rel": "canonical"})
        data["Canonical_URL"] = canon_tag["href"] if canon_tag and canon_tag.has_attr("href") else ""

        meta_robots = soup.find("meta", attrs={"name": "robots"})
        if meta_robots and meta_robots.has_attr("content"):
            data["Meta_Robots"] = meta_robots["content"]

        x_robots = headers.get("X-Robots-Tag", "")
        data["X_Robots_Tag"] = x_robots

        html_tag = soup.find("html")
        if html_tag and html_tag.has_attr("lang"):
            data["HTML_Lang"] = html_tag["lang"]

        data["HTTP_Last_Modified"] = headers.get("Last-Modified", "")

        # check noindex
        comb = f"{data['Meta_Robots'].lower()} {x_robots.lower()}"
        if "noindex" in comb:
            data["Is_Indexable"] = "No"
            data["Indexability_Reason"] = "Noindex directive"
        elif final_status_str != "200":
            data["Is_Indexable"] = "No"
            data["Indexability_Reason"] = "Non-200"
        else:
            data["Is_Indexable"] = "Yes"
            data["Indexability_Reason"] = "Page is indexable"

        return data

    def status_type_label(self, code: int) -> str:
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


# ----------------------------------
# LAYER-BASED BFS w/ as_completed
# ----------------------------------
async def layer_bfs_crawl(
    seed_urls: List[str],
    checker: URLChecker,
    max_urls: int,
    include_pattern: Optional[str] = None,
    exclude_pattern: Optional[str] = None,
    show_partial_callback=None
) -> List[Dict]:
    """
    1) current_layer = set of seed_urls
    2) For each layer:
       - fetch them in parallel, but update results as each finishes (as_completed).
       - gather discovered links, then next layer = those new links
    3) Stop if no new links or we reach max_urls.
    """

    visited: Set[str] = set()
    current_layer = {normalize_url(u) for u in seed_urls}
    results: List[Dict] = []

    await checker.setup()

    while current_layer and len(visited) < max_urls:
        layer_list = list(current_layer)
        current_layer.clear()

        # Mark them as visited so we don't pick them up next time
        for u in layer_list:
            visited.add(u)

        # fetch them in parallel
        tasks = [checker.fetch_and_parse(u) for u in layer_list]

        layer_results = []
        # Process each result as soon as it completes
        for coro in as_completed(tasks):
            res = await coro
            layer_results.append(res)
            results.append(res)

            # partial update to UI
            if show_partial_callback:
                show_partial_callback(results, len(visited), max_urls)

        # discover links from each result
        next_layer: Set[str] = set()
        for row in layer_results:
            # only discover if final status = "200" or "OK"? Up to you
            # We'll do it for any 200 + text/html.
            # We stored final URL in row["Final_URL"] or row["Original_URL"]
            final_url = row["Final_URL"] or row["Original_URL"]
            discovered = await discover_links(final_url, checker.session, checker.user_agent)

            for link in discovered:
                link = normalize_url(link)
                # if you want to restrict domain or subfolders, do that here
                # e.g.: if not in_same_domain(seed_urls[0], link): continue

                # also apply include/exclude
                if not pass_regex_filters(link, include_pattern, exclude_pattern):
                    continue

                if link not in visited and len(visited) + len(next_layer) < max_urls:
                    next_layer.add(link)

        current_layer = next_layer

    await checker.close()
    return results


async def discover_links(url: str, session: aiohttp.ClientSession, user_agent: str) -> List[str]:
    """
    Minimal fetch just to extract <a href> links.
    """
    out = []
    try:
        headers = {"User-Agent": user_agent}
        async with session.get(url, headers=headers, ssl=False, allow_redirects=False) as resp:
            if resp.status == 200 and resp.content_type and resp.content_type.startswith("text/html"):
                html = await resp.text(errors="replace")
                soup = BeautifulSoup(html, "lxml")
                for a in soup.find_all("a", href=True):
                    abs_link = urljoin(url, a["href"])
                    out.append(abs_link)
    except:
        pass
    return out

# ----------------------------------
# Streamlit UI
# ----------------------------------
def main():
    st.set_page_config(layout="wide")  # allows more horizontal space
    st.title("Advanced URL Crawler (Layer-based BFS w/ Immediate Updates)")

    # sidebar config
    st.sidebar.header("Configuration")
    concurrency = st.sidebar.slider("Concurrency", 1, 200, 10)
    ua_option = st.sidebar.selectbox("User Agent", list(USER_AGENTS.keys()))
    user_agent = USER_AGENTS[ua_option]

    include_regex = st.sidebar.text_input("Include Regex (optional)")
    exclude_regex = st.sidebar.text_input("Exclude Regex (optional)")

    st.write("""
    **Instructions**  
    - Enter one or more seed URLs below.  
    - Click "Start Crawl" to begin a BFS with unlimited depth, stopping only if no new pages or at 25k pages.  
    - Partial results appear as soon as each URL finishes.
    """)

    url_input = st.text_area("Enter seed URLs (one per line)")
    if st.button("Start Crawl"):
        lines = [l.strip() for l in url_input.splitlines() if l.strip()]
        if not lines:
            st.warning("No URLs to crawl.")
            return

        # partial UI placeholders
        progress_placeholder = st.empty()
        table_placeholder = st.empty()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        def partial_update(res_list, done_count, total_count):
            pct = int((done_count / total_count) * 100) if total_count else 100
            progress_placeholder.progress(min(pct, 100))

            # show entire DataFrame w/ scrollable height
            # so we can see older URLs too.
            df_temp = pd.DataFrame(res_list)
            table_placeholder.dataframe(df_temp, height=600)  # scrollable table

        checker = URLChecker(user_agent=user_agent, concurrency=concurrency, timeout=DEFAULT_TIMEOUT)

        final_results = loop.run_until_complete(
            layer_bfs_crawl(
                seed_urls=lines,
                checker=checker,
                max_urls=DEFAULT_MAX_URLS,
                include_pattern=include_regex,
                exclude_pattern=exclude_regex,
                show_partial_callback=partial_update
            )
        )
        loop.close()
        progress_placeholder.empty()

        if not final_results:
            st.warning("No results found.")
            return

        df = pd.DataFrame(final_results)
        st.subheader("Final Results")
        st.dataframe(df, height=600)

        # CSV download
        csv_data = df.to_csv(index=False).encode("utf-8")
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.download_button(
            "Download CSV",
            csv_data,
            file_name=f"crawl_results_{now}.csv",
            mime="text/csv"
        )

        # optional summary
        show_summary(df)


def show_summary(df: pd.DataFrame):
    st.subheader("Summary")
    init_counts = df["Initial_Status_Code"].value_counts(dropna=False)
    st.write("**Initial Status Code Distribution**:")
    for code, cnt in init_counts.items():
        st.write(f"{code}: {cnt}")

    final_counts = df["Final_Status_Code"].value_counts(dropna=False)
    st.write("**Final Status Code Distribution**:")
    for code, cnt in final_counts.items():
        st.write(f"{code}: {cnt}")

    block_counts = df["Is_Blocked_by_Robots"].value_counts(dropna=False) \
        if "Is_Blocked_by_Robots" in df.columns else {}
    st.write("**Blocked by Robots.txt?**")
    for val, cnt in block_counts.items():
        st.write(f"{val}: {cnt}")

    indexable_counts = df["Is_Indexable"].value_counts(dropna=False)
    st.write("**Indexable?**")
    for val, cnt in indexable_counts.items():
        st.write(f"{val}: {cnt}")


if __name__ == "__main__":
    main()
