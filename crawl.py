import streamlit as st
import pandas as pd
import re
import asyncio
import nest_asyncio
from datetime import datetime
from typing import List, Dict, Set
import gc
import logging
from collections import deque
import aiohttp
import orjson
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import xml.etree.ElementTree as ET

# We apply nest_asyncio so we can run async code in Streamlit
nest_asyncio.apply()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='url_checker.log'
)

# -----------------------------
# GLOBAL CONSTANTS
# -----------------------------
MAX_URLS = 25000   # total limit for BFS + sitemaps + manual
TIMEOUT_DURATION = 15
CHUNK_SIZE = 250
RETRY_TIMEOUT_DURATION = 30

# Some default user agents + "Custom"
DEFAULT_UA_OPTIONS = [
    "Googlebot (Desktop)",
    "Googlebot (Mobile)",
    "Bingbot",
    "Chrome Desktop",
    "Custom"
]

# Pre-mapped user agent strings for convenience
UA_STRINGS = {
    "Googlebot (Desktop)": (
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    ),
    "Googlebot (Mobile)": (
        "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/41.0.2272.96 Mobile Safari/537.36 (compatible; Googlebot/2.1; "
        "+http://www.google.com/bot.html)"
    ),
    "Bingbot": (
        "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)"
    ),
    "Chrome Desktop": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/110.0.0.0 Safari/537.36"
    )
}


class NubelaChecker:
    """
    Main class that handles BFS crawling, sitemap parsing,
    one-hop redirect checks, and final HTML parsing for H1, Title, etc.
    """

    def __init__(self, user_agent: str):
        self.user_agent = user_agent
        self.discovered_urls: Set[str] = set()  # all unique URLs discovered so far
        self.results = []
        self.session = None
        self.robots_cache = {}
        self.timeout = TIMEOUT_DURATION

    async def setup_session(self, timeout_duration=TIMEOUT_DURATION):
        """
        Create an aiohttp session with concurrency limits and timeouts.
        """
        self.connector = aiohttp.TCPConnector(
            limit=200,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            force_close=False
        )
        self.session_timeout = aiohttp.ClientTimeout(
            total=None, connect=timeout_duration, sock_read=timeout_duration
        )
        self.session = aiohttp.ClientSession(
            connector=self.connector,
            timeout=self.session_timeout,
            json_serialize=orjson.dumps
        )

    async def close_session(self):
        """Close the underlying aiohttp session."""
        if self.session:
            await self.session.close()

    # -----------------------------
    # SITEMAP LOGIC
    # -----------------------------
    async def fetch_sitemaps_from_robots(self, domain_url: str) -> List[str]:
        """
        Parse the domain's robots.txt for `Sitemap:` lines.
        Return a list of sitemap URLs found.
        """
        try:
            async with self.session.get(domain_url + "/robots.txt", ssl=False) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    # Grab lines like "Sitemap: https://example.com/sitemap.xml"
                    sitemap_urls = []
                    for line in text.splitlines():
                        line = line.strip()
                        if line.lower().startswith('sitemap:'):
                            parts = re.split(r"\s+", line, maxsplit=1)
                            if len(parts) == 2:
                                sm_url = parts[1].strip()
                                if sm_url:
                                    sitemap_urls.append(sm_url)
                    return sitemap_urls
        except Exception as e:
            logging.warning(f"Couldn't fetch robots.txt from {domain_url}: {str(e)}")
        return []

    async def parse_sitemap(self, sitemap_url: str) -> List[str]:
        """
        Fetch and parse an XML sitemap, extracting <loc> entries.
        If it's a sitemap index, parse each child sitemap (recursively).
        """
        found_urls = []
        try:
            async with self.session.get(sitemap_url, ssl=False) as resp:
                if resp.status == 200:
                    content = await resp.text()
                    # Parse as XML
                    root = ET.fromstring(content)

                    # Check if it's a sitemap index or a single sitemap
                    ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

                    # If we find <sitemap> tags, it's a sitemap index
                    sitemap_tags = root.findall('ns:sitemap', ns)
                    if sitemap_tags:
                        # It's a sitemap index
                        for sm in sitemap_tags:
                            loc_elem = sm.find('ns:loc', ns)
                            if loc_elem is not None and loc_elem.text:
                                child_sitemap = loc_elem.text.strip()
                                child_urls = await self.parse_sitemap(child_sitemap)
                                found_urls.extend(child_urls)
                                # Stop if we exceed the limit
                                if len(found_urls) >= MAX_URLS:
                                    break
                    else:
                        # It's a normal sitemap with <url> entries
                        url_tags = root.findall('ns:url', ns)
                        for ut in url_tags:
                            loc_elem = ut.find('ns:loc', ns)
                            if loc_elem is not None and loc_elem.text:
                                found_urls.append(loc_elem.text.strip())
                                if len(found_urls) >= MAX_URLS:
                                    break
        except Exception as e:
            logging.warning(f"Error parsing sitemap {sitemap_url}: {str(e)}")

        return found_urls

    # -----------------------------
    # BFS LOGIC
    # -----------------------------
    async def crawl_bfs(self, start_urls: List[str], discovered_callback=None):
        """
        Perform a BFS for each start URL, discovering internal domain links only.
        We do real-time checks (one-hop redirect) as we go, so we can see the final page
        and parse further links if final is 200.
        """
        queue = deque(start_urls)

        while queue:
            current_url = queue.popleft()
            # If we already processed or discovered this, skip
            if current_url in self.discovered_urls:
                continue

            # Mark discovered
            self.discovered_urls.add(current_url)
            if discovered_callback:
                discovered_callback(len(self.discovered_urls))

            # If we exceed limit, stop BFS
            if len(self.discovered_urls) >= MAX_URLS:
                break

            # 1. Check the URL (one-hop redirect)
            result = await self.check_url(current_url)
            self.results.append(result)

            # 2. If final is 200, parse the HTML for new links
            final_code = result.get("Redirected_Status_Code") or result.get("Initial_Status_Code", "")
            if str(final_code) == "200":
                # The final resolved URL is "Redirected_URL" if 3xx initially, else current
                final_url = result.get("Redirected_URL") or current_url

                # parse link from final HTML
                html_content = result.get("Final_HTML", "")
                if html_content:
                    extracted = self.extract_internal_links(html_content, final_url)
                    for link in extracted:
                        if link not in self.discovered_urls and len(self.discovered_urls) < MAX_URLS:
                            queue.append(link)

            # yield occasionally so the event loop can update UI
            await asyncio.sleep(0)

    def extract_internal_links(self, html_content: str, base_url: str) -> List[str]:
        """
        Parse <a href="..."> links from the HTML, return only internal (same domain).
        """
        domain = urlparse(base_url).netloc
        scheme = urlparse(base_url).scheme
        soup = BeautifulSoup(html_content, 'lxml')
        found_links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href'].strip()
            abs_link = urljoin(base_url, href)
            parsed = urlparse(abs_link)
            # Only follow same domain, http/https
            if parsed.netloc == domain and parsed.scheme in ["http", "https"]:
                found_links.append(abs_link)
        return found_links

    # -----------------------------
    # ONE-HOP REDIRECT CHECK
    # -----------------------------
    async def check_url(self, url: str) -> Dict:
        """
        1. Send first GET => store 'Initial_Status_Code'.
        2. If 3xx => do a second GET => store 'Redirected_Status_Code', 'Redirected_URL'.
        3. If final is 200, parse Title, Desc, H1, etc.
        Return a dictionary for CSV.
        """
        result = {
            "Original_URL": url,
            "Initial_Status_Code": "",
            "Redirected_URL": "",
            "Redirected_Status_Code": "",
            "Final_HTML": "",  # We'll store if final = 200 for BFS link extraction
            "Title": "",
            "Meta_Description": "",
            "H1_Text": "",
        }
        headers = {"User-Agent": self.user_agent}

        try:
            # FIRST GET
            async with self.session.get(url, headers=headers, ssl=False, allow_redirects=False) as resp:
                result["Initial_Status_Code"] = str(resp.status)
                if resp.status in [301, 302, 307, 308]:
                    location = resp.headers.get("Location")
                    if location:
                        redirected_url = urljoin(url, location)
                        result["Redirected_URL"] = redirected_url
                        # SECOND GET
                        async with self.session.get(redirected_url, headers=headers, ssl=False, allow_redirects=False) as resp2:
                            result["Redirected_Status_Code"] = str(resp2.status)
                            if resp2.status == 200 and resp2.content_type and \
                               resp2.content_type.startswith(("text/html", "application/xhtml+xml")):
                                final_html = await resp2.text(encoding='utf-8')
                                result["Final_HTML"] = final_html
                                self.parse_page_info(final_html, result)
                            # If second is also 3xx or other, we stop
                else:
                    # Not a redirect; if 200 => parse HTML
                    if resp.status == 200 and resp.content_type and \
                       resp.content_type.startswith(("text/html", "application/xhtml+xml")):
                        final_html = await resp.text(encoding='utf-8')
                        result["Final_HTML"] = final_html
                        self.parse_page_info(final_html, result)

        except asyncio.TimeoutError:
            logging.error(f"Timeout for {url}")
            result["Initial_Status_Code"] = "Timeout"
        except Exception as e:
            logging.error(f"Error for {url}: {str(e)}")
            result["Initial_Status_Code"] = "Error"

        return result

    def parse_page_info(self, html: str, result: Dict):
        """
        Extract Title, Meta Description, first H1 from HTML.
        """
        soup = BeautifulSoup(html, "lxml")
        # Title
        title_tag = soup.find("title")
        if title_tag:
            result["Title"] = title_tag.get_text(strip=True)

        # Meta Description
        desc_tag = soup.find("meta", attrs={"name": "description"})
        if desc_tag and desc_tag.has_attr("content"):
            result["Meta_Description"] = desc_tag["content"].strip()

        # First H1 text
        h1_tag = soup.find("h1")
        if h1_tag:
            result["H1_Text"] = h1_tag.get_text(strip=True)

    # -----------------------------
    # PUBLIC ENTRYPOINT
    # -----------------------------
    async def run_checks(
        self,
        start_url: str,
        manual_urls: List[str],
        fetch_sitemaps: bool = False,
        crawl_site: bool = False,
        sitemap_urls: List[str] = [],
        discovered_callback=None,
    ) -> pd.DataFrame:
        """
        1. Create session
        2. Possibly fetch sitemaps from robots.txt + user-provided
        3. BFS if enabled, or just check the discovered set
        4. Return DataFrame of results
        """
        # 1. Setup session
        await self.setup_session()

        # 2. Start with user manual URLs
        all_urls = set(u.strip() for u in manual_urls if u.strip())

        # 3. If fetchSitemaps => parse domain's robots + user-provided sitemap URLs
        if fetch_sitemaps:
            domain = self.get_domain(start_url)
            # from domain's robots
            robots_sitemaps = await self.fetch_sitemaps_from_robots(domain)
            combined_sm = robots_sitemaps + sitemap_urls

            # parse them in order until we reach 25k
            for sm_url in combined_sm:
                if len(all_urls) >= MAX_URLS:
                    break
                sm_urls = await self.parse_sitemap(sm_url)
                for su in sm_urls:
                    all_urls.add(su)
                    if len(all_urls) >= MAX_URLS:
                        break

        # 4. BFS logic if checked
        if crawl_site:
            # BFS from [start_url] + all_urls (so BFS can discover more pages from them)
            queue_urls = [start_url.strip()] + list(all_urls)
            await self.crawl_bfs(queue_urls, discovered_callback=discovered_callback)
        else:
            # If not BFS, we just check all_urls + the start_url itself
            final_check_set = set(all_urls)
            final_check_set.add(start_url.strip())
            final_check_list = list(final_check_set)[:MAX_URLS]

            # Process them in chunk-based approach
            discovered_count = 0
            for url_ in final_check_list:
                discovered_count += 1
                if discovered_callback:
                    discovered_callback(discovered_count)
                res = await self.check_url(url_)
                self.results.append(res)

        # 5. Close session
        await self.close_session()

        # 6. Convert self.results => DataFrame
        df = self.build_dataframe(self.results)
        return df

    def build_dataframe(self, results: List[Dict]) -> pd.DataFrame:
        """
        Convert the list of result dicts into a DataFrame with fields we want.
        """
        rows = []
        for r in results:
            init_code = r.get("Initial_Status_Code", "")
            redir_code = r.get("Redirected_Status_Code", "")
            final_code = redir_code or init_code

            row = {
                "Original_URL": r.get("Original_URL", ""),
                "Initial_Status_Code": init_code,
                "Redirected_URL": r.get("Redirected_URL", ""),
                "Redirected_Status_Code": redir_code,
                "Final_Status_Code": final_code,
                "Title": r.get("Title", ""),
                "Meta_Description": r.get("Meta_Description", ""),
                "H1_Text": r.get("H1_Text", ""),
            }
            rows.append(row)
        return pd.DataFrame(rows)

    @staticmethod
    def get_domain(url: str) -> str:
        """
        Return scheme://netloc from a URL, e.g. input "https://example.com/path"
        => "https://example.com"
        """
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"


def main():
    st.title("Nubela: One-Hop Redirect & Crawl (25k limit)")

    st.write(
        """
        This app checks URLs with a one-hop redirect approach:
        - Up to 25k URLs total from BFS, sitemaps, manual.
        - For each URL, we show Initial_Status_Code, plus one redirect hop if relevant.
        - If final is 200, we parse Title, Meta Description, and the **first** <h1>.
        """
    )

    # 1. User Agent Selection
    chosen_ua = st.selectbox(
        "Select a User-Agent",
        DEFAULT_UA_OPTIONS,
        index=0
    )
    user_agent_str = UA_STRINGS.get(chosen_ua, "")  # fallback in case not in dict
    if chosen_ua == "Custom":
        custom_agent = st.text_input("Enter your custom User-Agent", value="")
        if custom_agent.strip():
            user_agent_str = custom_agent.strip()

    if not user_agent_str:
        st.warning("No valid user agent chosen; falling back to Chrome Desktop UA.")
        user_agent_str = UA_STRINGS["Chrome Desktop"]

    # 2. Single Start URL
    start_url = st.text_input(
        "Enter a single 'start URL' for BFS or domain-based sitemap fetch, e.g. 'https://example.com'"
    )

    # 3. Checkboxes
    fetch_sitemaps = st.checkbox("Fetch Sitemaps from domain's robots.txt + (if in manual input)?")
    crawl_site = st.checkbox("Crawl Entire Site (BFS)?")

    st.write("You can also provide manual URLs or direct sitemap URLs below:")

    # 4. Manual URLs
    manual_text = st.text_area(
        "Paste URLs (could be pages or sitemaps) here, separated by whitespace:",
        height=150
    )
    manual_list = re.split(r"\s+", manual_text.strip()) if manual_text.strip() else []

    # 5. Button to run
    if st.button("Run Checks"):
        if not start_url.strip():
            st.warning("Please provide a start URL (for BFS or domain-based sitemap).")
            return

        st.write("Starting checks...")
        # We create an instance of NubelaChecker
        checker = NubelaChecker(user_agent=user_agent_str)

        progress_area = st.empty()

        def discovered_callback(count):
            progress_area.write(f"Discovered/Processed so far: {count} URLs")

        # Run in an asyncio loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        df = loop.run_until_complete(
            checker.run_checks(
                start_url=start_url,
                manual_urls=manual_list,
                fetch_sitemaps=fetch_sitemaps,
                crawl_site=crawl_site,
                sitemap_urls=[],  # user might have typed them in manual_list already
                discovered_callback=discovered_callback,
            )
        )
        loop.close()

        st.write("Done! Final results:")
        st.dataframe(df, use_container_width=True)

        csv_data = df.to_csv(index=False).encode("utf-8")
        filename = f"nubela_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        st.download_button(
            label="Download CSV",
            data=csv_data,
            file_name=filename,
            mime="text/csv"
        )

if __name__ == "__main__":
    main()
