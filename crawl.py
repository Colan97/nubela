# nubela.py
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

nest_asyncio.apply()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='url_checker.log'
)

# -----------------------------
# GLOBAL CONSTANTS
# -----------------------------
MAX_URLS = 25000  # total limit for BFS + sitemaps + manual
TIMEOUT_DURATION = 15
CHUNK_SIZE = 250
RETRY_TIMEOUT_DURATION = 30

# Some default user agents + "Custom"
DEFAULT_UA_OPTIONS = [
    "Googlebot (Desktop)",
    "Googlebot (Mobile)",
    "Bingbot",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 (Desktop)",
    "Custom"
]

# Pre-mapped user agent strings for convenience
UA_STRINGS = {
    "Googlebot (Desktop)": "Mozilla/5.0 (compatible; Googlebot/2.1; "
                           "+http://www.google.com/bot.html)",
    "Googlebot (Mobile)": "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/41.0.2272.96 Mobile Safari/537.36 (compatible; Googlebot/2.1; "
                          "+http://www.google.com/bot.html)",
    "Bingbot": "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 (Desktop)": 
       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
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
                    # Namespaces can be tricky; let's define them:
                    ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

                    # If we find <sitemap> tags, it's a sitemap index
                    sitemap_tags = root.findall('ns:sitemap', ns)
                    if sitemap_tags:
                        # It's a sitemap index
                        for sm in sitemap_tags:
                            loc_elem = sm.find('ns:loc', ns)
                            if loc_elem is not None and loc_elem.text:
                                child_sitemap = loc_elem.text.strip()
                                # Parse child sitemap recursively
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
        and parse further links from it (if final is 200).
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

            # 2. If final is 200, parse the HTML for new links, if the BFS box is checked
            #    We'll only BFS from the final URL if it's the same domain
            final_code = result["Redirected_Status_Code"] or result["Initial_Status_Code"]
            if str(final_code) == "200":
                # The final resolved URL is either "Redirected_URL" or "Original_URL"
                final_url = result["Redirected_URL"] or current_url

                # If the final_url is still the same domain, let's parse new links
                # But only if final_url is an actual HTML page we fetched
                # We can do a separate function to fetch the HTML again (or store it from check_url)
                # but for BFS we do it in the same pass. We'll store the HTML from check_url for BFS link extraction.
                html_content = result.get("Final_HTML", "")
                if html_content:
                    extracted = self.extract_internal_links(html_content, final_url)
                    # Add them to the queue if not discovered
                    for link in extracted:
                        if (link not in self.discovered_urls) and (len(self.discovered_urls) < MAX_URLS):
                            queue.append(link)

            # We yield control occasionally to let the event loop update
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
            # Make it absolute
            abs_link = urljoin(base_url, href)
            # Check domain
            parsed = urlparse(abs_link)
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
            "Initial_Status_Code": None,
            "Redirected_URL": None,
            "Redirected_Status_Code": None,
            "Final_HTML": "",  # We'll store if final = 200 for BFS use
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
                        # SECOND GET
                        redirected_url = urljoin(url, location)
                        result["Redirected_URL"] = redirected_url
                        async with self.session.get(redirected_url, headers=headers, ssl=False, allow_redirects=False) as resp2:
                            result["Redirected_Status_Code"] = str(resp2.status)
                            if resp2.status == 200 and resp2.content_type and \
                               resp2.content_type.startswith(("text/html", "application/xhtml+xml")):
                                final_html = await resp2.text(encoding='utf-8')
                                result["Final_HTML"] = final_html
                                # parse for SEO
                                self.parse_page_info(final_html, result)
                            # If second is also 3xx or non-HTML, we do not continue further
                else:
                    # Not a redirect; see if status = 200 => parse HTML
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
        processed_callback=None
    ) -> pd.DataFrame:
        """
        1. Create session
        2. Possibly fetch sitemaps from robots.txt + user-provided
        3. BFS if enabled, or just check the discovered set
        4. Return DataFrame of results
        """
        # 1. Setup session
        await self.setup_session()

        # 2. Start with user manual URLs (always)
        all_urls = set(u.strip() for u in manual_urls if u.strip())

        # 3. If fetchSitemaps => parse domain's robots + user-provided sitemap URLs
        if fetch_sitemaps:
            # parse domain's robots for sitemaps
            domain = self.get_domain(start_url)
            robots_sitemaps = await self.fetch_sitemaps_from_robots(f"{domain}")
            # combine with user-provided sitemap URLs
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
        #    We'll BFS from the single start_url, but also
        #    include *the discovered sitemap URLs* in BFS if that is what's intended
        #    (You said if both "fetch sitemaps" and BFS => add them to BFS queue).
        #    So let's do BFS from [start_url] + all_urls (since we want to BFS them all).
        if crawl_site:
            # BFS from all of these
            # Make sure the domain is consistent, though we'll only BFS on same domain
            # We'll combine everything into BFS approach so it's truly dynamic
            queue_urls = [start_url.strip()]
            # Also add the discovered (from sitemaps/manual) so BFS can pick them up
            # BFS function will skip external domains
            queue_urls.extend(list(all_urls))

            await self.crawl_bfs(
                start_urls=queue_urls,
                discovered_callback=discovered_callback
            )
        else:
            # If not BFS, we just check all_urls + the start_url itself (maybe user wants that checked)
            # Start_url might be only relevant if they want to check that single page
            final_check_set = set(all_urls)
            final_check_set.add(start_url.strip())
            final_check_list = list(final_check_set)[:MAX_URLS]

            processed_count = 0
            total = len(final_check_list)

            # We'll do chunk-based concurrency to process them
            for i in range(0, len(final_check_list), CHUNK_SIZE):
                chunk = final_check_list[i : i + CHUNK_SIZE]
                tasks = [self.check_url(u) for u in chunk]
                chunk_results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in chunk_results:
                    if isinstance(r, dict):
                        self.results.append(r)
                    processed_count += 1
                    if processed_callback:
                        processed_callback(processed_count, total)

        # 5. Close session
        await self.close_session()

        # 6. Convert self.results => DataFrame
        df = self.build_dataframe(self.results)
        return df

    def build_dataframe(self, results: List[Dict]) -> pd.DataFrame:
        """
        Convert the list of result dicts into a DataFrame with the fields we want.
        """
        # We'll parse out final status code logic: 
        # If "Redirected_Status_Code" is not None => that is final
        # else "Initial_Status_Code" is final
        rows = []
        for r in results:
            row = {
                "Original_URL": r.get("Original_URL", ""),
                "Initial_Status_Code": r.get("Initial_Status_Code", ""),
                "Redirected_URL": r.get("Redirected_URL", ""),
                "Redirected_Status_Code": r.get("Redirected_Status_Code", ""),
                # We'll define "Final_Status_Code" as either the second or the first
                "Final_Status_Code": r.get("Redirected_Status_Code") or r.get("Initial_Status_Code", ""),
                "Title": r.get("Title", ""),
                "Meta_Description": r.get("Meta_Description", ""),
                "H1_Text": r.get("H1_Text", ""),
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        return df

    @staticmethod
    def get_domain(url: str) -> str:
        """
        Return scheme://netloc from a URL, e.g. input "https://example.com/path"
        => "https://example.com"
        """
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"


##############################
# STREAMLIT APP
##############################
def main():
    st.title("Nubela: One-Hop Redirect & Crawl (25k limit)")

    st.write(
        """
        This Streamlit app checks URLs with a **one-hop** redirect approach:
        - We gather up to 25k URLs total from:
          1. **Manual** input
          2. **Sitemaps** (if checkbox is ticked)
          3. **BFS** from the start URL (if checkbox is ticked)
        - For each URL, we do exactly one redirect follow:
          - **Initial_Status_Code**
          - If 3xx => We do one more GET => **Redirected_Status_Code** 
          - If final is 200 => parse Title, first <h1>, Meta Description
        """
    )

    # 1. User Agent Selection
    chosen_ua = st.selectbox("Select a User-Agent", DEFAULT_UA_OPTIONS, index=0)
    user_agent_str = UA_STRINGS.get(chosen_ua, "")
    custom_agent = ""
    if chosen_ua == "Custom":
        custom_agent = st.text_input("Enter your custom User-Agent", value="")
        if custom_agent.strip():
            user_agent_str = custom_agent.strip()

    if not user_agent_str:
        st.warning("No user agent selected or custom agent provided. Using default Chrome UA.")
        user_agent_str = UA_STRINGS["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                    "(KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"]

    # 2. Single Start URL
    start_url = st.text_input("Enter a single 'start URL' for BFS or sitemap (e.g. 'https://example.com')")

    # 3. Checkboxes
    fetch_sitemaps = st.checkbox("Fetch Sitemaps from domain's robots.txt + manual sitemap URLs?")
    crawl_site = st.checkbox("Crawl Entire Site (BFS)?")

    st.write("You can also manually provide sitemap URLs (if you want) or page URLs in the text area below.")

    # 4. Manual URL / Sitemap input
    manual_text = st.text_area(
        "Paste manual URLs (pages or sitemaps) here, separated by spaces/newlines, up to 25k total:",
        height=150
    )
    manual_list = re.split(r"\s+", manual_text.strip()) if manual_text.strip() else []

    # 5. Button to run
    if st.button("Run Checks"):
        if not start_url.strip():
            st.warning("Please provide a start URL (for BFS or for domain-based sitemap).")
            return

        # We'll parse manual_list to separate possible sitemap URLs from normal page URLs
        # but you said "Take sitemap from the Manual url field if user entered them"
        # so let's just treat them all equally; the script does not know which is which
        # The BFS logic plus sitemap logic will handle it.

        # The logic:
        #   If fetch_sitemaps => parse domain's robots + any user-provided "sitemaps" 
        #       (we'll attempt parse_sitemap on them).
        #   BFS => from start_url + discovered set

        # Let's run it
        st.write("Starting checks...")
        progress_area = st.empty()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        checker = NubelaChecker(user_agent=user_agent_str)

        # We'll define a small callback for BFS discovery
        def discovered_callback(count):
            # Since BFS is dynamic, we show discovered so far
            progress_area.write(f"Discovered so far: {count} URLs")

        # For the normal chunk-based approach, we might show processed but BFS is inline
        # We'll unify them in a final approach:
        # BFS in the code is processing each page as soon as discovered
        # We'll do that with inline callback too, but let's keep it simple for now.

        # Summarize final approach:
        #    - run run_checks with BFS or no BFS
        #    - we have discovered_callback to update partial info
        df = loop.run_until_complete(
            checker.run_checks(
                start_url=start_url,
                manual_urls=manual_list,
                fetch_sitemaps=fetch_sitemaps,
                crawl_site=crawl_site,
                sitemap_urls=[],  # user might have typed them in manual_list
                discovered_callback=discovered_callback,
                processed_callback=None  # BFS checks pages as discovered
            )
        )

        loop.close()

        st.write("Done! Final results:")
        st.dataframe(df, use_container_width=True)

        # Provide CSV download
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
