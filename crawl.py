import streamlit as st
import pandas as pd
import re
import asyncio
import nest_asyncio
import aiohttp
import gc
import logging
import requests
from datetime import datetime
from typing import List, Dict, Tuple, Set
from collections import deque
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
import xml.etree.ElementTree as ET

nest_asyncio.apply()

# --------------------------
# Constants
# --------------------------
DEFAULT_TIMEOUT = 15
DEFAULT_MAX_URLS = 25000
DEFAULT_MAX_DEPTH = 3
MAX_REDIRECTS = 5

USER_AGENTS = {
    "Googlebot Desktop": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Googlebot Mobile": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Chrome Desktop": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.5481.100 Safari/537.36",
    "Custom Adidas SEO Bot": "custom_adidas_seo_x3423/1.0"
}

# --------------------------
# URL Checker Class
# --------------------------
class URLChecker:
    def __init__(self, user_agent: str, follow_robots: bool = True, concurrency: int = 10, timeout: int = 15):
        self.user_agent = user_agent
        self.follow_robots = follow_robots
        self.max_concurrency = concurrency
        self.timeout_duration = timeout
        self.robots_cache = {}
        self.connector = None
        self.session = None
        self.semaphore = None

    async def setup(self):
        self.connector = aiohttp.TCPConnector(limit=self.max_concurrency)
        timeout = aiohttp.ClientTimeout(connect=self.timeout_duration, sock_read=self.timeout_duration)
        self.session = aiohttp.ClientSession(connector=self.connector, timeout=timeout)
        self.semaphore = asyncio.Semaphore(self.max_concurrency)

    async def close(self):
        if self.session:
            await self.session.close()

    async def check_robots_txt(self, url: str) -> Tuple[bool, str]:
        if not self.follow_robots:
            return True, "Robots.txt ignored"
        
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        if base_url not in self.robots_cache:
            robots_url = f"{base_url}/robots.txt"
            try:
                async with self.session.get(robots_url, ssl=False) as resp:
                    if resp.status == 200:
                        self.robots_cache[base_url] = await resp.text()
                    else:
                        self.robots_cache[base_url] = None
            except:
                self.robots_cache[base_url] = None
        
        robots_content = self.robots_cache.get(base_url)
        if not robots_content:
            return True, "N/A"
        
        return self.parse_robots(robots_content, parsed.path)

    def parse_robots(self, content: str, path: str) -> Tuple[bool, str]:
        lines = content.splitlines()
        user_agent = self.user_agent.lower()
        path = path.lower()
        is_relevant = False
        
        for line in lines:
            line = line.strip()
            if line.startswith('User-agent:'):
                ua = line.split(':', 1)[1].strip().lower()
                is_relevant = ua == '*' or user_agent in ua
            elif is_relevant and line.startswith('Disallow:'):
                dis_path = line.split(':', 1)[1].strip().lower()
                if path.startswith(dis_path):
                    return False, f"Disallow: {dis_path}"
        return True, "N/A"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=10))
    async def fetch_and_parse(self, url: str) -> Dict:
        headers = {"User-Agent": self.user_agent}
        async with self.semaphore:
            try:
                is_allowed, block_rule = await self.check_robots_txt(url)
                initial_url = url
                final_url = url
                status_history = []
                redirect_count = 0

                # Follow redirect chain
                while redirect_count < MAX_REDIRECTS:
                    async with self.session.get(
                        final_url, 
                        headers=headers, 
                        ssl=False, 
                        allow_redirects=False
                    ) as resp:
                        status = resp.status
                        content_type = resp.headers.get('Content-Type', '')
                        status_history.append({
                            "url": final_url,
                            "status": status,
                            "type": self.get_status_type(status)
                        })

                        if 300 <= status < 400:
                            location = resp.headers.get('Location')
                            if location:
                                final_url = urljoin(final_url, location)
                                redirect_count += 1
                                continue
                        break

                # Get final page data
                async with self.session.get(final_url, headers=headers, ssl=False) as resp:
                    final_status = resp.status
                    final_content_type = resp.headers.get('Content-Type', '')
                    x_robots = resp.headers.get('X-Robots-Tag', '')
                    
                    # Parse HTML if available
                    html = ""
                    if 'text/html' in final_content_type:
                        html = await resp.text(errors='replace')
                        soup = BeautifulSoup(html, 'lxml')
                        
                        # Extract SEO elements
                        title = soup.title.string if soup.title else ''
                        meta_desc = self.get_meta_description(soup)
                        h1_tags = soup.find_all('h1')
                        h1_count = len(h1_tags)
                        h1_text = h1_tags[0].get_text(strip=True) if h1_count > 0 else ''
                        canonical = self.get_canonical_url(soup)
                        meta_robots = self.get_robots_meta(soup)
                        html_lang = self.get_html_lang(soup)
                        
                        # Indexability check
                        is_indexable, index_reason = self.check_indexability(
                            final_status, is_allowed, meta_robots, x_robots, canonical, initial_url
                        )
                    else:
                        title = meta_desc = h1_text = canonical = meta_robots = html_lang = ''
                        h1_count = 0
                        is_indexable, index_reason = False, "Non-HTML content"

                    return {
                        "Original_URL": initial_url,
                        "Initial_Status_Code": status_history[0]['status'] if status_history else final_status,
                        "Initial_Status_Type": status_history[0]['type'] if status_history else self.get_status_type(final_status),
                        "Final_URL": final_url,
                        "Final_Status_Code": final_status,
                        "Final_Status_Type": self.get_status_type(final_status),
                        "Is_Blocked_by_Robots": "Yes" if not is_allowed else "No",
                        "Robots_Block_Rule": block_rule,
                        "Title": title,
                        "Meta_Description": meta_desc,
                        "H1_Text": h1_text,
                        "H1_Count": h1_count,
                        "Canonical_URL": canonical,
                        "Meta_Robots": meta_robots,
                        "X_Robots_Tag": x_robots,
                        "HTML_Lang": html_lang,
                        "Is_Indexable": "Yes" if is_indexable else "No",
                        "Indexability_Reason": index_reason,
                        "Content_Type": final_content_type,
                        "Redirect_Chain_Length": redirect_count,
                        "Timestamp": datetime.now().isoformat(),
                        "Error": ""
                    }
            except Exception as e:
                return {
                    "Original_URL": url,
                    "Error": f"{type(e).__name__}: {str(e)}",
                    **{k: "" for k in [
                        "Initial_Status_Code", "Initial_Status_Type", "Final_URL",
                        "Final_Status_Code", "Final_Status_Type", "Is_Blocked_by_Robots",
                        "Robots_Block_Rule", "Title", "Meta_Description", "H1_Text",
                        "H1_Count", "Canonical_URL", "Meta_Robots", "X_Robots_Tag",
                        "HTML_Lang", "Is_Indexable", "Indexability_Reason", "Content_Type",
                        "Redirect_Chain_Length"
                    ]},
                    "Timestamp": datetime.now().isoformat()
                }

    def check_indexability(self, status, allowed, meta_robots, x_robots, canonical, original_url):
        reasons = []
        if status != 200:
            reasons.append(f"Status {status}")
        if not allowed:
            reasons.append("Blocked by robots.txt")
        
        robots_directives = f"{meta_robots.lower()} {x_robots.lower()}"
        if "noindex" in robots_directives:
            reasons.append("Noindex directive")
            
        if canonical and canonical != original_url:
            reasons.append(f"Canonical mismatch: {canonical}")
            
        is_indexable = len(reasons) == 0
        reason = "Indexable" if is_indexable else "; ".join(reasons)
        return is_indexable, reason

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
    def get_meta_description(soup: BeautifulSoup) -> str:
        tag = soup.find("meta", {"name": "description"})
        return tag["content"] if tag and tag.has_attr("content") else ""

    @staticmethod
    def get_canonical_url(soup: BeautifulSoup) -> str:
        tag = soup.find("link", {"rel": "canonical"})
        return tag["href"] if tag and tag.has_attr("href") else ""

    @staticmethod
    def get_robots_meta(soup: BeautifulSoup) -> str:
        tag = soup.find("meta", {"name": "robots"})
        return tag["content"] if tag and tag.has_attr("content") else ""

    @staticmethod
    def get_html_lang(soup: BeautifulSoup) -> str:
        html = soup.find("html")
        return html["lang"] if html and html.has_attr("lang") else ""

# --------------------------
# Crawling Functions
# --------------------------
def is_valid_scope(url: str, seed_url: str, scope: str) -> bool:
    parsed = urlparse(url)
    seed_parsed = urlparse(seed_url)
    
    if scope == "Exact URL Only":
        return url == seed_url
    elif scope == "In Subfolder":
        return parsed.netloc == seed_parsed.netloc and parsed.path.startswith(seed_parsed.path)
    elif scope == "Same Subdomain":
        return parsed.netloc == seed_parsed.netloc
    elif scope == "All Subdomains":
        main_domain = ".".join(seed_parsed.netloc.split('.')[-2:])
        return parsed.netloc.endswith(main_domain)
    return False

async def discover_links(parent_url: str, session: aiohttp.ClientSession, user_agent: str, scope: str) -> List[str]:
    try:
        async with session.get(parent_url, headers={"User-Agent": user_agent}) as resp:
            if resp.status == 200:
                html = await resp.text()
                soup = BeautifulSoup(html, 'lxml')
                links = set()
                for link in soup.find_all('a', href=True):
                    absolute_url = urljoin(parent_url, link['href'])
                    if is_valid_scope(absolute_url, parent_url, scope):
                        links.add(absolute_url)
                return list(links)
    except:
        return []

# --------------------------
# Streamlit UI
# --------------------------
def main():
    st.title("Advanced SEO Crawler")
    
    # Session state initialization
    if 'results' not in st.session_state:
        st.session_state.results = pd.DataFrame()
    if 'crawling' not in st.session_state:
        st.session_state.crawling = False

    # Configuration
    with st.sidebar:
        st.header("Configuration")
        crawl_mode = st.radio("Operation Mode", ["Website Crawl", "URL List Check"], index=0)
        concurrency = st.slider("Concurrency", 1, 200, 10)
        user_agent = st.selectbox("User Agent", list(USER_AGENTS.keys()), index=3)
        follow_robots = st.checkbox("Respect robots.txt", True)
        
        if crawl_mode == "Website Crawl":
            crawl_scope = st.radio("Crawl Scope", [
                "Exact URL Only",
                "In Subfolder",
                "Same Subdomain",
                "All Subdomains"
            ], index=2)
            max_depth = st.slider("Max Depth", 1, 5, 3)
            use_sitemap = st.checkbox("Include sitemap URLs")

    # Input Section
    st.header("Input Configuration")
    urls = []
    
    if crawl_mode == "Website Crawl":
        col1, col2 = st.columns([3, 1])
        with col1:
            seed_url = st.text_input("Enter website URL to crawl")
        with col2:
            if use_sitemap:
                sitemap_url = st.text_input("Sitemap URL")
        
        if seed_url:
            urls = [seed_url.strip()]
            if use_sitemap and sitemap_url:
                try:
                    resp = requests.get(sitemap_url, timeout=15)
                    if resp.status_code == 200:
                        root = ET.fromstring(resp.content)
                        urls += [loc.text.strip() for loc in root.findall(".//{*}loc")]
                except Exception as e:
                    st.error(f"Failed to fetch sitemap: {str(e)}")
    else:
        input_method = st.radio("Input method", ["Direct Input", "File Upload", "Sitemap"], index=0)
        if input_method == "Direct Input":
            url_input = st.text_area("Enter URLs (one per line)")
            urls = [u.strip() for u in url_input.split('\n') if u.strip()]
        elif input_method == "File Upload":
            uploaded = st.file_uploader("Upload URL list", type=["txt", "csv"])
            if uploaded:
                content = uploaded.read().decode("utf-8")
                urls = [u.strip() for u in content.split('\n') if u.strip()]
        else:
            sitemap_url = st.text_input("Sitemap URL")
            if st.button("Fetch Sitemap"):
                try:
                    resp = requests.get(sitemap_url, timeout=15)
                    if resp.status_code == 200:
                        root = ET.fromstring(resp.content)
                        urls = [loc.text.strip() for loc in root.findall(".//{*}loc")]
                        st.success(f"Found {len(urls)} URLs in sitemap")
                except Exception as e:
                    st.error(f"Failed to fetch sitemap: {str(e)}")

    # Deduplication
    seen = set()
    final_urls = [u for u in urls if not (u in seen or seen.add(u))][:DEFAULT_MAX_URLS]

    # Crawl Control
    if st.button("Start Crawling") and not st.session_state.crawling:
        st.session_state.crawling = True
        st.session_state.results = pd.DataFrame()
        
        async def process_urls():
            checker = URLChecker(
                user_agent=USER_AGENTS[user_agent],
                follow_robots=follow_robots,
                concurrency=concurrency
            )
            
            try:
                await checker.setup()
                
                if crawl_mode == "Website Crawl":
                    visited = set()
                    queue = deque([(url, 0) for url in final_urls])
                    
                    while queue and len(visited) < DEFAULT_MAX_URLS:
                        current_url, depth = queue.popleft()
                        if current_url in visited:
                            continue
                        visited.add(current_url)
                        
                        result = await checker.fetch_and_parse(current_url)
                        new_row = pd.DataFrame([result])
                        st.session_state.results = pd.concat([st.session_state.results, new_row], ignore_index=True)
                        
                        if depth < max_depth:
                            links = await discover_links(
                                current_url, 
                                checker.session, 
                                checker.user_agent, 
                                crawl_scope
                            )
                            for link in links:
                                if link not in visited:
                                    queue.append((link, depth + 1))
                                    
                        st.rerun()
                else:
                    for url in final_urls:
                        result = await checker.fetch_and_parse(url)
                        new_row = pd.DataFrame([result])
                        st.session_state.results = pd.concat([st.session_state.results, new_row], ignore_index=True)
                        st.rerun()
            
            finally:
                await checker.close()
                st.session_state.crawling = False
                st.rerun()

        asyncio.run(process_urls())

    # Results Display
    st.header("Crawl Results")
    if not st.session_state.results.empty:
        st.dataframe(
            st.session_state.results,
            use_container_width=True,
            height=600,
            column_config={
                "Original_URL": "Original URL",
                "Final_URL": "Final URL",
                "Initial_Status_Code": "Initial Status",
                "Final_Status_Code": "Final Status",
                "Is_Blocked_by_Robots": "Blocked by Robots",
                "Robots_Block_Rule": "Block Rule",
                "Title": "Title",
                "Meta_Description": "Meta Description",
                "H1_Text": "H1 Text",
                "H1_Count": "H1 Count",
                "Canonical_URL": "Canonical URL",
                "Meta_Robots": "Meta Robots",
                "X_Robots_Tag": "X-Robots-Tag",
                "HTML_Lang": "HTML Lang",
                "Is_Indexable": "Indexable",
                "Indexability_Reason": "Indexability Reason",
                "Content_Type": "Content Type",
                "Redirect_Chain_Length": "Redirects",
                "Timestamp": "Timestamp",
                "Error": "Error"
            }
        )
        st.download_button(
            "Download CSV",
            st.session_state.results.to_csv(index=False),
            "seo_crawl_results.csv"
        )

    if st.session_state.crawling:
        st.warning("Crawling in progress... This may take several minutes depending on website size.")

if __name__ == "__main__":
    main()
