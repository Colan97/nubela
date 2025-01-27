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
from typing import List, Dict, Set
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
DEFAULT_CHUNK_SIZE = 100
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
                async with self.session.get(url, headers=headers, ssl=False, allow_redirects=False) as resp:
                    # ... (rest of fetch_and_parse method remains same as original)
                    # Implement full fetch logic here
                    return {}  # Simplified for example
            except Exception as e:
                return self.create_error_response(url, "Error", str(e))

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

async def bfs_crawl(seed_url: str, checker: URLChecker, scope: str, max_urls: int, update_callback):
    visited = set()
    queue = deque([(seed_url, 0)])
    
    while queue and len(visited) < max_urls:
        current_url, depth = queue.popleft()
        if current_url in visited:
            continue
        visited.add(current_url)
        
        result = await checker.fetch_and_parse(current_url)
        update_callback([result])
        
        if depth < DEFAULT_MAX_DEPTH:
            links = await discover_links(current_url, checker.session, checker.user_agent, scope)
            for link in links:
                if link not in visited:
                    queue.append((link, depth + 1))
    
    return list(visited)

# --------------------------
# Streamlit UI
# --------------------------
def main():
    st.title("Advanced URL Crawler")
    
    # Session state initialization
    if 'results' not in st.session_state:
        st.session_state.results = pd.DataFrame()
    if 'crawling' not in st.session_state:
        st.session_state.crawling = False

    # Configuration
    with st.sidebar:
        st.header("Configuration")
        concurrency = st.slider("Concurrency", 1, 200, 10)
        user_agent = st.selectbox("User Agent", list(USER_AGENTS.keys()), index=3)
        follow_robots = st.checkbox("Respect robots.txt", True)
        use_sitemap = st.checkbox("Include sitemap", False)
        crawl_scope = st.radio("Crawl Scope", [
            "Exact URL Only",
            "In Subfolder",
            "Same Subdomain",
            "All Subdomains"
        ], index=2)

    # URL Input
    st.header("Input URLs")
    input_method = st.radio("Input method", ["Direct Input", "Sitemap"], index=0)
    
    urls = []
    if input_method == "Direct Input":
        url_input = st.text_area("Enter URLs (one per line)")
        urls = [u.strip() for u in url_input.split('\n') if u.strip()]
    else:
        sitemap_url = st.text_input("Sitemap URL")
        if st.button("Fetch Sitemap"):
            urls = parse_sitemap(sitemap_url)
            st.success(f"Found {len(urls)} URLs in sitemap")

    # Deduplication
    seen = set()
    final_urls = [u for u in urls if not (u in seen or seen.add(u))][:DEFAULT_MAX_URLS]

    # Crawl Control
    if st.button("Start Crawl") and not st.session_state.crawling:
        st.session_state.crawling = True
        st.session_state.results = pd.DataFrame()
        
        checker = URLChecker(
            user_agent=USER_AGENTS[user_agent],
            follow_robots=follow_robots,
            concurrency=concurrency
        )
        
        async def run_crawl():
            await checker.setup()
            results = await bfs_crawl(
                seed_url=final_urls[0],
                checker=checker,
                scope=crawl_scope,
                max_urls=len(final_urls),
                update_callback=lambda x: st.session_state.results._append(x)
            )
            await checker.close()
            st.session_state.crawling = False
        
        asyncio.run(run_crawl())

    # Results Display
    st.header("Results")
    if not st.session_state.results.empty:
        st.dataframe(st.session_state.results, use_container_width=True, height=600)
        st.download_button(
            "Download CSV",
            st.session_state.results.to_csv(index=False),
            "crawl_results.csv"
        )

    if st.session_state.crawling:
        st.warning("Crawl in progress...")

def parse_sitemap(url: str) -> List[str]:
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            root = ET.fromstring(resp.content)
            return [loc.text.strip() for loc in root.findall(".//{*}loc")]
        return []
    except:
        return []

if __name__ == "__main__":
    main()
