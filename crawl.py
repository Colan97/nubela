import streamlit as st
import pandas as pd
import re
import asyncio
import aiohttp
import orjson
import gc
import logging
import requests
import ssl
import sys
import nest_asyncio
from datetime import datetime
from typing import List, Dict, Tuple, Set, Optional
from collections import deque
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
import xml.etree.ElementTree as ET
from urllib.robotparser import RobotFileParser

# Apply async patch for Streamlit
nest_asyncio.apply()

# --------------------------
# Constants
# --------------------------
DEFAULT_TIMEOUT = 15
DEFAULT_CHUNK_SIZE = 100
DEFAULT_MAX_URLS = 25000
DEFAULT_MAX_DEPTH = 5
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
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.set_ciphers('DEFAULT@SECLEVEL=1')

    async def check_robots_txt(self, url: str) -> Tuple[bool, str]:
        if not self.follow_robots:
            return True, "Robots.txt ignored"
        
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        if base_url not in self.robots_cache:
            robots_url = f"{base_url}/robots.txt"
            try:
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=self.ssl_context)) as session:
                    async with session.get(robots_url, timeout=self.timeout_duration) as resp:
                        if resp.status == 200:
                            self.robots_cache[base_url] = await resp.text()
                        else:
                            self.robots_cache[base_url] = None
            except Exception as e:
                self.robots_cache[base_url] = None
        
        robots_content = self.robots_cache.get(base_url)
        if not robots_content:
            return True, "N/A"
        
        rp = RobotFileParser()
        rp.parse(robots_content.splitlines())
        return rp.can_fetch(self.user_agent, parsed.path), "Custom rule"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=10))
    async def fetch_and_parse(self, url: str) -> Dict:
        headers = {"User-Agent": self.user_agent}
        result = {"URL": url, "Timestamp": datetime.now().isoformat()}
        
        try:
            is_allowed, block_rule = await self.check_robots_txt(url)
            if not is_allowed:
                result.update({
                    "Status": 403,
                    "Allowed": False,
                    "Block Rule": block_rule
                })
                return result

            async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=self.ssl_context),
                timeout=aiohttp.ClientTimeout(total=self.timeout_duration)
            ) as session:
                async with session.get(url, headers=headers, allow_redirects=True) as resp:
                    content_type = resp.headers.get('Content-Type', '')
                    html = await resp.text(errors='replace') if 'text/html' in content_type else ""
                    
                    soup = BeautifulSoup(html, 'lxml') if html else None
                    title = soup.title.string if soup and soup.title else ''
                    h1 = soup.find('h1').get_text() if soup and soup.find('h1') else ''

                    result.update({
                        "Final URL": str(resp.url),
                        "Status": resp.status,
                        "Allowed": True,
                        "Block Rule": "N/A",
                        "Title": title,
                        "H1": h1,
                        "Content Type": content_type
                    })

        except Exception as e:
            result["Error"] = f"{type(e).__name__}: {str(e)}"
        
        return result

# --------------------------
# Streamlit UI
# --------------------------
def main():
    st.set_page_config(layout="wide", page_title="Advanced URL Crawler")
    st.title("Advanced URL Crawler")
    
    # Session state initialization
    if 'results' not in st.session_state:
        st.session_state.results = pd.DataFrame()
    if 'crawling' not in st.session_state:
        st.session_state.crawling = False
    if 'progress' not in st.session_state:
        st.session_state.progress = 0.0

    # Configuration
    with st.sidebar:
        st.header("Configuration")
        crawl_mode = st.radio("Operation Mode", ["Website Crawl", "URL List Check"], index=0)
        concurrency = st.slider("Concurrency", 1, 200, 10)
        user_agent = st.selectbox("User Agent", list(USER_AGENTS.keys()), index=3)
        follow_robots = st.checkbox("Respect robots.txt", True)

    # Input Handling
    urls = []
    if crawl_mode == "Website Crawl":
        seed_url = st.text_input("Enter website URL to crawl")
        if seed_url:
            urls = [seed_url.strip()]
    else:
        url_input = st.text_area("Enter URLs (one per line)")
        urls = [u.strip() for u in url_input.split('\n') if u.strip()]

    # Processing Control
    if st.button("Start Processing") and not st.session_state.crawling and urls:
        st.session_state.crawling = True
        st.session_state.progress = 0.0
        st.session_state.results = pd.DataFrame()
        
        async def process_urls():
            checker = URLChecker(
                user_agent=USER_AGENTS[user_agent],
                follow_robots=follow_robots,
                concurrency=concurrency
            )
            
            try:
                total = len(urls)
                for i, url in enumerate(urls):
                    if len(st.session_state.results) >= DEFAULT_MAX_URLS:
                        break
                    
                    result = await checker.fetch_and_parse(url)
                    new_row = pd.DataFrame([result])
                    st.session_state.results = pd.concat([st.session_state.results, new_row], ignore_index=True)
                    
                    # Update progress
                    st.session_state.progress = (i + 1) / total
                    if i % 5 == 0:  # Update UI every 5 URLs
                        st.experimental_rerun()
                    
                    # Memory management
                    if i % 50 == 0:
                        gc.collect()
            
            except Exception as e:
                st.error(f"Processing failed: {str(e)}")
            finally:
                st.session_state.crawling = False
                st.experimental_rerun()

        # Run async loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(process_urls())

    # UI Updates
    if st.session_state.crawling:
        st.progress(st.session_state.progress)
        st.warning(f"Processing {len(st.session_state.results)} URLs...")
    
    if not st.session_state.results.empty:
        st.dataframe(
            st.session_state.results,
            use_container_width=True,
            height=600,
            column_config={
                "URL": "URL",
                "Final URL": "Final URL",
                "Status": {"label": "Status", "format": "{:d}"},
                "Allowed": "Allowed",
                "Block Rule": "Block Rule",
                "Title": "Title",
                "H1": "H1",
                "Content Type": "Content Type",
                "Timestamp": "Timestamp",
                "Error": "Error"
            }
        )
        
        csv = st.session_state.results.to_csv(index=False)
        st.download_button(
            "Download CSV",
            csv,
            "crawl_results.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        main()
