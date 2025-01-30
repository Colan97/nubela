import streamlit as st
import pandas as pd
import re
import asyncio
import aiohttp
import orjson
import gc
import logging
from datetime import datetime
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
from robotexclusionrulesparser import RobotExclusionRulesParser  # Enhanced robots.txt parser

# Constants
DEFAULT_TIMEOUT = 15
DEFAULT_CHUNK_SIZE = 100
DEFAULT_MAX_URLS = 25000
DEFAULT_MAX_DEPTH = 3
MAX_REDIRECTS = 5
SEED_URL_LIMIT = 10

# User-Agent configurations
USER_AGENTS = {
    "Googlebot Desktop": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Googlebot Mobile": (
        "Mozilla/5.0"
        " (iPhone; CPU iPhone OS 14_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko)"
        " Version/14.0.1 Mobile/15E148 Safari/604.1 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    ),
    "Chrome Desktop": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    ),
    "Chrome Mobile": (
        "Mozilla/5.0 (Linux; Android 10; Pixel 3)"
        " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Mobile Safari/537.36"
    ),
    "Custom Adidas SEO Bot": "custom_adidas_seo_x3423/1.0"
}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("url_checker.log"),
        logging.StreamHandler()
    ]
)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=15))
async def get_robotsparser(url: str, session: aiohttp.ClientSession) -> RobotExclusionRulesParser:
    async with session.get(url, ssl=False, timeout=30) as response:
        parser = RobotExclusionRulesParser()
        if response.status == 200:
            content = await response.text()
            parser.parse(content)
        return parser

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=15))
async def head_only(url: str, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
    async with session.head(url, ssl=False, allow_redirects=True, timeout=5) as response:
        return response

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=15))
async def get_full_content(url: str, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
    async with session.get(url, ssl=False, timeout=20) as response:
        return response

class URLProcessor:
    def __init__(self, user_agent: str, timeout: int, concurrency_limit: int):
        self.user_agent = user_agent
        self.timeout = timeout
        self.concurrency_limit = concurrency_limit
        self ssize = aiohttp.ClientTimeout(sock_read=timeout)
        self.session = None
        self.rate_limit_semaphore = asyncio.Semaphore(concurrency_limit)
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": self.user_agent},
            timeout=aiohttp.ClientTimeout(total=timeout),
            json_serialize=orjson.dumps
        )

    async def check_url(self, url: str, return_content: bool = False) -> dict:
        init_response = await head_only(url, self.session)
        await init_response.read()
        final_response = await get_full_content(url, self.session)
        await final_response.read()
        
        # Robots.txt check
        robot_url = f"{urlparse(url)._replace(path='robots.txt').geturl()}"
        robots_parser = await get_robotsparser(robot_url, self.session)
        
        # Analysis
        return {
            "Original URL": url,
            "Initial Status": init_response.status,
            "Redirected URL": final_response.url,
            "Final Status": final_response.status,
            "Blocked by Robots": robots_parser.is_allowed("*", url) is False,
            "Allow Directive": robots_parser.get_allowed(url),
            "Disallow Directive": robots_parser.get_disallowed(url)
        }

async def chunked_processing(urls, processor):
    async with aiohttp.ClientSession() as processor.session:
        tasks = []
        for url in urls:
            tasks.append(processor.check_url(url))
        return await asyncio.gather(*tasks)

def main():
    st.title("Enhanced Async URL Checker")
    st.sidebar.markdown("### User-Agent Configuration")
    chosen_ua = st.sidebar.selectbox("Select User Agent", list(USER_AGENTS.keys()))
    selected_agent = USER_AGENTS[chosen_ua]

    max_depth = st.sidebar.number_input("Max BFS Depth", 1, 10, 3)
    max_urls = st.sidebar.number_input("Max URLs to Process", 10, 10000, 500)
    timeout = st.sidebar.slider("Request Timeout", 1, 60, 15)
    concurrency = st.sidebar.slider("Concurrency Limit", 1, 200, 10)

    seed_urls = st.text_area("Enter Seed URLs (optional)", height=100).strip().split("\n")
    sitemap_url = st.text_input("Enter Sitemap URL (optional)")

    if sitemap_url:
        async def parse_sitemap(sitemap_url):
            async with aiohttp.ClientSession() as session:
                async with session.get(sitemap_url) as resp:
                    content = await resp.text()
                    # Simplified sitemap parsing - handle only typical cases
                    loc_tags = re.findall(r'<loc>([^<]+)</loc>', content)
                    return [urljoin(sitemap_url, loc) for loc in loc_tags]

        sitemap_urls = asyncio.run(parse_sitemap(sitemap_url))
        st.write(f"Parsed {len(sitemap_urls)} URLs from sitemap.")
    else:
        sitemap_urls = []

    target_urls = sitemap_urls + seed_urls
    target_urls = list(set(target_urls))[:max_urls]

    if st.button("Start Processing"):
        processor = URLProcessor(selected_agent, timeout, concurrency)
        results = asyncio.run(chunked_processing(target_urls[:max_urls], processor))
        
        df = pd.DataFrame(results)
        st.subheader("Results")
        st.dataframe(df, use_container_width=True)
        
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name="url_results.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main()
