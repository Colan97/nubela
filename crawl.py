import streamlit as st
import pandas as pd
import aiohttp
import asyncio
import json
import logging
import re
from urllib.parse import urlparse, urljoin
from datetime import datetime
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
import reppy
from reppy.cache import Robots

# --- Constants ---
DEFAULT_REQUEST_TIMEOUT = 15
DEFAULT_CHUNK_SIZE = 100
DEFAULT_MAX_URLS = 25000
DEFAULT_BFS_DEPTH = 3
MAX_REDIRECT_ATTEMPTS = 10
SEED_URL_LIMIT = 10

# --- User-Agent Configurations ---
USER_AGENTS = {
    "Googlebot Desktop": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Googlebot Mobile": (
        "Mozilla/5.0 "
        "(Linux; Android 10; Pixel 3) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Mobile Safari/537.36 "
        "(compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    ),
    "Chrome Desktop": (
        "Mozilla/5.0 "
        "(Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    ),
    "Chrome Mobile": (
        "Mozilla/5.0 "
        "(Linux; Android 10; Pixel 3) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Mobile Safari/537.36"
    ),
    "Custom Adidas SEO Bot": "custom_adidas_seo_x3423/1.0"
}

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("url_checker.log"),
        logging.StreamHandler()
    ]
)

# --- Asynchronous Request Functions --- 
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=15))
async def fetch_robots(url: str, session: aiohttp.ClientSession) -> Robots:
    parsed_url = urlparse(url)
    robot_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
    async with session.get(robot_url, ssl=False) as response:
        if response.status == 200:
            return Robots.from_string(await response.text(), robot_url)
        return Robots.empty()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=15))
async def fetch_head(url: str, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
    async with session.head(url, ssl=False, allow_redirects=True) as response:
        await response.read()
        return response

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=15))
async def fetch_content(url: str, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
    async with session.get(url, ssl=False) as response:
        await response.read()
        return response

# --- URL Processor Class ---
class URLProcessor:
    def __init__(self, user_agent: str, timeout: int, concurrency_limit: int):
        self.user_agent = user_agent
        self.timeout = timeout
        self.concurrency_limit = concurrency_limit
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": self.user_agent},
            timeout=aiohttp.ClientTimeout(total=self.timeout),
            json_serialize=json.dumps
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.close()

    async def process_url(self, url: str) -> dict:
        try:
            async with self.session:
                head_resp = await fetch_head(url, self.session)
                content_resp = await fetch_content(url, self.session)
                
                # Robots.txt analysis
                robots_rules = await fetch_robots(url, self.session)
                
                # SEO analysis
                soup = BeautifulSoup(await content_resp.text(), "lxml")
                title = soup.find("title").get_text(strip=True) if soup.find("title") else None
                meta_desc = soup.find("meta", attrs={"name": "description"}).get("content") if soup.find("meta", attrs={"name": "description"}) else None
                
                return {
                    "Original URL": url,
                    "Initial Status": head_resp.status,
                    "Redirected URL": str(content_resp.url),
                    "Final Status": content_resp.status,
                    "Blocked by Robots": not robots_rules.allowed(url, "*") if robots_rules else False,
                    "Title": title if title != "None" else None,
                    "Meta Description": meta_desc if meta_desc != "None" else None,
                    "Processing Time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
        except Exception as e:
            logging.error(f"Error processing {url}: {str(e)}")
            return {
                "Original URL": url,
                "Initial Status": "Error",
                "Redirected URL": "Error",
                "Final Status": "Error",
                "Blocked by Robots": False,
                "Title": None,
                "Meta Description": None,
                "Error Details": str(e),
                "Processing Time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

# --- Async Processing Function ---
async def process_urls_chunked(urls: list, user_agent: str, timeout: int, concurrency_limit: int, chunk_size: int = DEFAULT_CHUNK_SIZE) -> list:
    async with URLProcessor(user_agent, timeout, concurrency_limit) as processor:
        results = []
        for i in range(0, len(urls), chunk_size):
            chunk = urls[i:i+chunk_size]
            tasks = [processor.process_url(url) for url in chunk]
            chunk_results = await asyncio.gather(*tasks)
            results.extend(chunk_results)
        return results

# --- Sitemap Parsing ---
async def parse_sitemap(sitemap_url: str) -> list:
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(sitemap_url) as resp:
                if resp.status != 200:
                    logging.warning(f"Failed to fetch sitemap {sitemap_url}: status {resp.status}")
                    return []
                content = await resp.text()
                loc_tags = re.findall(r'<loc>([^<]+)</loc>', content)
                return [urljoin(sitemap_url, loc) for loc in loc_tags]
        except Exception as e:
            logging.error(f"Error parsing sitemap {sitemap_url}: {str(e)}")
            return []

# --- Main Function ---
def main():
    st.title("Advanced URL Checker")
    st.sidebar.markdown("### Configuration")
    chosen_ua = st.sidebar.selectbox("Select User Agent", list(USER_AGENTS.keys()))
    timeout = st.sidebar.slider("Request Timeout (seconds)", 10, 60, 30)
    concurrency = st.sidebar.slider("Concurrency Level", 1, 200, 50)
    max_urls = st.sidebar.number_input("Maximum URLs to Process", 10, 10000, 500)
    
    seed_urls = st.text_area("Enter Seed URLs (one per line)", height=100).strip().split("\n")
    sitemap_url = st.text_input("Enter Sitemap URL")
    target_urls = []

    if sitemap_url:
        sitemap_urls = asyncio.run(parse_sitemap(sitemap_url))
        st.write(f"Found {len(sitemap_urls)} URLs in sitemap.")
        target_urls.extend(sitemap_urls)
    
    cleaned_seed_urls = [url.strip() for url in seed_urls if url.strip()]
    target_urls.extend(cleaned_seed_urls)
    deduplicated_urls = list(set(target_urls))[:max_urls]
    
    if deduplicated_urls:
        if st.button("Start Processing"):
            user_agent = USER_AGENTS[chosen_ua]
            
            with st.spinner("Processing URLs..."):
                results = asyncio.run(
                    process_urls_chunked(deduplicated_urls, user_agent, timeout, concurrency)
                )
            
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
