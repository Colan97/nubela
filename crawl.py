<!DOCTYPE html>
<html>
<head>
    <title>Async URL Checker with Optional BFS Crawl</title>
</head>
<body>
<script type="text/python">
import streamlit as st
import pandas as pd
import re
import asyncio
import nest_asyncio
import aiohttp
import ujson
import gc
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Set
from collections import deque
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
# Tenacity for retry logic
from tenacity import retry, stop_after_attempt, wait_exponential

# Logging Configuration (Optional)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='url_checker.log'
)

# Constants (default values)
DEFAULT_TIMEOUT = 15
DEFAULT_CHUNK_SIZE = 100
DEFAULT_MAX_URLS = 25000  # BFS or overall cap
DEFAULT_MAX_DEPTH = 3     # BFS depth
MAX_REDIRECTS = 5
# We'll fill these from user inputs in the UI
DEFAULT_USER_AGENT = "custom_adidas_seo_x3423/1.0"
# Predefined user agents
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

# URL Checker Class: For Final SEO Checks (Including robots.txt check)
class URLChecker:
    """
    A class that checks various SEO and technical aspects of URLs.
    Used in BFS or in simple chunked processing.
    """
    def __init__(self, user_agent: str, concurrency: int = 10, timeout: int = 15):
        self.user_agent = user_agent
        self.max_concurrency = concurrency
        self.timeout_duration = timeout
        self.robots_cache = {}  # base_url -> robots content or True if missing
        self.connector = None
        self.session = None
        self.semaphore = None

    async def setup(self):
        """
        Prepare an aiohttp session with concurrency limits and timeouts.
        """
        self.connector = aiohttp.TCPConnector(
            limit=self.max_concurrency,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            force_close=False
        )
        # Connect & read timeouts
        timeout = aiohttp.ClientTimeout(
            total=None,
            connect=self.timeout_duration,
            sock_read=self.timeout_duration
        )
        self.session = aiohttp.ClientSession(
            connector=self.connector,
            timeout=timeout,
            json_serialize=ujson.dumps
        )
        self.semaphore = asyncio.Semaphore(self.max_concurrency)

    async def close(self):
        """Close the aiohttp session."""
        if self.session:
            await self.session.close()

    # Robots.txt check
    async def check_robots_txt(self, url: str) -> Tuple[bool, str]:
        """
        Returns (is_allowed, block_rule).
        If robots.txt is unreachable or not found -> assume allowed, block_rule = 'N/A'.
        """
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        path = parsed.path
        # If we have not cached this domain, attempt to fetch & parse
        if base_url not in self.robots_cache:
            robots_url = f"{base_url}/robots.txt"
            try:
                headers = {"User-Agent": self.user_agent}
                async with self.session.get(robots_url, ssl=False, headers=headers) as resp:
                    if resp.status == 200:
                        text_content = await resp.text()
                        self.robots_cache[base_url] = text_content
                    else:
                        self.robots_cache[base_url] = None
            except:
                self.robots_cache[base_url] = None
        robots_content = self.robots_cache.get(base_url)
        if not robots_content:
            # No robots.txt or failed fetch => assume allowed
            return True, "N/A"
        # Check if path is disallowed under relevant user-agent
        is_allowed, block_rule = self.parse_robots(robots_content, path)
        return is_allowed, block_rule

    def parse_robots(self, robots_content: str, path: str) -> Tuple[bool, str]:
        """
        Basic parse of robots.txt. We look for user-agent: * or custom user agent lines,
        then check Disallow. If multiple disallows match, we return the first.
        We do NOT parse 'Allow:' lines. It's simplistic but meets your requirement.
        """
        lines = robots_content.splitlines()
        is_relevant_section = False
        block_rule = "N/A"
        path_lower = path.lower()
        user_agent_lower = self.user_agent.lower()
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split(':', 1)
            if len(parts) < 2:
                continue
            field, value = parts[0].lower(), parts[1].strip().lower()
            # Identify user agent section
            if field == "user-agent":
                # Match if it's '*' or contains our custom user agent substring
                if value == '*' or (user_agent_lower in value):
                    is_relevant_section = True
                else:
                    is_relevant_section = False
            elif field == "disallow" and is_relevant_section:
                disallow_path = value
                if disallow_path and path_lower.startswith(disallow_path):
                    block_rule = disallow_path
                    return False, block_rule
        return True, block_rule

    # Fetch URL status and details
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_url(self, url: str) -> Dict:
        """
        Fetches a single URL and returns its status, response time, etc.
        Handles redirects up to MAX_REDIRECTS.
        """
        async with self.semaphore:
            start_time = datetime.now()
            headers = {"User-Agent": self.user_agent}
            redirect_count = 0
            original_url = url
            while redirect_count <= MAX_REDIRECTS:
                try:
                    async with self.session.get(url, ssl=False, headers=headers, allow_redirects=False) as resp:
                        status = resp.status
                        elapsed = (datetime.now() - start_time).total_seconds()
                        if 300 <= status < 400:
                            # Handle redirects
                            redirect_count += 1
                            location = resp.headers.get('Location')
                            if not location:
                                return {
                                    "url": original_url,
                                    "final_url": url,
                                    "status": status,
                                    "response_time": elapsed,
                                    "error": "Redirect without Location header",
                                    "redirect_count": redirect_count
                                }
                            url = urljoin(url, location)
                            continue
                        # Non-redirect responses
                        content_type = resp.headers.get('Content-Type', '')
                        content_length = resp.headers.get('Content-Length', '')
                        robots_allowed, block_rule = await self.check_robots_txt(url)
                        return {
                            "url": original_url,
                            "final_url": url,
                            "status": status,
                            "content_type": content_type,
                            "content_length": content_length,
                            "response_time": elapsed,
                            "robots_allowed": robots_allowed,
                            "block_rule": block_rule,
                            "error": None,
                            "redirect_count": redirect_count
                        }
                except Exception as e:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    return {
                        "url": original_url,
                        "final_url": url,
                        "status": None,
                        "response_time": elapsed,
                        "error": str(e),
                        "redirect_count": redirect_count
                    }

    # Process a batch of URLs concurrently
    async def process_urls(self, urls: List[str]) -> List[Dict]:
        """
        Processes a list of URLs concurrently and returns their results.
        """
        tasks = [self.fetch_url(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results


# BFS Crawler Class: For crawling starting from a seed URL
class BFSCrawler:
    """
    A breadth-first crawler that starts from a seed URL and explores links up to a maximum depth.
    """
    def __init__(self, user_agent: str, max_urls: int = DEFAULT_MAX_URLS, max_depth: int = DEFAULT_MAX_DEPTH):
        self.user_agent = user_agent
        self.max_urls = max_urls
        self.max_depth = max_depth
        self.visited = set()
        self.queue = deque()
        self.results = []
        self.url_checker = URLChecker(user_agent=user_agent)

    async def setup(self):
        """Initialize the URL checker."""
        await self.url_checker.setup()

    async def close(self):
        """Close the URL checker."""
        await self.url_checker.close()

    def extract_links(self, html: str, base_url: str) -> Set[str]:
        """
        Extracts all valid links from HTML content.
        """
        soup = BeautifulSoup(html, 'html.parser')
        links = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            full_url = urljoin(base_url, href)
            parsed = urlparse(full_url)
            if parsed.scheme in ['http', 'https']:
                links.add(full_url)
        return links

    async def crawl(self, seed_url: str) -> List[Dict]:
        """
        Starts BFS crawling from the seed URL.
        """
        self.queue.append((seed_url, 0))  # (url, depth)
        while self.queue and len(self.results) < self.max_urls:
            current_url, depth = self.queue.popleft()
            if current_url in self.visited:
                continue
            self.visited.add(current_url)
            result = await self.url_checker.fetch_url(current_url)
            self.results.append(result)
            if depth < self.max_depth and result.get("status") == 200:
                try:
                    async with self.url_checker.session.get(current_url, ssl=False, headers={"User-Agent": self.user_agent}) as resp:
                        if resp.status == 200:
                            html = await resp.text()
                            links = self.extract_links(html, current_url)
                            for link in links:
                                if link not in self.visited and len(self.results) < self.max_urls:
                                    self.queue.append((link, depth + 1))
                except Exception as e:
                    logging.error(f"Error fetching links from {current_url}: {e}")
        return self.results


# Streamlit App
def main():
    st.title("Async URL Checker with Optional BFS Crawl")

    # Sidebar for configuration
    st.sidebar.header("Configuration")
    user_agent = st.sidebar.selectbox(
        "Select User-Agent",
        options=list(USER_AGENTS.keys()),
        index=len(USER_AGENTS) - 1  # Default to Custom Adidas SEO Bot
    )
    custom_user_agent = st.sidebar.text_input(
        "Custom User-Agent (if selected above)",
        value=DEFAULT_USER_AGENT
    )
    user_agent = USER_AGENTS[user_agent] if user_agent != "Custom Adidas SEO Bot" else custom_user_agent
    timeout = st.sidebar.number_input("Timeout (seconds)", min_value=5, max_value=60, value=DEFAULT_TIMEOUT)
    chunk_size = st.sidebar.number_input("Chunk Size", min_value=10, max_value=1000, value=DEFAULT_CHUNK_SIZE)
    max_urls = st.sidebar.number_input("Max URLs (for BFS)", min_value=100, max_value=100000, value=DEFAULT_MAX_URLS)
    max_depth = st.sidebar.number_input("Max Depth (for BFS)", min_value=1, max_value=10, value=DEFAULT_MAX_DEPTH)

    # Main panel
    st.header("URL Input")
    input_mode = st.radio("Input Mode", options=["Single URL", "Bulk URLs", "BFS Crawl"])
    if input_mode == "Single URL":
        url = st.text_input("Enter URL to check:")
        if st.button("Check URL"):
            if url:
                url_checker = URLChecker(user_agent=user_agent, concurrency=1, timeout=timeout)
                asyncio.run(url_checker.setup())
                result = asyncio.run(url_checker.fetch_url(url))
                asyncio.run(url_checker.close())
                st.write("Result:", result)
    elif input_mode == "Bulk URLs":
        uploaded_file = st.file_uploader("Upload a CSV file with URLs", type=["csv"])
        if uploaded_file:
            df = pd.read_csv(uploaded_file)
            urls = df.iloc[:, 0].tolist()  # Assuming URLs are in the first column
            url_checker = URLChecker(user_agent=user_agent, concurrency=chunk_size, timeout=timeout)
            asyncio.run(url_checker.setup())
            results = asyncio.run(url_checker.process_urls(urls))
            asyncio.run(url_checker.close())
            st.write("Results:", pd.DataFrame(results))
    elif input_mode == "BFS Crawl":
        seed_url = st.text_input("Enter Seed URL for BFS Crawl:")
        if st.button("Start BFS Crawl"):
            if seed_url:
                bfs_crawler = BFSCrawler(user_agent=user_agent, max_urls=max_urls, max_depth=max_depth)
                asyncio.run(bfs_crawler.setup())
                results = asyncio.run(bfs_crawler.crawl(seed_url))
                asyncio.run(bfs_crawler.close())
                st.write("Crawl Results:", pd.DataFrame(results))


if __name__ == "__main__":
    main()
</script>
</body>
</html>
