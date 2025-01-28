import streamlit as st
import pandas as pd
import asyncio
import aiohttp
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Set
from collections import deque
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
import xml.etree.ElementTree as ET

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
        self.connector = None
        self.session = None
        self.semaphore = None

    async def setup(self):
        # Fix: Use limit_per_host to prevent connection issues
        self.connector = aiohttp.TCPConnector(limit=self.max_concurrency, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=self.timeout_duration)
        self.session = aiohttp.ClientSession(connector=self.connector, timeout=timeout)
        self.semaphore = asyncio.Semaphore(self.max_concurrency)

    async def close(self):
        if self.session:
            await self.session.close()
        if self.connector:
            await self.connector.close()

    async def check_robots_txt(self, url: str) -> Tuple[bool, str]:
        if not self.follow_robots:
            return True, "Robots.txt ignored"
        
        try:
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
                except Exception:
                    self.robots_cache[base_url] = None
            
            robots_content = self.robots_cache.get(base_url)
            if not robots_content:
                return True, "N/A"
            
            return self.parse_robots(robots_content, parsed.path)
        except Exception as e:
            logging.error(f"Error checking robots.txt: {str(e)}")
            return True, "Error checking robots.txt"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=10))
    async def fetch_and_parse(self, url: str) -> Dict:
        headers = {"User-Agent": self.user_agent}
        async with self.semaphore:
            try:
                is_allowed, block_rule = await self.check_robots_txt(url)
                final_url = url
                
                try:
                    async with self.session.get(final_url, headers=headers, ssl=False) as resp:
                        status = resp.status
                        content_type = resp.headers.get('Content-Type', '')
                        
                        # Parse HTML content
                        html = ""
                        title = ""
                        h1 = ""
                        
                        if 'text/html' in content_type.lower():
                            try:
                                html = await resp.text(errors='replace')
                                soup = BeautifulSoup(html, 'lxml')
                                title = soup.title.string if soup.title else ''
                                h1 = soup.find('h1').get_text() if soup.find('h1') else ''
                            except Exception as e:
                                logging.error(f"Error parsing HTML: {str(e)}")

                        return {
                            "URL": url,
                            "Final URL": final_url,
                            "Status": status,
                            "Allowed": is_allowed,
                            "Block Rule": block_rule,
                            "Title": title,
                            "H1": h1,
                            "Content Type": content_type,
                            "Timestamp": datetime.now().isoformat()
                        }
                except Exception as e:
                    return {
                        "URL": url,
                        "Error": f"Request failed: {str(e)}",
                        "Timestamp": datetime.now().isoformat()
                    }
            except Exception as e:
                return {
                    "URL": url,
                    "Error": f"Processing failed: {str(e)}",
                    "Timestamp": datetime.now().isoformat()
                }

# --------------------------
# Streamlit UI
# --------------------------
def main():
    st.set_page_config(page_title="Advanced URL Crawler", layout="wide")
    
    # Initialize session state
    if 'results' not in st.session_state:
        st.session_state.results = pd.DataFrame()
    if 'crawling' not in st.session_state:
        st.session_state.crawling = False
    if 'processed_urls' not in st.session_state:
        st.session_state.processed_urls = 0

    # ... [Rest of the UI code remains the same]

    # Modified process_urls function with better error handling
    async def process_urls():
        checker = None
        try:
            checker = URLChecker(
                user_agent=USER_AGENTS[user_agent],
                follow_robots=follow_robots,
                concurrency=concurrency
            )
            
            await checker.setup()
            
            if crawl_mode == "Website Crawl":
                visited = set()
                queue = deque([(url, 0) for url in final_urls])
                
                while queue and len(visited) < DEFAULT_MAX_URLS:
                    current_url, depth = queue.popleft()
                    if current_url in visited:
                        continue
                    visited.add(current_url)
                    
                    try:
                        result = await checker.fetch_and_parse(current_url)
                        new_row = pd.DataFrame([result])
                        st.session_state.results = pd.concat([st.session_state.results, new_row], ignore_index=True)
                        st.session_state.processed_urls += 1
                        
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
                    except Exception as e:
                        logging.error(f"Error processing URL {current_url}: {str(e)}")
                    
                    st.experimental_rerun()
            else:
                for url in final_urls:
                    try:
                        result = await checker.fetch_and_parse(url)
                        new_row = pd.DataFrame([result])
                        st.session_state.results = pd.concat([st.session_state.results, new_row], ignore_index=True)
                        st.session_state.processed_urls += 1
                        st.experimental_rerun()
                    except Exception as e:
                        logging.error(f"Error processing URL {url}: {str(e)}")
        
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
        finally:
            if checker:
                await checker.close()
            st.session_state.crawling = False
            st.experimental_rerun()

    if st.button("Start Processing") and not st.session_state.crawling:
        st.session_state.crawling = True
        st.session_state.results = pd.DataFrame()
        st.session_state.processed_urls = 0
        asyncio.run(process_urls())

if __name__ == "__main__":
    main()
