#!/usr/bin/env python3
"""
Fast Oracle Documentation Scraper
=================================

An optimized web scraper with concurrent processing for Oracle Cloud Infrastructure documentation.
Uses asyncio and aiohttp for maximum performance.

Usage:
    python scrapper_fast.py https://docs.oracle.com/en-us/iaas/Content/services.htm
"""

import os
import re
import asyncio
import aiohttp
import time
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass
from tqdm.asyncio import tqdm
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ScrapedPage:
    """Represents a scraped documentation page."""
    url: str
    title: str
    level: int
    parent_url: Optional[str] = None
    children_urls: Optional[List[str]] = None

    def __post_init__(self):
        if self.children_urls is None:
            self.children_urls = []


class FastOracleDocsScraper:
    """
    A high-performance concurrent scraper for Oracle Cloud Infrastructure documentation.
    
    Uses asyncio and aiohttp for concurrent requests to maximize throughput.
    """
    
    def __init__(self, max_depth: int = 3, max_concurrent: int = 10, delay: float = 0.1, 
                 output_dir: str = "scraped_docs"):
        """
        Initialize the Fast Oracle Documentation Scraper.
        
        Args:
            max_depth: Maximum depth to crawl (default: 3)
            max_concurrent: Maximum concurrent requests (default: 10)
            delay: Delay between requests in seconds (default: 0.1)
            output_dir: Directory to save markdown files (default: "scraped_docs")
        """
        self.max_depth = max_depth
        self.max_concurrent = max_concurrent
        self.delay = delay
        self.output_dir = output_dir
        self.visited_urls: Set[str] = set()
        self.scraped_pages: List[ScrapedPage] = []
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Headers to mimic a real browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        logger.info(f"Fast Oracle Docs Scraper initialized with max_depth={max_depth}, "
                   f"max_concurrent={max_concurrent}, delay={delay}s")

    def is_valid_oracle_url(self, url: str) -> bool:
        """Check if URL is a valid Oracle documentation URL."""
        parsed = urlparse(url)
        return (
            parsed.netloc in ['docs.oracle.com'] and
            '/iaas/' in parsed.path and
            not parsed.fragment and  # Avoid fragment URLs
            not any(ext in parsed.path for ext in ['.pdf', '.zip', '.tar', '.gz'])
        )

    def clean_url(self, url: str) -> str:
        """Clean and normalize URL by removing unnecessary parameters."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

    async def fetch_page(self, session: aiohttp.ClientSession, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch and parse page content asynchronously.
        
        Args:
            session: aiohttp ClientSession
            url: URL to fetch
            
        Returns:
            BeautifulSoup object or None if failed
        """
        async with self.semaphore:  # Limit concurrent requests
            try:
                if self.delay > 0:
                    await asyncio.sleep(self.delay)
                
                logger.debug(f"Fetching: {url}")
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    response.raise_for_status()
                    content = await response.read()
                    
                    # Parse with BeautifulSoup (this is still CPU-bound, but faster than full content extraction)
                    soup = BeautifulSoup(content, 'lxml')
                    return soup
                    
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                return None

    def extract_navigation_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract navigation links from Oracle documentation sidebar."""
        nav_links = []
        
        # Optimized selectors for Oracle docs navigation
        nav_selectors = [
            'nav a[href]',
            '.toc a[href]',
            '.ohc-toc a[href]',
            'aside a[href]',
            '.sidebar a[href]',
            '.left-nav a[href]',
            'a[href*="/Content/"]',
            'a[href*="/iaas/"]',
        ]
        
        for selector in nav_selectors:
            links = soup.select(selector)
            
            for link in links:
                href = link.get('href')
                if href and isinstance(href, str):
                    # Convert relative URLs to absolute
                    absolute_url = urljoin(base_url, href)
                    cleaned_url = self.clean_url(absolute_url)
                    
                    if self.is_valid_oracle_url(cleaned_url) and cleaned_url not in self.visited_urls:
                        nav_links.append(cleaned_url)
        
        # Fallback: broader search if no navigation links found
        if not nav_links:
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href')
                if href and isinstance(href, str):
                    absolute_url = urljoin(base_url, href)
                    cleaned_url = self.clean_url(absolute_url)
                    
                    if self.is_valid_oracle_url(cleaned_url) and cleaned_url not in self.visited_urls:
                        nav_links.append(cleaned_url)
        
        unique_links = list(set(nav_links))  # Remove duplicates
        return unique_links

    def extract_title_only(self, soup: BeautifulSoup) -> str:
        """Extract only the title from a page (optimized for speed)."""
        title_selectors = ['h1', '.page-title', '.title', 'title']
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                return title_elem.get_text().strip()
        return "Untitled"

    async def scrape_page(self, session: aiohttp.ClientSession, url: str, level: int = 0, 
                         parent_url: Optional[str] = None) -> Optional[ScrapedPage]:
        """
        Scrape a single page asynchronously.
        
        Args:
            session: aiohttp ClientSession
            url: URL to scrape
            level: Current depth level
            parent_url: Parent page URL
            
        Returns:
            ScrapedPage object or None if failed
        """
        if level > self.max_depth or url in self.visited_urls:
            return None
        
        self.visited_urls.add(url)
        
        soup = await self.fetch_page(session, url)
        if not soup:
            return None
        
        # Extract only title (optimized for speed)
        title = self.extract_title_only(soup)
        
        # Extract navigation links for children
        nav_links = self.extract_navigation_links(soup, url)
        
        # Create scraped page object
        scraped_page = ScrapedPage(
            url=url,
            title=title,
            level=level,
            parent_url=parent_url,
            children_urls=nav_links
        )
        
        return scraped_page

    async def crawl_batch(self, session: aiohttp.ClientSession, 
                         pages_batch: List[Tuple[str, int, Optional[str]]]) -> List[ScrapedPage]:
        """Crawl a batch of pages concurrently."""
        tasks = []
        for url, level, parent_url in pages_batch:
            if url not in self.visited_urls and level <= self.max_depth:
                task = self.scrape_page(session, url, level, parent_url)
                tasks.append(task)
        
        if not tasks:
            return []
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        valid_pages = [page for page in results if isinstance(page, ScrapedPage)]
        return valid_pages

    async def crawl(self, start_url: str) -> List[ScrapedPage]:
        """
        Start crawling from the given URL using concurrent processing.
        
        Args:
            start_url: Starting URL for crawling
            
        Returns:
            List of scraped pages
        """
        logger.info(f"Starting fast crawl from: {start_url}")
        
        if not self.is_valid_oracle_url(start_url):
            logger.error(f"Invalid Oracle documentation URL: {start_url}")
            return []
        
        connector = aiohttp.TCPConnector(limit=self.max_concurrent, limit_per_host=self.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(headers=self.headers, connector=connector, timeout=timeout) as session:
            pages_to_visit: List[Tuple[str, int, Optional[str]]] = [(start_url, 0, None)]
            
            # Initialize progress tracking
            total_discovered = 1  # Start with the initial URL
            total_processed = 0
            
            with tqdm(desc="Fast crawling", unit="pages", colour="green") as pbar:
                while pages_to_visit:
                    # Process pages in batches for better concurrency
                    batch_size = min(self.max_concurrent, len(pages_to_visit))
                    current_batch = pages_to_visit[:batch_size]
                    pages_to_visit = pages_to_visit[batch_size:]
                    
                    batch_results = await self.crawl_batch(session, current_batch)
                    
                    for scraped_page in batch_results:
                        if scraped_page:
                            self.scraped_pages.append(scraped_page)
                            total_processed += 1
                            queue_size = len(pages_to_visit)
                            
                            # Update total discovered count with new children
                            if scraped_page.children_urls:
                                new_children = len([url for url in scraped_page.children_urls if url not in self.visited_urls])
                                total_discovered += new_children
                            
                            # Calculate completion percentage
                            completion_pct = (total_processed / total_discovered) * 100 if total_discovered > 0 else 0
                            
                            pbar.set_description(f"📄 {scraped_page.title[:30]}... | {completion_pct:.1f}% | L{scraped_page.level}")
                            pbar.set_postfix({
                                'Done': total_processed,
                                'Queue': queue_size,
                                'Found': total_discovered
                            })
                            pbar.update(1)
                            
                            # Add children to visit queue
                            if scraped_page.children_urls:
                                for child_url in scraped_page.children_urls:
                                    if child_url not in self.visited_urls:
                                        pages_to_visit.append((child_url, scraped_page.level + 1, scraped_page.url))
        
        logger.info(f"Fast crawling completed. Scraped {len(self.scraped_pages)} pages.")
        return self.scraped_pages

    def save_urls_list(self, filename: Optional[str] = None) -> str:
        """Save scraped URLs to a simple text file."""
        if filename is None:
            filename = f"{self.output_dir}/oracle_urls_fast.txt"
        
        with open(filename, 'w', encoding='utf-8') as f:
            for page in self.scraped_pages:
                f.write(f"{page.url}\n")
        
        logger.info(f"Saved {len(self.scraped_pages)} URLs to {filename}")
        return filename

    def save_to_markdown(self, filename: Optional[str] = None) -> str:
        """Save scraped URLs to a structured markdown file."""
        if filename is None:
            filename = f"{self.output_dir}/oracle_docs_fast.md"
        
        # Group pages by level for hierarchical display
        pages_by_level = {}
        for page in self.scraped_pages:
            level = page.level
            if level not in pages_by_level:
                pages_by_level[level] = []
            pages_by_level[level].append(page)
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("# Oracle Cloud Infrastructure Documentation URLs\n\n")
            f.write(f"**Total URLs scraped:** {len(self.scraped_pages)}\n")
            f.write(f"**Scraping date:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            for level in sorted(pages_by_level.keys()):
                pages = pages_by_level[level]
                f.write(f"## Level {level} ({len(pages)} pages)\n\n")
                
                for page in pages:
                    indent = "  " * level
                    f.write(f"{indent}- [{page.title}]({page.url})\n")
                
                f.write("\n")
        
        logger.info(f"Saved structured markdown to {filename}")
        return filename


async def main():
    """Main async function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Fast Oracle Documentation Scraper")
    parser.add_argument("url", help="Starting URL to scrape")
    parser.add_argument("--max-depth", type=int, default=2, help="Maximum crawl depth (default: 2)")
    parser.add_argument("--max-concurrent", type=int, default=15, help="Maximum concurrent requests (default: 15)")
    parser.add_argument("--delay", type=float, default=0.05, help="Delay between requests in seconds (default: 0.05)")
    parser.add_argument("--output", type=str, help="Output markdown filename")
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = FastOracleDocsScraper(
        max_depth=args.max_depth,
        max_concurrent=args.max_concurrent,
        delay=args.delay
    )
    
    # Start scraping
    start_time = time.time()
    pages = await scraper.crawl(args.url)
    end_time = time.time()
    
    if pages:
        # Save results in scraped_docs directory
        if args.output:
            # Just use the basename since save methods will add the output_dir
            scraper.save_to_markdown(os.path.basename(args.output))
            # Also save simple URL list
            url_filename = os.path.basename(args.output).replace('.md', '_urls.txt')
            scraper.save_urls_list(url_filename)
        else:
            scraper.save_to_markdown()
            scraper.save_urls_list()
        
        elapsed_time = end_time - start_time
        pages_per_second = len(pages) / elapsed_time if elapsed_time > 0 else 0
        
        print(f"\n✅ Fast scraping completed successfully!")
        print(f"📊 **Performance Stats:**")
        print(f"   • Scraped: {len(pages)} pages")
        print(f"   • Time: {elapsed_time:.2f} seconds")
        print(f"   • Speed: {pages_per_second:.2f} pages/second")
        print(f"   • Concurrent requests: {args.max_concurrent}")
        print(f"   • Delay per request: {args.delay}s")
    else:
        print("❌ No pages found to scrape.")
        return 1
    
    return 0


if __name__ == "__main__":
    asyncio.run(main())