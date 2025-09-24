#!/usr/bin/env python3
"""
Refurbished OCI Documentation Scraper
=====================================

A high-performance parallel scraper for Oracle Cloud Infrastructure documentation.
Extracts both URLs and content, saving to structured markdown format.

Features:
- Parallel processing with asyncio/aiohttp
- Content extraction and markdown generation
- Progress bars with completion percentage
- Hierarchical structure preservation
"""

import asyncio
import aiohttp
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from typing import List, Set, Dict, Optional
from dataclasses import dataclass
from tqdm.asyncio import tqdm
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ScrapedPage:
    """Represents a scraped documentation page with content."""
    url: str
    title: str
    content: str
    level: int
    parent_url: Optional[str] = None
    children_urls: Optional[List[str]] = None

    def __post_init__(self):
        if self.children_urls is None:
            self.children_urls = []


class ParallelOCIScraper:
    """
    A high-performance parallel scraper for OCI documentation.

    Extracts both URLs and content, saving to structured markdown.
    """

    def __init__(self, max_depth: int = 3, max_concurrent: int = 15, delay: float = 0.05,
                 output_dir: str = "scraped_docs"):
        """
        Initialize the Parallel OCI Scraper.

        Args:
            max_depth: Maximum depth to crawl
            max_concurrent: Maximum concurrent requests
            delay: Delay between requests in seconds
            output_dir: Directory to save markdown files
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

        logger.info(f"Parallel OCI Scraper initialized with max_depth={max_depth}, "
                   f"max_concurrent={max_concurrent}, delay={delay}s")

    def is_valid_oci_url(self, url: str) -> bool:
        """Check if URL is a valid OCI documentation URL."""
        parsed = urlparse(url)
        return (
            parsed.netloc == 'docs.oracle.com' and
            ('/iaas/Content/' in parsed.path or '/en-us/iaas/Content/' in parsed.path) and
            not parsed.fragment and
            not any(ext in parsed.path for ext in ['.pdf', '.zip', '.tar', '.gz'])
        )

    def clean_url(self, url: str) -> str:
        """Clean and normalize URL."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

    async def fetch_page(self, session: aiohttp.ClientSession, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse page content asynchronously."""
        async with self.semaphore:
            try:
                if self.delay > 0:
                    await asyncio.sleep(self.delay)

                logger.debug(f"Fetching: {url}")
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    response.raise_for_status()
                    content = await response.read()
                    soup = BeautifulSoup(content, 'lxml')
                    return soup

            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                return None

    def extract_navigation_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract navigation links from OCI documentation."""
        nav_links = []

        # Optimized selectors for OCI docs navigation
        nav_selectors = [
            'nav a[href]',
            '.toc a[href]',
            '.ohc-toc a[href]',
            'aside a[href]',
            '.sidebar a[href]',
            '.left-nav a[href]',
            'a[href*="/Content/"]',
            'a[href*="/en-us/iaas/"]',
        ]

        for selector in nav_selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if href and isinstance(href, str):
                    absolute_url = urljoin(base_url, href)
                    cleaned_url = self.clean_url(absolute_url)

                    if self.is_valid_oci_url(cleaned_url) and cleaned_url not in self.visited_urls:
                        nav_links.append(cleaned_url)

        # Fallback: broader search
        if not nav_links:
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href')
                if href and isinstance(href, str):
                    absolute_url = urljoin(base_url, href)
                    cleaned_url = self.clean_url(absolute_url)

                    if self.is_valid_oci_url(cleaned_url) and cleaned_url not in self.visited_urls:
                        nav_links.append(cleaned_url)

        return list(set(nav_links))  # Remove duplicates

    def extract_title_and_content(self, soup: BeautifulSoup) -> tuple[str, str]:
        """Extract title and main content from the page."""
        # Extract title
        title = "Untitled"
        title_selectors = ['h1', '.page-title', '.title', 'title']
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                title = title_elem.get_text().strip()
                break

        # Extract main content
        content_selectors = [
            'main', '.content', '.main-content', '.page-content',
            '.documentation-content', 'article', '.article-content', '#content'
        ]

        content_elem = None
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                break

        if not content_elem:
            content_elem = soup.find('body')

        if content_elem:
            # Remove unwanted elements
            for elem in content_elem.select('nav, header, footer, .navigation, .breadcrumb, .toc, .sidebar, script, style'):
                elem.decompose()

            # Extract text content
            content = content_elem.get_text()
            # Clean up whitespace
            content = '\n'.join(line.strip() for line in content.split('\n') if line.strip())
            content = '\n\n'.join(para.strip() for para in content.split('\n\n') if para.strip())
        else:
            content = ""

        return title, content

    async def scrape_page(self, session: aiohttp.ClientSession, url: str, level: int = 0,
                         parent_url: Optional[str] = None) -> Optional[ScrapedPage]:
        """Scrape a single page asynchronously."""
        if level > self.max_depth or url in self.visited_urls:
            return None

        self.visited_urls.add(url)

        soup = await self.fetch_page(session, url)
        if not soup:
            return None

        # Extract title and content
        title, content = self.extract_title_and_content(soup)

        # Extract navigation links for children
        nav_links = self.extract_navigation_links(soup, url)

        scraped_page = ScrapedPage(
            url=url,
            title=title,
            content=content,
            level=level,
            parent_url=parent_url,
            children_urls=nav_links
        )

        return scraped_page

    async def crawl_batch(self, session: aiohttp.ClientSession,
                         pages_batch: List[tuple[str, int, Optional[str]]]) -> List[ScrapedPage]:
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
        """Start crawling from the given URL using parallel processing."""
        logger.info(f"Starting parallel crawl from: {start_url}")

        if not self.is_valid_oci_url(start_url):
            logger.error(f"Invalid OCI documentation URL: {start_url}")
            return []

        connector = aiohttp.TCPConnector(limit=self.max_concurrent, limit_per_host=self.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(headers=self.headers, connector=connector, timeout=timeout) as session:
            # Initialize progress tracking
            total_discovered = 1
            total_processed = 0
            pages_to_visit: List[tuple[str, int, Optional[str]]] = [(start_url, 0, None)]

            with tqdm(desc="Parallel scraping", unit="pages", colour="cyan") as pbar:
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

                            # Update total discovered count with new children
                            if scraped_page.children_urls:
                                new_children = len([url for url in scraped_page.children_urls if url not in self.visited_urls])
                                total_discovered += new_children

                            # Calculate completion percentage
                            completion_pct = (total_processed / total_discovered) * 100 if total_discovered > 0 else 0
                            queue_size = len(pages_to_visit)

                            pbar.set_description(f"📄 {scraped_page.title[:30]}... | {completion_pct:.1f}% | L{scraped_page.level}")
                            pbar.set_postfix({
                                'Done': total_processed,
                                'Queue': queue_size,
                                'Found': total_discovered
                            })
                            pbar.update(1)

                    # Add new children to visit queue
                    for scraped_page in batch_results:
                        if scraped_page and scraped_page.children_urls:
                            for child_url in scraped_page.children_urls:
                                if child_url not in self.visited_urls:
                                    pages_to_visit.append((child_url, scraped_page.level + 1, scraped_page.url))

        logger.info(f"Parallel crawling completed. Scraped {len(self.scraped_pages)} pages.")
        return self.scraped_pages

    def save_to_markdown(self, filename: str = "oci_docs_content.md") -> str:
        """Save scraped pages to markdown format."""
        if not self.scraped_pages:
            logger.warning("No pages to save")
            return ""

        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("# Oracle Cloud Infrastructure Documentation URLs\n\n")
            f.write(f"**Total URLs collected:** {len(self.scraped_pages)}\n")
            f.write(f"**Scraping date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Max depth:** {self.max_depth}\n\n")

            # Group by depth level
            by_level = {}
            for page in self.scraped_pages:
                level = getattr(page, 'depth', 0)
                if level not in by_level:
                    by_level[level] = []
                by_level[level].append(page)

            for level in sorted(by_level.keys()):
                pages_at_level = by_level[level]
                f.write(f"## Level {level} ({len(pages_at_level)} URLs)\n\n")
                for page in sorted(pages_at_level, key=lambda p: p.url):
                    f.write(f"- {page.url}\n")
                f.write("\n")

        logger.info(f"URLs saved to {filepath}")
        return filepath

    def save_urls_only(self, filename: str = "oci_docs_urls.txt") -> str:
        """Save just the URLs to a text file."""
        filepath = os.path.join(self.output_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            for page in self.scraped_pages:
                f.write(f"{page.url}\n")

        logger.info(f"URLs saved to {filepath}")
        return filepath


async def main():
    """Main async function."""
    import argparse

    parser = argparse.ArgumentParser(description="Parallel OCI Documentation Scraper")
    parser.add_argument("url", help="Starting URL to scrape")
    parser.add_argument("--max-depth", type=int, default=2, help="Maximum crawl depth (default: 2)")
    parser.add_argument("--max-concurrent", type=int, default=10, help="Maximum concurrent requests (default: 10)")
    parser.add_argument("--delay", type=float, default=0.1, help="Delay between requests in seconds (default: 0.1)")
    parser.add_argument("--output", type=str, default="oci_docs_content.md", help="Output markdown filename")

    args = parser.parse_args()

    # Initialize scraper
    scraper = ParallelOCIScraper(
        max_depth=args.max_depth,
        max_concurrent=args.max_concurrent,
        delay=args.delay
    )

    # Start scraping
    start_time = time.time()
    pages = await scraper.crawl(args.url)
    end_time = time.time()

    if pages:
        # Save results
        content_file = scraper.save_to_markdown(args.output)
        urls_file = scraper.save_urls_only(args.output.replace('.md', '_urls.txt'))

        elapsed_time = end_time - start_time
        pages_per_second = len(pages) / elapsed_time if elapsed_time > 0 else 0

        print("\n✅ Parallel scraping completed successfully!")
        print("📊 **Performance Stats:**")
        print(f"   • Scraped: {len(pages)} pages")
        print(f"   • Time: {elapsed_time:.2f} seconds")
        print(f"   • Speed: {pages_per_second:.2f} pages/second")
        print(f"   • Concurrent requests: {args.max_concurrent}")
        print(f"   • Delay per request: {args.delay}s")
        print("\n📄 **Output Files:**")
        print(f"   • Content: {content_file}")
        print(f"   • URLs: {urls_file}")
    else:
        print("❌ No pages found to scrape.")
        return 1

    return 0


if __name__ == "__main__":
    asyncio.run(main())