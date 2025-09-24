#!/usr/bin/env python3
"""
Oracle Documentation Scraper
============================

A web scraper designed to crawl Oracle Cloud Infrastructure documentation
and convert it to markdown format suitable for NotebookLM.

Usage:
    python scrapper.py https://docs.oracle.com/en-us/iaas/Content/services.htm
"""

import os
import re
import time
import requests
from urllib.parse import urljoin, urlparse, parse_qs
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup, NavigableString
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass
from tqdm import tqdm
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ScrapedPage:
    """Represents a scraped documentation page."""
    url: str
    title: str
    content: str
    level: int
    parent_url: Optional[str] = None
    children_urls: Optional[List[str]] = None

    def __post_init__(self):
        if self.children_urls is None:
            self.children_urls = []


class OracleDocsScraper:
    """
    A scraper for Oracle Cloud Infrastructure documentation.
    
    This scraper is designed to follow the hierarchical navigation structure
    of Oracle documentation and extract content in a format suitable for
    markdown conversion.
    """
    
    def __init__(self, max_depth: int = 3, delay: float = 1.0, output_dir: str = "scraped_docs"):
        """
        Initialize the Oracle Documentation Scraper.
        
        Args:
            max_depth: Maximum depth to crawl (default: 3)
            delay: Delay between requests in seconds (default: 1.0)
            output_dir: Directory to save markdown files (default: "scraped_docs")
        """
        self.max_depth = max_depth
        self.delay = delay
        self.output_dir = output_dir
        self.visited_urls: Set[str] = set()
        self.scraped_pages: List[ScrapedPage] = []
        
        # Initialize session with headers to mimic a real browser
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        logger.info(f"Oracle Docs Scraper initialized with max_depth={max_depth}, delay={delay}s")

    def is_valid_oracle_url(self, url: str) -> bool:
        """
        Check if URL is a valid Oracle documentation URL.
        
        Args:
            url: URL to validate
            
        Returns:
            True if URL is valid Oracle docs URL, False otherwise
        """
        parsed = urlparse(url)
        return (
            parsed.netloc in ['docs.oracle.com'] and
            '/iaas/' in parsed.path and
            not parsed.fragment and  # Avoid fragment URLs
            not any(ext in parsed.path for ext in ['.pdf', '.zip', '.tar', '.gz'])
        )

    def clean_url(self, url: str) -> str:
        """
        Clean and normalize URL by removing unnecessary parameters.
        
        Args:
            url: URL to clean
            
        Returns:
            Cleaned URL
        """
        parsed = urlparse(url)
        # Remove tracking parameters and fragments
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

    def get_page_content(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch and parse page content.
        
        Args:
            url: URL to fetch
            
        Returns:
            BeautifulSoup object or None if failed
        """
        try:
            logger.info(f"Fetching: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(response.content, 'lxml')
            return soup
            
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing {url}: {e}")
            return None

    def extract_navigation_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """
        Extract navigation links from Oracle documentation sidebar.
        
        Args:
            soup: BeautifulSoup object of the page
            base_url: Base URL for resolving relative links
            
        Returns:
            List of absolute URLs found in navigation
        """
        nav_links = []
        
        # Look for navigation elements in Oracle docs
        # Oracle docs typically use specific classes for navigation
        nav_selectors = [
            'nav a[href]',  # General navigation links
            '.toc a[href]',  # Table of contents links
            '.ohc-toc a[href]',  # Oracle help center TOC
            '.navigation a[href]',  # Navigation section
            'aside a[href]',  # Sidebar links
            '.sidebar a[href]',  # Sidebar navigation
            '.left-nav a[href]',  # Left navigation
            '.menu a[href]',  # Menu links
            # More specific Oracle selectors
            'a[href*="/Content/"]',  # Oracle content links
            'a[href*="/iaas/"]',  # Oracle IaaS links
            '.ohc-link a[href]',  # Oracle help center links
            '.ohc-book-part a[href]',  # Oracle book part links
            '[class*="nav"] a[href]',  # Any element with nav in class
            '[class*="menu"] a[href]',  # Any element with menu in class
            '[class*="toc"] a[href]',  # Any element with toc in class
        ]
        
        for selector in nav_selectors:
            links = soup.select(selector)
            logger.debug(f"Selector '{selector}' found {len(links)} links")
            
            for link in links:
                if hasattr(link, 'get'):
                    href = link.get('href')
                    if href and isinstance(href, str):
                        # Convert relative URLs to absolute
                        absolute_url = urljoin(base_url, href)
                        cleaned_url = self.clean_url(absolute_url)
                        
                        logger.debug(f"Checking URL: {cleaned_url}")
                        
                        if self.is_valid_oracle_url(cleaned_url) and cleaned_url not in self.visited_urls:
                            nav_links.append(cleaned_url)
                            logger.debug(f"Added navigation link: {cleaned_url}")
        
        # If no links found with standard selectors, try broader search
        if not nav_links:
            logger.warning("No navigation links found with standard selectors, trying broader search...")
            all_links = soup.find_all('a', href=True)
            logger.info(f"Found {len(all_links)} total links on page")
            
            for link in all_links:
                if hasattr(link, 'get'):
                    href = link.get('href')
                    if href and isinstance(href, str):
                        absolute_url = urljoin(base_url, href)
                        cleaned_url = self.clean_url(absolute_url)
                        
                        if self.is_valid_oracle_url(cleaned_url) and cleaned_url not in self.visited_urls:
                            nav_links.append(cleaned_url)
                            logger.debug(f"Added broad search link: {cleaned_url}")
        
        unique_links = list(set(nav_links))  # Remove duplicates
        logger.info(f"Extracted {len(unique_links)} unique navigation links from {base_url}")
        
        # Log first few links for debugging
        for i, link in enumerate(unique_links[:5]):
            logger.info(f"  Navigation link {i+1}: {link}")
        
        return unique_links

    def extract_content(self, soup: BeautifulSoup) -> Tuple[str, str]:
        """
        Extract title and main content from Oracle documentation page.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Tuple of (title, content)
        """
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
            'main',
            '.content',
            '.main-content',
            '.page-content',
            '.documentation-content',
            'article',
            '.article-content',
            '#content'
        ]
        
        content_elem = None
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                break
        
        if not content_elem:
            # Fallback to body if no specific content area found
            content_elem = soup.find('body')
        
        if not content_elem:
            return title, ""
        
        # Clean up the content
        content = self._clean_content(content_elem)
        return title, content

    def _clean_content(self, content_elem) -> str:
        """
        Clean and format extracted content.
        
        Args:
            content_elem: BeautifulSoup element containing content
            
        Returns:
            Cleaned content string
        """
        # Remove unwanted elements
        self._remove_unwanted_elements(content_elem)
        
        # Extract text content while preserving some structure
        content_parts = self._extract_content_parts(content_elem)
        
        # Join and clean up the content
        content = ' '.join(content_parts)
        content = self._clean_text(content)
        
        return content.strip()

    def _remove_unwanted_elements(self, content_elem):
        """Remove unwanted HTML elements from content."""
        unwanted_selectors = [
            'nav', 'header', 'footer', '.navigation', '.breadcrumb',
            '.toc', '.sidebar', '.left-nav', '.right-nav', 'script',
            'style', '.advertisement', '.ads', '.social-share'
        ]
        
        for selector in unwanted_selectors:
            for elem in content_elem.select(selector):
                elem.decompose()

    def _extract_content_parts(self, content_elem) -> List[str]:
        """Extract content parts with proper formatting."""
        content_parts = []
        
        for element in content_elem.descendants:
            text = self._process_element(element)
            if text:
                content_parts.append(text)
        
        return content_parts

    def _process_element(self, element) -> Optional[str]:
        """Process individual HTML elements and return formatted text."""
        if isinstance(element, NavigableString):
            text = str(element).strip()
            return text if text else None
        
        if not hasattr(element, 'name') or not element.name:
            return None
        
        element_text = element.get_text().strip()
        if not element_text:
            return None
        
        if element.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            level = int(element.name[1:])
            return f"\n\n{'#' * level} {element_text}\n"
        elif element.name == 'p':
            return f"\n{element_text}\n"
        elif element.name in ['ul', 'ol']:
            return f"\n{element_text}\n"
        elif element.name == 'pre':
            return f"\n```\n{element_text}\n```\n"
        
        return None

    def _clean_text(self, content: str) -> str:
        """Clean up text formatting."""
        content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)  # Remove extra newlines
        content = re.sub(r' +', ' ', content)  # Remove extra spaces
        return content

    def scrape_page(self, url: str, level: int = 0, parent_url: Optional[str] = None) -> Optional[ScrapedPage]:
        """
        Scrape a single page and extract only URL and title (no content).
        
        Args:
            url: URL to scrape
            level: Current depth level
            parent_url: Parent page URL
            
        Returns:
            ScrapedPage object or None if failed
        """
        if level > self.max_depth or url in self.visited_urls:
            return None
        
        self.visited_urls.add(url)
        
        # Add delay to be respectful
        if self.delay > 0:
            time.sleep(self.delay)
        
        soup = self.get_page_content(url)
        if not soup:
            return None
        
        # Extract only title (skip content extraction for performance)
        title = self.extract_title_only(soup)
        
        # Extract navigation links for children
        nav_links = self.extract_navigation_links(soup, url)
        
        # Create scraped page object (no content)
        scraped_page = ScrapedPage(
            url=url,
            title=title,
            content="",  # Empty content since we only want URLs
            level=level,
            parent_url=parent_url,
            children_urls=nav_links
        )
        
        return scraped_page

    def extract_title_only(self, soup: BeautifulSoup) -> str:
        """
        Extract only the title from a page (faster than full content extraction).
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Page title
        """
        title = "Untitled"
        title_selectors = ['h1', '.page-title', '.title', 'title']
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                title = title_elem.get_text().strip()
                break
        return title

    def crawl(self, start_url: str) -> List[ScrapedPage]:
        """
        Start crawling from the given URL.
        
        Args:
            start_url: Starting URL for crawling
            
        Returns:
            List of scraped pages
        """
        logger.info(f"Starting crawl from: {start_url}")
        
        if not self.is_valid_oracle_url(start_url):
            logger.error(f"Invalid Oracle documentation URL: {start_url}")
            return []
        
        pages_to_visit: List[Tuple[str, int, Optional[str]]] = [(start_url, 0, None)]  # (url, level, parent_url)
        
        with tqdm(desc="Crawling pages") as pbar:
            while pages_to_visit:
                url, level, parent_url = pages_to_visit.pop(0)
                
                if level > self.max_depth:
                    continue
                
                scraped_page = self.scrape_page(url, level, parent_url)
                if scraped_page:
                    self.scraped_pages.append(scraped_page)
                    pbar.set_description(f"Crawled: {scraped_page.title[:50]}...")
                    pbar.update(1)
                    
                    # Add children to visit queue
                    if scraped_page.children_urls:
                        for child_url in scraped_page.children_urls:
                            if child_url not in self.visited_urls:
                                pages_to_visit.append((child_url, level + 1, url))
        
        logger.info(f"Crawling completed. Scraped {len(self.scraped_pages)} pages.")
        return self.scraped_pages

    def save_to_markdown(self, filename: Optional[str] = None) -> str:
        """
        Save scraped URLs to a markdown file (URLs only, no content).
        
        Args:
            filename: Output filename (optional)
            
        Returns:
            Path to the saved markdown file
        """
        if not filename:
            filename = "oracle_docs_urls.md"
        
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("# Oracle Cloud Infrastructure Documentation URLs\n\n")
            f.write(f"*Scraped on {time.strftime('%Y-%m-%d %H:%M:%S')}*\n\n")
            f.write(f"**Total URLs found:** {len(self.scraped_pages)}\n\n")
            f.write("---\n\n")
            
            # Group pages by level for better organization
            pages_by_level = {}
            for page in self.scraped_pages:
                if page.level not in pages_by_level:
                    pages_by_level[page.level] = []
                pages_by_level[page.level].append(page)
            
            # Write URLs organized by level
            for level in sorted(pages_by_level.keys()):
                level_name = "Root" if level == 0 else f"Level {level}"
                f.write(f"## {level_name} ({len(pages_by_level[level])} URLs)\n\n")
                
                for page in pages_by_level[level]:
                    # Write only title and URL
                    f.write(f"- **{page.title}**  \n")
                    f.write(f"  {page.url}\n\n")
                
                f.write("---\n\n")
        
        logger.info(f"URLs markdown file saved to: {filepath}")
        return filepath

    def save_urls_list(self, filename: Optional[str] = None) -> str:
        """
        Save just the URLs as a simple list (one URL per line).
        
        Args:
            filename: Output filename (optional)
            
        Returns:
            Path to the saved file
        """
        if not filename:
            filename = "oracle_docs_urls.txt"
        
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("# Oracle Cloud Infrastructure Documentation URLs\n")
            f.write(f"# Scraped on {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total URLs: {len(self.scraped_pages)}\n\n")
            
            # Write all URLs, one per line
            for page in self.scraped_pages:
                f.write(f"{page.url}\n")
        
        logger.info(f"URLs list saved to: {filepath}")
        return filepath


def main():
    """Main function to run the scraper from command line."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Oracle Documentation Scraper")
    parser.add_argument("url", help="Starting URL to scrape")
    parser.add_argument("--max-depth", type=int, default=3, help="Maximum crawling depth (default: 3)")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests in seconds (default: 1.0)")
    parser.add_argument("--output", help="Output filename (default: oracle_docs_urls.md)")
    parser.add_argument("--output-dir", default="scraped_docs", help="Output directory (default: scraped_docs)")
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = OracleDocsScraper(
        max_depth=args.max_depth,
        delay=args.delay,
        output_dir=args.output_dir
    )
    
    # Start crawling
    try:
        pages = scraper.crawl(args.url)
        
        if pages:
            # Save URLs in markdown format
            markdown_file = scraper.save_to_markdown(args.output)
            
            # Also save as simple URL list
            url_list_filename = args.output.replace('.md', '.txt') if args.output else 'oracle_docs_urls.txt'
            urls_file = scraper.save_urls_list(url_list_filename)
            
            print(f"\n✅ Successfully scraped {len(pages)} URLs!")
            print(f"📄 Structured markdown: {markdown_file}")
            print(f"📋 URL list: {urls_file}")
            print("\n📊 Summary:")
            print(f"   - Total URLs found: {len(pages)}")
            print(f"   - Max depth reached: {max(page.level for page in pages) if pages else 0}")
            print(f"   - Output directory: {args.output_dir}")
            print("\n💡 Use the URL list file for NotebookLM input!")
        else:
            print("❌ No URLs were found. Please check the URL and try again.")
            
    except KeyboardInterrupt:
        print("\n⚠️  Scraping interrupted by user.")
    except Exception as e:
        print(f"❌ Error during scraping: {e}")
        logger.exception("Scraping failed")


if __name__ == "__main__":
    main()
