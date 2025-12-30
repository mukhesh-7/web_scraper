import json
import logging
import re
import time
from collections import defaultdict
from typing import Dict, List, Set, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class WebScraper:
    def __init__(self, start_url: str, delay: float = 1.0, timeout: int = 30):
        """
        Initialize the web scraper with Playwright.
        
        Args:
            start_url: The starting URL to scrape
            delay: Delay between requests (seconds)
            timeout: Request timeout (seconds)
        """
        self.start_url = start_url
        self.delay = delay
        self.timeout = timeout * 1000  # Playwright uses milliseconds
        
        parsed = urlparse(start_url)
        self.domain = f"{parsed.scheme}://{parsed.netloc}"
        
        self.visited: Set[str] = set()
        self.to_visit: List[str] = [start_url]
        self.data: List[Dict] = []
        self.errors: List[Dict] = []
        
        self.playwright = None
        self.browser = None
        self.context = None
        
        logger.info(f"Initialized scraper for domain: {self.domain}")

    def start_browser(self):
        """Start the Playwright browser."""
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=True)
        self.context = self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )

    def stop_browser(self):
        """Stop the Playwright browser."""
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()

    def is_internal_link(self, url: str) -> bool:
        """Check if URL belongs to the same domain."""
        try:
            parsed = urlparse(url)
            return parsed.netloc == urlparse(self.start_url).netloc
        except:
            return False

    def normalize_url(self, url: str) -> str:
        """Normalize and resolve relative URLs."""
        try:
            resolved = urljoin(self.start_url, url)
            parsed = urlparse(resolved)
            # Remove fragments
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}{'?' + parsed.query if parsed.query else ''}"
        except:
            return url

    def extract_dom_properties(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract detailed DOM element properties."""
        dom_props = []
        try:
            # Helper to generate a simple CSS selector path
            def get_path(element):
                path = []
                for parent in element.parents:
                    if parent.name == '[document]':
                        break
                    siblings = parent.find_all(element.name, recursive=False)
                    if len(siblings) > 1:
                        index = siblings.index(element) + 1
                        path.append(f"{element.name}:nth-of-type({index})")
                    else:
                        path.append(element.name)
                    element = parent
                return " > ".join(reversed(path))

            for elem in soup.find_all(True):
                if elem.name and elem.name not in ['script', 'style']:
                    text = elem.get_text(strip=True)[:200]
                    
                    # Convert attrs to a serializable dict
                    attrs = {}
                    for k, v in elem.attrs.items():
                        if isinstance(v, list):
                            attrs[k] = " ".join(v)
                        else:
                            attrs[k] = str(v)

                    dom_props.append({
                        'tag': elem.name,
                        'path': get_path(elem),
                        'attributes': attrs,
                        'text_preview': text if text else None
                    })
        except Exception as e:
            logger.warning(f"Error extracting DOM properties: {e}")
        
        return dom_props

    def scrape_page(self, page, url: str) -> Optional[Dict]:
        """
        Scrape a single page using Playwright and extract data.
        """
        try:
            logger.info(f"Navigating to: {url}")
            page.goto(url, timeout=self.timeout, wait_until='domcontentloaded')
            
            # Wait for some dynamic content if needed, key heuristic: check if body is not empty or network idle
            try:
                page.wait_for_load_state('networkidle', timeout=5000)
            except PlaywrightTimeoutError:
                logger.warning(f"Timeout waiting for network idle on {url}, proceeding with current content")
            
            # Additional small wait for JS rendering
            time.sleep(1)
            
            content_html = page.content()
            soup = BeautifulSoup(content_html, 'html.parser')
            
            # Extract title
            title = soup.title.string if soup.title else page.title()
            
            # Extract all text content
            content = soup.get_text(separator='\n', strip=True)
            
            # Extract internal links
            internal_links = set()
            for a in soup.find_all('a', href=True):
                href = a['href']
                if href and not href.startswith(('mailto:', 'tel:', 'javascript:')):
                    normalized = self.normalize_url(href)
                    if self.is_internal_link(normalized):
                        internal_links.add(normalized)
            
            # Extract DOM properties
            dom_properties = self.extract_dom_properties(soup)
            
            data = {
                'url': url,
                'title': title,
                'content': content,
                'internal_links': sorted(list(internal_links)),
                'dom_properties': dom_properties
            }
            
            logger.info(f"Successfully scraped {url} ({len(content)} chars, {len(internal_links)} links)")
            return data
            
        except Exception as e:
            error_msg = f"Error scraping {url}: {str(e)}"
            logger.error(error_msg)
            self.errors.append({'url': url, 'error': error_msg})
            return None

    def crawl(self, max_pages: int = None) -> None:
        """
        Recursively crawl the website using Playwright.
        """
        logger.info(f"Starting crawl from {self.start_url}")
        self.start_browser()
        
        try:
            # Create a reusable page
            page = self.context.new_page()
            
            while self.to_visit and (max_pages is None or len(self.data) < max_pages):
                url = self.to_visit.pop(0)
                
                if url in self.visited:
                    continue
                
                self.visited.add(url)
                page_data = self.scrape_page(page, url)
                
                if page_data:
                    self.data.append(page_data)
                    # Add new internal links to the queue
                    for link in page_data['internal_links']:
                        if link not in self.visited and link not in self.to_visit:
                            self.to_visit.append(link)
                
                time.sleep(self.delay)
                
        except Exception as e:
            logger.error(f"Crawl aborted due to error: {e}")
        finally:
            self.stop_browser()
            logger.info(f"Crawl complete. Scraped {len(self.data)} pages, {len(self.errors)} errors")

    def save_to_xlsx(self, filename: str = 'scraped_content.xlsx') -> None:
        """Save text content to XLSX file."""
        try:
            import pandas as pd
            
            xlsx_data = [{'URL': item['url'], 'Content': item['content']} for item in self.data]
            df = pd.DataFrame(xlsx_data)
            df.to_excel(filename, index=False)
            logger.info(f"Saved XLSX to {filename}")
        except Exception as e:
            logger.error(f"Failed to save XLSX: {e}. Please close the file if it is open.")

    def save_to_json(self, filename: str = 'scraped_data.json') -> None:
        """Save structured data (excluding content) to JSON file."""
        try:
            json_data = [
                {
                    'url': item['url'],
                    'title': item['title'],
                    'internal_links': item['internal_links'],
                    'dom_properties': item['dom_properties']
                }
                for item in self.data
            ]
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved JSON to {filename}")
        except Exception as e:
            logger.error(f"Failed to save JSON: {e}")

    def save_errors(self, filename: str = 'scraper_errors.json') -> None:
        """Save error log to JSON file."""
        try:
            if self.errors:
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(self.errors, f, indent=2)
                logger.info(f"Saved errors to {filename}")
        except Exception as e:
            logger.error(f"Failed to save errors: {e}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Web Scraper: Extract content and DOM properties from websites (w/ JS support)'
    )
    parser.add_argument('url', help='Starting URL to scrape')
    parser.add_argument('--xlsx', default='scraped_content.xlsx', help='XLSX output file')
    parser.add_argument('--json', default='scraped_data.json', help='JSON output file')
    parser.add_argument('--max-pages', type=int, help='Maximum pages to scrape')
    parser.add_argument('--delay', type=float, default=1.0, help='Delay between requests (seconds)')
    parser.add_argument('--timeout', type=int, default=30, help='Request timeout (seconds)')
    
    args = parser.parse_args()
    
    scraper = WebScraper(args.url, delay=args.delay, timeout=args.timeout)
    scraper.crawl(max_pages=args.max_pages)
    
    scraper.save_to_xlsx(args.xlsx)
    scraper.save_to_json(args.json)
    scraper.save_errors()


if __name__ == '__main__':
    main()
