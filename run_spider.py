#!/usr/bin/env python3
"""Runner script for web scraper with multiple backend options."""

import argparse
import sys


def run_standalone(start_url, xlsx, json_file, max_pages, delay, timeout):
    """Run the standalone BeautifulSoup-based scraper."""
    from scraper import WebScraper
    
    scraper = WebScraper(start_url, delay=delay, timeout=timeout)
    scraper.crawl(max_pages=max_pages)
    scraper.save_to_xlsx(xlsx)
    scraper.save_to_json(json_file)
    scraper.save_errors()


def run_scrapy(start_url, output, xlsx, json_file, log_level):
    """Run the Scrapy-based spider with Playwright."""
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.log import configure_logging
    from site_spider import SiteSpider
    
    configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})
    
    settings = {
        'USER_AGENT': 'site-mirror-bot/1.0',
        'LOG_LEVEL': log_level,
        'EXCEL_PATH': xlsx,
        'JSON_PATH': json_file,
        'ITEM_PIPELINES': {
            'pipelines.ExcelPipeline': 300,
            'pipelines.JSONPipeline': 310,
        },
        'ROBOTSTXT_OBEY': False,
        'COOKIES_ENABLED': False,
        'RETRY_ENABLED': False,
        'CONCURRENT_REQUESTS': 4,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'DOWNLOAD_DELAY': 1.0,
        'PLAYWRIGHT_LAUNCH_ARGS': ['--disable-blink-features=AutomationControlled'],
    }
    
    process = CrawlerProcess(settings)
    process.crawl(SiteSpider, start_url=start_url, output_dir=output)
    process.start()


def main():
    parser = argparse.ArgumentParser(
        description='Web Scraper: Extract website content and DOM properties'
    )
    parser.add_argument('url', help='Starting URL to scrape')
    parser.add_argument(
        '--mode',
        choices=['standalone', 'scrapy'],
        default='standalone',
        help='Scraper backend (default: standalone for static sites, use scrapy for JS-heavy sites)'
    )
    parser.add_argument('--output', '-o', default='output', help='Output directory (Scrapy mode)')
    parser.add_argument('--xlsx', '-x', default='scraped_content.xlsx', help='XLSX output file')
    parser.add_argument('--json', '-j', default='scraped_data.json', help='JSON output file')
    parser.add_argument('--max-pages', type=int, help='Max pages to scrape')
    parser.add_argument('--delay', type=float, default=1.0, help='Delay between requests (seconds)')
    parser.add_argument('--timeout', type=int, default=10, help='Request timeout (seconds)')
    parser.add_argument('--log', default='INFO', help='Log level (Scrapy mode)')
    
    args = parser.parse_args()
    
    try:
        if args.mode == 'standalone':
            run_standalone(args.url, args.xlsx, args.json, args.max_pages, args.delay, args.timeout)
        else:
            run_scrapy(args.url, args.output, args.xlsx, args.json, args.log)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
