import os
import re
from urllib.parse import urlparse, urldefrag
import scrapy


EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
PHONE_RE = re.compile(r"\+?\d[\d\-().\s]{6,}\d")


class SiteSpider(scrapy.Spider):
    name = 'site_spider'
    custom_settings = {
        'DOWNLOAD_HANDLERS': {
            'http': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
            'https': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
        },
        'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
    }

    def __init__(self, start_url, output_dir='output', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = [start_url]
        self.output_dir = output_dir
        parsed = urlparse(start_url)
        self.allowed_domains = [parsed.netloc]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                meta={
                    'playwright': True,
                    'playwright_page_coroutines': [
                        'wait_for_load_state("networkidle")',
                    ],
                },
                callback=self.parse
            )

    def parse(self, response):
        parsed = urlparse(response.url)
        path = parsed.path

        if not path or path.endswith('/'):
            filename = 'index.html'
            dir_rel = path.lstrip('/')
        else:
            dir_rel = os.path.dirname(path.lstrip('/'))
            filename = os.path.basename(path.lstrip('/')) or 'index.html'

        dirpath = os.path.join(self.output_dir, parsed.netloc, dir_rel)
        os.makedirs(dirpath, exist_ok=True)
        file_path = os.path.join(dirpath, filename)

        with open(file_path, 'wb') as f:
            f.write(response.body)

        self.log(f'Saved {response.url} -> {file_path}')

        # Extract structured data
        title = (response.css('title::text').get() or '').strip()
        meta_desc = (response.css('meta[name=description]::attr(content)').get() or '').strip()
        body_texts = response.xpath('//body//text()[normalize-space()]').getall()
        page_text = ' '.join(t.strip() for t in body_texts if t and t.strip())

        internal_links = []
        external_links = []
        raw_links = response.css('a::attr(href)').getall()
        for href in raw_links:
            if not href:
                continue
            if href.startswith('mailto:') or href.startswith('tel:') or href.startswith('javascript:'):
                continue
            href = response.urljoin(href)
            href, _ = urldefrag(href)
            parsed_href = urlparse(href)
            if parsed_href.netloc and parsed_href.netloc != parsed.netloc:
                external_links.append(href)
            else:
                internal_links.append(href)

        # Extract contacts
        emails = set(EMAIL_RE.findall(response.text))
        phones = set(PHONE_RE.findall(response.text))

        item = {
            'url': response.url,
            'title': title,
            'meta_description': meta_desc,
            'text': page_text,
            'html': response.text,
            'internal_links': '; '.join(sorted(set(internal_links))),
            'external_links': '; '.join(sorted(set(external_links))),
            'emails': '; '.join(sorted(emails)),
            'phones': '; '.join(sorted(phones)),
            'saved_path': file_path,
        }

        yield item

        # Follow internal links
        for href in set(internal_links):
            yield response.follow(
                href,
                callback=self.parse,
                meta={
                    'playwright': True,
                    'playwright_page_coroutines': [
                        'wait_for_load_state("networkidle")',
                    ],
                }
            )
