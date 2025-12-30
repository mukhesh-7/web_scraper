import json
import pandas as pd


class ExcelPipeline:
    """Export items to XLSX with URL and text content."""
    
    def open_spider(self, spider):
        self.items = []

    def process_item(self, item, spider):
        self.items.append(item)
        return item

    def close_spider(self, spider):
        if not self.items:
            return
        
        # Create DataFrame with URL and text content
        xlsx_data = [
            {'URL': item['url'], 'Content': item.get('text', '')}
            for item in self.items
        ]
        df = pd.DataFrame(xlsx_data)
        excel_path = spider.crawler.settings.get('EXCEL_PATH', 'output.xlsx')
        
        try:
            df.to_excel(excel_path, index=False)
            spider.log(f'Wrote {len(self.items)} rows to {excel_path}')
        except Exception as e:
            spider.log(f'Failed to write Excel file: {e}', level='ERROR')


class JSONPipeline:
    """Export items to JSON with structured data (URL, title, links, DOM properties)."""
    
    def open_spider(self, spider):
        self.items = []

    def process_item(self, item, spider):
        self.items.append(item)
        return item

    def close_spider(self, spider):
        if not self.items:
            return
        
        # Create JSON with structured data
        json_data = []
        for item in self.items:
            # Parse internal links from semicolon-separated string
            internal_links = [
                link.strip()
                for link in item.get('internal_links', '').split(';')
                if link.strip()
            ]
            
            json_data.append({
                'url': item['url'],
                'title': item.get('title', ''),
                'internal_links': internal_links,
                'dom_properties': []  # Will be added separately if needed
            })
        
        json_path = spider.crawler.settings.get('JSON_PATH', 'output.json')
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)
            spider.log(f'Wrote {len(self.items)} items to {json_path}')
        except Exception as e:
            spider.log(f'Failed to write JSON file: {e}', level='ERROR')
