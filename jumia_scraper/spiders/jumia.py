#jumia.py

import scrapy
import datetime
import re
from scrapy.loader import ItemLoader
from jumia_scraper.items import JumiaProductItem
from urllib.parse import urljoin

class JumiaSpider(scrapy.Spider):
    name = 'jumia'
    allowed_domains = ['jumia.co.ke']
    start_urls = ['https://www.jumia.co.ke/smartphones/']

    custom_settings = {
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml',
            'X-Forwarded-For': '41.90.0.1'
        },
        'ITEM_PIPELINES': {
            'jumia_scraper.pipelines.PostgresPipeline': 300,
        },
        'ROBOTSTXT_OBEY': False
    }

    def parse(self, response):
        product_links = response.css('article.prd a.core::attr(href)').getall()
        for link in product_links:
            yield response.follow(
                urljoin(response.url, link),
                callback=self.parse_product,
                meta={'original_url': response.url}
            )

        next_page = response.css('a.pg[aria-label="Next"]::attr(href)').get()
        if next_page:
            yield response.follow(
                urljoin(response.url, next_page),
                callback=self.parse
            )

    def parse_product(self, response):
        loader = ItemLoader(item=JumiaProductItem(), response=response)

        loader.add_css('current_price', [
            'span.-b.-ltr.-tal.-fs24::text',
            'div.prc::text',
            'meta[itemprop="price"]::attr(content)'
        ])

        loader.add_css('review_count', [
            'a[href="#reviews"] span::text',
            'div.rev::text',
            'span.-plxs::text',
            'meta[itemprop="reviewCount"]::attr(content)'
        ])

        shipping = response.css('div.shipping::text, div.-df.-i-ctr.-pbs::text').get()
        loader.add_value('shipping', 
            shipping if shipping else 
            'Free' if 'free delivery' in response.text.lower() else 'Paid'
        )

        title = response.css('h1.-fs20::text').get()
        loader.add_value('title', title)
        loader.add_css('original_price', 'div.old::text, span.-tal.-gy5.-lthr.-fs16::text')
        loader.add_css('discount', 'span.bdg._dsct::text')
        loader.add_css('rating', 'div.stars._m._al::text, meta[itemprop="ratingValue"]::attr(content)')
        loader.add_css('seller', [
            'a[href*="/merchant/"]::text',
            'div.merchant-name::text',
            'div.-pvxs a:not([href*="/brand/"])::text'
        ])
        loader.add_css('brand', 'a[href*="/brand/"]::text, meta[itemprop="brand"]::attr(content)') 
        loader.add_value('link', response.url)
        loader.add_value('scraped_at', datetime.datetime.now())

        description = response.css('div.markup.-mhm.-pvl::text, div.markup.-mhm.-pvl *::text').getall()
        loader.add_value('description', ' '.join([d.strip() for d in description if d.strip()]))

        image_urls = response.css('div.sldr img::attr(data-src)').getall()
        image_urls = [urljoin(response.url, img) for img in image_urls]
        loader.add_value('image_urls', image_urls)

        specs = []
        extracted_specs = []
        ram_gb = storage_gb = screen_size_inches = battery_mah = None
        camera_mp_main = camera_mp_selfie = None
        has_dual_sim = False
        network_type = None

        for section in response.css('section.card.-pvs'):
            category = section.css('h2::text').get()
            rows = section.css('tr')
            for row in rows:
                key = row.css('th::text').get()
                val = row.css('td::text').get()
                if not key and not val:
                    self.logger.warning(f"[SKIP:EMPTY] link={response.url} category={category} row={row.get()}")
                    continue
                if not key and val:
                    self.logger.info(f"[SKIP:KEY_MISSING] link={response.url} category={category} val={val}")
                    continue
                spec_dict = {
                    'category': category,
                    'spec_key': key.strip(),
                    'spec_value': val.strip()
                }
                specs.append(spec_dict)

        # Extraction from title
        title_l = title.lower() if title else ""
        def append_extracted(k, v):
            if v is not None:
                extracted_specs.append({
                    'category': 'extracted',
                    'spec_type': 'extracted',
                    'spec_key': k,
                    'spec_value': str(v)
                })

        match = re.search(r'(\d{1,2})\s*gb\s*ram', title_l)
        if match:
            ram_gb = int(match.group(1))
        append_extracted('RAM (extracted)', ram_gb)

        match = re.search(r'(\d{2,4})\s*gb\s*(rom|storage)?', title_l)
        if match:
            storage_gb = int(match.group(1))
        append_extracted('Storage (extracted)', storage_gb)

        match = re.search(r'(\d{1,2}\.\d{1,2})\s*\"', title_l)
        if match:
            screen_size_inches = float(match.group(1))
        append_extracted('Screen Size (inches)', screen_size_inches)

        match = re.search(r'(\d{4,5})\s*m?ah', title_l)
        if match:
            battery_mah = int(match.group(1))
        append_extracted('Battery (mAh)', battery_mah)

        match = re.search(r'(\d{1,3})\s*mp', title_l)
        if match:
            camera_mp_main = int(match.group(1))
        append_extracted('Camera MP Main', camera_mp_main)

        match = re.search(r'(5g|4g|3g)', title_l)
        if match:
            network_type = match.group(1).upper()
        append_extracted('Network Type', network_type)

        if 'dual sim' in title_l:
            has_dual_sim = True
        append_extracted('Dual SIM', has_dual_sim)

        loader.add_value('specifications', specs + extracted_specs)

        yield loader.load_item()