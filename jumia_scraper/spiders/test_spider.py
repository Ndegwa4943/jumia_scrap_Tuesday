import scrapy

class TestSpider(scrapy.Spider):
    name = "test"
    
    def start_requests(self):
        yield scrapy.Request('https://httpbin.org/get', self.parse)

    def parse(self, response):
        yield {'hello': 'world'}