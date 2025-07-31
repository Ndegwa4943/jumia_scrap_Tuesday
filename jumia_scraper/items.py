# items.py
import scrapy
from itemloaders.processors import TakeFirst, MapCompose
from w3lib.html import remove_tags

def clean_price(value):
    if value:
        return float(value.replace('KSh', '').replace(',', '').strip())
    return None

def clean_rating(value):
    if value:
        return float(value.split()[0])
    return None

def clean_review_count(value):
    if value:
        return int(value.replace('(', '').replace(')', '').replace(',', ''))
    return None

def clean_int(value):
    try:
        return int(float(value))
    except:
        return None

def clean_float(value):
    try:
        return float(value)
    except:
        return None

class JumiaProductItem(scrapy.Item):
    title = scrapy.Field(output_processor=TakeFirst())
    current_price = scrapy.Field(
        input_processor=MapCompose(remove_tags, clean_price),
        output_processor=TakeFirst()
    )
    original_price = scrapy.Field(
        input_processor=MapCompose(remove_tags, clean_price),
        output_processor=TakeFirst()
    )
    discount = scrapy.Field(
        input_processor=MapCompose(remove_tags, lambda x: x.replace('%', '')),
        output_processor=TakeFirst()
    )
    rating = scrapy.Field(
        input_processor=MapCompose(clean_rating),
        output_processor=TakeFirst()
    )
    review_count = scrapy.Field(
        input_processor=MapCompose(clean_review_count),
        output_processor=TakeFirst()
    )
    seller = scrapy.Field(output_processor=TakeFirst())
    shipping = scrapy.Field(output_processor=TakeFirst())
    link = scrapy.Field(output_processor=TakeFirst())
    brand = scrapy.Field(output_processor=TakeFirst())
    description = scrapy.Field(output_processor=TakeFirst())
    scraped_at = scrapy.Field(output_processor=TakeFirst())
    image_urls = scrapy.Field()
    specifications = scrapy.Field()

    # Newly added fields
    ram_gb = scrapy.Field(
        input_processor=MapCompose(clean_int),
        output_processor=TakeFirst()
    )
    storage_gb = scrapy.Field(
        input_processor=MapCompose(clean_int),
        output_processor=TakeFirst()
    )
    screen_size_inches = scrapy.Field(
        input_processor=MapCompose(clean_float),
        output_processor=TakeFirst()
    )
    battery_mah = scrapy.Field(
        input_processor=MapCompose(clean_int),
        output_processor=TakeFirst()
    )
    camera_mp_main = scrapy.Field(
        input_processor=MapCompose(clean_int),
        output_processor=TakeFirst()
    )
    camera_mp_selfie = scrapy.Field(
        input_processor=MapCompose(clean_int),
        output_processor=TakeFirst()
    )
    network_type = scrapy.Field(output_processor=TakeFirst())
    has_dual_sim = scrapy.Field(output_processor=TakeFirst())
