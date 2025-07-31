# pipelines.py
import psycopg2
import json
import logging
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

class PostgresPipeline:
    def __init__(self):
        self.conn = None
        self.cur = None

    def open_spider(self, spider):
        try:
            self.conn = psycopg2.connect(
                dbname="jumia_data",
                user="postgres",
                password="Ndegwa",
                host="localhost",
                port="5432"
            )
            self.cur = self.conn.cursor()

            self.cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'products'
                )
            """)
            if not self.cur.fetchone()[0]:
                raise Exception("Products table doesn't exist")

            spider.logger.info("✅ Successfully connected to PostgreSQL")
        except Exception as e:
            spider.logger.error(f"❌ Database connection failed: {e}")
            raise

    def process_item(self, item, spider):
        try:
            item = {k: v if v not in ('', [], {}) else None for k, v in item.items()}

            if not item.get('title') or not item.get('link'):
                raise DropItem("Missing required fields (title or link)")

            self.cur.execute("""
                INSERT INTO products (
                    title, current_price, original_price, discount, 
                    rating, review_count, seller, shipping, 
                    link, brand, description, scraped_at, image_urls
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (link) DO UPDATE SET
                    current_price = EXCLUDED.current_price,
                    original_price = EXCLUDED.original_price,
                    discount = EXCLUDED.discount,
                    rating = EXCLUDED.rating,
                    review_count = EXCLUDED.review_count,
                    scraped_at = EXCLUDED.scraped_at
                RETURNING id
            """, (
                item.get('title'),
                float(item['current_price']) if item.get('current_price') else None,
                float(item['original_price']) if item.get('original_price') else None,
                float(item['discount']) if item.get('discount') else None,
                float(item['rating']) if item.get('rating') else None,
                int(item['review_count']) if item.get('review_count') else None,
                item.get('seller'),
                item.get('shipping'),
                item['link'],
                item.get('brand'),
                item.get('description'),
                item.get('scraped_at'),
                json.dumps(item.get('image_urls', []))
            ))

            product_id = self.cur.fetchone()[0]

            if isinstance(item.get('specifications'), list):
                self.cur.execute("DELETE FROM specifications WHERE product_id = %s", (product_id,))

                for spec in item['specifications']:
                    try:
                        self.cur.execute("""
                            INSERT INTO specifications 
                            (product_id, category, spec_type, spec_key, spec_value)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            product_id,
                            spec.get('category'),
                            spec.get('spec_type'),
                            spec.get('spec_key'),
                            str(spec.get('spec_value', ''))[:500]
                        ))
                    except Exception as spec_error:
                        spider.logger.warning(f"⚠️ Failed to insert spec: {spec}. Error: {spec_error}")
                        continue

            self.conn.commit()
            spider.logger.info(f"✅ Successfully processed product: {item['title'][:50]}... (ID: {product_id})")
            return item

        except psycopg2.Error as e:
            spider.logger.error(f"❌ Database error: {e.pgerror}")
            self.conn.rollback()
            raise DropItem(f"Database error: {e.pgerror}")
        except Exception as e:
            spider.logger.error(f"❌ Error processing item: {str(e)}")
            self.conn.rollback()
            raise DropItem(f"Processing error: {str(e)}")

    def close_spider(self, spider):
        try:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()
            spider.logger.info("✅ Database connection closed")
        except Exception as e:
            spider.logger.error(f"❌ Error closing connection: {e}")


