# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class ScrapyBookItem(scrapy.Item):
    # 图书id、书名、作者、出版日期、出版社、小图、中图、大图、类别id、类别名称、单价
    isbn = scrapy.Field()
    # 书名
    title = scrapy.Field()
    # 作者
    author = scrapy.Field()
    # 出版日期
    pub_date = scrapy.Field()
    # 出版社
    publisher = scrapy.Field()
    # 小图
    s_img = scrapy.Field()
    # 中图
    m_img = scrapy.Field()
    # 大图
    b_img = scrapy.Field()
    # 类别id
    category_id = scrapy.Field()
    # 类别名称
    category = scrapy.Field()
    # 单价
    price = scrapy.Field()
    # 图书一句话描述
    scrible = scrapy.Field()

    sub_url = scrapy.Field()
    img = scrapy.Field()
    score = scrapy.Field()
    num = scrapy.Field()
