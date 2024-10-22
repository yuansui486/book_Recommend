# -*- coding: utf-8 -*-
import scrapy

from scrapy_book.items import ScrapyBookItem


class SpiderForCSS(scrapy.Spider):
    name = 'spider_css_douban'

    def start_requests(self):
        for a in range(10):
            url = 'https://book.douban.com/top250?start={}'.format(a * 25)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        items = []
        for book in response.css('tr.item'):
            item = ScrapyBookItem()
            item['title'] = book.css('div.pl2 a::text').extract_first().replace('\n', '').strip()
            item['score'] = book.css('div.star.clearfix span.rating_nums::text').extract_first().replace('\n',
                                                                                                         '').strip()
            item['scrible'] = book.css('p.quote span.inq::text').extract_first().replace('\n', '').strip()
            item['num'] = book.css('div.star.clearfix span.pl::text').extract_first().strip("(").strip(")").replace(
                '\n', '').strip()
            item['img'] = book.css('a.nbg img').xpath('@src').extract_first().replace('\n', '').strip()
            items.append(item)

        print(items)
        return items
