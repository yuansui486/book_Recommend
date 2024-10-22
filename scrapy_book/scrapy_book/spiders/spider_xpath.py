# -*- coding: utf-8 -*-
import scrapy

from scrapy_book.items import ScrapyBookItem


class SpiderForXPath(scrapy.Spider):
    name = 'spider_xpath_douban'

    def start_requests(self):
        for a in range(10):
            url = 'https://book.douban.com/top250?start={}'.format(a * 25)
            yield scrapy.Request(url=url, callback=self.parse)


    def parse(self, response):
        items = []

        for book in response.xpath('//*[@id="content"]/div/div[1]/div/table'):
            item = ScrapyBookItem()

            title1 = book.xpath("./tr/td[2]/div[1]/a/@title").extract_first().replace('\n', '').strip()
            title2 = "无" if book.xpath("./tr/td[2]/div[1]/span/text()").extract_first() == None else book.xpath(
                "./tr/td[2]/div[1]/span/text()").extract_first().replace('\n', '').strip()
            item['title'] = title1 + "(" + title2 + ")"
            item['s_img'] = book.xpath("./tr/td[1]/a/img/@src").extract_first().replace('\n', '').strip()
            item['scrible'] = "无" if book.xpath("./tr/td[2]/p[2]/span/text()").extract_first() == None else book.xpath(
                "./tr/td[2]/p[2]/span/text()").extract_first().replace('\n', '').strip()
            sub_url = book.xpath("./tr/td[2]/div/a/@href").extract_first().replace('\n', '').strip()
            items.append(item)

            # meta={"item":item} 传递item引用SinaItem对象
            yield scrapy.Request(url=sub_url, callback=self.parse_second, meta={"item": item})

    def parse_second(self, response):
        item = response.meta["item"]
        item["category_id"] = ""
        item["category"] = ""
        # book = response.xpath('//div[@class="indent"]/div').extract_first()
        # item["author"] = book.xpath("./div[1]/a[1]/text()").extract_first().replace('\n', '').strip()
        # item["publisher"] = book.xpath("./div[1]/a/@href").extract_first().replace('\n', '').strip()
        # item["pub_date"] = book.xpath("./div[1]/a/@href").extract_first().replace('\n', '').strip()
        # item["price"] = book.xpath("./div[1]/a/@href").extract_first().replace('\n', '').strip()
        # item["m_img"] = book.xpath("./div[1]/a/@href").extract_first().replace('\n', '').strip()
        # item["b_img"] = book.xpath("./div[1]/a/@href").extract_first().replace('\n', '').strip()
        # item["isbn"] = book.xpath("./div[2]/a[1]/text()").extract_first().replace('\n', '').strip()
        yield item