# -*- coding: utf-8 -*-
import scrapy
import re
from scrapy_book.items import ScrapyBookItem


class SpiderForXPath(scrapy.Spider):
    name = 'spider_xpath_douban'

    def start_requests(self):
        for a in range(10):
            url = 'https://book.douban.com/top250?start={}'.format(a * 25)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        for book in response.xpath('//*[@id="content"]/div/div[1]/div/table'):
            item = ScrapyBookItem()

            # 书名和副标题
            title1 = book.xpath("./tr/td[2]/div[1]/a/@title").extract_first().replace('\n', '').strip()
            title2 = "无" if book.xpath("./tr/td[2]/div[1]/span/text()").extract_first() is None else book.xpath(
                "./tr/td[2]/div[1]/span/text()").extract_first().replace('\n', '').strip()
            item['title'] = title1 + "(" + title2 + ")"

            # 小图
            item['s_img'] = book.xpath("./tr/td[1]/a/img/@src").extract_first().replace('\n', '').strip()

            # 图书简介
            item['scrible'] = "无" if book.xpath("./tr/td[2]/p[2]/span/text()").extract_first() is None else book.xpath(
                "./tr/td[2]/p[2]/span/text()").extract_first().replace('\n', '').strip()

            # 作者、出版社、出版日期、价格
            book_info = book.xpath("./tr/td[2]/p[1]/text()").extract_first().replace('\n', '').strip().split('/')
            if len(book_info) == 4:
                item['author'] = book_info[0].strip()
                item['publisher'] = book_info[1].strip()
                item['pub_date'] = book_info[2].strip()
                item['price'] = book_info[3].strip()
            else:
                item['author'] = item['publisher'] = item['pub_date'] = item['price'] = "未知"

            # 评分
            item['score'] = book.xpath("./tr/td[2]/div[2]/span[2]/text()").extract_first().strip()

            # 评价人数
            num_text = book.xpath("./tr/td[2]/div[2]/span[3]/text()").extract_first()
            if num_text:
                item['num'] = re.sub(r'\D', '', num_text)  # 只提取数字部分
            else:
                item['num'] = "未知"

            # 详情页URL
            sub_url = book.xpath("./tr/td[2]/div/a/@href").extract_first().replace('\n', '').strip()

            yield scrapy.Request(url=sub_url, callback=self.parse_second, meta={"item": item})

    def parse_second(self, response):
        item = response.meta["item"]

        # 修改后的作者提取逻辑
        author = response.xpath('//span[contains(text(),"作者")]/following-sibling::a/text()').extract_first()
        if author:
            # 清理掉作者字段中的换行符和空格
            item["author"] = re.sub(r'\s+', ' ', author).strip()
        else:
            item["author"] = "未知"

        # 出版社
        publisher = response.xpath('//span[text()="出版社:"]/following-sibling::a/text()').extract_first()
        item["publisher"] = publisher.strip() if publisher else "未知"

        # 出版日期
        pub_date = response.xpath('//span[text()="出版年:"]/following-sibling::text()').extract_first()
        item["pub_date"] = pub_date.strip() if pub_date else "未知"

        # 价格
        price = response.xpath('//span[text()="定价:"]/following-sibling::text()').extract_first()
        item["price"] = price.strip() if price else "未知"

        yield item
