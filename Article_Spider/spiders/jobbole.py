# -*- coding: utf-8 -*-
import datetime
import scrapy
import re
from scrapy.http import Request
from urllib import parse    # 实现URL拼接
from Article_Spider.items import JobBoleArticleItem, ArticleItemLoader
from Article_Spider.utils.common import get_md5
from scrapy.loader import ItemLoader


class JobboleSpider(scrapy.Spider):
    name = "jobbole"
    allowed_domains = ["blog.jobbole.com"]
    start_urls = ['http://blog.jobbole.com/all-posts/']

    def parse(self, response):
        post_urls = response.css("#archive .floated-thumb .post-thumb a")
        for post_url in post_urls:
            image_url = post_url.css("img::attr('src')").extract_first("")
            post_url = post_url.css("::attr(href)").extract_first("")
            # 使用yield将回调方法交给scrapy调用.并且获取URL后,回调文章的具体抓取方法.
            # urllib中的parse.urljoin()方法可以自动匹配URL.
            yield Request(url=parse.urljoin(response.url, post_url), meta={"front_image_url": image_url}, callback=self.parse_detail)

        next_url = response.css('.next.page-numbers::attr(href)').extract_first("")
        if next_url:
            yield Request(url=parse.urljoin(response.url, next_url), callback=self.parse)

    def parse_detail(self, response):
        # article_item = JobBoleArticleItem()

        # 以xpath方式获取数据
        # title = response.xpath("//div[@class='entry-header']/h1/text()")
        # create_date = response.xpath("//p[@class='entry-meta-hide-on-mobile']/text()").extract()[0].strip().replace("·", "").strip()
        # praise_nums = response.xpath("//span[contains(@class, 'vote-post-up')]/h10/text()").extract()[0]
        # if praise_nums:
        #     praise_nums = int(praise_nums)
        # else:
        #     praise_nums = 0
        # fav_nums = response.xpath("//span[contains(@class, 'bookmark-btn')]/text()").extract()[0]
        # match_re = re.match(".*(\d+).*", fav_nums)
        # if match_re:
        #     fav_nums = int(match_re.group(1))
        # else:
        #     fav_nums = 0
        #
        # comment_nums = response.xpath("//a[@href='#article-comment']/text()").extract()[0]
        # match_re = re.match(".*(\d+).*", comment_nums)
        # if match_re:
        #     comment_nums = int(match_re.group(1))
        # else:
        #     comment_nums = 0
        # content = response.xpath("//div[@class='entry']").extract()
        # tag_list = response.xpath("//p[@class='entry-meta-hide-on-mobile']/a/text()").extract()
        # tag_list = [element for element in tag_list if not element.strip().endswith('评论')]
        # tags = ','.join(tag_list)

        # 以css方式获取数据
        # front_image_url = response.meta.get("front_image_url", "")      # 封面图
        # title = response.css('.entry-header h1::text').extract()
        # create_date = response.css('.entry-meta-hide-on-mobile::text').extract()[0].strip().replace('·','').strip()
        # praise_nums = response.css('.vote-post-up h10::text').extract()[0]
        # if praise_nums:
        #     praise_nums = int(praise_nums)
        # else:
        #     praise_nums = 0
        # fav_nums = response.css('.bookmark-btn::text').extract()[0]
        # match_re = re.match(".*(\d+).*", fav_nums)
        # if match_re:
        #     fav_nums = int(match_re.group(1))
        # else:
        #     fav_nums = 0
        # comment_nums = response.css("a[href='#article-comment'] span::text").extract()[0]
        # match_re = re.match(".*(\d+).*", comment_nums)
        # if match_re:
        #     comment_nums = int(match_re.group(1))
        # else:
        #     comment_nums = 0
        # content = response.css('div.entry').extract()[0]
        # tag_list = response.css('.entry-meta-hide-on-mobile a::text').extract()
        # tag_list = [element for element in tag_list if not element.strip().endswith('评论')]
        # tags = ','.join(tag_list)
        #
        # article_item["url_object_id"] = get_md5(response.url)
        # article_item["title"] = title
        # article_item["url"] = response.url      #可以通过response.url获取要爬取的URL
        # try:
        #     create_date = datetime.datetime.strptime(create_date, '%Y/%m/%d').date()
        # except Exception as e:
        #     create_date = datetime.datetime().now().date()
        # article_item["create_date"] = create_date
        # article_item["front_image_url"] = [front_image_url]     # 注意!如果存放URL(抓取数据的URL),需要用列表存放
        # article_item["praise_nums"] = praise_nums
        # article_item["comment_nums"] = comment_nums
        # article_item["fav_nums"] = fav_nums
        # article_item["tags"] = tags
        # article_item["content"] = content

        # 需要使用ItemLoader来简化数据提取过程
        front_image_url = response.meta.get("front_image_url", "")  # 封面图
        # 使用自定义的ItemLoader
        item_loader = ArticleItemLoader(item=JobBoleArticleItem(), response=response)
        item_loader.add_css('title', '.entry-header h1::text')
        item_loader.add_value('url', response.url)
        item_loader.add_value('url_object_id', get_md5(response.url))
        item_loader.add_css('create_date', 'p.entry-meta-hide-on-mobile::text')
        item_loader.add_value('front_image_url', [front_image_url])
        item_loader.add_css('praise_nums', '.vote-post-up h10::text')
        item_loader.add_css('comment_nums', "a[href='#article-comment'] span::text")
        item_loader.add_css('fav_nums', '.bookmark-btn::text')
        item_loader.add_css('tags', '.entry-meta-hide-on-mobile a::text')
        item_loader.add_css('content', 'div.entry')

        article_item = item_loader.load_item()

        # yield可以将article_item传递到piplines中.
        yield article_item