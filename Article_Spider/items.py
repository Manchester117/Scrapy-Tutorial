# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html
import re
import scrapy
import datetime

from Article_Spider.settings import SQL_DATETIME_FORMAT
from Article_Spider.utils.common import extract_num
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Join
from w3lib.html import remove_tags


class ArticleSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


# class JobBoleArticleItem(scrapy.Item):
#     title = scrapy.Field()
#     create_date = scrapy.Field()
#     url = scrapy.Field()
#     url_object_id = scrapy.Field()
#     front_image_url = scrapy.Field()
#     front_image_path = scrapy.Field()
#     praise_nums = scrapy.Field()
#     comment_nums = scrapy.Field()
#     fav_nums = scrapy.Field()
#     tags = scrapy.Field()
#     content = scrapy.Field()


def add_jobbole(value):
    return value + '-jobbole'


def date_convert(value):
    value = value.strip().replace("·", "").strip()
    try:
        create_date = datetime.datetime.strptime(value, '%Y/%m/%d').date()
    except Exception as e:
        create_date = datetime.datetime.now().date()
    return create_date


def get_nums(value):
    match_re = re.match(".*?(\d+).*", value)
    if match_re:
        nums = int(match_re.group(1))
    else:
        nums = 0
    return nums


def remove_comment_tags(value):
    if "评论" in value:
        return ""
    else:
        return value


def return_value(value):
    return value


class ArticleItemLoader(ItemLoader):
    # 自定义ItemLoader
    # 使用TakeFirst方法,只取数组元素中的第一个
    default_output_processor = TakeFirst()


class JobBoleArticleItem(scrapy.Item):
    title = scrapy.Field(
        # input_processor=MapCompose(add_jobbole)
    )
    create_date = scrapy.Field(
        input_processor=MapCompose(date_convert)
    )
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    front_image_url = scrapy.Field(
        output_processor=MapCompose(return_value)
    )
    front_image_path = scrapy.Field()
    praise_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    comment_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    fav_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    tags = scrapy.Field(
        input_processor=MapCompose(remove_comment_tags),
        output_processor=Join(",")
    )
    content = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            INSERT INTO
              jobbole_article(title, url, create_date, fav_nums, front_image_url, front_image_path, praise_nums,
              comment_nums, tags, content)
            VALUES
              (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE content=VALUES(fav_nums)
        """

        front_image_url = ''
        if self["front_image_url"]:
            front_image_url = self["front_image_url"][0]

        params = (self["title"],
                  self["url"],
                  self["create_date"],
                  self["fav_nums"],
                  front_image_url,
                  self["front_image_path"],
                  self["praise_nums"],
                  self["comment_nums"],
                  self["tags"],
                  self["content"])

        return insert_sql, params


class ZhihuQuestionItem(scrapy.Item):
    zhihu_id = scrapy.Field()
    topics = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    answer_num = scrapy.Field()
    comments_num = scrapy.Field()
    watch_user_num = scrapy.Field()
    click_num = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            INSERT INTO 
              zhihu_question(zhihu_id, topics, url, title, content, answer_num, comments_num, watch_user_num, click_num, crawl_time)
            VALUES
              (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
              content=VALUES(content), answer_num=VALUES(answer_num), comments_num=VALUES(comments_num), 
              watch_user_num=VALUES(watch_user_num), click_num=VALUES(click_num)
        """

        zhihu_id = self['zhihu_id'][0]
        topics = ','.join(self['topics'])
        url = self['url'][0]
        title = ''.join(self['title'])
        content = ''.join(self['content'])
        answer_num = extract_num(''.join(self['answer_num']))
        comments_num = extract_num(''.join(self['comments_num']))

        if len(self['watch_user_num']) == 2:
            watch_user_num = int(self['watch_user_num'][0])
            click_num = int(self['watch_user_num'][0])
        else:
            watch_user_num = int(self['watch_user_num'][0])
            click_num = 0

        crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)

        params = (zhihu_id, topics, url, title, content, answer_num, comments_num, watch_user_num, click_num, crawl_time)

        return insert_sql, params


class ZhihuAnswerItem(scrapy.Item):
    zhihu_id = scrapy.Field()
    url = scrapy.Field()
    question_id = scrapy.Field()
    author_id = scrapy.Field()
    content = scrapy.Field()
    praise_num = scrapy.Field()
    comments_num = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            INSERT INTO 
              zhihu_answer(zhihu_id, url, question_id, author_id, content, praise_num, comments_num, create_time, update_time, crawl_time)
            VALUES
              (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              content=VALUES(content), comments_num=VALUES(comments_num), praise_num=VALUES(praise_num), update_time=VALUES(update_time)
        """

        create_time = datetime.datetime.fromtimestamp(self['create_time']).strftime(SQL_DATETIME_FORMAT)
        update_time = datetime.datetime.fromtimestamp(self['update_time']).strftime(SQL_DATETIME_FORMAT)

        params = (
            self['zhihu_id'],
            self['url'],
            self['question_id'],
            self['author_id'],
            self['content'],
            self['praise_num'],
            self['comments_num'],
            create_time,
            update_time,
            self['crawl_time'].strftime(SQL_DATETIME_FORMAT),
        )

        return insert_sql, params


def replace_splash(value):
    return value.replace("/", "")


def handle_strip(value):
    return value.strip()


def handle_jobaddr(value):
    addr_list = value.split("\n")
    addr_list = [item.strip() for item in addr_list if item.strip() != '查看地图']
    return "".join(addr_list)


class LagouJobItemLoader(ItemLoader):
    # 自定义itemLoader
    default_output_processor = TakeFirst()


class LagouJobItem(scrapy.Item):
    title = scrapy.Field()
    url = scrapy.Field()
    salary = scrapy.Field()
    job_city = scrapy.Field(
        input_processor=MapCompose(replace_splash)
    )
    work_years = scrapy.Field(
        input_processor=MapCompose(replace_splash)
    )
    degree_need = scrapy.Field(
        input_processor=MapCompose(replace_splash)
    )
    job_type = scrapy.Field()
    publish_time = scrapy.Field()
    job_advantage = scrapy.Field()
    job_desc = scrapy.Field(
        input_processor=MapCompose(handle_strip)
    )
    job_addr = scrapy.Field(
        input_processor=MapCompose(remove_tags, handle_jobaddr)
    )
    company_name = scrapy.Field(
        input_processor=MapCompose(handle_strip)
    )
    company_url = scrapy.Field()
    crawl_time = scrapy.Field()
    crawl_update_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            INSERT INTO 
                lagou_job(title, url, salary, job_city, work_years, degree_need, job_type, publish_time, job_advantage, 
                job_desc, job_addr, company_url, company_name, job_id)
            VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            ON DUPLICATE KEY UPDATE job_desc=VALUES(job_desc)
        """
        job_id = extract_num(self['url'])
        params = (
            self["title"],
            self["url"],
            self["salary"],
            self["job_city"],
            self["work_years"],
            self["degree_need"],
            self["job_type"],
            self["publish_time"],
            self["job_advantage"],
            self["job_desc"],
            self["job_addr"],
            self["company_url"],
            self["company_name"],
            job_id
        )

        return insert_sql, params
