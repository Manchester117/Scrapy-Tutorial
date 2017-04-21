# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import codecs
import json
# import pymysql.cursors
import MySQLdb
import MySQLdb.cursors
from scrapy.exporters import JsonItemExporter
from scrapy.pipelines.images import ImagesPipeline
from twisted.enterprise import adbapi


class ArticleSpiderPipeline(object):
    def process_item(self, item, spider):
        return item


class JsonWithEncodingPipeline(object):
    # 自定义将爬取的内容保存成JSON格式,并写入文件
    def __init__(self):
        self.file = codecs.open('article.json', 'w', encoding="utf-8")

    def process_item(self, item, spider):
        lines = json.dumps(dict(item), ensure_ascii=False) + '\n'
        self.file.write(lines)
        return item

    def spider_closed(self, spider):
        self.file.close()


class JsonExporterPipeline(object):
    def __init__(self):
        self.file = open('articleexport.json', 'wb')
        self.exporter = JsonItemExporter(self.file, encoding='utf-8', ensure_ascii=False)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class ArticleImagePipeline(ImagesPipeline):
    # 获取图片存放路径
    # 实际上,图片的本地存放路径是在results当中
    def item_completed(self, results, item, info):
        if "front_image_url" in item:
            image_file_path = ""
            for ok, value in results:
                image_file_path = value['path']
            item['front_image_path'] = image_file_path
        return item


class MysqlPipeline(object):
    def __init__(self):
        # 使用MySQLdb
        self.conn = MySQLdb.connect(host='127.0.0.1',
                                    user='root',
                                    passwd='123456',
                                    db='article_spider',
                                    charset='utf8',
                                    use_unicode=True)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        insert_sql = """
            INSERT INTO jobbole_article(title, url, create_date, fav_nums)
            VALUES (%s, %s, %s, %s)
        """
        # 这里报异常: TypeError: sequence item 0: expected str instance, bytes found
        # 解决方案: 换用1.3.7的mysqlclient
        self.cursor.execute(insert_sql, (item["title"], item["url"], item["create_date"], item["fav_nums"]))
        self.conn.commit()

    # def __init__(self):
    #     # 使用PyMySQL
    #     # 建立数据库连接
    #     self.conn = pymysql.connect(host='127.0.0.1',
    #                                 port=3306,
    #                                 user='root',
    #                                 password='123456',
    #                                 db='article_spider',
    #                                 charset='UTF8')
    #     self.cursor = self.conn.cursor()
    #
    # def process_item(self, item, spider):
    #     insert_sql = """
    #         INSERT INTO jobbole_article(title, url, create_date, fav_nums)
    #         VALUES (%s, %s, %s, %s)
    #     """
    #     self.cursor.execute(insert_sql, (item['title'], item['url'], item['create_date'], item['fav_nums']))
    #     self.conn.commit()


class MysqlTwistPipeline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        dbparams = dict(
            host=settings["MYSQL_HOST"],
            db=settings["MYSQL_DBNAME"],
            user=settings["MYSQL_USER"],
            passwd=settings["MYSQL_PASSWORD"],
            charset='UTF8',
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=True,
        )
        dbpool = adbapi.ConnectionPool("MySQLdb", **dbparams)

        return cls(dbpool)

    def process_item(self, item, spider):
        # 使用twisted将mysql插入变成异步执行
        query = self.dbpool.runInteraction(self.do_insert, item)
        # 这里视频源码有错误,不应该是addErrorback,而是addErrback
        # query.addErrorback(self.handle_error)
        query.addErrback(self.handle_error, item, spider)

    def handle_error(self, failure, item, spider):
        print(failure)

    # def do_insert(self, cursor, item):
    #     # 具体的插入方法
    #     insert_sql = """
    #         INSERT INTO jobbole_article(title, url, create_date, fav_nums)
    #         VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE content=VALUES(fav_nums)
    #     """
    #     cursor.execute(insert_sql, (item['title'], item['url'], item['create_date'], item['fav_nums']))

    def do_insert(self, cursor, item):
        # 具体的插入方法
        # 根据不同的item,构建不同的SQL语句并插入到mysql中
        insert_sql, params = item.get_insert_sql()
        print(insert_sql, params)
        cursor.execute(insert_sql, params)

