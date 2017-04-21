# -*- coding: utf-8 -*-
import re
import json
import scrapy
import datetime
from Article_Spider.items import ZhihuQuestionItem, ZhihuAnswerItem
from scrapy.loader import ItemLoader

try:
    import urlparse as parse    # Python 2.7
except:
    from urllib import parse    # Python 3.5


class ZhihuSpider(scrapy.Spider):
    name = "zhihu"
    allowed_domains = ["www.zhihu.com"]
    start_urls = ['http://www.zhihu.com/']

    headers = {
        'Host': 'www.zhihu.com',
        'Referer': 'https://www.zhihu.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
        # 'X-Requested-With': 'XMLHttpRequest',
        # 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'X-Xsrftoken': '',
    }

    start_answer_url = """https://www.zhihu.com/api/v4/questions/{0}/answers?include=data%5B*%5D.is_normal%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Cmark_infos%2Ccreated_time%2Cupdated_time%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cupvoted_followees%3Bdata%5B*%5D.author.badge%5B%3F(type%3Dbest_answerer)%5D.topics&offset={1}&limit={2}&sort_by=default"""

    def parse(self, response):
        all_urls = response.css('a::attr(href)').extract()                                  # 提取登录之后的url
        all_urls = [parse.urljoin(response.url, url) for url in all_urls]                   # 将分段的url和主域名进行拼接
        all_urls = filter(lambda x: True if x.startswith('https') else False, all_urls)     # 只获取到开头为https的url
        for url in all_urls:
            match_obj = re.match("(.*zhihu.com/question/(\d+))(/|$).*", url)
            # if语句中是递归调用
            if match_obj:
                request_url = match_obj.group(1)
                # 回调parse_question
                yield scrapy.Request(request_url, headers=self.headers, callback=self.parse_question)
                # break
            else:
                yield scrapy.Request(url, headers=self.headers, callback=self.parse)
                # pass
        pass

    def parse_question(self, response):
        if "QuestionHeader-title" in response.text:
            match_obj = re.match("(.*zhihu.com/question/(\d+))(/|$).*", response.url)
            question_id = ''
            if match_obj:
                question_id = int(match_obj.group(2))
            # 处理新版本
            item_loader = ItemLoader(item=ZhihuQuestionItem(), response=response)
            item_loader.add_css('title', 'h1.QuestionHeader-title::text')
            item_loader.add_css('content', '.QuestionHeader-detail')
            item_loader.add_value('url', response.url)
            item_loader.add_value('zhihu_id', question_id)
            item_loader.add_css('answer_num', '.List-headerText span::text')
            item_loader.add_css('comments_num', '.QuestionHeader-actions button::text')
            item_loader.add_css('watch_user_num', '.NumberBoard-value::text')
            item_loader.add_css('topics', '.QuestionHeader-topics .Popover div::text')

            question_item = item_loader.load_item()
        else:
            match_obj = re.match("(.*zhihu.com/question/(\d+))(/|$).*", response.url)
            question_id = ''
            if match_obj:
                question_id = int(match_obj.group(2))
            # 处理新版本
            item_loader = ItemLoader(item=ZhihuQuestionItem(), response=response)
            # item_loader.add_css("title", ".zh-question-title h2 a::text")
            item_loader.add_xpath("title", "//*[@id='zh-question-title']/h2/a/text()|//*[@id='zh-question-title']/h2/span/text()")
            item_loader.add_css("content", "#zh-question-detail")
            item_loader.add_value("url", response.url)
            item_loader.add_value("zhihu_id", question_id)
            item_loader.add_css("answer_num", "#zh-question-answer-num::text")
            item_loader.add_css("comments_num", "#zh-question-meta-wrap a[name='addcomment']::text")
            # item_loader.add_css("watch_user_num", "#zh-question-side-header-wrap::text")
            item_loader.add_xpath("watch_user_num", "//*[@id='zh-question-side-header-wrap']/text()|//*[@class='zh-question-followers-sidebar']/div/a/strong/text()")
            item_loader.add_css("topics", ".zm-tag-editor-labels a::text")

            question_item = item_loader.load_item()

        yield scrapy.Request(url=self.start_answer_url.format(question_id, 3, 20), headers=self.headers, callback=self.parse_answer)
        yield question_item

    def parse_answer(self, response):
        # 获取问题回答的json串
        ans_json = json.loads(response.text)
        is_end = ans_json["paging"]["is_end"]
        next_url = ans_json["paging"]["next"]

        for answer in ans_json["data"]:
            answer_item = ZhihuAnswerItem()
            answer_item["zhihu_id"] = answer["id"]
            answer_item["url"] = answer["url"]
            answer_item["question_id"] = answer["question"]["id"]
            answer_item["author_id"] = answer["author"]["id"] if "id" in answer["author"] else None
            answer_item["content"] = answer["content"] if "content" in answer else None
            answer_item["praise_num"] = answer["voteup_count"]
            answer_item["comments_num"] = answer["comment_count"]
            answer_item["create_time"] = answer["created_time"]
            answer_item["update_time"] = answer["updated_time"]
            answer_item["crawl_time"] = datetime.datetime.now()

            yield answer_item

        if not is_end:
            yield scrapy.Request(next_url, headers=self.headers, callback=self.parse_answer)

    def start_requests(self):
        # 请求知乎的登录页面
        return [scrapy.Request('https://www.zhihu.com/#signin', headers=self.headers, callback=self.login)]

    def login(self, response):
        # 获取知乎的xsrf,并且请求知乎的验证码图片
        # 从start_requests中的Request中获取请求返回值
        response_text = response.text
        match_obj = re.match('.*name="_xsrf" value="(.*?)"', response_text, re.DOTALL)      # re.DOTALL是在整个HTML中进行匹配,否则只匹配第一行.
        xsrf = ''
        if match_obj:
            xsrf = match_obj.group(1)

        if xsrf:
            self.headers['X-Xsrftoken'] = xsrf
            post_data = {
                "_xsrf": xsrf,
                # 需要添加用户名密码
                "phone_num": '',
                "password": '',
                # "captcha_type": 'cn'
            }

            import time
            t = str(int(time.time() * 1000))
            captcha_url = "https://www.zhihu.com/captcha.gif?r={0}&type=login".format(t)
            yield scrapy.Request(captcha_url, headers=self.headers, meta={'post_data': post_data}, callback=self.login_after_captcha)

    # def login_after_captcha(self, response):
    #     # 中文倒立,获取验证码图片并获取点击坐标,进行登录.
    #     with open("captcha.jpg", "wb") as f:
    #         f.write(response.body)      # 获取验证码图片,并写入到本地,注意,需要用response.body来获取图片
    #         f.close()
    #
    #     from PIL import Image
    #     try:
    #         image = Image.open('captcha.jpg').convert('RGB')
    #         width, height = image.size
    #         image.thumbnail((width//2, height//2), Image.ANTIALIAS)
    #         image.save('captcha_new.jpg')
    #         image.close()
    #     except:
    #         pass
    #
    #     captcha_before = '{"img_size":[200,44],"input_points":['
    #     captcha_after = ']}'
    #     captcha_input = input("输入文字坐标(格式:[15.29,28.45],[115.29,26.45]):\n>")
    #     captcha_list = [captcha_before, captcha_input, captcha_after]
    #     captcha = ''.join(captcha_list)
    #
    #     post_url = "https://www.zhihu.com/login/phone_num"
    #     post_data = response.meta.get('post_data', {})
    #     post_data['captcha'] = captcha
    #     return [scrapy.FormRequest(
    #         url=post_url,
    #         formdata=post_data,
    #         headers=headers,
    #         cookies=cookies,
    #         callback=self.check_login
    #     )]

    def login_after_captcha(self, response):
        # 普通验证码
        with open("captcha.jpg", "wb") as f:
            f.write(response.body)      # 获取验证码图片,并写入到本地,注意,需要用response.body来获取图片
            f.close()

        from PIL import Image
        try:
            image = Image.open('captcha.jpg').convert('RGB')
            image.show()
            image.close()
        except:
            pass

        captcha = input("输入验证码:\n>")

        post_url = "https://www.zhihu.com/login/phone_num"
        post_data = response.meta.get('post_data', {})
        post_data['captcha'] = captcha

        # headers['X-Requested-With'] = 'XMLHttpRequest'
        # headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'

        # Post请求
        return [scrapy.FormRequest(
            url=post_url,
            formdata=post_data,
            headers=self.headers,
            callback=self.check_login
        )]

    def check_login(self, response):
        # 验证登录后的结果
        text_json = json.loads(response.text)
        if "msg" in text_json and text_json['msg'] == '登录成功':
            # 如果登录成功则使用以下语句来调用parse方法(注意!如果此处没有使用call_back回调函数,则代码会自动进入到parse函数中)
            for url in self.start_urls:
                yield scrapy.Request(url, dont_filter=True, headers=self.headers)