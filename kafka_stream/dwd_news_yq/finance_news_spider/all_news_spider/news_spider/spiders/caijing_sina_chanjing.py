# -*- coding: utf-8 -*-
import scrapy
from scrapy_redis.spiders import RedisSpider
import os
import sys
import htmlparser
from urllib.parse import urljoin
import json
from scrapy.utils.request import request_fingerprint
import redis
import re
import time
import datetime
from spider_util.utils.util import add_uuid, local_timestamp
from spider_util.utils.download_util import dow_img_acc, parse_main
from scrapy.conf import settings


class MySpider(RedisSpider):
    name = 'caijing_sina_chanjing'
    allowed_domains = ['finance.sina.com.cn']
    ori_path = settings.get('ORI_PATH')
    encoding = "utf-8"
    start_urls = [
        "http://feed.mix.sina.com.cn/api/roll/get?pageid=164&lid=1693&num=10&page=1&callback=feedCardJsonpCallback",
        "http://feed.mix.sina.com.cn/api/roll/get?pageid=164&lid=1693&num=10&page=2&callback=feedCardJsonpCallback",
        "http://feed.mix.sina.com.cn/api/roll/get?pageid=164&lid=1693&num=10&page=3&callback=feedCardJsonpCallback",
        "http://feed.mix.sina.com.cn/api/roll/get?pageid=164&lid=1693&num=10&page=4&callback=feedCardJsonpCallback",
        "http://feed.mix.sina.com.cn/api/roll/get?pageid=164&lid=1693&num=10&page=5&callback=feedCardJsonpCallback",
    ]
    headers = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:55.0) Gecko/20100101 Firefox/55.0'
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, headers=self.headers, dont_filter=True)

    def parse(self, response):
        start_url = response.url
        try:
            p_data = re.findall('({"result.*]}})',response.body.decode(self.encoding))[0]
            data = json.loads(p_data)
        except Exception as e:
            print('response failed %s' % e)
            return
        org_list = data.get('result').get('data')
        for org in org_list:
            if org:
                title = org.get('title')
                ctime = org.get('ctime')
                org_url = org.get('url')
                if title:
                    url = urljoin(start_url,org_url)
                    print(url)
                    ctime = int(ctime)
                    c_time = time.localtime(ctime)
                    c_time = time.strftime("%Y-%m-%d",c_time)
                    item = {'ctime': ctime, 'title': title}
                    print(item)
                    yield scrapy.Request(url, callback=self.detail_parse, meta={'item': item,"c_time":c_time}, headers=self.headers, dont_filter=True)

    def detail_parse(self, response):
        item = response.meta['item']
        try:
            data = htmlparser.Parser(response.body.decode(self.encoding))
        except Exception as e:
            print('second response failed %s' % e)
            return
        c_time = response.meta['c_time']
        url = response.url
        contents = []  # 全部的文本内容
        content_list = data.xpathall('''//div[@id='artibody']/p//text()''')
        for con in content_list:
            con = con.text().strip()
            if con:
                contents.append(con)
        content_x = data.xpath('''//div[@id='artibody']''').data
        content_xml = content_x
        label = {}
        img_list = data.xpathall('''//div[@id='artibody']//img''')
        if img_list:
            for count, image in enumerate(img_list):
                image_dict = {}
                image_url = image.xpath('//@src').text().strip()
                if image_url:
                    image_url = urljoin(url, image_url)
                    node = '#image{}#'.format(count)
                    file_name = image_url.split('/')[-1]
                    image_dict['url'] = image_url
                    image_dict['name'] = ''
                    image_dict['file_name'] = file_name
                    label[node] = image_dict

        table_list = data.xpathall('''//div[@id='artibody']//table''')
        if table_list:
            for count, table in enumerate(table_list):
                table_dict = {}
                node = "#table{}#".format(count)
                table_sele = table.data
                table_dict['table_xml'] = table_sele
                node_p = "<p>" + node + "</p>"
                content_x = content_x.replace(table_sele, node_p)
                label[node] = table_dict
        xml = htmlparser.Parser(content_x)
        web_contents = []  # web直接展示的content(表格替换成node)
        content_list = xml.xpathall('''//p''')
        for con in content_list:
            con = con.text().strip()
            if con:
                web_contents.append(con)
        breadcrumb = [
            "首页",
            "产经"
        ]
        article_info = {}
        channel = '产经'
        accessory = []  # 附件
        # all_acc = data.xpathall('''//div[@class="ewb-info-con"]//a''')
        # if all_acc:
        #     for acc in all_acc:
        #         temp = {}
        #         acc_url = acc.xpath('//@href').text().strip()
        #         if acc_url and '@' not in acc_url:
        #             acc_url = urljoin(url, acc_url)
        #             name = acc.text().strip()
        #             file_name = acc_url.split('/')[-1].split('=')[-1]
        #             temp['url'] = acc_url
        #             temp['name'] = name
        #             temp['file_name'] = file_name
        #             dir_path = os.path.join(self.ori_path, self.dir_name)
        #             if not os.path.isdir(dir_path):
        #                 os.makedirs(dir_path)
        #             path = os.path.join(dir_path, file_name)
        #             dow_img_acc(path, acc_url)
        #             # file_content = parse_main(path)
        #             temp['file_content'] = '' # file_content
        #             accessory.append(temp)
        gtime = int(time.time())
        main_business = ''
        source = data.xpath('''//span[@class="source ent-source"]/text()|//a[@class="source ent-source"]/text()''').text().strip()
        webname = '新浪财经'
        domain = self.allowed_domains[0]
        uid = add_uuid(url)
        item["collection_name"] = "news_finance_sina_raw"    # 集合名
        item["url"] = url    # 链接
        item["uid"] = uid    # 去重id
        item["contents"] = contents    # 数据处理的内容
        item["web_contents"] = web_contents    # 前端使用的内容
        item["article_info"] = article_info    # 文章的相关信息
        item["label"] = label    # 图片、表格
        item["accessory"] = accessory    # 附件
        item["gtime"] = gtime    # 爬虫时间
        item['breadcrumb'] = breadcrumb    # 导航
        item['channel'] = channel    # 频道
        item["spider_name"] = self.name    # 爬虫名
        item["webname"] = webname    # 网站名
        item["domain"] = domain    # 域名
        item["source"] = source    # 来源
        item["main_business"] = main_business    # 相关行业
        item['path'] = ''    # 附件路径
        yield item