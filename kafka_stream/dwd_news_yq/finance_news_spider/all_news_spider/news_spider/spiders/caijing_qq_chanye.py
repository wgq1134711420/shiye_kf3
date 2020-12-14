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
    name = 'caijing_qq_chanye'
    allowed_domains = ['new.qq.com']
    ori_path = settings.get('ORI_PATH')
    encoding = "utf-8"
    start_urls = [
        "https://pacaio.match.qq.com/irs/rcd?cid=52&token=8f6b50e1667f130c10f981309e1d8200&ext=3913,3914,3915,&page=0&isForce=1",
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
            data = json.loads(response.body.decode(self.encoding))
        except Exception as e:
            print('response failed %s' % e)
            return
        org_list = data.get('data')
        for org in org_list:
            if org:
                title = org.get('title')
                ctime = org.get('publish_time')
                org_url = org.get('vurl')
                if title:
                    url = urljoin(start_url,org_url)
                    print(url)
                    ctime = local_timestamp(ctime)
                    item = {'ctime': ctime, 'title': title}
                    print(item)
                    yield scrapy.Request(url, callback=self.detail_parse, meta={'item': item}, headers=self.headers, dont_filter=True)

    def detail_parse(self, response):
        item = response.meta['item']
        try:
            data = htmlparser.Parser(response.body.decode('gbk'))
        except Exception as e:
            print('second response failed %s' % e)
            return
        url = response.url
        contents = []  # 全部的文本内容
        content_list = data.xpathall('''//div[@class="content-article"]/p//text()''')
        for con in content_list:
            con = con.text().strip()
            if con:
                contents.append(con)
        content_x = data.xpath('''//div[@class="content-article"]''').data
        content_xml = content_x
        label = {}
        img_list = data.xpathall('''//div[@class="content-article"]//img''')
        if img_list:
            for count, image in enumerate(img_list):
                image_dict = {}
                image_url = image.xpath('//@src').text().strip()
                if image_url:
                    image_url = urljoin(url, image_url)
                    node = '#image{}#'.format(count)
                    file_name = image_url.split('/')[-2]
                    image_dict['url'] = image_url
                    image_dict['name'] = ''
                    image_dict['file_name'] = file_name
                    label[node] = image_dict

        table_list = data.xpathall('''//div[@class="content-article"]//table''')
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
            "产业公司"
        ]
        article_info = {}
        channel = '产业公司'
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
        source = data.xpath('''//a[@class="author"]/div/text()''').text().strip()
        if not source:
            source = data.regex('''"media": "(.*?)"''').text().strip()
        webname = '腾讯网'
        domain = self.allowed_domains[0]
        uid = add_uuid(url)
        item["collection_name"] = "news_finance_qq_raw"    # 集合名
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