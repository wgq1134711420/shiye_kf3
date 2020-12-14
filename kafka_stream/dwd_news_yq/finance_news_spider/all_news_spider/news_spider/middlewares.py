# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import base64
from scrapy import signals
import requests
from selenium import webdriver
from scrapy.http import HtmlResponse
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


class FspSpiderSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class FspSpiderDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class ProxyMiddleware(object):

    def process_request(self, request, spider):
        proxy_server = 'http://http-dyn.abuyun.com:9020'
        proxy_user = 'H7LWHMG2U0Y91P3D'
        proxy_pass = '40E30052E470B7C6'

        proxy_auth = "Basic " + base64.urlsafe_b64encode(
            bytes((proxy_user + ":" + proxy_pass), "ascii")).decode("utf8")
        # print(proxy_auth)
        request.meta["proxy"] = proxy_server
        # print(proxy_server)
        request.headers["Proxy-Authorization"] = proxy_auth


class JavaScriptMiddleware(object):
    def process_request(self, request, spider):

        # if spider.name == "mid":
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-gpu')

        driver = webdriver.Chrome(options=chrome_options,service_args=['--ignore-ssl-errors=true', '--ssl-protocol=any']) #指定使用的浏览器
        driver.get(request.url)
        driver.refresh()
        # time.sleep(3*(random.random()))
        driver.implicitly_wait(3)
        js = "var q=document.documentElement.scrollTop=10000"
        driver.execute_script(js) #可执行js，模仿用户操作。此处为将页面拉至最底端。
        time.sleep(2*(random.random()))
        body = driver.page_source
        print ("访问"+request.url)
        return HtmlResponse(driver.current_url, body=body, encoding='utf-8', request=request)