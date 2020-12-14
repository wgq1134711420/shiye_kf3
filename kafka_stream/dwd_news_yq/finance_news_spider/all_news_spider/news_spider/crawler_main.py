import os, sys
sys.path.append("..")

from twisted.internet import reactor, defer

from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging



from spiders.dangdang import MySpider
from spiders.dangdang1 import MySpider1


configure_logging()

# 创建一个CrawlerRunner对象
runner = CrawlerRunner()


@defer.inlineCallbacks
def crawl():
    crawler_set = list()
    for spider1 in [MySpider, MySpider1]:
        crawler = runner.crawl(spider1)
        crawler_set.append(crawler)
    defer.DeferredList(crawler_set).addBoth(lambda _: reactor.stop())
    reactor.run()


# 调用crawl()
crawl()
