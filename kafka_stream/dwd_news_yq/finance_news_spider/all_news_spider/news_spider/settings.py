# -*- coding: utf-8 -*-

#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:


BOT_NAME = 'news_spider'

SPIDER_MODULES = ['news_spider.spiders']
NEWSPIDER_MODULE = 'news_spider.spider'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False
IMAGES_STORE = 'C:\\docdata\\'
ORI_PATH = 'C:\\docdata\\'

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# 设置超时时间
# DOWNLOAD_TIMEOUT = 300

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 0.5

# 限制每分钟的并发量，
# RANDOMIZE_DOWNLOAD_DELAY = False
# DOWNLOAD_DELAY = 60 / 40
# CONCURRENT_REQUESTS_PER_IP = 40

DOWNLOADER_MIDDLEWARES = {
    'news_spider.middlewares.ProxyMiddleware': 600,
    'news_spider.ClearSpider.ClearSpider': 600,
}

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html

MYEXT_ENABLED = True  # 开启扩展
IDLE_NUMBER = 20  # 配置空闲持续时间单位为 60个 ，一个时间单位为5s
# 在 EXTENSIONS 配置，激活扩展
EXTENSIONS = {
    'scrapy.extensions.telnet.TelnetConsole': None,
    'news_spider.extensions.RedisSpiderSmartIdleClosedExensions': 500,
}

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html

# DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"  # 使用了scrapy_redis的去重组件， 在redis数据库里做去重
# SCHEDULER = "scrapy_redis.scheduler.Scheduler"  # 使用了scrapy_redis的调度器
# SCHEDULER_PERSIST = True  # 在redis中保持scrapy-redis用到的各个队列，从而允许暂停和暂停后恢复，也就是不清理redis queuest
# 默认的scrapy-redis请求队列形式（按优先级）
# SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.SpiderPriorityQueue"
# 通过配置RedisPipeline将item写入key为 spider.name : items 的redis的list中，供后面的分布式处理item 这个已经由 scrapy-redis 实现
ITEM_PIPELINES = {
    'news_spider.pipelines.MongodbPipeline': 300,

}

RETRY_ENABLED = True
RETRY_TIMES = 30
RETRY_HTTP_CODECS = [500, 502, 503, 504, 408, 404, 403, 521]
HTTPERROR_ALLOWED_CODES = [521, 555]

# todo 线上redis
# REDIS_HOST = '192.168.1.129'
# REDIS_PORT = 6379
# todo 本地redis
REDIS_HOST = '192.168.1.222'
REDIS_PORT = 6379

# todo 库
REDIS_DB = 9
# todo 所采集数据的行业
REDIS_BASE_INDUSTRY = 'caijing'

import uuid
REDIS_KEY_NAME_UUID = uuid.uuid4()

# todo 线上mongo
# MONGODB_URI = 'mongodb://root:shiye1805A@192.168.1.125:10011/?authSource=admin'
# MONGODB_DB = 'news_raw'
# todo 本地mongo
MONGODB_URI = 'mongodb://192.168.1.222:27017/admin'
MONGODB_DB = 'news_raw'

MONGODB_COLLECTIONS = ["news_sina_china_raw"]
