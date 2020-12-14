# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import redis
import json
import time
from scrapy.pipelines.images import ImagesPipeline
from scrapy.pipelines.files import FilesPipeline
from scrapy import Request
from spider_util.config.db_info import collections
from scrapy.conf import settings


class CollectionError(Exception):
    pass


class MongodbPipeline(object):

    @classmethod
    def from_crawler(cls, crawler):
        cls.DB_URI = crawler.settings.get('MONGODB_URI')
        cls.DB_NAME = crawler.settings.get('MONGODB_DB')
        # cls.COLLECTIONS = crawler.settings.get('MONGODB_COLLECTIONS')
        return cls()

    def open_spider(self, spider):
        # spider (Spider 对象) – 被开启的spider
        # 可选实现，当spider被开启时，这个方法被调用。
        self.client = pymongo.MongoClient(self.DB_URI)
        self.db = self.client[self.DB_NAME]

    def close_spider(self, spider):
        # spider (Spider 对象) – 被关闭的spider
        # 可选实现，当spider被关闭时，这个方法被调用
        self.client.close()

    def process_item(self, item, spider):
        import copy
        uid = item.get('uid', '')
        item = copy.deepcopy(item)
        spider_name = item.get("spider_name", "")
        collection_name = item.get('collection_name', '')
        if not collection_name.startswith('news') and not collection_name.endswith('raw'):
            raise CollectionError('非法集合！%s' % collection_name)
        collection = self.db[collection_name]
        try:
            collection.insert_one(item)
        except Exception as e:
            print(1111111, e)
            return '--插入失败--%s' % uid

        # todo settings
        redis_new_name = settings.get('REDIS_KEY_NAME_UUID')
        redis_host = settings.get('REDIS_HOST')
        redis_port = settings.get('REDIS_PORT')
        redis_db = settings.get('REDIS_DB')
        redis_base_industry = settings.get('REDIS_BASE_INDUSTRY')

        time = item.get('gtime', '')
        conn = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        # todo 对应的采集时间set
        direct_set = '{}:{}'.format(redis_base_industry, redis_new_name)
        if direct_set:
            conn.sadd(direct_set,time)
        # todo 对应的log hash
        caijing_log_hash = '{}_log_hash:{}'.format(redis_base_industry,redis_new_name)
        if caijing_log_hash:
            conn.hset(caijing_log_hash,'spider_name',spider_name)
            conn.hset(caijing_log_hash, 'collection_name', collection_name)
        return '--插入成功--%s' % uid  # json.dumps(item)  # 控制台输出item数据


