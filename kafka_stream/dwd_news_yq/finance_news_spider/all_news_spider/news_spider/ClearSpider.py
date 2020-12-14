# -*- coding:utf-8 -*-

import pika
import redis
from scrapy import signals
from scrapy.conf import settings
from scrapy.xlib.pydispatch import dispatcher

HOST = '101.132.168.72'  # rabbitmq服务地址
PORT = 5672              # 服务端口
USER = 'admin'           # 用户名
PASSWORD = 'qq123123'    # 密码


class MQConnection:

    def __init__(self, host=HOST, port=PORT, user=USER, password=PASSWORD, vhosts='/'):
        super(MQConnection, self).__init__()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.vhosts = vhosts

        credentials = pika.PlainCredentials(username=self.user, password=self.password)

        parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.vhosts,
                credentials=credentials
        )
        self.connection = pika.BlockingConnection(parameters)

    def close(self):
        if self.connection:
            self.connection.close()

    def channel(self):
        return self.connection.channel()


class MQChannel(object):

    def __init__(self, connection):
        super(MQChannel, self).__init__()
        self.mqc = connection
        self.channel = connection.channel()


class MQSender(MQChannel):

    def send(self, exchange='', routing_key='', message=None):
        self.channel.exchange_declare(exchange=exchange, durable=True)
        self.channel.confirm_delivery()
        import pickle
        try:
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=pickle.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print('Message was published')
        except:
            print('Message was returned')

        self.mqc.close()


class ClearSpider:

    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        super(ClearSpider, self).__init__()

    def spider_closed(self):
        # todo settings
        redis_new_name = settings.get('REDIS_KEY_NAME_UUID')
        redis_base_industry = settings.get('REDIS_BASE_INDUSTRY')
        redis_mess_key = '{}:{}'.format(redis_base_industry, redis_new_name)
        reids_log_key = '{}_log_hash:{}'.format(redis_base_industry, redis_new_name)
        redis_host = settings.get('REDIS_HOST')
        redis_port = settings.get('REDIS_PORT')
        redis_db = settings.get('REDIS_DB')

        try:
            conn = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
            conn.ping()
        except Exception as e:
            print(e)
            return

        news_time = conn.smembers(redis_mess_key)
        if news_time:
            msg = {
                'start_time': min(news_time),
                'end_time': max(news_time),
                'collection_name': conn.hget(reids_log_key, 'collection_name'),
                'spider_name': conn.hget(reids_log_key, 'spider_name')
            }
            sender = MQSender(MQConnection())
            print("@@@@@@@@@@@@@@@@@@@@@@@@测试发送@@@@@@@@@@@@@@")
            sender.send(exchange='financial_news', routing_key='fn', message=msg)
            print("@@@@@@@@@@@@@@@@@@@@@@@@发送成功@@@@@@@@@@@@@@")
