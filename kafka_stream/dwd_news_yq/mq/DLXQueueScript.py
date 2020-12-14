# -*- coding:utf-8 -*-

"""
创建死信队列脚本
"""


import pika

from setting import MQ_HOST, MQ_PORT, MQ_USER, MQ_PASSWORD


class MQConnection(object):
    """连接rabbitMQ"""

    def __init__(self, host=MQ_HOST, port=MQ_PORT, user=MQ_USER, password=MQ_PASSWORD, vhosts='/finance_news'):
        """默认/虚拟主机"""
        super(MQConnection, self).__init__()
        self.host = host
        self.port = port
        self.name = user
        self.password = password
        self.vhosts = vhosts

        # 凭证
        credentials = pika.PlainCredentials(self.name, self.password)
        # 连接参数
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.vhosts,
            credentials=credentials,
            heartbeat=0,  # 关闭心跳检测  消息处理时间过长导致与MQ断开连接
        )
        self.connection = pika.BlockingConnection(parameters)

    def close(self):
        if self.connection:
            self.connection.close()

    def channel(self):
        return self.connection.channel()


class MQChannel(object):
    """MQChannel文档提示"""

    def __init__(self, connection):
        """
        初始化
        :param connection: mq实例对象
        """
        super(MQChannel, self).__init__()
        self.mqc = connection
        self.channel = connection.channel()


mq = MQChannel(MQConnection())


# 创建死信队列
mq.channel.exchange_declare(exchange='dlx.financial_news', exchange_type='direct', durable=True)
mq.channel.queue_declare(queue='dlx.financial_news', passive=False, durable=True, exclusive=False, auto_delete=True)
mq.channel.queue_bind(queue='dlx.financial_news', exchange='dlx.financial_news', routing_key='dlx')


# # 创建正常队列
# mq.channel.exchange_declare(exchange='financial_news', exchange_type='direct', durable=True)
# mq.channel.queue_declare(queue='financial_news', passive=False, durable=True, exclusive=False, auto_delete=False,
#                          arguments={  # 创建业务队列并且添加死信设置
#                              "x-dead-letter-exchange": "dlx.financial_news",  # 指定死信队列的交换器
#                              "x-dead-letter-routing-key": "dlx"               # 死信队列交换器和队列绑定key
#                          })
# mq.channel.queue_bind(queue='financial_news', exchange='financial_news', routing_key='fn')


