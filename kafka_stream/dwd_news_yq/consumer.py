# -*- coding:utf-8 -*-


"""
消费者
"""

import pickle
import traceback

from bin.log import globalLog
from bin.process_data import Processing
from mq.mq import ExchangeChannel, MQConnection


# 创建MQ连接
mq = ExchangeChannel(MQConnection())


@mq.callback
def callback(channel, method, properties, message):
    try:
        print('[*] Waiting for logs. To exit press CTRL+C')
        msg = pickle.loads(message)
        print("消息内容:{}".format(msg))
        return Processing(msg).process_data(channel, method)
    except:
        traceback.print_exc()
        globalLog.error(traceback.format_exc())
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        channel.close(reply_text='callback func running Error')


if __name__ == '__main__':
    mq.listening(queue_name='financial_news', exchange_name='financial_news', routing_key='fn', callback=callback)
