# -*- coding:utf-8 -*-

"""
提取消息中的时间段获取mongo数据
提取新闻数据中公司名称并存入es
"""
import time
import traceback
from threading import Thread

import pandas as pd
from elasticsearch import helpers
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

import utils
from bin.log import globalLog
from bin.model.use_model_service import GetCompanyNameModel
from monitoring import mt_company_model
from setting import ES_INDEX, DB_SERVER_URI


class MyThread(Thread):
    """
    重写线程类添加子线程执行结果方法
    """
    def __init__(self, func, args):
        super(MyThread, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            #  如果子线程不使用join方法，此处可能会报没有self.result的错误
            return self.result
        except:
            return None


class TestService(object):
    """
    用于测试各个服务是否开启类
    """
    def __init__(self):
        # 检测聪聪算法模型服务是否开启
        self.mt_model = mt_company_model.test_model()


class Processing(TestService):
    """
    1. 在处理之前检测模型服务是否开启
        1. 没开启: 数据程序终止, 此条消息返回队列, 并发送邮件通知管理员
        2. 开启: (1)检测服务运行, 如模型服务报错, 终止数据程序, 此条消息返回队列
                (2)未报错, 处理数据程序
    2. 数据程序运行报错, 数据程序终止, 未手动ack响应,此条消息返回队列,  并发送邮件通知管理员
    """

    def __init__(self, msg):
        """
        初始化: 1.获取MQ消息中的内容
               2.mongo对象
        :param msg: MQ消息内容
        """
        super(Processing, self).__init__()

        self.msg = msg
        self.start_time = int(msg.get('start_time'))            # 获取mongo最小时间点
        self.end_time = int(msg.get('end_time'))                # 获取mongo最大时间点
        self.collection_name = msg.get('collection_name')       # 获取mongo集合名
        self.mongodb = utils.con_mongo(self.collection_name)    # 获取mongo游标
        self.es = utils.ES().con_es()                           # 获取es对象  TODO 优化直接查询获取es对象 不用需要重新创建

    def get_mongo(self):
        """获取mongo数据对象"""
        # 获取mongo取值时间范围
        time_range = {'$gte': self.start_time, '$lte': self.end_time}
        mongo = self.mongodb.find({'gtime': time_range})
        self.mongo_count = mongo.count()
        print("消息处理总量:{}条\n\r".format(mongo.count()))
        return mongo

    # TODO 代码未完成
    def save_es(self, actions=None):
        """
        储存数据到es
        :param actions: 源数据 ==> list
        """
        result = []
        for action in actions:
            if len(result) == 4000:
                # actions->dict字典类型
                helpers.bulk(client=self.es, actions=result)
                result = []
            else:
                result.append(action)
        else:
            helpers.bulk(client=self.es, actions=result)

    @staticmethod
    def save_sql(sql_actions):
        """
        储存数据到mysql
        :param sql_actions: 源数据 ==> list

            format_data方法中的_sql变量
            format_data._sql = {xx: xx}
            sql_actions = [_sql, _sql, _sql]

        TODO 是否将数据外键
        TODO 资料: https://stackoverflow.com/questions/38085624/pandas-create-foreign-id-column-based-on-name-column

        """
        df_data_sql = pd.DataFrame(sql_actions)
        # 需要将一条新闻含有多个公司名称拆分, 拆分成一条新闻对应一个公司
        df_data_sql = df_data_sql.drop('company', axis=1).join(
            df_data_sql['company'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('company')
        ).astype("str")

        # sqlAlchemy 连接池限制
        con = create_engine(DB_SERVER_URI, poolclass=NullPool)
        df_data_sql.to_sql('new', con=con, if_exists='append', index=False)

    @staticmethod
    def hit_db(db_data, text):
        """
        撞库获取公司名称
        :param db_data: 撞库数据
        :param text: 被撞原文数据
        :return: 公司名称->list数组
        """
        company_df = db_data[db_data['stakeholderName'].apply(lambda x:x in text)]
        if len(company_df) == 0:
            return []
        else:
            # 将DataFrame去重转换为list
            return company_df['stakeholderName'].drop_duplicates().values.tolist()

    @staticmethod
    def redis_cache():
        """
        通过缓存获取数据, 缓存无数据查询数据库再写入缓存
        :return: 公司撞库名单->DataFrame
        """

        # TODO 需url 需要换成常量
        url = 'mysql+pymysql://batchdata_3dep:shiye1805A@192.168.1.129:3306/csc_risk?charset=utf8'

        company_sql = 'SELECT stakeholderName FROM sy_bond_stakeholder_name'
        company_count_sql = 'SELECT COUNT(*) FROM sy_bond_stakeholder_name'

        con_redis = utils.con_redis()
        cache_company = con_redis.get('cache:sy_bond_stakeholder_name')
        # 缓存有数据
        if cache_company:
            data_df = pd.read_msgpack(cache_company)
            count_num = utils.con_mysql(company_count_sql, url)
            # 判断缓存和数据库数量是否相同
            if len(data_df) == int(count_num):
                return data_df
            else:
                # 如果不同->重新读取数据库数据并存入缓存
                now_df = pd.read_sql(company_sql, con=url)
                # to_msgpack()将DataFrame转换为字节流    过期redis存6H
                con_redis.set('cache:sy_bond_stakeholder_name', now_df.to_msgpack(), 21600)
                return now_df
        else:
            now_df = pd.read_sql(company_sql, con=url)
            con_redis.set('cache:sy_bond_stakeholder_name', now_df.to_msgpack(), 21600)
            return now_df

    @staticmethod
    def format_data(mongo_data, company_list):
        """
        保存数据到库

        集合名, 链接, 去重id, 标题, 数据处理的内容, 前端使用的内容, 图片、表格, 发布时间, 来源, 相关行业, 相关公司
        :param mongo_data: mongo数据
        """
        id = str(mongo_data.get('_id'))
        url = mongo_data.get('url')
        title = mongo_data.get('title')
        web_contents = mongo_data.get('web_contents')
        contents = mongo_data.get('contents')
        label = mongo_data.get('label')
        ctime = mongo_data.get('ctime')
        source = mongo_data.get('source')
        contents_xml = mongo_data.get('content_xml')
        main_business = mongo_data.get('main_business')
        collection_name = mongo_data.get('collection_name')

        # 数据库&es共有数据字段
        public = {
            'id': id,
            'title': title,
            'url': url,
            'pushTime': ctime,
            'source': source,
            'mainBusiness': main_business,
            'collectionName': collection_name,
            'xmlContents': contents_xml,

        }
        # 数据库字段
        _sql = {
            'company': ','.join(company_list),
            'contents': ','.join(contents),
            'webContents': '\n'.join(web_contents),
            'label': str(label)

        }
        # es字段
        _es = {
            "_index": ES_INDEX,
            "_type": "financial_industry_news_type",
            "_source": {
                'company': company_list,
                'contents': contents,
                'webContents': web_contents,
                'source': source,
                'label': label
            }
        }

        _sql.update(public)
        _es['_source'].update(public)

        return _es, _sql

    def thread_process_data(self, channel=None, method=None):
        """
        子线程执行处理数据任务
        :param channel: 信道
        :param method: mq方法 可以获取任务id
        :return: 是否执行ok
        """
        try:
            # 处理数据量
            data = self.get_mongo()

            # 检测模型是否开启
            if self.mt_model is False:
                # 拒绝传入的消息。此方法允许客户端拒绝消息
                # 如果requeue为true，则服务器将尝试重新排队该消息。
                # 如果requeue为false或requeue尝试失败，则消息将被丢弃或置为死信。
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
                globalLog.error('算法模型服务端口未开启')
                print('算法模型服务端口未开启, 消息以重新队列消息')
                return 'ERROR'

            # 通过缓存获取抓撞库数据
            data_df = self.redis_cache()

            es_actions = []
            sql_actions = []
            for n, mongo_data in enumerate(data, start=1):
                contents = ''.join(mongo_data.get('contents'))  # 数据处理的内容
                # 1. 调用模型提取公司名称 ==> list
                cnm = GetCompanyNameModel()
                # # TODO 旧模型
                # client1_list = cnm.get_api(text=contents, client_num=cnm.bclient_1)
                # client2_list = cnm.get_api(text=contents, client_num=cnm.bclient_2)
                # model_company_list = client1_list + client2_list
                # TODO 新模型  需要将全角转成半角
                model_company_list = cnm.get_api(text=contents, client_num=cnm.bclient_3)
                # 2. 调用撞库 ==> list
                hit_company_list = self.hit_db(db_data=data_df, text=contents)
                # 3. 拼接两个方法返回值 ==> list
                company_list = list(set(model_company_list + hit_company_list))

                # TODO 模型调用获取APP并找到公司名称
                # TODO 所有公司名称需要过滤别名库获取公司全称

                # 拼装数据 ES Mysql
                action_data, sql_data = self.format_data(mongo_data, company_list)
                es_actions.append(action_data)
                sql_actions.append(sql_data)

            # 保存数据库
            self.save_sql(sql_actions)   # [Mysql]
            self.save_es(es_actions)     # [ES]
        except:
            # 数据处理报错记录并返回error
            traceback.print_exc()
            globalLog.error(traceback.format_exc())
            return 'ERROR'
        else:
            return True

    def process_data(self, channel=None, method=None):
        """
        采用多线程分布式数据处理 => callback

        主线程: 限时/主动发送心跳操作
        子线程: 处理数据方法

        :param channel: 信道
        :param method: mq方法
        """

        thread_process = MyThread(self.thread_process_data, args=(channel, method))
        thread_process.setDaemon(True)
        thread_process.start()

        # 限制任务时间10分钟 超过就把次消息拒绝
        for i in range(1, 61*10):
            time.sleep(1)
            result = thread_process.get_result()

            # 每10s消费者主动发送一次心跳检测
            if i % 30 == 0:
                # 将确保处理数据方法完整执行完成
                # 防止server端主动提出分手 => 认为消费者死掉
                channel.connection.process_data_events()
            # 子线程任务处理完 => true
            if result is True:
                # 记录完成消息数据内容
                globalLog.success("消息内容:{}  消息处理总量:{}条".format(self.msg, self.mongo_count))
                return result
            # 子线程任务报错 => error 或者 thread_process_data 返回 False
            elif result == 'ERROR':
                globalLog.error("处理报错消息内容:{}  消息处理总量:{}条".format(self.msg, self.mongo_count))
                return 'ERROR'
            elif result is False:
                globalLog.error("处理报错消息内容:{}  消息处理总量:{}条".format(self.msg, self.mongo_count))
                return False

        else:
            globalLog.warning("处理超时消息内容:{}  消息处理总量:{}条".format(self.msg, self.mongo_count))
            # 拒绝此条消息 放入死信队列
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

