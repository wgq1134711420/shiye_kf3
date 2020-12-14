# -*- coding:utf-8 -*-

"""
提取消息中的时间段获取mongo数据
提取新闻数据中公司名称并存入es
"""

import time
import traceback
from threading import Thread
import decimal
import pandas as pd
from elasticsearch import helpers
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import re
import utils
from bin.log import globalLog
from bin.model.use_model_service import GetCompanyNameModel
from monitoring import mt_company_model
from setting import ES_INDEX, DB_SERVER_URI

import requests
import json
from bin.process_helper import *
from DBUtils.PooledDB import PooledDB

# from bin.compare_test import *

# 获取全称、简称、公司代码
quancheng_list,jiancheng_list,code_list = quan_jian_code()
# # 获取人名及对应公司
# person_name_list,co_dep_name_list = get_person_name()

importance_dict = {'1': '相对不重要', '2': '相对不重要', '3': '相对不重要', '4': '重要', '5': '非常重要'}


# 读取连接数据库配置文件
load_config='/home/seeyii/increase_nlp/db_config.json'
with open(load_config,'r', encoding="utf-8") as f:
    reader = f.readlines()[0]
config_local = json.loads(reader)

# db1 = pymysql.connect(**config_local['129_project'])

# mongo集群
mongocli = pymongo.MongoClient(config_local['cluster_mongo'])
# print('connected to mongo')
db_name = mongocli['sy_project_raw']


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
        mongo = self.mongodb.find({'gtime': time_range,'ctime' : {'$gte':1577808000}})
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

    # @staticmethod
    # def save_sql(sql_actions):
    #     """
    #     储存数据到mysql
    #     :param sql_actions: 源数据 ==> list

    #         format_data方法中的_sql变量
    #         format_data._sql = {xx: xx}
    #         sql_actions = [_sql, _sql, _sql]

    #     TODO 是否将数据外键
    #     TODO 资料: https://stackoverflow.com/questions/38085624/pandas-create-foreign-id-column-based-on-name-column

    #     """
    #     df_data_sql = pd.DataFrame(sql_actions)
    #     # 需要将一条新闻含有多个公司名称拆分, 拆分成一条新闻对应一个公司
    #     df_data_sql = df_data_sql.drop('company', axis=1).join(
    #         df_data_sql['company'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('company')
    #     ).astype("str")

    #     # sqlAlchemy 连接池限制
    #     con = create_engine(DB_SERVER_URI, poolclass=NullPool)
    #     df_data_sql.to_sql('new', con=con, if_exists='append', index=False)

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
            if count_num and len(data_df) == int(count_num):
                # print("!@#")
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


    # @staticmethod
    # def format_data(mongo_data, company_list):
    #     """
    #     ES数据
    #     """    
    #     _id = str(mongo_data.get('_id'))
    #     url = mongo_data.get('url')
    #     title = mongo_data.get('title')
    #     web_contents = mongo_data.get('web_contents')
    #     contents = mongo_data.get('contents')
    #     label = mongo_data.get('label')

    #     ctimeStamp = mongo_data.get('ctime')
    #     ctimeArray = time.localtime(ctimeStamp)
    #     ctime = time.strftime('%Y-%m-%d', ctimeArray)

    #     source = mongo_data.get('source')
    #     main_business = mongo_data.get('main_business')
    #     collection_name = mongo_data.get('collection_name')
    #     content_xml = mongo_data.get('content_xml')

    #     # es字段
    #     _es = {
    #         "_index": ES_INDEX,
    #         "_type": "financial_industry_news_type",
    #         "_source": {
    #             'company': company_list,
    #             'contents': ''.join(contents),
    #             'webContents': web_contents,
    #             'source': source,
    #             'label': label,
    #             'id': _id,
    #             'title': title,
    #             'url': url,
    #             'pushTime': ctime,
    #             'mainBusiness': main_business,
    #             'collectionName': collection_name,
    #             'xmlContents': content_xml,
    #         }
    #     }

    #     return _es


    def thread_process_data(self, channel=None, method=None):
        """
        子线程执行处理数据任务
        :param channel: 信道
        :param method: mq方法 可以获取任务id
        :return: 是否执行ok
        """
        pool = PooledDB(creator=pymysql, **config_local['129_project_pool'])
        cnm = GetCompanyNameModel()  # 调用模型客户端
        
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

            # result_es = []
            # result_sql_yq_base = []
            # result_sql_cmp_yq_emo = []
            # result_sql_ind_yq_emo = []
            # result_sql_cpt_yq_emo = []
            # result_sql_area_yq_emo = []
            # result_sql_yq_imp = []
            # result_sql_cmp_yq_rel = []
            # result_sql_ind_yq_rel = []
            # result_sql_cpt_yq_rel = []
            # result_sql_area_yq_rel = []
            # result_sql_yq_sim = []
            # result_sql_yq_cls = []
            # result_sql_yq_lvl_code = []

            for n, mongo_data in enumerate(data, start=1):
                db1 = pool.connection()

                start = time.clock()

                # Jason 对数据的处理放在此处
                _id = str(mongo_data.get('_id'))
                uid = mongo_data.get('uid')
                url = mongo_data.get('url')
                # 对过长的url数据进行截断处理
                if len(url) > 1990:
                    url = url[:1990]
                title = DBC2SBC(mongo_data.get('title')).replace('\n','').replace('\xa0','').replace('\xb3','').replace('\u3000','').replace('\u2800','').replace('\u2022','').replace('\xae','').replace('\u2fa6','').replace('\u2f9c','').replace('\u2005','').replace('\u2003','').replace('\u200b','').replace('\u200c','').replace('\xe6','').replace('\u2028','').replace('\u2002','').replace('\u2122','').replace('\xe4','').replace('\u25aa','').replace('\xb5','').replace('\xa5','').replace('\xa9','').replace('\u2f06','').replace('\u25ba','')
                if len(title) > 990:
                    title = title[:990]

                contents_raw = mongo_data.get('contents')
                # contents_raw = re.sub(r'[^\\x00-xFF]','',contents_raw)
                content = DBC2SBC(''.join(contents_raw).replace('\n','').replace('\xa0','').replace('\xb3','').replace('\u3000','').replace('\u2800','').replace('\u2022','').replace('\xae','').replace('\u2fa6','').replace('\u2f9c','').replace('\u2005','').replace('\u2003','').replace('\u200b','').replace('\u200c','').replace('\xe6','').replace('\u2028','').replace('\u2002','').replace('\u2122','').replace('\xe4','').replace('\u25aa','').replace('\xb5','').replace('\xa5','').replace('\xa9','').replace('\u2f06','').replace('\u25ba','').replace('TRS_Editor','').replace('A{}','').replace('LI{}','').replace('UL{}','').replace('FONT{}','').replace('SPAN{}','').replace('TH{}','').replace('TD{}','').replace('DIV{}','').replace('P{}','').replace('.','').replace('\xF0','').replace('\x9F','').replace('\x90','').replace('\x82','').replace('\xF0',''))
                search_content = DBC2SBC('\n'.join(contents_raw).replace('\xa0','').replace('\xb3','').replace('\u3000','').replace('\u2800','').replace('\u2022','').replace('\xae','').replace('\u2fa6','').replace('\u2f9c','').replace('\u2005','').replace('\u2003','').replace('\u200b','').replace('\u200c','').replace('\xe6','').replace('\u2028','').replace('\u2002','').replace('\u2122','').replace('\xe4','').replace('\u25aa','').replace('\xb5','').replace('\xa5','').replace('\xa9','').replace('\u2f06','').replace('\u25ba','').replace('TRS_Editor','').replace('A{}','').replace('LI{}','').replace('UL{}','').replace('FONT{}','').replace('SPAN{}','').replace('TH{}','').replace('TD{}','').replace('DIV{}','').replace('P{}','').replace('.','').replace('\xF0','').replace('\x9F','').replace('\x90','').replace('\x82','').replace('\xF0',''))
                web_contents = mongo_data.get('web_contents')
                label = mongo_data.get('label')
                source = mongo_data.get('source')
                content_xml = mongo_data.get('content_xml')
                main_business = mongo_data.get('main_business')
                collection_name = mongo_data.get('collection_name')
                webname = mongo_data.get('webname')
                if len(webname) > 200:
                    webname = webname[:200]
                channel = mongo_data.get('channel')
                path = mongo_data.get('path')
                spider_name = mongo_data.get('spider_name')
                domain = mongo_data.get('domain')
                editor = mongo_data.get('editor')

                ctimeStamp = mongo_data.get('ctime')
                # 抓取的舆情发布日期为空
                if ctimeStamp == '':
                    continue
                ctimeArray = time.localtime(ctimeStamp)
                ctime = time.strftime('%Y-%m-%d %H:%M:%S', ctimeArray)
                ctimeDate = time.strftime('%Y-%m-%d', ctimeArray)

                gtimeStamp = mongo_data.get('gtime')
                gtimeArray = time.localtime(gtimeStamp)
                gtime = time.strftime('%Y-%m-%d %H:%M:%S', gtimeArray)

                # 去除文章末尾的作者信息
                author_list = ['返回搜狐','(此稿由证券时报e公司写稿机器人']
                for author in author_list:
                    content = cut_author(content,author)

                # 去除图片占位符
                new_web_contents = []
                for con in web_contents:
                    # print('con',[con])
                    if '#image' not in con:
                        # print('con',[con])
                        new_web_contents.append(con)
                # print('new_web_contents',[new_web_contents])
                WEB_con = DBC2SBC(''.join(new_web_contents)).replace('\xa0','').replace('\xb3','').replace('\u3000','').replace('\u2800','').replace('\u2022','').replace('\xae','').replace('\u2fa6','').replace('\u2f9c','').replace('\u2005','').replace('\u2003','').replace('\u200b','').replace('\u200c','').replace('\xe6','').replace('\u2028','').replace('\u2002','').replace('\u2122','').replace('\xe4','').replace('\u25aa','').replace('\xb5','').replace('\xa5','').replace('\xa9','').replace('\u2f06','').replace('\u25ba','').replace('TRS_Editor','').replace('A{}','').replace('LI{}','').replace('UL{}','').replace('FONT{}','').replace('SPAN{}','').replace('TH{}','').replace('TD{}','').replace('DIV{}','').replace('P{}','').replace('.','')

                elapsed = (time.clock() - start)
                # print("Time used 事件分类之前:",elapsed)
                
                # 事件分类
                firstLevelCode = ''
                firstLevelName = ''
                secondLevelCode = ''
                secondLevelName = ''
                thirdLevelCode = ''
                thirdLevelName = ''
                fourthLevelCode = ''
                fourthLevelName = ''
                eventCode = ''
                eventName = ''
                importanceDgree = ''
                # 优先从标题中匹配规则关键词
                # lvl_list = lvl_title(title)
                lvl_list = lvl_title_content(title,content)
                if len(lvl_list) > 0:
                    for lvl_result in lvl_list:
                        if lvl_result[0]:
                            firstLevelCode = lvl_result[0][:95]
                        if lvl_result[1]:
                            firstLevelName = lvl_result[1][:95]
                        if lvl_result[2]:
                            secondLevelCode = lvl_result[2][:95]
                        if lvl_result[3]:
                            secondLevelName = lvl_result[3][:95]
                        if lvl_result[4]:
                            thirdLevelCode = lvl_result[4][:95]
                        if lvl_result[5]:
                            thirdLevelName = lvl_result[5][:95]
                        if lvl_result[6]:
                            fourthLevelCode = lvl_result[6][:95]
                        if lvl_result[7]:
                            fourthLevelName = lvl_result[7][:95]
                        if lvl_result[8]:
                            eventCode = lvl_result[8][:45]
                        if lvl_result[9]:
                            eventName = lvl_result[9][:95]
                        if lvl_result[11]:
                            importanceDgree = lvl_result[11][:95]

                importanceLabel = ''
                # if importanceDgree != '':
                #     importanceDgree = str(int(importanceDgree))
                if importanceDgree in ['1','2','3','4','5']:
                    importanceLabel = importance_dict[importanceDgree]


                elapsed = (time.clock() - start)
                # print("Time used 舆情接口之前:",elapsed)


                # 舆情接口信息
                return_result = requests.post('http://172.17.23.150:11068/emotion/',data=json.dumps({'title':title,'content':content}))
                # print(type(result))
                emoScore = ''
                emoLabel = ''
                keyword = ''
                summary = ''
                emoConf = ''
                if return_result:
                    result = return_result.json()
                    emoScore = result['score']
                    emoLabel = result['sentiment']
                    keyword = result['keyWords']
                    summary = result['abstract']
                    emoConf = result['confidence']
                if len(summary) > 990:
                    summary = summary[:990]
                if len(keyword) > 490:
                    keyword = keyword[:490]



                # 关键词索引词初始化
                keyword_list = []
                if keyword != '':
                    keyword_list = keyword.split(',')



                elapsed = (time.clock() - start)
                # print("Time used 调用模型之前:",elapsed)


                # 1. 调用模型提取公司名称 ==> list

                # TODO 新模型  需要将全角转成半角
                model_company_list = cnm.get_api(text=content, client_num=cnm.bclient_3)
                # 2. 调用撞库 ==> list
                hit_company_list = self.hit_db(db_data=data_df, text=content)
                # 3. 拼接两个方法返回值 ==> list
                company_list = list(set(model_company_list + hit_company_list))

                # match_person_list = []
                # match_person_co_list = []
                # # 如果没有提取到公司，则提取人名
                # if len(company_list) < 1:
                #     match_person_list,match_person_co_list = match_person_name(text=content,person_name_list=person_name_list,co_dep_name_list=co_dep_name_list)

                # TODO 模型调用获取APP并找到公司名称
                # TODO 所有公司名称需要过滤别名库获取公司全称

                # # ES数据
                # _es = self.format_data(mongo_data, company_list)
                # result_es.append(_es)


                elapsed = (time.clock() - start)
                # print("Time used MySQL数据之前:",elapsed)


                # MySQL数据

                # 初始化去重列表，提高效率
                dupli_uid_pair_list = []

                # 关键词作为索引词进行查重
                for indexWords in keyword_list:
                    indexWords = indexWords[:100]

                    # if len(indexWords) > 2 and is_Chinese(indexWords):
                    
                    # 初步筛选相似文章
                    title_sim,words_uid = get_sim_uid(db=db1,title=title,indexWords=indexWords)

                    # 如标题完全相同，则直接判断为同一篇文章
                    for title_uid in title_sim:
                        # 先判断去重
                        if (uid,title_uid[0]) not in dupli_uid_pair_list:
                            dupli_uid_pair_list.append((uid,title_uid[0]))
                            insert_yq_sim(db=db1,yqid=uid,comYqid=title_uid[0],simScore='1',simYn='1',isValid=1,dataStatus=1)
                        else:
                            continue


                    # # 获得可能相似文章的内容列表
                    # if len(words_uid) > 0:
                    #     words_uid = list(list(zip(*words_uid))[0])
                    #     words_uid_list = [uid]
                    #     words_uid_list.extend(words_uid)
                    #     print('uid indexWords',uid)
                    #     print('words_uid_list indexWords',words_uid_list,len(words_uid_list))
                    #     # print('words_uid',words_uid,len(words_uid))
                    #     content_list = get_content(db=db1,uid_list=words_uid_list)
                    #     print('content_list',len(content_list))
                    #     if '' in content_list:
                    #         continue
                    #     result_list = sim_calculation(content_list)
                    #     if len(result_list) > 0:
                    #         for num,score in enumerate(result_list):
                    #             if (uid,words_uid[num]) not in dupli_uid_pair_list:
                    #                 dupli_uid_pair_list.append((uid,words_uid[num]))
                    #                 if score > 0.95:
                    #                     # print('score',score,type(score))
                    #                     simYn = '0'
                    #                     if score > 0.97:
                    #                         simYn = '1'
                    #                     if score >= 1:
                    #                         score = '1'
                    #                     else:
                    #                         score = str(round(score,2))
                    #                     insert_yq_sim(db=db1,yqid=uid,comYqid=words_uid[num],simScore=score,simYn=simYn,isValid=1,dataStatus=1)
                    #             else:
                    #                 continue

                    # 向比较相似度表中写数据
                    insert_sim_tmp(db=db1,uid=uid,title=title,indexWords=indexWords,pubTime=ctime)


                elapsed = (time.clock() - start)
                # print("Time used 舆情基本信息之前:",elapsed)


                try:
                    # 舆情基本信息
                    insert_yq_base(db=db1,yqid=uid,title=title,content=search_content,webname=webname,webnameConf='',srcType='新闻',srcUrl=url,summary=summary,keyword=keyword,snapshot='',pubTime=ctime,getTime=gtime,spareCol1='',spareCol2='',spareCol3='',spareCol4='',spareCol5='',isValid=1,dataStatus=1)
                except:
                    print('dupli',uid)
                # 'yqid': uid, # yqid 舆情标识ID(业务主键)
                # 'title': title, # title 标题
                # 'content': content, # content 正文
                # 'webname': webname, # webname 来源网站
                # 'webnameConf': '', # webnameConf 来源网站置信度
                # 'srcType': '', # srcType 来源类型（如新闻、A股公告、三板公告、债券公告等）
                # 'srcUrl': url, # srcUrl 来源URL
                # 'summary': summary, # summary 摘要
                # 'keyword': keyword, # keyword 关键词
                # 'snapshot': path, # snapshot 网页快照地址
                # 'pubTime': ctime, # pubTime 发布时间
                # 'getTime': gtime, # getTime 获取时间
                # 'spareCol1': '', # spareCol1 备用字段1
                # 'spareCol2': '', # spareCol2 备用字段2
                # 'spareCol3': '', # spareCol3 备用字段3
                # 'spareCol4': '', # spareCol4 备用字段4
                # 'spareCol5': '', # spareCol5 备用字段5
                # 'isValid': 1, # isValid 是否有效 0:否;1:是
                # 'dataStatus': 1, # dataStatus  数据状态:1->新增;2->更新;3->删除


                # 行业舆情情感信息
                insert_ind_yq_emo(db=db1,yqid=uid,eventCode=eventCode,industryCode='',emoScore=emoScore,emoLabel=emoLabel,emoConf=emoConf,isValid=1,dataStatus=1)
                # 'yqid': uid, # yqid 舆情标识ID(业务主键)
                # 'eventCode': '', # eventCode 舆情事件分类编码
                # 'industryCode': '', # industryCode 行业编码
                # 'emoScore': emoScore, # emoScore 情感分值
                # 'emoLabel': emoLabel, # emoLabel 情感标签
                # 'emoConf': emoConf, # emoConf 置信度
                # 'isValid': 1, # isValid 是否有效 0:否;1:是
                # 'dataStatus': 1, # dataStatus  数据状态:1->新增;2->更新;3->删除


                # 概念舆情情感信息
                insert_cpt_yq_emo(db=db1,yqid=uid,eventCode=eventCode,conceptCode='',emoScore=emoScore,emoLabel=emoLabel,emoConf=emoConf,isValid=1,dataStatus=1)
                # 'yqid': uid, # yqid 舆情标识ID(业务主键)
                # 'eventCode': '', # eventCode 舆情事件分类编码
                # 'conceptCode': '', # conceptCode 概念编码
                # 'emoScore': emoScore, # emoScore 情感分值
                # 'emoLabel': emoLabel, # emoLabel 情感标签
                # 'emoConf': emoConf, # emoConf 置信度
                # 'isValid': 1, # isValid 是否有效 0:否;1:是
                # 'dataStatus': 1, # dataStatus  数据状态:1->新增;2->更新;3->删除


                # 地域舆情情感信息
                insert_area_yq_emo(db=db1,yqid=uid,eventCode=eventCode,areaCode='',emoScore=emoScore,emoLabel=emoLabel,emoConf=emoConf,isValid=1,dataStatus=1)
                # 'yqid': uid, # yqid 舆情标识ID(业务主键)
                # 'eventCode': '', # eventCode 舆情事件分类编码
                # 'areaCode': '', # areaCode 地域编码
                # 'emoScore': emoScore, # emoScore 情感分值
                # 'emoLabel': emoLabel, # emoLabel 情感标签
                # 'emoConf': emoConf, # emoConf 置信度
                # 'isValid': 1, # isValid 是否有效 0:否;1:是
                # 'dataStatus': 1, # dataStatus  数据状态:1->新增;2->更新;3->删除


                # 舆情重要性信息
                insert_yq_imp(db=db1,yqid=uid,eventCode=eventCode,impScore=importanceDgree,impLabel=importanceLabel,isValid=1,dataStatus=1)
                # 'yqid': uid, # yqid 舆情标识ID(业务主键)
                # 'eventCode': '', # eventCode 舆情事件分类编码
                # 'impScore': '', # impScore 重要程度分值
                # 'impLabel': '', # impLabel 重要程度标签
                # 'isValid': 1, # isValid 是否有效 0:否;1:是
                # 'dataStatus': 1, # dataStatus  数据状态:1->新增;2->更新;3->删除


                # 行业舆情相关性信息
                insert_ind_yq_rel(db=db1,yqid=uid,industryCode='',relScore='',relLabel='',isValid=1,dataStatus=1)
                # 'yqid': uid, # yqid 舆情标识ID(业务主键)
                # 'industryCode': '', # industryCode 行业编码
                # 'relScore': '', # relScore 相关程度分值
                # 'relLabel': '', # relLabel 相关程度标签
                # 'isValid': 1, # isValid 是否有效 0:否;1:是
                # 'dataStatus': 1, # dataStatus  数据状态:1->新增;2->更新;3->删除


                # 概念舆情相关性信息
                insert_cpt_yq_rel(db=db1,yqid=uid,conceptCode='',relScore='',relLabel='',isValid=1,dataStatus=1)
                # 'yqid': uid, # yqid 舆情标识ID(业务主键)
                # 'conceptCode': '', # conceptCode 概念编码
                # 'relScore': '', # relScore 相关程度分值
                # 'relLabel': '', # relLabel 相关程度标签
                # 'isValid': 1, # isValid 是否有效 0:否;1:是
                # 'dataStatus': 1, # dataStatus  数据状态:1->新增;2->更新;3->删除


                # 地区舆情相关性信息
                insert_area_yq_rel(db=db1,yqid=uid,areaCode='',relScore='',relLabel='',isValid=1,dataStatus=1)
                # 'yqid': uid, # yqid 舆情标识ID(业务主键)
                # 'areaCode': '', # areaCode 地域编码
                # 'relScore': '', # relScore 相关程度分值
                # 'relLabel': '', # relLabel 相关程度标签
                # 'isValid': 1, # isValid 是否有效 0:否;1:是
                # 'dataStatus': 1, # dataStatus  数据状态:1->新增;2->更新;3->删除


                # 舆情相似度信息
                # insert_yq_sim(db=db1,yqid=uid,comYqid='',simScore='',simYn='',isValid=1,dataStatus=1)
                # 'yqid': uid, # yqid 舆情标识ID(业务主键)
                # 'comYqid': '', # comYqid 对比舆情标识ID
                # 'simScore': '', # simScore 相似度
                # 'simYn': '', # simYn 是否相似（1-相似，0-不相似）
                # 'isValid': 1, # isValid 是否有效 0:否;1:是
                # 'dataStatus': 1, # dataStatus  数据状态:1->新增;2->更新;3->删除


                # 舆情事件分类信息
                insert_yq_cls(db=db1,yqid=uid,eventCode=eventCode,isValid=1,dataStatus=1)
                # 'yqid': uid, # yqid 舆情标识ID(业务主键)
                # 'eventCode': '', # eventCode 事件分类编码
                # 'isValid': 1, # isValid 是否有效 0:否;1:是
                # 'dataStatus': 1, # dataStatus  数据状态:1->新增;2->更新;3->删除


                elapsed = (time.clock() - start)
                # print("Time used 舆情事件分类之前:",elapsed)


                # 舆情事件分类信息
                if eventName != '':
                    try:
                        insert_yq_lvl_code(db=db1,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,eventCode=eventCode,eventName=eventName,isValid=1,dataStatus=1)
                    except:
                        print('dupli,eventCode:',eventCode)
                    # 'firstLevelCode': '', # firstLevelCode 一级分类编码
                    # 'firstLevelName': '', # firstLevelName 一级分类名称
                    # 'secondLevelCode': '', # secondLevelCode 二级分类编码
                    # 'secondLevelName': '', # secondLevelName 二级分类名称
                    # 'thirdLevelCode': '', # thirdLevelCode 三级分类编码
                    # 'thirdLevelName': '', # thirdLevelName 三级分类名称
                    # 'fourthLevelCode': '', # fourthLevelCode 四级分类编码
                    # 'fourthLevelName': '', # fourthLevelName 四级分类名称
                    # 'eventCode': '', # eventCode 事件分类编码
                    # 'eventName': '', # eventName 舆情事件分类名称
                    # 'isValid': 1, # isValid 是否有效 0:否;1:是
                    # 'dataStatus': 1, # dataStatus  数据状态:1->新增;2->更新;3->删除



                # # 开始遍历人名列表：
                # if len(match_person_list) > 0:
                #     tfidf_result = tfidf_company(match_person_co_list,content)
                #     # print('tfidf_result',tfidf_result)

                #     companiesInfo = ''
                #     for num,company in enumerate(match_person_co_list):
                #         personName = match_person_list[num]

                #         # print('company',company)
                #         companyShortName = ''
                #         companyCode = ''
                #         relativity = 0
                #         alias_company = ''

                #         # 如果公司名称为全称，匹配简称和代码
                #         if company in quancheng_list:
                #             if company in title:
                #                 relativity = 1
                #             companyShortName = jiancheng_list[quancheng_list.index(company)]
                #             if companyShortName in title:
                #                 relativity = 1
                #             companyCode = code_list[quancheng_list.index(company)]
                #             alias_company = jiancheng_list[quancheng_list.index(company)]

                #         # print('alias_company',alias_company)
                #         # print('company',company)
                #         # print('tfidf_result',tfidf_result)

                #         tfidf_value = 0
                #         if company in tfidf_result.keys():
                #             tfidf_value += tfidf_result[company]
                #             # print('tfidf_value',tfidf_value)
                #         if alias_company != '' and alias_company in tfidf_result.keys():
                #             tfidf_value += tfidf_result[alias_company]
                #         # print('tfidf_value',tfidf_value)

                #         relativity = tfidf_value * 10

                #         if relativity >= 1:
                #             relativity = 1

                #         relLabel = 0
                #         if relativity > 0.5:
                #             relLabel = 1

                #         relativity = str(round(relativity,2))
                #         relLabel = str(relLabel)

                #         # 舆情事件检索表
                #         try:
                #             # if relativity != '0':
                #             insert_news_search(db=db1,yqid=uid,objName=company,companyShortName=companyShortName,companyCode=companyCode,indirectObjName=company,transScore='1',relPath='',relType='直接关联',relScore=relativity,relLabel=relLabel,personName=personName,eventCode=eventCode,eventName=eventName,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,emoLabel=emoLabel,emoScore=emoScore,emoConf=emoConf,impScore=importanceDgree,impLabel=importanceLabel,pubTime=ctime,title=title,summary=summary,keyword=keyword,srcUrl=url,srcType='新闻',source=source,content=search_content,isValid=1,dataStatus=1)
                #         except:
                #             print('search',uid)


                elapsed = (time.clock() - start)
                # print("Time used 开始遍历公司之前:",elapsed)

                relativity_value = 0
                relativity_company = ''

                print('company_list',company_list)
                # 开始遍历公司：
                if len(company_list) > 0:
                    tfidf_result = tfidf_company(company_list,content)
                    # print('tfidf_result',tfidf_result)

                    companiesInfo = ''
                    for company in company_list:

                        # if len(company) > 2 and is_Chinese(company):
                    
                        # 初步筛选相似文章
                        title_sim,words_uid = get_sim_uid(db=db1,title=title,indexWords=company)


                        # 如标题完全相同，则直接判断为同一篇文章
                        for title_uid in title_sim:
                            # 先判断去重
                            if (uid,title_uid[0]) not in dupli_uid_pair_list:
                                dupli_uid_pair_list.append((uid,title_uid[0]))
                                insert_yq_sim(db=db1,yqid=uid,comYqid=title_uid[0],simScore='1',simYn='1',isValid=1,dataStatus=1)
                            else:
                                continue


                        # # 获得可能相似文章的内容列表
                        # if len(words_uid) > 0:
                        #     words_uid = list(list(zip(*words_uid))[0])
                        #     words_uid_list = [uid]
                        #     words_uid_list.extend(words_uid)
                        #     print('uid company',uid)
                        #     print('words_uid_list company',words_uid_list,len(words_uid_list))
                        #     # print('words_uid',words_uid,len(words_uid))
                        #     content_list = get_content(db=db1,uid_list=words_uid_list)
                        #     print('content_list',len(content_list))
                        #     if '' in content_list:
                        #         continue
                        #     result_list = sim_calculation(content_list)
                        #     if len(result_list) > 0:
                        #         for num,score in enumerate(result_list):
                        #             if (uid,words_uid[num]) not in dupli_uid_pair_list:
                        #                 dupli_uid_pair_list.append((uid,words_uid[num]))
                        #                 if score > 0.95:
                        #                     # print('score',score,type(score))
                        #                     simYn = '0'
                        #                     if score > 0.97:
                        #                         simYn = '1'
                        #                     if score >= 1:
                        #                         score = '1'
                        #                     else:
                        #                         score = str(round(score,2))
                        #                     insert_yq_sim(db=db1,yqid=uid,comYqid=words_uid[num],simScore=score,simYn=simYn,isValid=1,dataStatus=1)
                        #             else:
                        #                 continue

                        # 向比较相似度表中写数据
                        insert_sim_tmp(db=db1,uid=uid,title=title,indexWords=company,pubTime=ctime)

                        # print('company',company)
                        companyShortName = ''
                        companyCode = ''
                        relativity = 0
                        alias_company = ''

                        # 如果公司名称为简称，匹配全称和代码
                        if company in jiancheng_list:
                            if company in title:
                                relativity = 1
                            companyShortName = company
                            companyCode = code_list[jiancheng_list.index(company)]
                            alias_company = company
                            company = quancheng_list[jiancheng_list.index(company)]

                        # 如果公司名称为全称，匹配简称和代码
                        elif company in quancheng_list:
                            if company in title:
                                relativity = 1
                            companyShortName = jiancheng_list[quancheng_list.index(company)]
                            if companyShortName in title:
                                relativity = 1
                            companyCode = code_list[quancheng_list.index(company)]
                            alias_company = jiancheng_list[quancheng_list.index(company)]

                        # print('alias_company',alias_company)
                        # print('company',company)
                        # print('tfidf_result',tfidf_result)


                        tfidf_value = 0
                        if company in tfidf_result.keys():
                            tfidf_value += tfidf_result[company]
                            # print('tfidf_value',tfidf_value)
                        if alias_company != '' and alias_company in tfidf_result.keys():
                            tfidf_value += tfidf_result[alias_company]
                        # print('tfidf_value',tfidf_value)

                        relativity = tfidf_value * 10

                        if relativity > relativity_value:
                            relativity_company = company
                            relativity_value = relativity

                        if relativity >= 1:
                            relativity = 1

                        relLabel = 0
                        if relativity > 0.5:
                            relLabel = 1

                        relativity = str(round(relativity,2))
                        relLabel = str(relLabel)


                        # 插入数据统计表
                        insert_statistics(db=db1,yqid=uid,company=company,ctimeDate=ctimeDate,relType='直接关联',emoLabel=emoLabel,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,eventCode=eventCode,eventName=eventName)

                        # print('company',company)
                        # print('relativity',relativity)
                        
                        # 公司舆情相关性信息
                        if relativity != '0':
                            insert_cmp_yq_rel(db=db1,yqid=uid,companyName=company,relScore=relativity,relLabel=relLabel,isValid=1,dataStatus=1)
                        # 'yqid': uid, # yqid 舆情标识ID(业务主键)
                        # 'companyName': company, # companyName 公司名称
                        # 'relScore': relativity, # relScore 相关程度分值
                        # 'relLabel': relLabel, # relLabel 相关程度标签
                        # 'isValid': 1, # isValid 是否有效 0:否;1:是
                        # 'dataStatus': 1, # dataStatus  数据状态:1->新增;2->更新;3->删除

                        # 公司相关度信息整合
                        companiesInfo += (company + ':' + relativity + ',')

                        # 公司舆情情感信息
                        company_emoScore = ''
                        company_emoLabel = ''
                        if emoScore != '' and relativity != '':
                            company_emoScore = str(round(float(emoScore)*float(relativity)*2,2))
                            if decimal.Decimal(company_emoScore) < -1:
                                company_emoScore = '-1'
                            elif decimal.Decimal(company_emoScore) > 1:
                                company_emoScore = '1'
                            company_emoLabel = str(round(float(emoScore)*float(relativity)*2))
                            if decimal.Decimal(company_emoLabel) < -1:
                                company_emoLabel = '-1'
                            elif decimal.Decimal(company_emoLabel) > 1:
                                company_emoLabel = '1'
                        insert_cmp_yq_emo(db=db1,yqid=uid,eventCode=eventCode,companyName=company,emoScore=company_emoScore,emoLabel=company_emoLabel,emoConf=emoConf,isValid=1,dataStatus=1)
                        # 'yqid': uid, # yqid 舆情标识ID(业务主键)
                        # 'eventCode': '', # eventCode 舆情事件分类编码
                        # 'companyName': company, # companyName 公司名称
                        # 'emoScore': emoScore*relativity, # emoScore 情感分值
                        # 'emoLabel': round(emoLabel*relativity), # emoLabel 情感标签
                        # 'emoConf': emoConf, # emoConf 置信度
                        # 'isValid': 1, # isValid 是否有效 0:否;1:是
                        # 'dataStatus': 1, # dataStatus  数据状态:1->新增;2->更新;3->删除

                        indirectObjName = ''
                        transScore = ''
                        if indirectObjName == '':
                            indirectObjName = company
                            transScore = '1'

                        # 舆情事件检索表
                        try:
                    
                            # if relativity != '0':
                            insert_news_search(db=db1,yqid=uid,objName=company,companyShortName=companyShortName,companyCode=companyCode,indirectObjName=indirectObjName,transScore=transScore,relPath='',relType='直接关联',relScore=relativity,relLabel=relLabel,personName='',eventCode=eventCode,eventName=eventName,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,emoLabel=emoLabel,emoScore=emoScore,emoConf=emoConf,impScore=importanceDgree,impLabel=importanceLabel,pubTime=ctime,title=title,summary=summary,keyword=keyword,srcUrl=url,srcType='新闻',source=source,content=search_content,isValid=1,dataStatus=1)

                            # insert_news_search_add(db=db1,yqid=uid,objName=company,companyShortName=companyShortName,companyCode=companyCode,indirectObjName=indirectObjName,transScore=transScore,relPath='',relType='直接关联',relScore=relativity,relLabel=relLabel,personName='',eventCode=eventCode,eventName=eventName,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,emoLabel=emoLabel,emoScore=emoScore,emoConf=emoConf,impScore=importanceDgree,impLabel=importanceLabel,pubTime=ctime,title=title,summary=summary,keyword=keyword,srcUrl=url,srcType='新闻',source=source,content=search_content,isValid=1,dataStatus=1)

                        except:
                            print('search error',uid)

                        insert_mongo(db_name=db_name,yqid=uid,objName=company,companyShortName=companyShortName,companyCode=companyCode,indirectObjName=indirectObjName,transScore=transScore,relPath='',relType='直接关联',relScore=relativity,relLabel=relLabel,personName='',eventCode=eventCode,eventName=eventName,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,emoLabel=emoLabel,emoScore=emoScore,emoConf=emoConf,impScore=importanceDgree,impLabel=importanceLabel,pubTime=ctime,title=title,summary=summary,keyword=keyword,srcUrl=url,srcType='新闻',source=source,content=search_content)


                    # 舆情事件标题检索表
                    try:
                        insert_news_title_search(db=db1,yqid=uid,title=title,companiesInfo=companiesInfo.strip(','),eventCode=eventCode,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,emoLabel=emoLabel,emoScore=emoScore,emoConf=emoConf,impScore=importanceDgree,impLabel=importanceLabel,pubTime=ctime,summary=summary,srcUrl=url,srcType='新闻',content=search_content,relPath='',isValid=1,dataStatus=1)
                    except:
                        print('search',uid)

                else:
                    # 公司舆情相关性信息
                    insert_cmp_yq_rel(db=db1,yqid=uid,companyName='',relScore='',relLabel='',isValid=1,dataStatus=1)
                    # 'yqid': uid, # yqid 舆情标识ID(业务主键)
                    # 'companyName': '', # companyName 公司名称
                    # 'relScore': '', # relScore 相关程度分值
                    # 'relLabel': '', # relLabel 相关程度标签
                    # 'isValid': 1, # isValid 是否有效 0:否;1:是
                    # 'dataStatus': 1, # dataStatus  数据状态:1->新增;2->更新;3->删除

                    
                    # 公司舆情情感信息
                    insert_cmp_yq_emo(db=db1,yqid=uid,eventCode=eventCode,companyName='',emoScore='',emoLabel='',emoConf='',isValid=1,dataStatus=1)
                    # 'yqid': uid, # yqid 舆情标识ID(业务主键)
                    # 'eventCode': '', # eventCode 舆情事件分类编码
                    # 'companyName': '', # companyName 公司名称
                    # 'emoScore': '', # emoScore 情感分值
                    # 'emoLabel': '', # emoLabel 情感标签
                    # 'emoConf': '', # emoConf 置信度
                    # 'isValid': 1, # isValid 是否有效 0:否;1:是
                    # 'dataStatus': 1, # dataStatus  数据状态:1->新增;2->更新;3->删除


                # 将与文章关联关系最高的公司进行传导关联查询
                if relativity_company != '':
                    relation_path_add(db1=db1,db_name=db_name,relativity_company=relativity_company,uid=uid,ctimeDate=ctimeDate,emoLabel=emoLabel,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,relativity=relativity,relLabel=relLabel,eventCode=eventCode,eventName=eventName,emoScore=emoScore,emoConf=emoConf,importanceDgree=importanceDgree,importanceLabel=importanceLabel,ctime=ctime,title=title,summary=summary,keyword=keyword,url=url,srcType='新闻',source=source,content=search_content)

                    # # 获取关联公司数据
                    # r = requests.post('http://192.168.1.129:11068/relation/',data=json.dumps({'company':relativity_company.replace('(','（').replace(')','）')}))
                    # relation_result_list = r.json()['data']

                
                    # # 将间接关联数据插入数据统计表
                    # for relation_result in relation_result_list:

                    #     company = relation_result[0]
                    #     relativity_company = DBC2SBC(relation_result[1])
                    #     relativity_score = str(relation_result[2])
                    #     relPath = relation_result[3]

                    #     relativity_companyShortName = ''
                    #     relativity_companyCode = ''
                    #     if relativity_company in quancheng_list:
                    #         relativity_companyShortName = jiancheng_list[quancheng_list.index(relativity_company)]
                    #         relativity_companyCode = code_list[quancheng_list.index(relativity_company)]

                    #     insert_statistics(db=db1,yqid=uid,company=relativity_company.replace('(','（').replace(')','）'),ctimeDate=ctimeDate,relType='传导关联',emoLabel=emoLabel,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName)

                    #     insert_news_search(db=db1,yqid=uid,objName=company,companyShortName=relativity_companyShortName.replace('(','（').replace(')','）'),companyCode=relativity_companyCode,indirectObjName=relativity_company.replace('(','（').replace(')','）'),transScore=relativity_score,relPath=relPath,relType='传导关联',relScore=relativity,relLabel=relLabel,personName='',eventCode=eventCode,eventName=eventName,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,emoLabel=emoLabel,emoScore=emoScore,emoConf=emoConf,impScore=importanceDgree,impLabel=importanceLabel,pubTime=ctime,title=title,summary=summary,keyword=keyword,srcUrl=url,srcType='新闻',source=source,content=search_content,isValid=1,dataStatus=1)

                    #     insert_news_search_add(db=db1,yqid=uid,objName=company,companyShortName=relativity_companyShortName.replace('(','（').replace(')','）'),companyCode=relativity_companyCode,indirectObjName=relativity_company.replace('(','（').replace(')','）'),transScore=relativity_score,relPath=relPath,relType='传导关联',relScore=relativity,relLabel=relLabel,personName='',eventCode=eventCode,eventName=eventName,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,emoLabel=emoLabel,emoScore=emoScore,emoConf=emoConf,impScore=importanceDgree,impLabel=importanceLabel,pubTime=ctime,title=title,summary=summary,keyword=keyword,srcUrl=url,srcType='新闻',source=source,content=search_content,isValid=1,dataStatus=1)

                # 数据库关闭
                db1.commit()
                db1.close()
                # 关闭模型接口
                cnm.bclient_3.close()


                elapsed = (time.clock() - start)
                # print("Time used END！！！:",elapsed)

            # TODO 保存数据库
            # self.save_es(result_es)     # [ES]
        except:
            # 数据处理报错记录并返回error
            traceback.print_exc()
            globalLog.error(traceback.format_exc())
            return 'ERROR'
        else:
            return True
        finally:
            pool.close()


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