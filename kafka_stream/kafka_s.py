# -*- coding: utf-8 -*-
# @Author : wgq
# @time   : 2020/8/10 17:47
# @File   : kafka_s.py
# Software: PyCharm
"""
新三板-基于mongoshake =>> kafka 的流处理
1.读取kafka的数据
2.读取全称表的数据生成date数据
3.将kfka数据中的简称取出 -> 在全称的date匹配对应的全称 -> 将全称保存一个变量
4.利用全称进入行业表中进行关联查询 -> 获取行业信息保存变量
5.将分类表中的信息读出 -> 转成dateframe格式
6.利用kafka中的title信息 -> 和dateframe数据进行匹配 -> 获取到分类信息-情感分值-重要度分值 -> 利用分值匹配名称
                                                   ↓
                                                    →在每一次匹配时-都要检测分类表是否发生变化-发生变化-更新dateframe
7.将以保存的变量整合成需要的列表 -> 进行保存
"""
from kafka import KafkaConsumer
import json
from mysql_yi import mysql_kafka_sq
from kafka.structs import TopicPartition
from bson import BSON
import pandas as pd
from mysql_yi.mysql_pool import PymysqlPool
import uuid
import pymysql
import time, datetime
import pymongo
from dateutil import parser
from dwd_news_yq.bin.process_helper import relation_path_add
import redis

class Kafka_consumer_s():

    def __init__(self):
        """
        self.consumer：kafka-连接对象
        self.page：kafka-消费偏移量
        self.emo_label：情感分值-标签
        self.imp_label：重要程度分值-标签
        self.full_name_date：全称表的数据
        self.df：分类表的数据
        self.companyName：全称
        self.mysql_full_name：行业数据
        self.title：标题
        self.srcUrl：网源链接
        self.pubTime：发布时间
        self.cmpCode：简称代码
        self.yqid：唯一标识
        self.srcType：数据类型
        self.webname：来源
        self.cmpShortName：简称
        self.emoConf：置信度 A股信息没有
        self.firstLevelCode：一级分类编码
        self.firstLevelName：一级分类名称
        self.secondLevelCode：二级分类编码
        self.secondLevelName：二级分类名称
        self.threeLevelCode：三级分类编码
        self.threeLevelName：三级分类名称
        self.fourLevelCode：四级分类编码
        self.fourLevelName：司机分类名称
        self.eventCode：舆情事件分类编码
        self.eventName：舆情事件分类名称
        self.emoScore：情感分值
        self.emolabel：情感标签
        self.impScore：重要程度分值
        self.impLabel：重要程度标签
        self.comp_info：行业分级字典
        self.onlyId：mongo主键
        self.mydict : 储存的mongo数据
        self.other：判断是否匹配上分类表规则  0 = 没匹配上    1 = 匹配上了
        """

        self.consumer = KafkaConsumer(bootstrap_servers = ["192.168.1.172:9092"],auto_offset_reset='earliest',group_id='san_MY_GROUP')
        self.tp = TopicPartition('s_topic_s', 0)
        self.myclient = pymongo.MongoClient(
            "mongodb://root:shiye1805A@192.168.1.125:10011,192.168.1.126:10011,192.168.1.127:10011/admin")
        self.page = 0
        self.emo_label = {'1': '正向', '-1': '负向', '0': '中性'}
        self.imp_label = {'1': '相对不重要', '2': '相对不重要', '3': '相对不重要', '4': '重要', '5': '非常重要'}
        self.full_name_date = {}
        self.df = ""
        self.companyName = ""
        self.mysql_full_name = ()
        self.title = ""
        self.srcUrl = ""
        self.pubTime = ""
        self.cmpCode = ""
        self.yqid = ""
        self.srcType = ""
        self.webname = ""
        self.cmpShortName = ""
        self.emoConf = ""
        self.firstLevelCode = ""
        self.secondLevelCode = ""
        self.secondLevelName = ""
        self.threeLevelCode = ""
        self.threeLevelName = ""
        self.fourLevelCode = ""
        self.fourLevelName = ""
        self.eventCode = ""
        self.eventName = ""
        self.emoScore = ""
        self.emolabel = ""
        self.impScore = ""
        self.impLabel = ""
        self.comp_info = {}
        self.onlyId = ""
        self.mydict = {}
        self.other = 0
    # def mysql_client(self):
    #     return PymysqlPool('129')
    def mysql_client_125(self):
        return PymysqlPool('125')
    def mysql_client_180(self):
        return PymysqlPool('180')
    def redis_conn(self):
        """
        连接redis
        """
        pool = redis.ConnectionPool(host='192.168.1.149', port=6379, password='', db=15, decode_responses=True)  # 服务器
        r = redis.Redis(connection_pool=pool)
        return r

    def get_ssgsdmjc(self):
        if self.cmpShortName is None: self.cmpShortName = ''
        '''从上市公司表中获取公司简称、代码'''
        redis_data = self.redis_conn().hget('pd_bond_ann_ss:pp_gsjcqcdm_07', self.cmpShortName)
        redis_data_list = [eval(i) for i in redis_data.split('#*#') if i] if redis_data else []
        if redis_data_list:
            if len(redis_data_list) > 1:
                redis_data_list_tmp = [i for i in redis_data_list if i['cmp_code'] != '' and i['all_name'] != '']
                if len(redis_data_list_tmp) > 0: redis_data_list = redis_data_list_tmp
            redis_data_list = sorted(redis_data_list, key=lambda keys: keys['i_time'])
            return redis_data_list[-1]
        else:
            return {}
    def mysql_l(self):
        """
        连接行业表获取行业数据
        :param cmpShortName:
        :return:
        """
        if self.companyName:
            conn = PymysqlPool('industry')
            sql = "SELECT A.compName, A.categoryCode, B.constValueDesc, B.constCode FROM(SELECT * FROM seeyii_assets_database.sy_cd_ms_ind_comp_gm WHERE compName = '{}') AS A INNER JOIN ( SELECT * FROM seeyii_assets_database.sy_cd_mt_sys_const WHERE constCode IN ( 3, 4, 5 ) ) AS B ON A.categoryCode = B.cValue".format(
                self.companyName)
            counts, infos = conn.getAll(sql)
            return infos
        else:
            return ""


    def kafka_take_out(self):
        """
        连接kafka获取数据 将数据装换成字符串
        :return:
        """
        self.consumer.assign([self.tp])
        self.consumer.seek(self.tp,0)
        for each in self.consumer:
            # try:
                kafa_str = BSON.decode(each.value)
                if kafa_str:
                    self.kafka_data_processing(kafa_str)
            # except:
            #     print(each.value, "错误的数据")

    def kafka_data_processing(self,kafka_json):
        """
        处理数据  比对两个表
        :param kafka_json: 字典
        :return:
        """

        if kafka_json.get("o"):
            kafka_set = kafka_json.get("o")
            if kafka_set.get("$set"):
                pass
            else:
                print("进入kafka")
                self.page += 1
                print(self.page,"+++++++++++++++++++++++++++++++++++++++++++++++++")
                kafka_of = kafka_json.get("o")
                cmpShortName = kafka_of.get("st_name")
                title = kafka_of.get("title")
                self.cmpCode = kafka_of.get("st_code")
                pubTime = kafka_of.get("publish_date").strftime("%Y-%m-%d %H:%M:%S")
                dateArray = datetime.datetime.fromtimestamp(time.time() - 7 * 24 * 60 * 60)
                otherStyleTime = dateArray.strftime("%Y-%m-%d %H:%M:%S")
                print("进行日期比对")
                if pubTime > otherStyleTime:
                    srcUrl = kafka_of.get("url")
                    self.title = title
                    self.srcUrl = srcUrl
                    self.pubTime = pubTime
                    if self.pubTime < "2020-10-20":
                        return
                    self.cmpShortName = cmpShortName
                    print(self.cmpShortName)
                    "----------------------"
                    self.full_name_date = self.get_ssgsdmjc()
                    "----------------------"
                    self.companyName = self.full_name_date.get('all_name', '')
                    if len(self.companyName) < 7:
                        return
                    "----------------------"
                    mysql_full_name = self.mysql_l()
                    self.mysql_full_name = mysql_full_name
                    "----------------------"
                    self.pd_dataframe(title)
    def logs_dateframe(self):
        """
        检测分类表数据是否发生变化
        :return: 状态 False：有变化  True：无变化
        """
        with open("/shiye_kf3/gonggao/kafka_stream/logs/log_s.log","r") as r:
            date_time =  r.read()
        print(date_time)
        conn = self.mysql_client_180()
        sql = "SELECT count(id) FROM sy_yq_raw.sy_yq_lvl_rules_code_ggcf WHERE modifyTime >= '{}'".format(date_time)
        count, infos = conn.getAll(sql)
        conn.dispose()
        dateArray = datetime.datetime.fromtimestamp(time.time())
        otherStyleTime = dateArray.strftime("%Y-%m-%d %H:%M:%S")
        page = infos[0]["count(id)"]
        print(page,"每次查询的分类表变化数量")
        with open("/shiye_kf3/gonggao/kafka_stream/logs/log_s.log","w") as w:
            w.write(otherStyleTime)
        if page > 0:
            return False
        else:
            return True
    def pd_dataframe(self,test):
        """
        获取dateframe数据进行处理
        :param test:
        :return:
        """
        if test:
            if len(self.df):
                pass
            else:
                self.pandsa()
                print("第一次进入")
            bool_if = self.logs_dateframe()
            if bool_if:
                pass
                print("分类表数据无变化")
            else:
                self.pandsa()
                print("分类表存在变化--对dateframe进行修改")
            print(self.len_list)
            self.other = 0
            for i in range(self.len_list):
                inRules_list = [self.df.loc[i,"inRules"]][0]
                filterRules_list = [self.df.loc[i, "filterRules"]][0]
                in_list = [rule.strip() for rule in inRules_list.split('、') if inRules_list]
                in_lists = [rule.split('&') for rule in in_list]
                filter_rules = [[rule.strip()] for rule in filterRules_list.split('、') if filterRules_list]
                if_csv = self.list_if(in_lists,filter_rules,test)
                if if_csv:
                    print("需要存储")
                    self.other = 1
                    self.pands_dateframe_csv(i)
                else:
                    pass
            if self.other == 1:
                print("匹配上了")
            else:
                print("没匹配上")
                self.pands_dateframe_csv(0)
    def list_if(self,in_lists,filter_rules,test):

        """
        对传传入的数据进项判断
        :param in_lists: 符合要求的规则   判断test中的数据是否符合要求
        :param filter_rules: 过滤的规则   判断test中的数据是否不符合要求
        :param test: 标题
        :return:
        """
        is_match = False
        for words in in_lists:
            result = self.pandas_dataframe_if(words, test)
            if result == words:
                is_match = True
                break
        if filter_rules and is_match:
            for fwords in filter_rules:
                filter_result = self.pandas_dataframe_if(fwords, test)
                if filter_result == fwords:
                    is_match = False
                    break
        return is_match
    def pandas_dataframe_if(self,words,test):
        """
        处理已被切割的数据 讲判断结果返回调用方
        :param words:
        :param test:
        :return:
        """
        result = []
        for word in words:
            if word in test:
                result.append(word)
        return result
    def pandsa(self):
        """
        将从分类表中的数据取出 转换成dateframe
        :return:
        """
        conn = self.mysql_client_180()
        sql = "SELECT id,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,threeLevelCode,threeLevelName,fourLevelCode,fourLevelName,cfEventCode,eventCode,eventName,inRules,filterRules,emoScore,impScore,isChange,isValid,dataStatus FROM sy_yq_raw.sy_yq_lvl_rules_code_ggcf WHERE eventName != '其他公告' and inRules != '' and inRules IS NOT NULL"
        count, infos = conn.getAll(sql)
        conn.dispose()
        self.len_list = len(infos)
        df = pd.DataFrame(data=infos,columns=["id","firstLevelCode","firstLevelName","secondLevelCode","secondLevelName","threeLevelCode","threeLevelName","fourLevelCode","fourLevelName","cfEventCode","eventCode","eventName","inRules","filterRules","emoScore","impScore","isChange","isValid","dataStatus"])
        self.df = df
    def mysql_other_categories(self):
        """
        获取其他分类的数据
        :return:
        """
        conn = self.mysql_client_180()
        sql = "SELECT id,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,threeLevelCode,threeLevelName,fourLevelCode,fourLevelName,cfEventCode,eventCode,eventName,inRules,filterRules,emoScore,impScore,isChange,isValid,dataStatus FROM sy_yq_raw.sy_yq_lvl_rules_code_ggcf WHERE eventName = '其他公告';"
        count, infos = conn.getAll(sql)
        conn.dispose()
        return infos
    def mysql_full_name_data(self):
        """
        将顺序错乱的分类数据归位
        :return:
        """
        self.comp_info = {}
        if self.mysql_full_name:
            for sin in self.mysql_full_name:
                if sin.get("constCode") == 3:
                    self.comp_info['firstIndustry'] = sin.get("constValueDesc")
                    self.comp_info['firstIndustryCode'] = str(sin.get("categoryCode")) + "##" + str(sin.get("constCode"))
                elif sin.get("constCode") == 4:
                    self.comp_info['secondIndustry'] = sin.get("constValueDesc")
                    self.comp_info['secondIndustryCode'] = str(sin.get("categoryCode")) + "##" + str(sin.get("constCode"))
                elif sin.get("constCode") == 5:
                    self.comp_info['threeIndustry'] = sin.get("constValueDesc")
                    self.comp_info['threeIndustryCode'] = str(sin.get("categoryCode")) + "##" + str(sin.get("constCode"))
    def mysql_redis_relType(self):
        """
        将数据传入判断传导关系  直接传导就返回   间接传导储存
        :return:
        """
        mydb = self.myclient["sy_project_raw"]
        conn = self.mysql_client_125()
        connn = conn._conn
        conn_180 = self.mysql_client_180()
        connn_180 = conn_180._conn
        timeArray = time.strptime(self.pubTime, "%Y-%m-%d %H:%M:%S")
        ctime = time.strftime("%Y-%m-%d", timeArray)
        keyword = ""
        summary = ""
        search_content = ""
        relLabel = ""
        relScore = ""
        relation_path_add(db1=connn, db2=connn_180, db_name=mydb, relativity_company=self.companyName, uid=self.yqid, ctimeDate=ctime,
                          emoLabel=self.emolabel, firstLevelCode=self.firstLevelCode, firstLevelName=self.firstLevelName, secondLevelCode=self.secondLevelCode,
                          secondLevelName=self.secondLevelName, thirdLevelCode=self.threeLevelCode, thirdLevelName=self.threeLevelName, fourthLevelCode=self.fourLevelCode,
                          fourthLevelName=self.fourLevelName, relativity=relScore, relLabel=relLabel, eventCode=self.eventCode, eventName=self.eventName, emoScore=self.emoScore,
                          emoConf=self.emoConf, importanceDgree=self.impScore, importanceLabel=self.impLabel, ctime=self.pubTime, title=self.title, summary=summary, keyword=keyword,
                          url=self.srcUrl,srcType=self.srcType, source=self.webname, content=search_content)
        conn.dispose()
        conn_180.dispose()
    def pands_dateframe_csv(self,i):
        """
        将结果保存
        :param i: dateframe数据定位
        :return:
        """
        self.webname = "全国中小企业股份转让系统"
        self.srcType = "新三板公告"
        print(self.title)

        if self.other == 1:
            self.mysql_full_name_data()
            self.yqid = self.add_uuid(self.title + self.srcUrl + str(self.pubTime))
            self.firstLevelCode = self.df.loc[i, "firstLevelCode"]
            self.firstLevelName = self.df.loc[i, "firstLevelName"]
            self.secondLevelCode = self.df.loc[i, "secondLevelCode"]
            self.secondLevelName = self.df.loc[i, "secondLevelName"]
            self.threeLevelCode = self.df.loc[i, "threeLevelCode"]
            self.threeLevelName = self.df.loc[i, "threeLevelName"]
            self.fourLevelCode = self.df.loc[i, "fourLevelCode"]
            if self.df.loc[i, "fourLevelName"]:
                self.fourLevelName = self.df.loc[i, "fourLevelName"]
            else:
                self.fourLevelName = ""
            self.eventCode = self.df.loc[i, "eventCode"]
            self.eventName = self.df.loc[i, "eventName"]
            self.emoScore = self.df.loc[i, "emoScore"]
            self.impScore = self.df.loc[i, "impScore"]
            list_g = self.list_mysql_g_gao()
            list_yu = self.list_mysql_u_s()
            if self.companyName:
                print(list_g)
                print("``````````````")
                print(list_yu)
                # self.mysql_insert_g_gao(list_g)
                # self.mysql_insert_u_yuqing(list_yu)
                print("写入180==================================================")
                self.mysql_insert_g_gao_to(list_g)
                self.mysql_insert_u_yuqing_to(list_yu)
                print("写入统计表==============")
                # self.mysql_insert_stat()
                self.mysql_insert_stat_180()
                print("写入预警sq表============================================")
                # self.mysql_insert_alert()
                self.mysql_insert_alert_180()
                print("附表=========================================================")
                try:
                    self.mysql_insert_bsae()
                except:
                    print("yqid重复")
                self.mysql_insert_cls()
                self.mysql_insert_code()
                self.mysql_insert_emo()
                self.mysql_insert_imp()
                print("mongo_________________________________________________________")
                self.mongo_insert()
        else:
            self.mysql_full_name_data()
            self.yqid = self.add_uuid(self.title + self.srcUrl + str(self.pubTime))
            mysql_on = self.mysql_other_categories()
            self.firstLevelCode = mysql_on[0]["firstLevelCode"]
            self.firstLevelName = mysql_on[0]["firstLevelName"]
            self.secondLevelCode = mysql_on[0]["secondLevelCode"]
            self.secondLevelName = mysql_on[0]["secondLevelName"]
            self.threeLevelCode = mysql_on[0]["threeLevelCode"]
            self.threeLevelName = mysql_on[0]["threeLevelName"]
            self.fourLevelCode = mysql_on[0]["fourLevelCode"]
            try:
                self.fourLevelName = mysql_on[0]["fourLevelName"]
            except:
                self.fourLevelName = ""
            self.eventCode = mysql_on[0]["eventCode"]
            self.eventName = mysql_on[0]["eventName"]
            self.emoScore = mysql_on[0]["emoScore"]
            self.impScore = mysql_on[0]["impScore"]

            list_g = self.list_mysql_g_gao()
            list_yu = self.list_mysql_u_s()
            print(list_g)
            if self.companyName:
                print("``````````````")
                print(list_yu)
                # self.mysql_insert_g_gao(list_g)
                # self.mysql_insert_u_yuqing(list_yu)
                print("写入180==================================================")
                self.mysql_insert_g_gao_to(list_g)
                self.mysql_insert_u_yuqing_to(list_yu)
                print("写入统计表==============")
                # try:
                #     self.mysql_insert_stat()
                # except:
                #     print("写入统计表重复")
                try:
                    self.mysql_insert_stat_180()
                except:
                    print("写入统计表重复")
                print("写入预警sq表============================================")
                # try:
                #     self.mysql_insert_alert()
                # except:
                #     print("预警信息重复")
                try:
                    self.mysql_insert_alert_180()
                except:
                    print("预警信息重复")
                print("附表=========================================================")
                # try:
                #     self.mysql_insert_bsae()
                # except:
                #     print("yqid重复")
                # self.mysql_insert_cls()
                # self.mysql_insert_code()
                # self.mysql_insert_emo()
                # self.mysql_insert_imp()
                # print("mongo_________________________________________________________")
                # self.mongo_insert()
        print("查看传导关系****************************")
        if self.companyName:
            self.mysql_redis_relType()

    def list_mysql_g_gao(self):
        """
        将数据组合 组合成公告表需要的结构
        :return:
        """

        list_mysql_g_s = []
        list_mysql_g = []
        list_mysql_g.append(self.yqid)
        list_mysql_g.append(self.title)
        list_mysql_g.append(self.webname)
        list_mysql_g.append(self.companyName)
        list_mysql_g.append(self.cmpShortName)
        list_mysql_g.append(self.cmpCode)
        list_mysql_g.append("")
        list_mysql_g.append("")
        list_mysql_g.append("")
        list_mysql_g.append(self.comp_info.get("firstIndustry",""))
        list_mysql_g.append(self.comp_info.get("firstIndustryCode",""))
        list_mysql_g.append(self.comp_info.get("secondIndustry",""))
        list_mysql_g.append(self.comp_info.get("secondIndustryCode",""))
        list_mysql_g.append(self.comp_info.get("threeIndustry",""))
        list_mysql_g.append(self.comp_info.get("threeIndustryCode",""))
        list_mysql_g.append(self.firstLevelCode)
        list_mysql_g.append(self.firstLevelName)
        list_mysql_g.append(self.secondLevelCode)
        list_mysql_g.append(self.secondLevelName)
        list_mysql_g.append(self.threeLevelCode)
        list_mysql_g.append(self.threeLevelName)
        list_mysql_g.append(self.fourLevelCode)
        list_mysql_g.append(self.fourLevelName)
        list_mysql_g.append(self.eventCode)
        list_mysql_g.append(self.eventName)
        list_mysql_g.append(self.emoScore)
        self.emolabel = self.emoLabel_i()
        list_mysql_g.append(self.emolabel)
        list_mysql_g.append(self.emoConf)
        list_mysql_g.append(self.impScore)
        self.impLabel = self.list_impLabel_i()
        list_mysql_g.append(self.impLabel)
        list_mysql_g.append(self.srcType)
        list_mysql_g.append(self.srcUrl)
        list_mysql_g.append(self.pubTime)
        list_mysql_g_s.append(list_mysql_g)
        return list_mysql_g_s
    def list_mysql_u_s(self):
        """
        将数据组合成 舆情表需要的结构
        :return:
        """
        transScore = ""
        relPath = ""
        personName = ""
        relType = "直接关联"
        summary = ""
        keyword = ""
        content = ""
        relScore = ""
        relLabel = ""
        list_mysql_u_s = []
        list_mysql_u = []
        list_mysql_u.append(self.yqid)
        list_mysql_u.append(self.companyName)
        list_mysql_u.append(self.cmpShortName)
        list_mysql_u.append(self.cmpCode)
        list_mysql_u.append(self.companyName)
        list_mysql_u.append(transScore)
        list_mysql_u.append(relPath)
        list_mysql_u.append(relType)
        list_mysql_u.append(relScore)
        list_mysql_u.append(relLabel)
        list_mysql_u.append(personName)
        list_mysql_u.append(self.eventCode)
        list_mysql_u.append(self.eventName)
        list_mysql_u.append(self.firstLevelCode)
        list_mysql_u.append(self.firstLevelName)
        list_mysql_u.append(self.secondLevelCode)
        list_mysql_u.append(self.secondLevelName)
        list_mysql_u.append(self.threeLevelCode)
        list_mysql_u.append(self.threeLevelName)
        list_mysql_u.append(self.fourLevelCode)
        list_mysql_u.append(self.fourLevelName)
        self.emolabel = self.emoLabel_i()
        list_mysql_u.append(self.emolabel)
        list_mysql_u.append(self.emoScore)
        list_mysql_u.append(self.emoConf)
        list_mysql_u.append(self.impScore)
        self.impLabel = self.list_impLabel_i()
        list_mysql_u.append(self.impLabel)
        list_mysql_u.append(self.pubTime)
        list_mysql_u.append(self.title)
        list_mysql_u.append(summary)
        list_mysql_u.append(keyword)
        list_mysql_u.append(self.srcUrl)
        list_mysql_u.append(self.srcType)
        list_mysql_u.append(self.webname)
        list_mysql_u.append(content)
        list_mysql_u_s.append(list_mysql_u)
        return list_mysql_u_s
    def list_mysql_insert_bsae(self):
        """
        将数据组合成base表所用列表
        :return:
        """
        list_base_s = []
        list_base = []
        list_base.append(self.yqid)
        list_base.append(self.title)
        list_base.append(self.webname)
        list_base.append(self.srcType)
        list_base.append(self.srcUrl)
        list_base.append(self.pubTime)
        list_base_s.append(list_base)
        return list_base_s
    def list_mysql_insert_imp(self):
        """
        将数据组合成imp表所用的列表
        :return:
        """
        list_imp_s = []
        list_imp = []
        list_imp.append(self.yqid)
        list_imp.append(self.eventCode)
        list_imp.append(self.impScore)
        list_imp.append(self.impLabel)
        list_imp_s.append(list_imp)
        return list_imp_s
    def list_mysql_insert_cls(self):
        """
        将数据组合成cls表所用的列表
        :return:
        """
        list_cls_s = []
        list_clas = []
        list_clas.append(self.yqid)
        list_clas.append(self.eventCode)
        list_cls_s.append(list_clas)
        return list_cls_s
    def list_mysql_insert_code(self):
        """
        将数据组合成code表所用的列表
        :return:
        """
        list_code_s = []
        list_code = []
        list_code.append(self.firstLevelCode)
        list_code.append(self.firstLevelName)
        list_code.append(self.secondLevelCode)
        list_code.append(self.secondLevelName)
        list_code.append(self.threeLevelCode)
        list_code.append(self.threeLevelName)
        list_code.append(self.fourLevelCode)
        list_code.append(self.fourLevelName)
        list_code.append(self.eventCode)
        list_code.append(self.eventName)
        list_code_s.append(list_code)
        return list_code_s
    def list_mysql_insert_emo(self):
        """
        将数据组合成emo表所用的数据
        :return:
        """
        list_emo_s = []
        list_emo = []
        list_emo.append(self.yqid)
        list_emo.append(self.eventCode)
        list_emo.append(self.companyName)
        list_emo.append(self.emoScore)
        list_emo.append(self.emolabel)
        list_emo.append(self.emoConf)
        list_emo_s.append(list_emo)
        return list_emo_s

    def list_mysql_insert_stat(self):
        list_stat_s = []
        list_stat = []
        relType = "直接关联"
        timeArray = time.strptime(self.pubTime, "%Y-%m-%d %H:%M:%S")
        ctime = time.strftime("%Y-%m-%d", timeArray)
        list_stat.append(self.yqid)
        list_stat.append(self.companyName)
        list_stat.append(ctime)
        list_stat.append(relType)
        self.emolabel = self.emoLabel_i()
        list_stat.append(self.emolabel)
        list_stat.append(self.firstLevelCode)
        list_stat.append(self.firstLevelName)
        list_stat.append(self.secondLevelCode)
        list_stat.append(self.secondLevelName)
        list_stat.append(self.threeLevelCode)
        list_stat.append(self.threeLevelName)
        list_stat.append(self.fourLevelCode)
        list_stat.append(self.fourLevelName)
        list_stat.append(self.eventCode)
        list_stat.append(self.eventName)
        list_stat_s.append(list_stat)
        return list_stat_s
    def list_mysql_insert_alert(self):
        list_alert_s = []
        list_alert = []
        relType = "直接关联"
        list_alert.append(self.yqid)
        list_alert.append(self.companyName)
        list_alert.append(self.companyName)
        list_alert.append(relType)
        list_alert.append(self.eventCode)
        list_alert.append(self.eventName)
        list_alert.append(self.firstLevelCode)
        list_alert.append(self.firstLevelName)
        list_alert.append(self.secondLevelCode)
        list_alert.append(self.secondLevelName)
        list_alert.append(self.threeLevelCode)
        list_alert.append(self.threeLevelName)
        list_alert.append(self.fourLevelCode)
        list_alert.append(self.fourLevelName)
        self.emolabel = self.emoLabel_i()
        list_alert.append(self.emolabel)
        list_alert.append(self.emoScore)
        list_alert.append(self.impScore)
        list_alert.append(self.impLabel)
        list_alert.append(self.pubTime)
        list_alert_s.append(list_alert)
        return list_alert_s
    def list_impLabel_i(self):
        """
        重要度处理
        :param impScore: 重要度分值
        :return: 重要度标签
        """
        imp_label = self.imp_label.get(str(self.impScore))
        return imp_label
    def emoLabel_i(self):
        """
        情感处理
        :param emoScore: 情感分值
        :return: 情感标签
        """
        emo_label = self.emo_label.get(str(self.emoScore))
        return emo_label
    def add_uuid(self,data):
        """
        对字符串进行加密
        :return: 加密字符串
        """
        data = uuid.uuid3(uuid.NAMESPACE_DNS, data)
        data = str(data)
        result_data = data.replace('-', '')
        return result_data

    def mysql_insert_g_gao(self,result):
        """
        储存数据  公告表 --125
        :param result:
        :return:
        """
        conn = self.mysql_client_125()
        sql = """INSERT IGNORE INTO sy_project_raw.dwa_me_gg_search (yqid,
                                                                    title,
                                                                    webname,
                                                                    companyName,
                                                                    cmpShortName,
                                                                    cmpCode,
                                                                    bondFull,
                                                                    bondAbbr,
                                                                    bondCode,
                                                                    firstIndustry,
                                                                    firstIndustryCode,
                                                                    secondIndustry,
                                                                    secondIndustryCode,
                                                                    threeIndustry,
                                                                    threeIndustryCode,
                                                                    firstLevelCode,
                                                                    firstLevelName,
                                                                    secondLevelCode,
                                                                    secondLevelName,
                                                                    threeLevelCode,
                                                                    threeLevelName,
                                                                    fourLevelCode,
                                                                    fourLevelName,
                                                                    eventCode,
                                                                    eventName,
                                                                    emoScore,
                                                                    emoLabel,
                                                                    emoConf,
                                                                    impScore,
                                                                    impLabel,
                                                                    srcType,
                                                                    srcUrl,
                                                                    pubTime) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        conn.insertMany(sql, result)
        conn.dispose()
        print("已存入--aa_dws_ggyq_search--125")

    def mysql_insert_g_gao_to(self,result):
        """
        储存数据  公告表 --180
        :param result:
        :return:
        """
        conn = self.mysql_client_180()
        sql = """INSERT IGNORE INTO sy_project_raw.dwa_me_gg_search (yqid,
                                                                    title,
                                                                    webname,
                                                                    companyName,
                                                                    cmpShortName,
                                                                    cmpCode,
                                                                    bondFull,
                                                                    bondAbbr,
                                                                    bondCode,
                                                                    firstIndustry,
                                                                    firstIndustryCode,
                                                                    secondIndustry,
                                                                    secondIndustryCode,
                                                                    threeIndustry,
                                                                    threeIndustryCode,
                                                                    firstLevelCode,
                                                                    firstLevelName,
                                                                    secondLevelCode,
                                                                    secondLevelName,
                                                                    threeLevelCode,
                                                                    threeLevelName,
                                                                    fourLevelCode,
                                                                    fourLevelName,
                                                                    eventCode,
                                                                    eventName,
                                                                    emoScore,
                                                                    emoLabel,
                                                                    emoConf,
                                                                    impScore,
                                                                    impLabel,
                                                                    srcType,
                                                                    srcUrl,
                                                                    pubTime) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        conn.insertMany(sql, result)
        conn.dispose()
        print("已存入--aa_dws_ggyq_search--180")
    def mysql_insert_u_yuqing(self,result):
        """
        储存数据  舆情表 --125
        :param result:
        :return:
        """
        conn = self.mysql_client_125()
        sql = """INSERT IGNORE INTO sy_project_raw.dwa_me_yq_search (yqid,
                                                                    objName,
                                                                    companyShortName,
                                                                    companyCode,
                                                                    indirectObjName,
                                                                    transScore,
                                                                    relPath,
                                                                    relType,
                                                                    relScore,
                                                                    relLabel,
                                                                    personName,
                                                                    eventCode,
                                                                    eventName,
                                                                    firstLevelCode,
                                                                    firstLevelName,
                                                                    secondLevelCode,
                                                                    secondLevelName,
                                                                    thirdLevelCode,
                                                                    thirdLevelName,
                                                                    fourthLevelCode,
                                                                    fourthLevelName,
                                                                    emoLabel,
                                                                    emoScore,
                                                                    emoConf,
                                                                    impScore,
                                                                    impLabel,
                                                                    pubTime,
                                                                    title,
                                                                    summary,
                                                                    keyword,
                                                                    srcUrl,
                                                                    srcType,
                                                                    source,
                                                                    content) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        conn.insertMany(sql, result)
        conn.dispose()
        print("已存入--dwa_me_yq_search--125")

    def mysql_insert_u_yuqing_to(self, result):
        """
        储存数据  舆情表 --180
        :param result:
        :return:
        """
        conn = self.mysql_client_180()
        sql = """INSERT IGNORE INTO sy_project_raw.dwa_me_yq_search (yqid,
                                                                    objName,
                                                                    companyShortName,
                                                                    companyCode,
                                                                    indirectObjName,
                                                                    transScore,
                                                                    relPath,
                                                                    relType,
                                                                    relScore,
                                                                    relLabel,
                                                                    personName,
                                                                    eventCode,
                                                                    eventName,
                                                                    firstLevelCode,
                                                                    firstLevelName,
                                                                    secondLevelCode,
                                                                    secondLevelName,
                                                                    thirdLevelCode,
                                                                    thirdLevelName,
                                                                    fourthLevelCode,
                                                                    fourthLevelName,
                                                                    emoLabel,
                                                                    emoScore,
                                                                    emoConf,
                                                                    impScore,
                                                                    impLabel,
                                                                    pubTime,
                                                                    title,
                                                                    summary,
                                                                    keyword,
                                                                    srcUrl,
                                                                    srcType,
                                                                    source,
                                                                    content) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        conn.insertMany(sql, result)
        conn.dispose()
        print("已存入--dwa_me_yq_search--180")

    def mysql_insert_bsae(self):
        """
        储存数据  base表
        :param result:
        :return:
        """
        result = self.list_mysql_insert_bsae()
        conn = self.mysql_client_180()
        sql = """INSERT INTO sy_yq_raw.dwd_me_yq_base_add (yqid,
                                                                title,
                                                                webname,
                                                                srcType,
                                                                srcUrl,
                                                                pubTime                                                                                                                                    
                                                                    ) VALUES(%s, %s, %s, %s, %s, %s);"""
        conn.insertMany(sql, result)
        conn.dispose()
        print("已存入--dwd_me_yq_base_add")
    def mysql_insert_imp(self):
        """
        储存数据  imp表
        :param result:
        :return:
        """
        result = self.list_mysql_insert_imp()
        conn = self.mysql_client_180()
        sql = """INSERT INTO sy_yq_raw.dwd_me_yq_imp_add (yqid,
                                                                    eventCode,
                                                                    impScore,
                                                                    impLabel
                                                                    ) VALUES(%s, %s, %s, %s);"""
        conn.insertMany(sql, result)
        conn.dispose()
        print("已存入--dwd_me_yq_imp_add")
    def mysql_insert_cls(self):
        """
        储存数据  cls表
        :param result:
        :return:
        """
        result = self.list_mysql_insert_cls()
        conn = self.mysql_client_180()
        sql = """INSERT INTO sy_yq_raw.dwd_me_yq_cls_add (yqid,
                                                                eventCode
                                                                    ) VALUES(%s, %s);"""
        conn.insertMany(sql, result)
        conn.dispose()
        print("已存入--dwd_me_yq_cls_add")
    def mysql_insert_code(self):
        """
        储存数据  code表
        :param result:
        :return:
        """
        result = self.list_mysql_insert_code()
        conn = self.mysql_client_180()
        sql = """INSERT INTO sy_yq_raw.dwd_me_yq_lvl_code_add (
                                                                    firstLevelCode,
                                                                    firstLevelName,
                                                                    secondLevelCode,
                                                                    secondLevelName,
                                                                    thirdLevelCode,
                                                                    thirdLevelName,
                                                                    fourthLevelCode,
                                                                    fourthLevelName,
                                                                    eventCode,
                                                                    eventName
                                                                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        conn.insertMany(sql, result)
        conn.dispose()
        print("已存入--dwd_me_yq_lvl_code_add")
    def mysql_insert_emo(self):
        """
        储存数据  emo表
        :param result:
        :return:
        """
        result = self.list_mysql_insert_emo()
        conn = self.mysql_client_180()
        sql = """INSERT INTO sy_yq_raw.dwd_me_cmp_yq_emo_add (yqid,
                                                                    eventCode,
                                                                    companyName,
                                                                    emoScore,
                                                                    emoLabel,
                                                                    emoConf
                                                                    ) VALUES(%s, %s, %s, %s, %s, %s);"""
        conn.insertMany(sql, result)
        conn.dispose()
        print("已存入--dwd_me_cmp_yq_emo_add")
    def mysql_insert_stat(self):
        """
        储存数据  stat表
        :param result:
        :return:
        """
        result = self.list_mysql_insert_stat()
        conn = self.mysql_client_125()
        sql = """INSERT IGNORE INTO sy_project_raw.dwa_me_yq_stat (yqid,
                                                              company,
                                                              ctimeDate,
                                                              relType,
                                                              emoLabel,
                                                              firstLevelCode,
                                                              firstLevelName,
                                                              secondLevelCode,
                                                              secondLevelName,
                                                              thirdLevelCode,
                                                              thirdLevelName,
                                                              fourthLevelCode,
                                                              fourthLevelName,
                                                              eventCode,
                                                              eventName
                                                            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        conn.insertMany(sql, result)
        conn.dispose()
        print("已存入--dwa_me_yq_stat")
    def mysql_insert_stat_180(self):
        """
        储存数据  stat表
        :param result:
        :return:
        """
        result = self.list_mysql_insert_stat()
        conn = self.mysql_client_180()
        sql = """INSERT IGNORE INTO sy_project_raw.dwa_me_yq_stat (yqid,
                                                              company,
                                                              ctimeDate,
                                                              relType,
                                                              emoLabel,
                                                              firstLevelCode,
                                                              firstLevelName,
                                                              secondLevelCode,
                                                              secondLevelName,
                                                              thirdLevelCode,
                                                              thirdLevelName,
                                                              fourthLevelCode,
                                                              fourthLevelName,
                                                              eventCode,
                                                              eventName
                                                            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        conn.insertMany(sql, result)
        conn.dispose()
        print("已存入--dwa_me_yq_stat180")
    def mysql_insert_alert(self):
        """
        储存数据  alert表
        :param result:
        :return:
        """
        result = self.list_mysql_insert_alert()
        conn = self.mysql_client_125()
        sql = """INSERT IGNORE INTO sy_project_raw.dwa_me_yq_search_alert (yqid,
                                                                  objName,
                                                                  indirectObjName,
                                                                  relType,
                                                                  eventCode,
                                                                  eventName,
                                                                  firstLevelCode,
                                                                  firstLevelName,
                                                                  secondLevelCode,
                                                                  secondLevelName,
                                                                  thirdLevelCode,
                                                                  thirdLevelName,
                                                                  fourthLevelCode,
                                                                  fourthLevelName,
                                                                  emoLabel,
                                                                  emoScore,
                                                                  impScore,
                                                                  impLabel,
                                                                  pubTime
                                                            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        conn.insertMany(sql, result)
        conn.dispose()
        print("已存入--dwa_me_yq_search_alert")

    def mysql_insert_alert_180(self):
        """
        储存数据  alert表
        :param result:
        :return:
        """
        result = self.list_mysql_insert_alert()
        conn = self.mysql_client_180()
        sql = """INSERT IGNORE INTO sy_project_raw.dwa_me_yq_search_alert (yqid,
                                                                  objName,
                                                                  indirectObjName,
                                                                  relType,
                                                                  eventCode,
                                                                  eventName,
                                                                  firstLevelCode,
                                                                  firstLevelName,
                                                                  secondLevelCode,
                                                                  secondLevelName,
                                                                  thirdLevelCode,
                                                                  thirdLevelName,
                                                                  fourthLevelCode,
                                                                  fourthLevelName,
                                                                  emoLabel,
                                                                  emoScore,
                                                                  impScore,
                                                                  impLabel,
                                                                  pubTime
                                                            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        conn.insertMany(sql, result)
        conn.dispose()
        print("已存入--dwa_me_yq_search_alert180")
    def time_tll(self):
        times = time.time() - 28800
        dateArray = datetime.datetime.fromtimestamp(times)
        otherStyleTime = dateArray.strftime("%Y-%m-%d %H:%M:%S")
        myDatetime = parser.parse(otherStyleTime)
        return myDatetime
    def mongo_date(self):
        """
        组成mongo所需要的的字典
        :return:
        """
        times = time.time() - 28800
        dateArray = datetime.datetime.fromtimestamp(time.time())
        otherStyleTime = dateArray.strftime("%Y-%m-%d %H:%M:%S")
        myDatetime = parser.parse(otherStyleTime)
        transScore = ""
        relPath = ""
        personName = ""
        relType = "直接关联"
        summary = ""
        keyword = ""
        content = ""
        relScore = ""
        relLabel = ""
        self.emolabel = self.emoLabel_i()
        self.impLabel = self.list_impLabel_i()
        self.mydict["onlyId"] = self.onlyId
        self.mydict["yqid"] = self.yqid
        self.mydict["objName"] = self.companyName
        self.mydict["companyShortName"] = self.cmpShortName
        self.mydict["companyCode"] = self.cmpCode
        self.mydict["indirectObjName"] = self.companyName
        self.mydict["transScore"] = transScore
        self.mydict["relPath"] = relPath
        self.mydict["relType"] = relType
        self.mydict["relScore"] = relScore
        self.mydict["relLabel"] = relLabel
        self.mydict["personName"] = personName
        self.mydict["eventCode"] = self.eventCode
        self.mydict["eventName"] = self.eventName
        self.mydict["firstLevelCode"] = self.firstLevelCode
        self.mydict["firstLevelName"] = self.firstLevelName
        self.mydict["secondLevelCode"] = self.secondLevelCode
        self.mydict["secondLevelName"] = self.secondLevelName
        self.mydict["thirdLevelCode"] = self.threeLevelCode
        self.mydict["thirdLevelName"] = self.threeLevelName
        self.mydict["fourthLevelCode"] = self.fourLevelCode
        self.mydict["fourthLevelName"] = self.fourLevelName
        self.mydict["emoLabel"] = self.emolabel
        self.mydict["emoScore"] = self.emoScore
        self.mydict["emoConf"] = self.emoConf
        self.mydict["impScore"] = self.impScore
        self.mydict["impLabel"] = self.impLabel
        self.mydict["pubTime"] = self.pubTime
        self.mydict["title"] = self.title
        self.mydict["summary"] = summary
        self.mydict["keyword"] = keyword
        self.mydict["srcUrl"] = self.srcUrl
        self.mydict["srcType"] = self.srcType
        self.mydict["source"] = self.webname
        self.mydict["content"] = content
        self.mydict["isValid"] = 1
        self.mydict["dataStatus"] = 1
        myDatetime = self.time_tll()
        self.mydict["ttlTime"] = myDatetime
        self.mydict["createTime"] = otherStyleTime
        self.mydict["modifyTime"] = otherStyleTime
    def mongo_insert(self):
        """
        将数据储存在mongo中
        :return:
        """
        mydb = self.myclient["sy_project_raw"]
        mycol = mydb["dwa_me_yq_search"]
        self.onlyId = self.add_uuid(self.yqid+self.companyName+str(self.eventCode))
        self.mongo_date()
        print(self.mydict)
        my_dict_new = {}
        try:
            my_dict_new.update(self.mydict)
            mycol.insert(my_dict_new)
        except:
            print("重复")
        print("保存mongo数据一份")
def main():
    """
    启动方法并处理准备参数
    :return:
    """
    kafka_losd = Kafka_consumer_s()
    kafka_losd.kafka_take_out()
if __name__ == '__main__':
    main()

