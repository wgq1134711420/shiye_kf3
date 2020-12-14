#-*-coding:utf-8-*-

'''
文件名： process_helper.py
功能： 新闻舆情处理辅助程序

代码历史：
2020-06-29：郭蓟申 创建代码
'''

# from collections import defaultdict
import os
import re
# import jieba
import codecs
import decimal
import pymongo
import pymysql
import time
import datetime
import sys
import json
import requests
import uuid
import redis
from dateutil import parser
# from sklearn.feature_extraction.text import TfidfVectorizer,TfidfTransformer,CountVectorizer

# from bin.ner_bc import BertClient #调用128服务器上的提取公司名称模型服务
# from DBUtils.PooledDB import PooledDB
# from sshtunnel import SSHTunnelForwarder


#！
# def quan_jian_code():

#     # # 读取连接数据库配置文件
#     # load_config='/home/seeyii/increase_nlp/db_config.json'
#     # with open(load_config,'r', encoding="utf-8") as f:
#     #     reader = f.readlines()[0]
#     # config_local = json.loads(reader)

#     # 创建公司全称和简称列表
#     # db2 = pymysql.connect(**config_local['129_sql'])
#     db2 = pymysql.connect(**{"host":"192.168.1.129","user":"aliyun_rw","password":"shiye_aliyun","db":"EI_BDP","port":3306,"charset":"utf8"})

#     cursor2 = db2.cursor()
#     quancheng_list = []
#     jiancheng_list = []
#     code_list = []
#     cursor2.execute('SELECT * FROM A_stock_code_name_fyi')
#     co_names = list(cursor2.fetchall())
#     cn1 = [desc[0] for desc in cursor2.description]
#     for i in range(len(co_names)):
#         quancheng_list.append(list(co_names[i])[cn1.index('all_name')])
#         jiancheng_list.append(list(co_names[i])[cn1.index('short_name')])
#         code_list.append(list(co_names[i])[cn1.index('cmp_code')])
#     # print('quancheng_list:',len(quancheng_list))
#     # print('jiancheng_list:',len(jiancheng_list))
#     # print('code_list:',len(code_list))

#     # dwd_news_all_short_name
#     cursor2.execute('SELECT * FROM dwd_news_all_short_name')
#     co_names = list(cursor2.fetchall())
#     cn1 = [desc[0] for desc in cursor2.description]
#     for i in range(len(co_names)):
#         quancheng_list.append(list(co_names[i])[cn1.index('all_name')])
#         jiancheng_list.append(list(co_names[i])[cn1.index('short_name')])
#         code_list.append(list(co_names[i])[cn1.index('cmp_code')])
#     # print('quancheng_list:',len(quancheng_list))
#     # print('jiancheng_list:',len(jiancheng_list))
#     # print('code_list:',len(code_list))

#     db2.close()

#     return quancheng_list,jiancheng_list,code_list


# # 获取全称、简称、公司代码
# quancheng_list,jiancheng_list,code_list = quan_jian_code()


def redis_conn_129():
    """
    连接redis
    """
    pool = redis.ConnectionPool(host='192.168.1.181', port=6379, password='',db=4, decode_responses=True)  # 服务器
    r = redis.Redis(connection_pool=pool)
    return r

def redis_conn():
    """
    连接redis
    """
    pool = redis.ConnectionPool(host='192.168.1.149', port=6379, password='',db=15, decode_responses=True)  # 服务器
    r = redis.Redis(connection_pool=pool)
    return r

def get_ssgsdmjc(companyName):
    '''从上市公司表中获取公司简称、代码'''
    if companyName is None: companyName = ''
    redis_data = redis_conn().hget('pd_bond_ann_ss:pp_gsjcqcdm_06', companyName)
    redis_data_list = [eval(i) for i in redis_data.split('#*#') if i] if redis_data else []
    if redis_data_list:
        if len(redis_data_list) > 1:
            redis_data_list_tmp = [i for i in redis_data_list if i['cmp_code'] != '' and i['short_name'] != '']
            if len(redis_data_list_tmp) > 0:redis_data_list = redis_data_list_tmp
        redis_data_list = sorted(redis_data_list,key=lambda keys:keys['i_time'])
        return redis_data_list[-1]
    else:
        return {}
def add_uuid(yqid,eventCode,objName):
    """
    对字符串进行加密
    :return:
    """
    result_data = str(uuid.uuid3(uuid.NAMESPACE_DNS, yqid+eventCode+objName)).replace('-', '')
    return result_data


def is_Chinese(word):
    '''
    检查是否为中文
    '''
    if word:
        for ch in word:
            if '\u4e00' <= ch <= '\u9fff':
                return True
    return False


# def format_str(content):
#     '''
#     只保留汉字
#     '''
#     if content == '':
#         return ''
#     content_str = ''
#     for i in content:
#         if is_Chinese(i):
#             content_str = content_str+i
#     return content_str


# def get_sim_uid(db,title,indexWords):
#     '''
#     比较相似度，返回有关联文章的uid
#     '''
    
#     curr_time = datetime.datetime.now()
#     date_str = curr_time.strftime("%Y-%m-%d")

#     # 如标题中含有单引号会引起MySQL语法错误，故替换成转义字符
#     if '\'' in title:
#         title = title.replace('\'','')

#     # print('date_str',date_str)
#     # print("SELECT distinct uid FROM dwd_me_sim_tmp WHERE title = '" + title + "' and pubTime = '" + date_str + "'")

#     cursor1 = db.cursor()
#     cursor1.execute("SELECT distinct uid FROM dwd_me_sim_tmp WHERE title = '" + title + "' and pubTime = '" + date_str + "'")
#     title_sim = list(cursor1.fetchall())
#     # print('dwd_me_sim_tmp title',title_sim)

#     words_sim = []
#     if len(indexWords) > 2 and is_Chinese(indexWords):
#         cursor1.execute("SELECT distinct uid FROM dwd_me_sim_tmp WHERE indexWords = '" + indexWords + "' and pubTime = '" + date_str + "'")
#         words_sim = list(cursor1.fetchall())
#         # print('dwd_me_sim_tmp indexWords',words_sim)
    
#     return title_sim,words_sim


# def get_content(db,uid_list):
#     '''
#     通过uid从base表中获得content
#     '''
#     cursor1 = db.cursor()

#     # 初始化文章列表用于存储可能相似的文章
#     content_list = []

#     for uid in uid_list:
#         cursor1.execute("SELECT content FROM dwd_me_yq_base WHERE yqid = '" + uid + "'")
#         content = list(cursor1.fetchall())
#         if len(content) > 0 and len(content[0]) > 0:
#             content = content[0][0]
#         else:
#             content = ''
#         # 只保留汉字用于比较
#         # content = format_str(content)
#         content_list.append(content)

#     # print(content,type(content),len(content))
#     # print('dwd_me_sim_tmp title',title_sim)

#     return content_list


# def insert_yq_base(db,yqid,title,content,webname,webnameConf,srcType,srcUrl,summary,keyword,snapshot,pubTime,getTime,spareCol1,spareCol2,spareCol3,spareCol4,spareCol5,isValid,dataStatus):
#     '''
#     dwd_me_yq_base插入操作
#     '''
#     cursor1 = db.cursor()
#     sql = '''INSERT INTO {0} (yqid,
#                               title,
#                               content,
#                               webname,
#                               webnameConf,
#                               srcType,
#                               srcUrl,
#                               summary,
#                               keyword,
#                               snapshot,
#                               pubTime,
#                               getTime,
#                               spareCol1,
#                               spareCol2,
#                               spareCol3,
#                               spareCol4,
#                               spareCol5,
#                               isValid,
#                               dataStatus
#                               )VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
#                                       %s,%s,%s,%s,%s,%s,%s,%s,%s)'''
#     cursor1.execute(sql.format('dwd_me_yq_base'),(yqid,
#                                                  title,
#                                                  content,
#                                                  webname,
#                                                  webnameConf,
#                                                  srcType,
#                                                  srcUrl,
#                                                  summary,
#                                                  keyword,
#                                                  snapshot,
#                                                  pubTime,
#                                                  getTime,
#                                                  spareCol1,
#                                                  spareCol2,
#                                                  spareCol3,
#                                                  spareCol4,
#                                                  spareCol5,
#                                                  isValid,
#                                                  dataStatus
#                                                  ))
    
    
# def insert_cmp_yq_emo(db,yqid,eventCode,companyName,emoScore,emoLabel,emoConf,isValid,dataStatus):
#     '''
#     dwd_me_cmp_yq_emo插入操作
#     '''
#     cursor1 = db.cursor()
#     sql = '''INSERT INTO {0} (yqid,
#                               eventCode,
#                               companyName,
#                               emoScore,
#                               emoLabel,
#                               emoConf,
#                               isValid,
#                               dataStatus
#                               )VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'''
#     cursor1.execute(sql.format('dwd_me_cmp_yq_emo'),(yqid,
#                                                      eventCode,
#                                                      companyName.replace('(','（').replace(')','）'),
#                                                      emoScore,
#                                                      emoLabel,
#                                                      emoConf,
#                                                      isValid,
#                                                      dataStatus
#                                                      ))
    
    
# def insert_ind_yq_emo(db,yqid,eventCode,industryCode,emoScore,emoLabel,emoConf,isValid,dataStatus):
#     '''
#     dwd_me_ind_yq_emo插入操作
#     '''
#     cursor1 = db.cursor()
#     sql = '''INSERT INTO {0} (yqid,
#                               eventCode,
#                               industryCode,
#                               emoScore,
#                               emoLabel,
#                               emoConf,
#                               isValid,
#                               dataStatus
#                               )VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'''
#     cursor1.execute(sql.format('dwd_me_ind_yq_emo'),(yqid,
#                                                      eventCode,
#                                                      industryCode,
#                                                      emoScore,
#                                                      emoLabel,
#                                                      emoConf,
#                                                      isValid,
#                                                      dataStatus
#                                                      ))
    
    
# def insert_cpt_yq_emo(db,yqid,eventCode,conceptCode,emoScore,emoLabel,emoConf,isValid,dataStatus):
#     '''
#     dwd_me_cpt_yq_emo插入操作
#     '''
#     cursor1 = db.cursor()
#     sql = '''INSERT INTO {0} (yqid,
#                               eventCode,
#                               conceptCode,
#                               emoScore,
#                               emoLabel,
#                               emoConf,
#                               isValid,
#                               dataStatus
#                               )VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'''
#     cursor1.execute(sql.format('dwd_me_cpt_yq_emo'),(yqid,
#                                                      eventCode,
#                                                      conceptCode,
#                                                      emoScore,
#                                                      emoLabel,
#                                                      emoConf,
#                                                      isValid,
#                                                      dataStatus
#                                                      ))
    
    
# def insert_area_yq_emo(db,yqid,eventCode,areaCode,emoScore,emoLabel,emoConf,isValid,dataStatus):
#     '''
#     dwd_me_area_yq_emo插入操作
#     '''
#     cursor1 = db.cursor()
#     sql = '''INSERT INTO {0} (yqid,
#                               eventCode,
#                               areaCode,
#                               emoScore,
#                               emoLabel,
#                               emoConf,
#                               isValid,
#                               dataStatus
#                               )VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'''
#     cursor1.execute(sql.format('dwd_me_area_yq_emo'),(yqid,
#                                                       eventCode,
#                                                       areaCode,
#                                                       emoScore,
#                                                       emoLabel,
#                                                       emoConf,
#                                                       isValid,
#                                                       dataStatus
#                                                       ))
    
    
# def insert_yq_imp(db,yqid,eventCode,impScore,impLabel,isValid,dataStatus):
#     '''
#     dwd_me_yq_imp插入操作
#     '''
#     cursor1 = db.cursor()
#     sql = '''INSERT INTO {0} (yqid,
#                               eventCode,
#                               impScore,
#                               impLabel,
#                               isValid,
#                               dataStatus
#                               )VALUES(%s,%s,%s,%s,%s,%s)'''
#     cursor1.execute(sql.format('dwd_me_yq_imp'),(yqid,
#                                                  eventCode,
#                                                  impScore,
#                                                  impLabel,
#                                                  isValid,
#                                                  dataStatus
#                                                  ))
    
    
# def insert_cmp_yq_rel(db,yqid,companyName,relScore,relLabel,isValid,dataStatus):
#     '''
#     dwd_me_cmp_yq_rel插入操作
#     '''
#     cursor1 = db.cursor()
#     sql = '''INSERT INTO {0} (yqid,
#                               companyName,
#                               relScore,
#                               relLabel,
#                               isValid,
#                               dataStatus
#                               )VALUES(%s,%s,%s,%s,%s,%s)'''
#     cursor1.execute(sql.format('dwd_me_cmp_yq_rel'),(yqid,
#                                                      companyName.replace('(','（').replace(')','）'),
#                                                      relScore,
#                                                      relLabel,
#                                                      isValid,
#                                                      dataStatus
#                                                      ))
    
    
# def insert_ind_yq_rel(db,yqid,industryCode,relScore,relLabel,isValid,dataStatus):
#     '''
#     dwd_me_ind_yq_rel插入操作
#     '''
#     cursor1 = db.cursor()
#     sql = '''INSERT INTO {0} (yqid,
#                               industryCode,
#                               relScore,
#                               relLabel,
#                               isValid,
#                               dataStatus
#                               )VALUES(%s,%s,%s,%s,%s,%s)'''
#     cursor1.execute(sql.format('dwd_me_ind_yq_rel'),(yqid,
#                                                      industryCode,
#                                                      relScore,
#                                                      relLabel,
#                                                      isValid,
#                                                      dataStatus
#                                                      ))
    
    
# def insert_cpt_yq_rel(db,yqid,conceptCode,relScore,relLabel,isValid,dataStatus):
#     '''
#     dwd_me_cpt_yq_rel插入操作
#     '''
#     cursor1 = db.cursor()
#     sql = '''INSERT INTO {0} (yqid,
#                               conceptCode,
#                               relScore,
#                               relLabel,
#                               isValid,
#                               dataStatus
#                               )VALUES(%s,%s,%s,%s,%s,%s)'''
#     cursor1.execute(sql.format('dwd_me_cpt_yq_rel'),(yqid,
#                                                      conceptCode,
#                                                      relScore,
#                                                      relLabel,
#                                                      isValid,
#                                                      dataStatus
#                                                      ))


# def insert_area_yq_rel(db,yqid,areaCode,relScore,relLabel,isValid,dataStatus):
#     '''
#     dwd_me_area_yq_rel插入操作
#     '''
#     cursor1 = db.cursor()
#     sql = '''INSERT INTO {0} (yqid,
#                               areaCode,
#                               relScore,
#                               relLabel,
#                               isValid,
#                               dataStatus
#                               )VALUES(%s,%s,%s,%s,%s,%s)'''
#     cursor1.execute(sql.format('dwd_me_area_yq_rel'),(yqid,
#                                                      areaCode,
#                                                      relScore,
#                                                      relLabel,
#                                                      isValid,
#                                                      dataStatus
#                                                      ))


# def insert_yq_sim(db,yqid,comYqid,simScore,simYn,isValid,dataStatus):
#     '''
#     dwd_me_yq_sim插入操作
#     '''
#     if yqid.replace(' ','') != comYqid.replace(' ',''):
#         cursor1 = db.cursor()
#         sql = '''INSERT INTO {0} (yqid,
#                                   comYqid,
#                                   simScore,
#                                   simYn,
#                                   isValid,
#                                   dataStatus
#                                   )VALUES(%s,%s,%s,%s,%s,%s)'''
#         try:
#             cursor1.execute(sql.format('dwd_me_yq_sim'),(yqid,
#                                                          comYqid,
#                                                          simScore,
#                                                          simYn,
#                                                          isValid,
#                                                          dataStatus
#                                                          ))
#         except:
#             return 0


# def insert_yq_cls(db,yqid,eventCode,isValid,dataStatus):
#     '''
#     dwd_me_yq_cls插入操作
#     '''
#     cursor1 = db.cursor()
#     sql = '''INSERT INTO {0} (yqid,
#                               eventCode,
#                               isValid,
#                               dataStatus
#                               )VALUES(%s,%s,%s,%s)'''
#     cursor1.execute(sql.format('dwd_me_yq_cls'),(yqid,
#                                                  eventCode,
#                                                  isValid,
#                                                  dataStatus
#                                                  ))


# def insert_sim_tmp(db,uid,title,indexWords,pubTime):
#     '''
#     dwd_me_sim_tmp插入操作
#     '''
#     title = title[:950]
#     cursor1 = db.cursor()
#     sql = '''INSERT INTO {0} (uid,
#                               title,
#                               indexWords,
#                               pubTime
#                               )VALUES(%s,%s,%s,%s)'''
#     cursor1.execute(sql.format('dwd_me_sim_tmp'),(uid,
#                                                   title,
#                                                   indexWords,
#                                                   pubTime
#                                                   ))



# def insert_yq_lvl_code(db,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,thirdLevelCode,thirdLevelName,fourthLevelCode,fourthLevelName,eventCode,eventName,isValid,dataStatus):
#     '''
#     dwd_me_yq_lvl_code插入操作
#     '''
#     cursor1 = db.cursor()
#     sql = '''INSERT INTO {0} (firstLevelCode,
#                               firstLevelName,
#                               secondLevelCode,
#                               secondLevelName,
#                               thirdLevelCode,
#                               thirdLevelName,
#                               fourthLevelCode,
#                               fourthLevelName,
#                               eventCode,
#                               eventName,
#                               isValid,
#                               dataStatus
#                               )VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
#                                       %s,%s)'''
#     cursor1.execute(sql.format('dwd_me_yq_lvl_code'),(firstLevelCode,
#                                                       firstLevelName,
#                                                       secondLevelCode,
#                                                       secondLevelName,
#                                                       thirdLevelCode,
#                                                       thirdLevelName,
#                                                       fourthLevelCode,
#                                                       fourthLevelName,
#                                                       eventCode,
#                                                       eventName,
#                                                       isValid,
#                                                       dataStatus
#                                                       ))


def insert_news_search(db,yqid,objName,companyShortName,companyCode,indirectObjName,transScore,relPath,relType,relScore,relLabel,personName,eventCode,eventName,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,thirdLevelCode,thirdLevelName,fourthLevelCode,fourthLevelName,emoLabel,emoScore,emoConf,impScore,impLabel,pubTime,title,summary,keyword,srcUrl,srcType,source,content,isValid,dataStatus):
    '''
    dwd_me_yq_lvl_code插入操作
    '''
    # emoScore = emoLabel
    # if emoLabel == '1':
    #     emoLabel = '正向'
    # elif emoLabel == '-1':
    #     emoLabel = '负向'
    # elif emoLabel == '0':
    #     emoLabel = '中性'

    if is_Chinese(personName)==False or len(personName) < 2:
        personName = ''

    source = source.replace(' ','').replace('\t','')
    if emoConf.replace('.','').isdigit():
        emoConf = str(round(float(emoConf),2))
    else:
        emoConf = ''

    cursor1 = db.cursor()
    sql = '''INSERT IGNORE INTO {0} (yqid,
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
                              content,
                              isValid,
                              dataStatus
                              )VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                      %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                      %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                      %s,%s,%s,%s,%s,%s)'''
    cursor1.execute(sql.format('sy_project_raw.dwa_me_yq_search'),(yqid,
                                                    objName.replace('(','（').replace(')','）'),
                                                    companyShortName.replace('(','（').replace(')','）'),
                                                    companyCode,
                                                    indirectObjName.replace('(','（').replace(')','）'),
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
                                                    content,
                                                    isValid,
                                                    dataStatus
                                                    ))


def insert_news_search_180(db,yqid,objName,companyShortName,companyCode,indirectObjName,transScore,relPath,relType,relScore,relLabel,personName,eventCode,eventName,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,thirdLevelCode,thirdLevelName,fourthLevelCode,fourthLevelName,emoLabel,emoScore,emoConf,impScore,impLabel,pubTime,title,summary,keyword,srcUrl,srcType,source,content,isValid,dataStatus):
    '''
    dwd_me_yq_lvl_code插入操作
    '''
    # emoScore = emoLabel
    # if emoLabel == '1':
    #     emoLabel = '正向'
    # elif emoLabel == '-1':
    #     emoLabel = '负向'
    # elif emoLabel == '0':
    #     emoLabel = '中性'

    if is_Chinese(personName)==False or len(personName) < 2:
        personName = ''

    source = source.replace(' ','').replace('\t','')
    if emoConf.replace('.','').isdigit():
        emoConf = str(round(float(emoConf),2))
    else:
        emoConf = ''

    cursor1 = db.cursor()
    sql = '''INSERT IGNORE INTO {0} (yqid,
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
                              content,
                              isValid,
                              dataStatus
                              )VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                      %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                      %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                      %s,%s,%s,%s,%s,%s)'''
    cursor1.execute(sql.format('sy_project_raw.dwa_me_yq_search'),(yqid,
                                                    objName.replace('(','（').replace(')','）'),
                                                    companyShortName.replace('(','（').replace(')','）'),
                                                    companyCode,
                                                    indirectObjName.replace('(','（').replace(')','）'),
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
                                                    content,
                                                    isValid,
                                                    dataStatus
                                                    ))



# def insert_news_search_his(db,yqid,objName,companyShortName,companyCode,indirectObjName,transScore,relPath,relType,relScore,relLabel,personName,eventCode,eventName,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,thirdLevelCode,thirdLevelName,fourthLevelCode,fourthLevelName,emoLabel,emoScore,emoConf,impScore,impLabel,pubTime,title,summary,keyword,srcUrl,srcType,source,content,isValid,dataStatus):
#     '''
#     dwd_me_yq_lvl_code插入操作
#     '''
#     emoScore = emoLabel
#     if emoLabel == '1':
#         emoLabel = '正向'
#     elif emoLabel == '-1':
#         emoLabel = '负向'
#     elif emoLabel == '0':
#         emoLabel = '中性'

#     if is_Chinese(personName)==False or len(personName) < 2:
#         personName = ''

#     source = source.replace(' ','').replace('\t','')
#     if emoConf.replace('.','').isdigit():
#         emoConf = str(round(float(emoConf),2))
#     else:
#         emoConf = ''

#     cursor1 = db.cursor()
#     sql = '''INSERT INTO {0} (yqid,
#                               objName,
#                               companyShortName,
#                               companyCode,
#                               indirectObjName,
#                               transScore,
#                               relPath,
#                               relType,
#                               relScore,
#                               relLabel,
#                               personName,
#                               eventCode,
#                               eventName,
#                               firstLevelCode,
#                               firstLevelName,
#                               secondLevelCode,
#                               secondLevelName,
#                               thirdLevelCode,
#                               thirdLevelName,
#                               fourthLevelCode,
#                               fourthLevelName,
#                               emoLabel,
#                               emoScore,
#                               emoConf,
#                               impScore,
#                               impLabel,
#                               pubTime,
#                               title,
#                               summary,
#                               keyword,
#                               srcUrl,
#                               srcType,
#                               source,
#                               content,
#                               isValid,
#                               dataStatus
#                               )VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
#                                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
#                                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
#                                       %s,%s,%s,%s,%s,%s)'''
#     cursor1.execute(sql.format('sy_project_raw.dwa_me_yq_search_his'),(yqid,
#                                                     objName.replace('(','（').replace(')','）'),
#                                                     companyShortName,
#                                                     companyCode,
#                                                     indirectObjName.replace('(','（').replace(')','）'),
#                                                     transScore,
#                                                     relPath,
#                                                     relType,
#                                                     relScore,
#                                                     relLabel,
#                                                     personName,
#                                                     eventCode,
#                                                     eventName,
#                                                     firstLevelCode,
#                                                     firstLevelName,
#                                                     secondLevelCode,
#                                                     secondLevelName,
#                                                     thirdLevelCode,
#                                                     thirdLevelName,
#                                                     fourthLevelCode,
#                                                     fourthLevelName,
#                                                     emoLabel,
#                                                     emoScore,
#                                                     emoConf,
#                                                     impScore,
#                                                     impLabel,
#                                                     pubTime,
#                                                     title,
#                                                     summary,
#                                                     keyword,
#                                                     srcUrl,
#                                                     srcType,
#                                                     source,
#                                                     content,
#                                                     isValid,
#                                                     dataStatus
#                                                     ))


# def insert_news_search_add(db,yqid,objName,companyShortName,companyCode,indirectObjName,transScore,relPath,relType,relScore,relLabel,personName,eventCode,eventName,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,thirdLevelCode,thirdLevelName,fourthLevelCode,fourthLevelName,emoLabel,emoScore,emoConf,impScore,impLabel,pubTime,title,summary,keyword,srcUrl,srcType,source,content,isValid,dataStatus):
#     '''
#     dwd_me_yq_lvl_code插入操作
#     '''
#     emoScore = emoLabel
#     if emoLabel == '1':
#         emoLabel = '正向'
#     elif emoLabel == '-1':
#         emoLabel = '负向'
#     elif emoLabel == '0':
#         emoLabel = '中性'

#     if is_Chinese(personName)==False or len(personName) < 2:
#         personName = ''

#     source = source.replace(' ','').replace('\t','')
#     if emoConf.replace('.','').isdigit():
#         emoConf = str(round(float(emoConf),2))
#     else:
#         emoConf = ''

#     cursor1 = db.cursor()
#     sql = '''INSERT INTO {0} (yqid,
#                               objName,
#                               companyShortName,
#                               companyCode,
#                               indirectObjName,
#                               transScore,
#                               relPath,
#                               relType,
#                               relScore,
#                               relLabel,
#                               personName,
#                               eventCode,
#                               eventName,
#                               firstLevelCode,
#                               firstLevelName,
#                               secondLevelCode,
#                               secondLevelName,
#                               thirdLevelCode,
#                               thirdLevelName,
#                               fourthLevelCode,
#                               fourthLevelName,
#                               emoLabel,
#                               emoScore,
#                               emoConf,
#                               impScore,
#                               impLabel,
#                               pubTime,
#                               title,
#                               summary,
#                               keyword,
#                               srcUrl,
#                               srcType,
#                               source,
#                               content,
#                               isValid,
#                               dataStatus
#                               )VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
#                                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
#                                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
#                                       %s,%s,%s,%s,%s,%s)'''
#     cursor1.execute(sql.format('sy_project_raw.dwa_me_yq_search_add'),(yqid,
#                                                     objName.replace('(','（').replace(')','）'),
#                                                     companyShortName,
#                                                     companyCode,
#                                                     indirectObjName.replace('(','（').replace(')','）'),
#                                                     transScore,
#                                                     relPath,
#                                                     relType,
#                                                     relScore,
#                                                     relLabel,
#                                                     personName,
#                                                     eventCode,
#                                                     eventName,
#                                                     firstLevelCode,
#                                                     firstLevelName,
#                                                     secondLevelCode,
#                                                     secondLevelName,
#                                                     thirdLevelCode,
#                                                     thirdLevelName,
#                                                     fourthLevelCode,
#                                                     fourthLevelName,
#                                                     emoLabel,
#                                                     emoScore,
#                                                     emoConf,
#                                                     impScore,
#                                                     impLabel,
#                                                     pubTime,
#                                                     title,
#                                                     summary,
#                                                     keyword,
#                                                     srcUrl,
#                                                     srcType,
#                                                     source,
#                                                     content,
#                                                     isValid,
#                                                     dataStatus
#                                                     ))


# def insert_news_title_search(db,yqid,title,companiesInfo,eventCode,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,thirdLevelCode,thirdLevelName,fourthLevelCode,fourthLevelName,emoLabel,emoScore,emoConf,impScore,impLabel,pubTime,summary,srcUrl,srcType,content,relPath,isValid,dataStatus):
#     '''
#     dwd_me_yq_lvl_code插入操作
#     '''
#     cursor1 = db.cursor()
#     sql = '''INSERT INTO {0} (yqid,
#                               title,
#                               companiesInfo,
#                               eventCode,
#                               firstLevelCode,
#                               firstLevelName,
#                               secondLevelCode,
#                               secondLevelName,
#                               thirdLevelCode,
#                               thirdLevelName,
#                               fourthLevelCode,
#                               fourthLevelName,
#                               emoLabel,
#                               emoScore,
#                               emoConf,
#                               impScore,
#                               impLabel,
#                               pubTime,
#                               summary,
#                               srcUrl,
#                               srcType,
#                               content,
#                               relPath,
#                               isValid,
#                               dataStatus
#                               )VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
#                                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
#                                       %s,%s,%s,%s,%s)'''
#     cursor1.execute(sql.format('dwa_me_yq_title_search'),(yqid,
#                                                       title,
#                                                       companiesInfo,
#                                                       eventCode,
#                                                       firstLevelCode,
#                                                       firstLevelName,
#                                                       secondLevelCode,
#                                                       secondLevelName,
#                                                       thirdLevelCode,
#                                                       thirdLevelName,
#                                                       fourthLevelCode,
#                                                       fourthLevelName,
#                                                       emoLabel,
#                                                       emoScore,
#                                                       emoConf,
#                                                       impScore,
#                                                       impLabel,
#                                                       pubTime,
#                                                       summary,
#                                                       srcUrl,
#                                                       srcType,
#                                                       content,
#                                                       relPath,
#                                                       isValid,
#                                                       dataStatus
#                                                       ))


def insert_statistics(db,yqid,company,ctimeDate,relType,emoLabel,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,thirdLevelCode,thirdLevelName,fourthLevelCode,fourthLevelName,eventCode,eventName):
    '''
    dwa_me_yq_stat插入操作
    '''
    cursor1 = db.cursor()
    sql = '''INSERT IGNORE INTO {0} (yqid,
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
                              )VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                      %s,%s,%s,%s,%s)'''
    cursor1.execute(sql.format('sy_project_raw.dwa_me_yq_stat'),(yqid,
                                                     company.replace('(','（').replace(')','）'),
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
                                                     ))


def insert_statistics_180(db,yqid,company,ctimeDate,relType,emoLabel,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,thirdLevelCode,thirdLevelName,fourthLevelCode,fourthLevelName,eventCode,eventName):
    '''
    dwa_me_yq_stat插入操作
    '''
    cursor1 = db.cursor()
    sql = '''INSERT IGNORE INTO {0} (yqid,
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
                              )VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                      %s,%s,%s,%s,%s)'''
    cursor1.execute(sql.format('sy_project_raw.dwa_me_yq_stat'),(yqid,
                                                     company.replace('(','（').replace(')','）'),
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
                                                     ))


# def tfidf_company(company_list,content):
#     '''
#     计算公司在文中的tfidf值
#     '''
#     if len(content) < 1:
#         return {}
#     result_dict = {}
#     for company in company_list:
#         jieba.add_word(company)
#     document = ' '.join(list(jieba.cut(content)))
#     # print(document)
#     vectorizer=CountVectorizer()#该类会将文本中的词语转换为词频矩阵，矩阵元素a[i][j] 表示j词在i类文本下的词频
#     transformer=TfidfTransformer()#该类会统计每个词语的tf-idf权值
#     tfidf=transformer.fit_transform(vectorizer.fit_transform([document]))#第一个fit_transform是计算tf-idf，第二个fit_transform是将文本转为词频矩阵
#     word=vectorizer.get_feature_names()#获取词袋模型中的所有词语
#     weight=tfidf.toarray()#将tf-idf矩阵抽取出来，元素a[i][j]表示j词在i类文本中的tf-idf权重
#     for i in range(len(weight)):#打印每类文本的tf-idf词语权重，第一个for遍历所有文本，第二个for便利某一类文本下的词语权重
#         # max_value = 0
#         # max_word = ''
#         for j in range(len(word)):
#             # if weight[i][j] > max_value:
#             #     max_value = weight[i][j]
#             #     max_word = word[j]
#             if word[j] in company_list:
#                 # print(word[j],weight[i][j])
#                 result_dict[word[j]] = weight[i][j]
#             # 因为分词的时候可能会将'ST'与汉字分开，所以加入下列判断
#             elif '*ST' + word[j] in company_list:
#                 result_dict['*ST' + word[j]] = weight[i][j]
#             elif 'ST' + word[j] in company_list:
#                 result_dict['ST' + word[j]] = weight[i][j]

#     # print(max_word,max_value)
#     return result_dict



# def get_person_name():
#     '''
#     获取人名
#     '''
#     db2 = pymysql.connect(**{"host":"192.168.1.129","user":"aliyun_rw","password":"shiye_aliyun","db":"EI_BDP","port":3306,"charset":"utf8"})

#     cursor2 = db2.cursor()
#     person_list = []
#     department_list = []
#     # dwd_yq_gov_leader
#     cursor2.execute('SELECT * FROM dwd_yq_gov_leader')
#     co_names = list(cursor2.fetchall())
#     cn1 = [desc[0] for desc in cursor2.description]
#     for i in range(len(co_names)):
#         person_list.append(list(co_names[i])[cn1.index('leaderName')])
#         department_list.append(list(co_names[i])[cn1.index('departmentName')])
#     # print('person_list:',len(person_list))
#     # print('department_list:',len(department_list))

#     # dwd_yq_unicorn500_leader
#     cursor2.execute('SELECT * FROM dwd_yq_unicorn500_leader')
#     co_names = list(cursor2.fetchall())
#     cn1 = [desc[0] for desc in cursor2.description]
#     for i in range(len(co_names)):
#         person_list.append(list(co_names[i])[cn1.index('personName')])
#         department_list.append(list(co_names[i])[cn1.index('companyName')])
#     # print('person_list:',len(person_list))
#     # print('department_list:',len(department_list))

#     # tmp_sq_comp_manager_main_01
#     cursor2.execute('SELECT * FROM tmp_sq_comp_manager_main_01')
#     co_names = list(cursor2.fetchall())
#     cn1 = [desc[0] for desc in cursor2.description]
#     for i in range(len(co_names)):
#         person_list.append(list(co_names[i])[cn1.index('CName')])
#         department_list.append(list(co_names[i])[cn1.index('companyName')])
#     # print('person_list:',len(person_list))
#     # print('department_list:',len(department_list))

#     # tmp_sq_comp_manager_main_1
#     cursor2.execute('SELECT * FROM tmp_sq_comp_manager_main_1')
#     co_names = list(cursor2.fetchall())
#     cn1 = [desc[0] for desc in cursor2.description]
#     for i in range(len(co_names)):
#         person_list.append(list(co_names[i])[cn1.index('LeaderName')])
#         department_list.append(list(co_names[i])[cn1.index('ChiName')])
#     # print('person_list:',len(person_list))
#     # print('department_list:',len(department_list))

#     # tb_leader_message
#     cursor2.execute('SELECT * FROM tb_leader_message')
#     co_names = list(cursor2.fetchall())
#     cn1 = [desc[0] for desc in cursor2.description]
#     for i in range(len(co_names)):
#         person_list.append(list(co_names[i])[cn1.index('LeaderName')])
#         department_list.append(list(co_names[i])[cn1.index('compName')])
#     print('person_list:',len(person_list))
#     print('department_list:',len(department_list))


#     db2.close()

#     return person_list,department_list


# def match_person_name(text,person_name_list,co_dep_name_list):
#     match_person_list = []
#     match_person_co_list = []
#     for num,p in enumerate(person_name_list):
#         if p in text:
#             match_person_list.append(p)
#             match_person_co_list.append(co_dep_name_list[num])
#     return match_person_list,match_person_co_list


# def delete_symbol(inputString):
#     '''
#     去除标点符号
#     '''
#     string = re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]", "",inputString)
#     return string


# def convert_num_time(num_time):
#     '''
#     将int类型日期转换成实际日期
#     '''
#     timeArray = time.localtime(num_time)
#     real_time = time.strftime('%Y-%m-%d %H:%M:%S', timeArray)
#     return(str(real_time))


# def convert_real_time(real_time):
#     '''
#     将实际日期转换成int类型以便于比较
#     '''
#     timeArray = datetime.datetime.strptime(real_time,'%Y-%m-%d %H:%M:%S')
#     #print(timeArray)
#     int_time = int(timeArray.timestamp())
#     return int_time


# def model3():
#     '''
#     调用模型3
#     '''
#     return BertClient(show_server_config=False,
#                       check_version=False,
#                       check_length=False,
#                       ip='192.168.1.128',
#                       port=6666,
#                       port_out=6667,
#                       mode='NER',
#                       timeout=60000)


def is_CN(uchar):
    '''
    判断一个unicode是否是汉字
    '''
    if uchar >= u'\u4e00' and uchar <= u'\u9fa5':
        return True
    else:
        return False


def keep_CN(text):
    '''
    只保留中文字符
    '''
    result = ''
    for w in text:
        if is_CN(w):
            result += w
    return result


def remove_num(text):
    '''
    去除数字
    '''
    result = ''
    for w in text:
        if w.isdigit() == False:
            result += w
    return result


def DBC2SBC(ustring):
    '''
    字符串全角转半角
    '''
    rstring = ''
    for uchar in ustring:
        inside_code=ord(uchar)
        if inside_code==0x3000:
            inside_code=0x0020
        else:
            inside_code-=0xfee0
        # 转完之后不是半角字符返回原来的字符
        if inside_code<0x0020 or inside_code>0x7e:
            rstring += uchar
        else:
            rstring += chr(inside_code)
    return rstring


def cut_author(content,name):
    '''
    去除文章末尾的作者信息
    '''
    if name in content[-50:]:
        # print('content',[content])
        content = content[:content.find(name,-50)]
    return content


def include_company(company,company_list):
    '''
    如果company在company_list中有被其他公司名称包含，则返回True，否则False
    '''
    for co in company_list:
        if co != company and company in co:
            return True
    return False


# def lvl_title(title):
#     '''
#     匹配事件类型
#     '''
#     # # 读取连接数据库配置文件
#     # load_config='/home/seeyii/increase_nlp/db_config.json'
#     # with open(load_config,'r', encoding="utf-8") as f:
#     #     reader = f.readlines()[0]
#     # config_local = json.loads(reader)

#     # db1 = pymysql.connect(**config_local['129_sql_yq'])
#     db1 = pymysql.connect(**{"host":"192.168.1.129","user":"aliyun_rw","password":"shiye_aliyun","db":"sy_yq_raw","port":3306,"charset":"utf8"})
#     cursor1 = db1.cursor()

#     # 创建分类列表
#     first_level_code_list = []
#     first_level_name_list = []
#     second_level_code_list = []
#     second_level_name_list = []
#     third_level_code_list = []
#     third_level_name_list = []
#     fourth_level_code_list = []
#     fourth_level_name_list = []
#     event_code_list = []
#     event_name_list = []
#     rules_list = []
#     filter_list = []
#     important_list = []

#     # 获取分类及关键词
#     cursor1.execute("SELECT * FROM sy_yq_lvl_rules_code_news WHERE inRules != '' and inRules is not NULL")
#     co_names = list(cursor1.fetchall())
#     cn1 = [desc[0] for desc in cursor1.description]
#     for i in range(len(co_names)):
#         first_level_code_list.append(list(co_names[i])[cn1.index('firstLevelCode')])
#         first_level_name_list.append(list(co_names[i])[cn1.index('firstLevelName')])
#         second_level_code_list.append(list(co_names[i])[cn1.index('secondLevelCode')])
#         second_level_name_list.append(list(co_names[i])[cn1.index('secondLevelName')])
#         third_level_code_list.append(list(co_names[i])[cn1.index('threeLevelCode')])
#         third_level_name_list.append(list(co_names[i])[cn1.index('threeLevelName')])
#         fourth_level_code_list.append(list(co_names[i])[cn1.index('fourLevelCode')])
#         fourth_level_name_list.append(list(co_names[i])[cn1.index('fourLevelName')])
#         event_code_list.append(list(co_names[i])[cn1.index('eventCode')])
#         event_name_list.append(list(co_names[i])[cn1.index('eventName')])
#         rules_list.append(list(co_names[i])[cn1.index('inRules')].split('、'))
#         filter_list.append(list(co_names[i])[cn1.index('filterRules')].split('、'))
#         important_list.append(list(co_names[i])[cn1.index('impScore')])
#     # print('first_level_code_list:',len(first_level_code_list))
#     # print('first_level_name_list:',len(first_level_name_list))
#     # print('second_level_code_list:',len(second_level_code_list))
#     # print('second_level_name_list:',len(second_level_name_list))
#     # print('third_level_code_list:',len(third_level_code_list))
#     # print('third_level_name_list:',len(third_level_name_list))
#     # print('fourth_level_code_list:',len(fourth_level_code_list))
#     # print('fourth_level_name_list:',len(fourth_level_name_list))
#     # print('event_code_list:',len(event_code_list))
#     # print('event_name_list:',len(event_name_list))
#     # print('rules_list:',len(rules_list))
#     # print('rules_list:',rules_list[0])
#     # print('filter_list:',len(filter_list))
#     # print('filter_list:',filter_list[0])
#     # print('important_list:',important_list)
#     db1.close()

#     result_list = []
#     for r_num,rule in enumerate(rules_list):
#         # print(rule)
#         for single_rule in rule:
#             if single_rule == '':
#                 continue

#             single_rule_split = single_rule.split('&')

#             flag = True
#             # 匹配规则关键词
#             for r in single_rule_split:
#                 # print(r)
#                 if r not in title:
#                     # print([r],[title])
#                     flag = False

#             # 过滤关键词
#             # print(filter_list[r_num])
#             for f in filter_list[r_num]:
#                 if flag == True and f != '' and f in title:
#                     # print('filter',title,f)
#                     flag = False

#             if flag == True:
#                 result_list.append([first_level_code_list[r_num],first_level_name_list[r_num],second_level_code_list[r_num],second_level_name_list[r_num],third_level_code_list[r_num],third_level_name_list[r_num],fourth_level_code_list[r_num],fourth_level_name_list[r_num],event_code_list[r_num],event_name_list[r_num],single_rule,important_list[r_num]])

#     return result_list



# def lvl_title_content(title,content):
#     '''
#     匹配事件类型
#     '''

#     # db1 = pymysql.connect(**config_local['129_sql_yq'])
#     db1 = pymysql.connect(**{"host":"192.168.1.129","user":"aliyun_rw","password":"shiye_aliyun","db":"sy_yq_raw","port":3306,"charset":"utf8"})
#     cursor1 = db1.cursor()

#     # 创建分类列表
#     first_level_code_list = []
#     first_level_name_list = []
#     second_level_code_list = []
#     second_level_name_list = []
#     third_level_code_list = []
#     third_level_name_list = []
#     fourth_level_code_list = []
#     fourth_level_name_list = []
#     event_code_list = []
#     event_name_list = []
#     rules_list = []
#     filter_list = []
#     important_list = []

#     # 获取分类及关键词
#     cursor1.execute("SELECT * FROM sy_yq_lvl_rules_code_news WHERE inRules != '' and inRules is not NULL")
#     co_names = list(cursor1.fetchall())
#     cn1 = [desc[0] for desc in cursor1.description]
#     for i in range(len(co_names)):
#         first_level_code_list.append(list(co_names[i])[cn1.index('firstLevelCode')])
#         first_level_name_list.append(list(co_names[i])[cn1.index('firstLevelName')])
#         second_level_code_list.append(list(co_names[i])[cn1.index('secondLevelCode')])
#         second_level_name_list.append(list(co_names[i])[cn1.index('secondLevelName')])
#         third_level_code_list.append(list(co_names[i])[cn1.index('threeLevelCode')])
#         third_level_name_list.append(list(co_names[i])[cn1.index('threeLevelName')])
#         fourth_level_code_list.append(list(co_names[i])[cn1.index('fourLevelCode')])
#         fourth_level_name_list.append(list(co_names[i])[cn1.index('fourLevelName')])
#         event_code_list.append(list(co_names[i])[cn1.index('eventCode')])
#         event_name_list.append(list(co_names[i])[cn1.index('eventName')])
#         rules_list.append(list(co_names[i])[cn1.index('inRules')].split('、'))
#         filter_list.append(list(co_names[i])[cn1.index('filterRules')].split('、'))
#         important_list.append(list(co_names[i])[cn1.index('impScore')])
#     # print('first_level_code_list:',len(first_level_code_list))
#     # print('first_level_name_list:',len(first_level_name_list))
#     # print('second_level_code_list:',len(second_level_code_list))
#     # print('second_level_name_list:',len(second_level_name_list))
#     # print('third_level_code_list:',len(third_level_code_list))
#     # print('third_level_name_list:',len(third_level_name_list))
#     # print('fourth_level_code_list:',len(fourth_level_code_list))
#     # print('fourth_level_name_list:',len(fourth_level_name_list))
#     # print('event_code_list:',len(event_code_list))
#     # print('event_name_list:',len(event_name_list))
#     # print('rules_list:',len(rules_list))
#     # print('rules_list:',rules_list[0])
#     # print('filter_list:',len(filter_list))
#     # print('filter_list:',filter_list[0])
#     # print('important_list:',important_list)
#     db1.close()

#     result_list = []
#     for r_num,rule in enumerate(rules_list):
#         # print(rule)
#         for single_rule in rule:
#             if single_rule == '':
#                 continue

#             single_rule_split = single_rule.split('&')

#             flag = True
#             # 优先在标题中匹配规则关键词
#             for r in single_rule_split:
#                 # print(r)
#                 # 如果标题中不包含关键词，则退出
#                 if r not in title:
#                     # print([r],[title])
#                     flag = False
#                     break

#             # 如果标题中未匹配到，则在正文中进行匹配
#             if flag == False:
#                 p = 0
#                 for r in single_rule_split:
#                     # print(r)
#                     # 如果正文中不包含关键词，则退出
#                     if r not in content:
#                         # print([r],[title])
#                         flag = False
#                         break
#                     # 如果找到第一个关键词，标记位置
#                     elif p == 0:
#                         p = content.find(r)
#                     # 如果两个关键词之间距离大于15或中间有断句符号，则退出
#                     elif content.find(r) - p > 15 or content[p:p+15].count(';') > 1 or content[p:p+15].count('。') > 1:
#                         flag = False

#             # 过滤关键词
#             # print(filter_list[r_num])
#             for f in filter_list[r_num]:
#                 if flag == True and f != '' and f in title:
#                     # print('filter',title,f)
#                     flag = False

#             if flag == True:
#                 result_list.append([first_level_code_list[r_num],first_level_name_list[r_num],second_level_code_list[r_num],second_level_name_list[r_num],third_level_code_list[r_num],third_level_name_list[r_num],fourth_level_code_list[r_num],fourth_level_name_list[r_num],event_code_list[r_num],event_name_list[r_num],single_rule,important_list[r_num]])

#     return result_list


def insert_news_search_alert(db,yqid,objName,indirectObjName,relType,eventCode,eventName,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,thirdLevelCode,thirdLevelName,fourthLevelCode,fourthLevelName,emoLabel,emoScore,impScore,impLabel,pubTime,isValid,dataStatus):
    '''
    dwa_me_yq_search_alert插入操作
    '''
    # emoScore = emoLabel
    # if emoLabel == '1':
    #     emoLabel = '正向'
    # elif emoLabel == '-1':
    #     emoLabel = '负向'
    # elif emoLabel == '0':
    #     emoLabel = '中性'

    cursor1 = db.cursor()
    sql = '''INSERT IGNORE INTO {0} (yqid,
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
                              pubTime,
                              isValid,
                              dataStatus
                              )VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                      %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                      %s)'''
    cursor1.execute(sql.format('sy_project_raw.dwa_me_yq_search_alert'),(yqid,
                                                    objName.replace('(','（').replace(')','）'),
                                                    indirectObjName.replace('(','（').replace(')','）'),
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
                                                    pubTime,
                                                    isValid,
                                                    dataStatus
                                                    ))

def insert_news_search_alert_180(db,yqid,objName,indirectObjName,relType,eventCode,eventName,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,thirdLevelCode,thirdLevelName,fourthLevelCode,fourthLevelName,emoLabel,emoScore,impScore,impLabel,pubTime,isValid,dataStatus):
    '''
    dwa_me_yq_search_alert插入操作
    '''
    # emoScore = emoLabel
    # if emoLabel == '1':
    #     emoLabel = '正向'
    # elif emoLabel == '-1':
    #     emoLabel = '负向'
    # elif emoLabel == '0':
    #     emoLabel = '中性'

    cursor1 = db.cursor()
    sql = '''INSERT IGNORE INTO {0} (yqid,
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
                              pubTime,
                              isValid,
                              dataStatus
                              )VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                      %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                      %s)'''
    cursor1.execute(sql.format('sy_project_raw.dwa_me_yq_search_alert'),(yqid,
                                                    objName.replace('(','（').replace(')','）'),
                                                    indirectObjName.replace('(','（').replace(')','）'),
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
                                                    pubTime,
                                                    isValid,
                                                    dataStatus
                                                    ))


def insert_mongo(db_name,yqid,objName,companyShortName,companyCode,indirectObjName,transScore,relPath,relType,relScore,relLabel,personName,eventCode,eventName,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,thirdLevelCode,thirdLevelName,fourthLevelCode,fourthLevelName,emoLabel,emoScore,emoConf,impScore,impLabel,pubTime,title,summary,keyword,srcUrl,srcType,source,content):
    times = time.time() - 28800
    dateArray = datetime.datetime.fromtimestamp(times)
    otherStyleTime = dateArray.strftime("%Y-%m-%d %H:%M:%S")
    myDatetime = parser.parse(otherStyleTime)

    onlyId = add_uuid(yqid,eventCode,objName)

    collection = db_name['dwa_me_yq_search']
    dupli_count = collection.count({'onlyId':onlyId})
    # print('dupli_count',[dupli_count],type(dupli_count))
    if dupli_count != 0:
        # print('mongo dupli')
        return 0

    # try:
    collection.insert({'objName':objName.replace('(','（').replace(')','）'),
                       'yqid':yqid,
                       'relPath':relPath,
                       'companyShortName':companyShortName.replace('(','（').replace(')','）'),
                       'companyCode':companyCode,
                       'indirectObjName':indirectObjName.replace('(','（').replace(')','）'),
                       'transScore':transScore,
                       'relType':relType,
                       'relScore':relScore,
                       'relLabel':relLabel,
                       'personName':personName,
                       'eventCode':eventCode,
                       'eventName':eventName,
                       'firstLevelCode':firstLevelCode,
                       'firstLevelName':firstLevelName,
                       'secondLevelCode':secondLevelCode,
                       'secondLevelName':secondLevelName,
                       'thirdLevelCode':thirdLevelCode,
                       'thirdLevelName':thirdLevelName,
                       'fourthLevelCode':fourthLevelCode,
                       'fourthLevelName':fourthLevelName,
                       'emoLabel':emoLabel,
                       'emoScore':emoScore,
                       'emoConf':emoConf,
                       'impScore':impScore,
                       'impLabel':impLabel,
                       'pubTime':pubTime,
                       'title':title,
                       'summary':summary,
                       'keyword':keyword,
                       'srcUrl':srcUrl,
                       'srcType':srcType,
                       'source':source,
                       'content':content,
                       'onlyId' : onlyId, 
                       'ttlTime' : myDatetime,
                       'createTime' : datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                       'modifyTime' : datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                       'isValid' : 1, 
                       'dataStatus' : 1
                       })
    # except:
    #     print('onlyId dupli',onlyId)



def relation_path_add(db1,db2,db_name,relativity_company,uid,ctimeDate,emoLabel,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,thirdLevelCode,thirdLevelName,fourthLevelCode,fourthLevelName,relativity,relLabel,eventCode,eventName,emoScore,emoConf,importanceDgree,importanceLabel,ctime,title,summary,keyword,url,srcType,source,content,personName='',isValid=1,dataStatus=1):
    print(db1)
    # print('被传导公司',relativity_company)
    relativity_company = relativity_company.replace('(','（').replace(')','）')

    # 判断key存不存在
    # if redis_conn().hexists('relation_company_path', relativity_company):
    #     print('Redis存在数据!!!!!',relativity_company)
    result = redis_conn_129().hget('relation_company_path', relativity_company)
    # print('result',relativity_company,result,type(result))
    relation_result_list = []
    if result is not None:
        if result == '' : return
        for item in result.split(';'):
            relation_result_list.append(item.split('#*#'))

        # print('relation_result_list',relation_result_list,len(relation_result_list))

        # 将间接关联数据插入数据统计表
        for relation_result in relation_result_list:
            if len(relation_result) == 4:
                company = relation_result[0]
                print(relation_result,"asdfasdfadfasdadsfasdfasdfadfaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                relativity_company = DBC2SBC(relation_result[1])
                # print('间接关联公司',relativity_company)

                relativity_score = str(relation_result[2])
                if bool(re.search(r'\d', relativity_score)):
                    relPath = relation_result[3]
                    if "-" in relPath:
                        # relativity_companyShortName = ''
                        # relativity_companyCode = ''
                        # if relativity_company in quancheng_list:
                        #     relativity_companyShortName = jiancheng_list[quancheng_list.index(relativity_company)]
                        #     relativity_companyCode = code_list[quancheng_list.index(relativity_company)]
                        relativity_companyShortName_info = get_ssgsdmjc(relativity_company)
                        relativity_companyShortName = relativity_companyShortName_info.get('short_name', '')
                        relativity_companyCode = relativity_companyShortName_info.get('cmp_code', '')


                        # insert_statistics(db=db1,yqid=uid,company=relativity_company,ctimeDate=ctimeDate,relType='传导关联',emoLabel=emoLabel,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,eventCode=eventCode,eventName=eventName)
                        insert_statistics_180(db=db2,yqid=uid,company=relativity_company,ctimeDate=ctimeDate,relType='传导关联',emoLabel=emoLabel,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,eventCode=eventCode,eventName=eventName)

                        try:
                            # insert_news_search(db=db1,yqid=uid,objName=company,companyShortName=relativity_companyShortName,companyCode=relativity_companyCode,indirectObjName=relativity_company,transScore=relativity_score,relPath=relPath,relType='传导关联',relScore=relativity,relLabel=relLabel,personName='',eventCode=eventCode,eventName=eventName,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,emoLabel=emoLabel,emoScore=emoScore,emoConf=emoConf,impScore=importanceDgree,impLabel=importanceLabel,pubTime=ctime,title=title,summary=summary,keyword=keyword,srcUrl=url,srcType=srcType,source=source,content=content,isValid=1,dataStatus=1)
                            insert_news_search_180(db=db2,yqid=uid,objName=company,companyShortName=relativity_companyShortName,companyCode=relativity_companyCode,indirectObjName=relativity_company,transScore=relativity_score,relPath=relPath,relType='传导关联',relScore=relativity,relLabel=relLabel,personName='',eventCode=eventCode,eventName=eventName,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,emoLabel=emoLabel,emoScore=emoScore,emoConf=emoConf,impScore=importanceDgree,impLabel=importanceLabel,pubTime=ctime,title=title,summary=summary,keyword=keyword,srcUrl=url,srcType=srcType,source=source,content=content,isValid=1,dataStatus=1)
                        except:
                            print('search error',uid)
                        # try:
                        #     insert_news_search_alert(db=db1,yqid=uid,objName=company,indirectObjName=relativity_company,relType='传导关联',eventCode=eventCode,eventName=eventName,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,emoLabel=emoLabel,emoScore=emoScore,impScore=importanceDgree,impLabel=importanceLabel,pubTime=ctime,isValid=1,dataStatus=1)
                        # except Exception as e:
                        #     print(e,"预警信息重复")
                        try:
                            print("预警180")
                            insert_news_search_alert_180(db=db2,yqid=uid,objName=company,indirectObjName=relativity_company,relType='传导关联',eventCode=eventCode,eventName=eventName,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,emoLabel=emoLabel,emoScore=emoScore,impScore=importanceDgree,impLabel=importanceLabel,pubTime=ctime,isValid=1,dataStatus=1)
                        except Exception as e:
                            print(e,"预警信息重复180")
                        # insert_mongo(db_name=db_name,yqid=uid,objName=company,companyShortName=relativity_companyShortName,companyCode=relativity_companyCode,indirectObjName=relativity_company,transScore=relativity_score,relPath=relPath,relType='传导关联',relScore=relativity,relLabel=relLabel,personName='',eventCode=eventCode,eventName=eventName,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,emoLabel=emoLabel,emoScore=emoScore,emoConf=emoConf,impScore=importanceDgree,impLabel=importanceLabel,pubTime=ctime,title=title,summary=summary,keyword=keyword,srcUrl=url,srcType=srcType,source=source,content=content)


    else:
        # 获取关联公司数据
        r = requests.post('http://192.168.1.179:11068/relation/',data=json.dumps({'company':relativity_company}))
        # print('r',r)
        try:
            relation_result_list = r.json()['data']
        except Exception as e:
            print(e)
            with open("/shiye_kf3/gonggao/kafka_stream/logs/log_relation_result_list.log","a+") as w:
                w.write(relativity_company+"\n")
        

        # print('relation_result_list',relation_result_list,len(relation_result_list))
        relation_result_str = ''
        for relation_result in relation_result_list:
            relation_result_str += (relation_result[0] + '#*#' + relation_result[1] + '#*#' + str(relation_result[2]) + '#*#' + relation_result[3] + ';')
        relation_result_str = relation_result_str.strip(';')
        # print('relation_result_str',relation_result_str)

        if relation_result_str != '':
            redis_conn_129().hset('relation_company_path', relativity_company, relation_result_str)

        # 将间接关联数据插入数据统计表
        for relation_result in relation_result_list:
            if len(relation_result) == 4:
                company = relation_result[0]

                relativity_company = DBC2SBC(relation_result[1])
                # print('间接关联公司',relativity_company)

                relativity_score = str(relation_result[2])
                if bool(re.search(r'\d', relativity_score)):
                    relPath = relation_result[3]
                    if "-" in relPath:
                        # relativity_companyShortName = ''
                        # relativity_companyCode = ''
                        # if relativity_company in quancheng_list:
                        #     relativity_companyShortName = jiancheng_list[quancheng_list.index(relativity_company)]
                        #     relativity_companyCode = code_list[quancheng_list.index(relativity_company)]
                        relativity_companyShortName_info = get_ssgsdmjc(relativity_company)
                        relativity_companyShortName = relativity_companyShortName_info.get('short_name', '')
                        relativity_companyCode = relativity_companyShortName_info.get('cmp_code', '')


                        # insert_statistics(db=db1,yqid=uid,company=relativity_company,ctimeDate=ctimeDate,relType='传导关联',emoLabel=emoLabel,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,eventCode=eventCode,eventName=eventName)
                        insert_statistics_180(db=db2,yqid=uid,company=relativity_company,ctimeDate=ctimeDate,relType='传导关联',emoLabel=emoLabel,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,eventCode=eventCode,eventName=eventName)

                        try:
                            # insert_news_search(db=db1,yqid=uid,objName=company,companyShortName=relativity_companyShortName,companyCode=relativity_companyCode,indirectObjName=relativity_company,transScore=relativity_score,relPath=relPath,relType='传导关联',relScore=relativity,relLabel=relLabel,personName='',eventCode=eventCode,eventName=eventName,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,emoLabel=emoLabel,emoScore=emoScore,emoConf=emoConf,impScore=importanceDgree,impLabel=importanceLabel,pubTime=ctime,title=title,summary=summary,keyword=keyword,srcUrl=url,srcType=srcType,source=source,content=content,isValid=1,dataStatus=1)
                            insert_news_search_180(db=db2,yqid=uid,objName=company,companyShortName=relativity_companyShortName,companyCode=relativity_companyCode,indirectObjName=relativity_company,transScore=relativity_score,relPath=relPath,relType='传导关联',relScore=relativity,relLabel=relLabel,personName='',eventCode=eventCode,eventName=eventName,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,emoLabel=emoLabel,emoScore=emoScore,emoConf=emoConf,impScore=importanceDgree,impLabel=importanceLabel,pubTime=ctime,title=title,summary=summary,keyword=keyword,srcUrl=url,srcType=srcType,source=source,content=content,isValid=1,dataStatus=1)
                        except:
                            print('search error',uid)
                        # try:
                        #     insert_news_search_alert(db=db1, yqid=uid, objName=company, indirectObjName=relativity_company,
                        #                              relType='传导关联', eventCode=eventCode, eventName=eventName,
                        #                              firstLevelCode=firstLevelCode, firstLevelName=firstLevelName,
                        #                              secondLevelCode=secondLevelCode, secondLevelName=secondLevelName,
                        #                              thirdLevelCode=thirdLevelCode, thirdLevelName=thirdLevelName,
                        #                              fourthLevelCode=fourthLevelCode, fourthLevelName=fourthLevelName,
                        #                              emoLabel=emoLabel, emoScore=emoScore, impScore=importanceDgree,
                        #                              impLabel=importanceLabel, pubTime=ctime, isValid=1, dataStatus=1)
                        #
                        # except Exception as e:
                        #     print(e, "预警信息重复")

                        try:
                            print("预警180")
                            insert_news_search_alert_180(db=db2, yqid=uid, objName=company,
                                                     indirectObjName=relativity_company,
                                                     relType='传导关联', eventCode=eventCode, eventName=eventName,
                                                     firstLevelCode=firstLevelCode, firstLevelName=firstLevelName,
                                                     secondLevelCode=secondLevelCode, secondLevelName=secondLevelName,
                                                     thirdLevelCode=thirdLevelCode, thirdLevelName=thirdLevelName,
                                                     fourthLevelCode=fourthLevelCode, fourthLevelName=fourthLevelName,
                                                     emoLabel=emoLabel, emoScore=emoScore, impScore=importanceDgree,
                                                     impLabel=importanceLabel, pubTime=ctime, isValid=1, dataStatus=1)

                        except Exception as e:
                            print(e, "预警信息重复180")
                    # insert_mongo(db_name=db_name,yqid=uid,objName=company,companyShortName=relativity_companyShortName,companyCode=relativity_companyCode,indirectObjName=relativity_company,transScore=relativity_score,relPath=relPath,relType='传导关联',relScore=relativity,relLabel=relLabel,personName='',eventCode=eventCode,eventName=eventName,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,emoLabel=emoLabel,emoScore=emoScore,emoConf=emoConf,impScore=importanceDgree,impLabel=importanceLabel,pubTime=ctime,title=title,summary=summary,keyword=keyword,srcUrl=url,srcType=srcType,source=source,content=content)


# def relation_path_his(relativity_company,db1,uid,ctimeDate,emoLabel,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,thirdLevelCode,thirdLevelName,fourthLevelCode,fourthLevelName,jiancheng_list,quancheng_list,code_list,relativity,relLabel,eventCode,eventName,emoScore,emoConf,importanceDgree,importanceLabel,ctime,title,summary,keyword,url,srcType,source,content,personName='',isValid=1,dataStatus=1):
#     # 获取关联公司数据
#     r = requests.post('http://192.168.1.129:11068/relation/',data=json.dumps({'company':relativity_company.replace('(','（').replace(')','）')}))
#     relation_result_list = r.json()['data']


#     # 将间接关联数据插入数据统计表
#     for relation_result in relation_result_list:

#         company = relation_result[0]
#         relativity_company = DBC2SBC(relation_result[1])
#         relativity_score = str(relation_result[2])
#         relPath = relation_result[3]

#         relativity_companyShortName = ''
#         relativity_companyCode = ''
#         if relativity_company in quancheng_list:
#             relativity_companyShortName = jiancheng_list[quancheng_list.index(relativity_company)]
#             relativity_companyCode = code_list[quancheng_list.index(relativity_company)]

#         insert_statistics(db=db1,yqid=uid,company=relativity_company,ctimeDate=ctimeDate,relType='传导关联',emoLabel=emoLabel,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,eventCode=eventCode,eventName=eventName)

#         insert_news_search_his(db=db1,yqid=uid,objName=company,companyShortName=relativity_companyShortName.replace('(','（').replace(')','）'),companyCode=relativity_companyCode,indirectObjName=relativity_company.replace('(','（').replace(')','）'),transScore=relativity_score,relPath=relPath,relType='传导关联',relScore=relativity,relLabel=relLabel,personName='',eventCode=eventCode,eventName=eventName,firstLevelCode=firstLevelCode,firstLevelName=firstLevelName,secondLevelCode=secondLevelCode,secondLevelName=secondLevelName,thirdLevelCode=thirdLevelCode,thirdLevelName=thirdLevelName,fourthLevelCode=fourthLevelCode,fourthLevelName=fourthLevelName,emoLabel=emoLabel,emoScore=emoScore,emoConf=emoConf,impScore=importanceDgree,impLabel=importanceLabel,pubTime=ctime,title=title,summary=summary,keyword=keyword,srcUrl=url,srcType='新闻',source=source,content=content,isValid=1,dataStatus=1)

