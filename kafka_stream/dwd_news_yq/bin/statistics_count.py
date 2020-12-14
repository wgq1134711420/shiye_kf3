#-*-coding:utf-8-*-

'''
文件名： statistics_count.py
功能： 新闻舆情计数统计

代码历史：
2020-08-14：郭蓟申 创建代码
'''

from collections import defaultdict
import os
import re
import jieba
import codecs
import decimal
import pymongo
import pymysql
import time
import datetime
import sys
import json
from sklearn.feature_extraction.text import TfidfVectorizer,TfidfTransformer,CountVectorizer

from DBUtils.PooledDB import PooledDB
from sshtunnel import SSHTunnelForwarder


def statistics_count():

    db2 = pymysql.connect(**{"host":"192.168.1.129","user":"aliyun_rw","password":"shiye_aliyun","db":"sy_project_raw","port":3306,"charset":"utf8"})

    cursor2 = db2.cursor()
    quancheng_list = []
    jiancheng_list = []
    code_list = []
    cursor2.execute('SELECT * FROM A_stock_code_name_fyi')
    co_names = list(cursor2.fetchall())
    cn1 = [desc[0] for desc in cursor2.description]
    for i in range(len(co_names)):
        quancheng_list.append(list(co_names[i])[cn1.index('all_name')])
        jiancheng_list.append(list(co_names[i])[cn1.index('short_name')])
        code_list.append(list(co_names[i])[cn1.index('cmp_code')])
    # print('quancheng_list:',len(quancheng_list))
    # print('jiancheng_list:',len(jiancheng_list))
    # print('code_list:',len(code_list))

    # dwd_news_all_short_name
    cursor2.execute('SELECT * FROM dwd_news_all_short_name')
    co_names = list(cursor2.fetchall())
    cn1 = [desc[0] for desc in cursor2.description]
    for i in range(len(co_names)):
        quancheng_list.append(list(co_names[i])[cn1.index('all_name')])
        jiancheng_list.append(list(co_names[i])[cn1.index('short_name')])
        code_list.append(list(co_names[i])[cn1.index('cmp_code')])
    # print('quancheng_list:',len(quancheng_list))
    # print('jiancheng_list:',len(jiancheng_list))
    # print('code_list:',len(code_list))

    db2.close()

    return quancheng_list,jiancheng_list,code_list

if __name__ == '__main__':
    curr_time = datetime.datetime.now().strftime("%Y-%m-%d")
    # print(curr_time)

    # statistics_count()
    # print(__name__)