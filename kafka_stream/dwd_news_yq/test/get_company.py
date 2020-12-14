# -*- coding:utf-8 -*-

"""
将mongo中的新闻数据全部过一遍提取内容中的公司名称

"""
import time
from asyncio import as_completed
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool

import pandas as pd
import pymongo
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

import utils
from bin.model.use_model_service import GetCompanyNameModel
from setting import MONGO_URL, MONGO_DB


def redis_cache():
    """
    通过缓存获取数据, 缓存无数据查询数据库再写入缓存
    :return: 公司撞库名单->DataFrame
    """
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


def hit_db(db_data, text):
    """
    撞库获取公司名称
    :param db_data: 撞库数据
    :param text: 被撞原文数据
    :return: 公司名称->list数组
    """
    company_df = db_data[db_data['stakeholderName'].apply(lambda x: x in text)]
    if len(company_df) == 0:
        return []
    else:
        # 将DataFrame去重转换为list
        return company_df['stakeholderName'].drop_duplicates().values.tolist()


def get_mongo_coll_names():
    """
    获取新闻资讯mongo集合名称
    :return: mg_db mongo库对象 , coll_names_list 集合名称 ==> list
    """
    mg_cli = pymongo.MongoClient(MONGO_URL)
    mg_db = mg_cli[MONGO_DB]
    coll_names_list = mg_db.list_collection_names(session=None)

    return coll_names_list


def time_stamp_date(time_stamp):
    """
    将时间戳转日期
    :param time_stamp: 时间戳
    :return: 日期
    """
    timeArray = time.localtime(int(time_stamp))
    date_time = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    return date_time


def use_model_get_company(contents, data_df):
    # 1. 调用模型提取公司名称 ==> list
    cnm = GetCompanyNameModel()
    client1_list = cnm.get_api(text=contents, client_num=cnm.bclient_1)
    client2_list = cnm.get_api(text=contents, client_num=cnm.bclient_2)
    model_company_list = client1_list + client2_list
    # 2. 调用撞库 ==> list
    hit_company_list = hit_db(db_data=data_df, text=contents)
    # 3. 拼接两个方法返回值 ==> list
    ret_company_list = list(set(model_company_list + hit_company_list))
    company = ','.join(ret_company_list)

    return company


def thread_task(data, data_df):
    """
    多线程任务
    :param data: mongo一条数据
    :param data_df: 公司撞库名单
    """
    post = {'id': [], 'title': [], 'ctime': [], 'gtime': [], 'url': [],
            'contents': [], 'company': [], 'collection_name': []}

    post.get('id').append(str(data.get('_id')))
    post.get('title').append(data.get('title'))
    post.get('ctime').append(time_stamp_date(data.get('ctime')))
    post.get('gtime').append(time_stamp_date(data.get('gtime')))
    post.get('url').append(data.get('url'))
    post.get('source').append(data.get('source'))
    post.get('content_xml').append(data.get('content_xml'))
    # 内容
    contents = ''.join(data.get('contents'))
    post.get('contents').append(contents)
    # 前端展示内容
    contents = ''.join(data.get('web_contents'))
    post.get('web_contents').append(contents)

    # 调用模型获取公司名称
    company = use_model_get_company(contents, data_df)
    post.get('company').append(company)

    df = pd.DataFrame(data=post)
    mysql_url = "mysql+pymysql://batchdata_3dep:shiye1805A@192.168.1.129:3306/sy_news_raw?charset=utf8"

    # 取消连接池
    con = create_engine(mysql_url, poolclass=NullPool)
    df.to_sql('sy_news_finance', con=con, if_exists='append', index=False)


def progress_task(coll_names_list, n, data_df):
    """
    多进程任务
    :param coll_names_list: mongo集合名
    :param n: 获取集合名索引位置
    :param data_df: 公司撞库名单
    """

    mg_cli = pymongo.MongoClient(MONGO_URL)
    mg_db = mg_cli[MONGO_DB]
    mg_cursor = mg_db[coll_names_list[n]]

    # TODO 添加获取mongo数据过滤条件
    # 先获取新闻发布时间在19年
    query_field = {'_id': 1, 'title': 1, 'ctime': 1, 'gtime': 1, 'url': 1, 'source': 1, 'contents': 1, 'web_contents': 1, 'content_xml': 1, 'company': 1, 'collection_name': 1}
    filter = {"ctime": {"$gte": 1514736000, "$lt": 1546272000}}
    # 通过条件筛查mongo数据
    for data in mg_cursor.find(filter, query_field):
        thread_task(data, data_df)


def done():
    # 通过缓存获取抓撞库数据
    data_df = redis_cache()

    # 获取 新闻资讯mongo集合名称
    coll_names_list = get_mongo_coll_names()

    # 进程池
    pool = Pool(4)
    ret_list = []
    for n in range(len(coll_names_list)):
        pool_ret = pool.apply_async(func=progress_task, args=(coll_names_list, n, data_df))
        ret_list.append(pool_ret)

    pool.close()
    pool.join()


if __name__ == '__main__':
    done()

