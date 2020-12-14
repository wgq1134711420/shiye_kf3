# -*- coding:utf-8 -*-

"""
工具类函数
"""

import json
import os
import traceback

import pymongo
import redis
from elasticsearch import Elasticsearch
from sqlalchemy import create_engine

from bin.log import globalLog
from setting import REDIS_DB, REDIS_HOST, REDIS_PORT, MONGO_URL, MONGO_DB, ES_HOST_PORT, ES_INDEX, REDIS_PASSWORD, BASE_DIR


class ES:

    def __init__(self):
        self.es = None

    def con_es(self, is_auth=False):
        """
        :param is_auth: 用户名密码状态
        :return: es_obj
        """

        # 读取mapping
        mapping_json_path = 'config/NewsESMapping.json'
        with open(os.path.join(BASE_DIR, mapping_json_path)) as f:
            mapping = json.load(f)
        try:
            if is_auth:
                es = Elasticsearch(
                    hosts=ES_HOST_PORT,                 # 主机 端口
                    http_auth=('elastic', 'password'),  # 认证
                    http_compress=True                  # 压缩
                )
            else:
                es = Elasticsearch(hosts=ES_HOST_PORT, http_compress=True)
        except:
            traceback.print_exc()
            globalLog.error(traceback.format_exc())
        else:
            self.es = es
            # 创建索引 忽视创建重复400状态码
            es.indices.create(index=ES_INDEX, body=mapping, ignore=[400])
            return es

    def set_rep_num(self, index_name):
        """
        设置ES副本为1
        :param index_name: 索引名
        """
        rep_num = {
            "number_of_replicas": 1
        }
        if self.es:
            # 实时更改特定的索引级别设置
            self.es.indices.put_settings(body=rep_num, index=[index_name])
        else:
            raise Exception('not es object')

    def exchange_aliases(self, index_name, alias_name):
        """
        为特定索引创建别名

        :param index_name: 索引名
        :param alias_name: 别名
        """
        if self.es:
            self.es.indices.put_alias(index=[index_name], name=alias_name)
        else:
            globalLog.error(traceback.format_exc())
            raise Exception('not es object')


# 连接redis
def con_redis():
    # decode_responses 将返回数据不是二级制byte
    redis_cli = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)
    try:
        redis_cli.ping()
    except:
        traceback.print_exc()
        globalLog.error(traceback.format_exc())
    else:
        return redis_cli


# 连接数据库
def con_mysql(sql, con):
    """
    通过sqlalchemy连接数据库
    :param sql: sql查询语句
    :param con: mysql_url 连接地址 ==>str
    :return: 数据
    """
    try:
        conn = create_engine(con)
        cursor = conn.execute(sql)
        result = cursor.fetchall()
    except:
        traceback.print_exc()
        globalLog.error(traceback.format_exc())
    else:
        # 查询数据长度1时直接从列表中获取元组中获取数据
        # 例: [(423507,)]
        if len(result) == 1:
            return result[0][0]
        else:
            return result


# 连接mongo
def con_mongo(collection_name=None):
    """
    :param collection_name: mongo集合名
    :return: mongo游标
    """
    try:
        mg_cli = pymongo.MongoClient(MONGO_URL)
        mg_db = mg_cli[MONGO_DB]
        mg_cursor = mg_db[collection_name]
    except:
        traceback.print_exc()
        globalLog.error(traceback.format_exc())
    else:
        return mg_cursor
