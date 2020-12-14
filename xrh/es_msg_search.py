# coding: utf-8
# linux代码
from elasticsearch import Elasticsearch, helpers
import os
from itertools import islice
import threading
import json
import pymongo
import pymysql
import argparse
import ast
import time
from datetime import datetime
import sys
import requests
import json
from DBUtils.PooledDB import PooledDB

# reload(sys)
# sys.setdefaultencoding('utf-8')
date_now = time.strftime('%Y-%m-%d', time.localtime(time.time()))
time_now_es = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

# 索引名
index_name = 'wxhf_index_' + date_now
# 别名
alias_name = 'wxhf_type_v1'
# 类型
_type = 'wxhf_type'
# es地址
# es_url = 'localhost:9200'
# es_url = '192.168.1.222:9200'
es_url = [
        {"host": "192.168.1.148", "port": 1482},
        {"host": "192.168.1.149", "port": 1492},
        {"host": "192.168.1.150", "port": 1502},
        # {"host": "192.168.1.128", "port": 1282}
]
# 节点名字
node_name = 'cluster_add'

# all:跑全量; add:跑增量
add_type_choice = 'all'
# add_type_choice = 'add'
# mongo数据表名
mongo_name = 'sy_msg_search_union'
# Mysql日志名称
mysql_log_name = 'data_log'

chunk_len = 10

parse = argparse.ArgumentParser()
parse.add_argument("--es_url", type=str, default=es_url, help="url:port")
parse.add_argument("--_index", type=str, default=index_name, help="索引名")
parse.add_argument("--id", type=str, default='8906180de5a032b08d367a26889ab6280', help="id")
parse.add_argument("--ch", type=str, default='3', help="channel")
parse.add_argument("--log_name", type=str, default='data_log', help="mysql log存储位置")


mapping_body = {
  "settings": {
    "index": {
      "mapping": {
        "total_fields": {
          "limit": "10000000"
        }
      },
      "number_of_shards": "3",
      "number_of_replicas": 1,
      "refresh_interval": "30s",
      "translog": {
        "flush_threshold_size": "3gb",
        "sync_interval": "100s",
        "durability": "async"
      }
    },
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "my_tokenizer"
        }
      },
      "tokenizer": {
        "my_tokenizer": {
          "type": "ngram",
          "min_gram": 2,
          "max_gram": 2,
          "token_chars": [
            "letter",
            "digit"
          ]
        }
      }
    }
  },
  "mappings": {
    "wxhf_type": {
      "properties": {
        "real_time": {
          "type": "keyword"
        },
        "company": {
          "type": "text",
          "analyzer": "my_analyzer",
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          }
        },
        "title": {
          "type": "text",
          "analyzer": "my_analyzer",
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          }
        },
        "contents": {
          "type": "text",
          "analyzer": "my_analyzer",
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          }
        },
        "department": {
          "type": "text",
          "analyzer": "my_analyzer",
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          }
        }
      }
    }
  }
}

FLAGS, unparsed = parse.parse_known_args(sys.argv[1:])
# 实例化es对象
es = Elasticsearch(FLAGS.es_url)
# 根据传参创建索引
es.indices.create(index=FLAGS._index, body=mapping_body, ignore=[400])
# # 创建别名
es.indices.put_alias(index=[index_name], name='wxhf_type_v1')
# 删除索引
# es.indices.delete(index=FLAGS._index, ignore=[400, 404])
# 删除别名
# es.indices.delete_alias(index=[index_name], name='wxhf_type_v1')
# 别名指向
# alias_List = es.indices.get_alias('wxhf_type_v1').keys()
# remove_alias = [alias for alias in alias_List if alias != index_name]
# es.indices.delete_alias(index=remove_alias, name='wxhf_type_v1')
client = pymongo.MongoClient('mongodb://root:shiye1805A@192.168.1.125:10011,192.168.1.126:10011,192.168.1.127:10011/admin')  # 服务器数据地址
# client = pymongo.MongoClient('mongodb://localhost')  # 服务器数据地址
print('connected to mongo')

mongodb = client['sy_multi_raw']
db_name = mongodb[mongo_name]

# 将实际日期转换成int类型以便于比较
def convert_real_time(real_time):
    timeArray = datetime.datetime.strptime(real_time, '%Y-%m-%d %H:%M:%S')
    #print(timeArray)
    int_time = int(timeArray.timestamp())
    return int_time


max_i_time='2020-03-18 14:51:07'


add_type={'all':'2020-03-17 14:51:07','add':max_i_time}
data=[]
data_list = db_name.find({"i_time": {"$gt": add_type[add_type_choice]}})
# data_list = db_name[mongo_name].find({"_id": "63531c07eae13420953000acc0b0538f"})

for i in data_list:
    data.append(i)

# 如果没有增量数据则中止程序
if len(data)==0:
    print('There is no new data!')
    os._exit(0)
add_data_num = len(data)
print('add_data_num:',add_data_num)

def write_get_index(command):
    aliases_now = ''
    index_now = ''
    resp = requests.get(command)
    search_obj = json.loads(resp.text)
    # print(search_obj)
    if 'error' in search_obj.keys():  # {'error': 'alias [supervision_split_index] missing', 'status': 404}
        print('当前无索引(别名)', search_obj['error'], resp.status_code)
    # os._exit(0)
    elif (len(search_obj.keys()) == 1):
        # try:
        for item in search_obj.keys():  # {'index_2019-02-24': {'aliases': {'supervision_split_index': {}}}}
            aliases_now = list(search_obj[item]['aliases'].keys())[0]
            index_now = list(search_obj.keys())[0]
            index_now = list(search_obj.keys())[0]
        count = es.count(index_now)['count']
        # if count<2:#可以修改
        #   print('错误:数据量小')
        #   os._exit(0)
    # except Exception as e:
    #   print('dd')
    #   os._exit(0)
    elif (len(search_obj.keys()) > 1):
        print('错误:存在多个索引指向同一个别名')
        os._exit(0)

    return aliases_now, index_now




def findall(p, s):
    '''Yields all the positions of the pattern p in the string s.'''
    i = s.find(p)
    while i != -1:
        yield i
        i = s.find(p, i + 1)


def delete_break(content):
    postions1 = []
    postions2 = []
    while ('\n\n\n' in content):
        content = content.replace('\n\n\n', '\n\n')
    [postions2.append(i) for i in findall('\n\n', content) if i > 15]
    # print(len(postions2))
    for item in postions2:
        content = content[:item] + '✘♂' + content[item + 2:]
    [postions1.append(i) for i in findall('\n', content) if i > 15 and i < len(content) - 18]
    # print(len(postions1))
    for item in postions1:
        if 'table_node_' not in content[item - 15:item] and '✘♂' not in content[item:item + 18]:
            content = content[:item] + '✿' + content[item + 1:]
    while ('✿' in content):
        content = content.replace('✿', '')
    while ('✘♂' in content):
        if len(postions1) == 0 and len(postions2) >= 3 and 'table_node_' not in content:
            content = content.replace('✘♂', '')
        else:
            content = content.replace('✘♂', '\n\n')
    return content

def get_table(mark, content):
    '''Yields all the positions of the pattern p in the string s.'''
    i = content.find(mark)
    while i != -1:
        yield i
        i = content.find(mark, i + 1)

def SBC2DBC(ustring):
    """半角转全角"""
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 32:  # 半角空格直接转化
            inside_code = 12288
        elif inside_code >= 32 and inside_code <= 126:  # 半角字符（除空格）根据关系转化
            inside_code += 65248

        rstring += chr(inside_code)
    return rstring

def bulk_es(chunk_data):
    bulks = []

    for i, item in enumerate(chunk_data):
        id_temp = item['_id']
        item['id'] = id_temp
        item.pop('_id', None)  # 删除原json中的id项
        item.pop('url_wx', None)  # 删除原json中的url_wx项
        item['real_time'] = time.strftime('%Y-%m-%d', time.localtime(item['ctime']))
        ######
        bulks.append({
            "_index": FLAGS._index,
            "_type": _type,
            "_id": id_temp,
            "_source": item
        })
    return bulks

# 将list分为chunk_len大小，数据过大易出问题。
def chunks(list_, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(list_), n):
        yield list_[i:i + n]


if (__name__ == "__main__"):
    ch = int(FLAGS.ch)
    data_chunks = list(chunks(data, chunk_len))
    for j, data_chunk in enumerate(data_chunks):
        bulks = bulk_es(data_chunk)
        helpers.bulk(es, bulks)  # 提交到ES

