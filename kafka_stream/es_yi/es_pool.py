import time
import traceback

import math
import pymysql
from elasticsearch import Elasticsearch, helpers

class ES:
    def __init__(self, mapping=None, index_name=None, index_type=None):
        # 创建es对象
        self.es = Elasticsearch(
            # hosts=["172.20.1.68:9200", "172.20.1.69:9200", "172.20.1.70:9200"],
            hosts=['192.168.1.148:1482', '192.168.1.149:1492'],
            http_compress=True
        )
        self.mapping = mapping  # 设置mapping
        self.index_name = index_name  # 创建新索引名称
        self.index_type = index_type  # 创建类型

    #  设置ES副本
    def setRepNum(self):
        rep_num = {
            "number_of_replicas": 1
        }
        # 实时更改特定的索引级别设置
        self.es.indices.put_settings(body=rep_num, index=[self.index_name])

    def create(self):
        self.es.indices.create(index=self.index_name, body=self.mapping, ignore=[400])  # 创建索引

    # 切换别名
    def exchangeAliases(self):
        try:
            alias_name = 'sy_comp_basic_info_index'
            # 为当前索引指向别名
            self.es.indices.put_alias(index=[self.index_name], name=alias_name)
            # 获取别名下对应的索引
            alias_List = self.es.indices.get_alias(alias_name).keys()
            remove_alias = [alias for alias in alias_List if alias != self.index_name]
            if remove_alias:
                self.es.indices.delete_alias(index=remove_alias, name=alias_name)
        except:
            logger.error(traceback.format_exc())

    def saveES(self, ACTIONS_DF):
        """
        将数据保存es
        :param ACTIONS_DF: 需要保存数据集
        """
        if len(ACTIONS_DF) == 0:
            return
        ret = []
        for source in ACTIONS_DF.to_dict(orient='records'):
            action = {
                '_op_type': 'index',  # 默认为index.
                "_index": self.index_name,
                "_type": self.index_type,
                "_source": source
            }
            ret.append(action)
            if len(ret) == 4000:
                try:
                    helpers.bulk(client=self.es, actions=ret, request_timeout=100)
                except Exception as e:
                    print(e)
                ret = []  # 初始化
        else:
            helpers.bulk(client=self.es, actions=ret)