from mysql_yi.mysql_pool import PymysqlPool
from es_yi.es_pool import ES
from threading_in.shee import Spider,worker

class es_select_where():
    def __init__(self):
        self.e_ = ES().es
        self.id = 0
        self._count = 0
        self.data_count = {}
        self.data_es_is = {}
        self.data = {}
        self.data_es_3 = {}
        self.title = ""
        self.subtractdate_pubtime = ""
    def mysql_125(self):
        return PymysqlPool('125')
    def mysql_180(self):
        return PymysqlPool('180')
    def es_count(self):
        """
        查询es的数据总量
        :return:
        """
        query = {
              "query": {
                  "bool": {"must": [{"range": {"id": {"gt": 0}}}]}
              }
            }
        self.data_count = self.e_.search(index='sy_comp_announ_index_his', body=query, doc_type='doc')
        self._count = self.data_count.get("hits").get("total")
        self.es_select_count()
    def es_select_count(self):
        """
        利用查询到的数据量做分页  抽取数据 并限定字段
        :return:
        """
        print(self._count)
        for i in range(6965490,self._count,10):
            query = {
                  "query": {
                    "bool": {"must":[{"range":{"id":{"gt":i,"lte":i + 10}}}]}
                  },
                "_source":[
                    "id",
                    "title",
                    "subtractdate_pubtime"
                ],
                "sort": [
                    {
                        "id": {
                            "order": "asc"
                        }
                    }
                ]
                }
            self.data = self.e_.search(index='sy_comp_announ_index_his', body=query, doc_type='doc')
            self.es_where()
    def es_where(self):
        """
        获取数据后 在es中进行返查  检查数据是否重复
        :return:
        """
        for i in self.data.get("hits").get("hits"):
            self.data_es_3 = i.get("_source")
            self.subtractdate_pubtime = self.data_es_3.get("subtractdate_pubtime")
            self.title = self.data_es_3.get("title")
            self.id = self.data_es_3.get("id")
            query = {
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "term": {
                                            "title.keyword": {
                                                "value": "{}".format(self.title)
                                            }
                                        }
                                    },
                                    {
                                        "term": {
                                            "subtractdate_pubtime": {
                                                "value": "{}".format(self.subtractdate_pubtime)
                                            }
                                        }
                                    },
                                    {
                                        "range": {
                                            "id": {
                                                "lt": self.id
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }
            self.data_es_is = self.e_.search(index='sy_comp_announ_index_his', body=query, doc_type='doc')

            if self.data_es_is.get("hits").get("total") > 0:
                print("跳过的数据id{}    和数量{}".format(self.id,self.data_es_is.get("hits").get("total")))
                continue
            else:
                print("id{}".format(self.id))
                self.mysql_select()
    def mysql_select(self):
        """
        获取不重复的 数据
        :return:
        """
        conn = self.mysql_125()
        sql = "SELECT id,yqid,title,webname,companyName,cmpShortName,cmpCode,bondFull,bondAbbr,bondCode,firstIndustry,firstIndustryCode,secondIndustry,secondIndustryCode,threeIndustry,threeIndustryCode,firstLevelCode,firstLevelName,secondLevelCode,secondLevelName,threeLevelCode,threeLevelName,fourLevelCode,fourLevelName,eventCode,eventName,emoScore,emoLabel,emoConf,impScore,impLabel,srcType,srcUrl,pubTime,getTime,isValid,dataStatus FROM sy_project_raw.dwa_me_gg_search_wgq_his_yue WHERE id = {}".format(self.id)
        count, infos = conn.getAll(sql)
        conn.dispose()
        self.infos = infos
        list_es = self.mysql_list()
        self.mysql_insert_g_gao(list_es)
    def mysql_insert_g_gao(self,list_es):
        """
        储存数据  公告表 --180
        :param result:
        :return:
        """
        print(list_es)
        print(self.id)
        print("asdfasdfad+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        conn = self.mysql_125()
        sql = """INSERT IGNORE INTO sy_project_raw.dwa_me_gg_search_wgq_his_test_0001 ( id,
                                                                                   yqid,
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
                                                                                   pubTime,
                                                                                   getTime,
                                                                                   isValid,
                                                                                   dataStatus
                                                                                   ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        conn.insertMany(sql, list_es)
        conn.dispose()
        print("已存入--dwa_me_gg_search_wgq_his_test--180")
        self.log_es_id()
    def log_es_id(self):
        """
        将存储成功的 id保存
        :return:
        """
        with open("/shiye_kf3/gonggao/kafka_stream/logs/es_id.log" ,"w") as w:
            w.write(str(self.id))
        print("日志以保存id为--{}".format(self.id))
    def mysql_list(self):
        """
        制作数据列表
        :return:
        """
        try:
            list_es = []
            list_da = []
            for j in self.infos:
                if j.get("id", ""):
                    list_da.append(j.get("id"))
                else:
                    list_da.append("")
                if j.get("yqid", ""):
                    list_da.append(j.get("yqid"))
                else:
                    list_da.append("")

                if j.get("title", ""):
                    list_da.append(j.get("title"))
                else:
                    list_da.append("")

                if j.get("webname", ""):
                    list_da.append(j.get("webname"))
                else:
                    list_da.append("")

                if j.get("companyName", ""):
                    list_da.append(j.get("companyName"))
                else:
                    list_da.append("")

                if j.get("cmpShortName", ""):
                    list_da.append(j.get("cmpShortName"))
                else:
                    list_da.append("")

                if j.get("cmpCode", ""):
                    list_da.append(j.get("cmpCode"))
                else:
                    list_da.append("")

                if j.get("bondFull", ""):
                    list_da.append(j.get("bondFull"))
                else:
                    list_da.append("")

                if j.get("bondAbbr", ""):
                    list_da.append(j.get("bondAbbr"))
                else:
                    list_da.append("")

                if j.get("bondCode", ""):
                    list_da.append(j.get("bondCode"))
                else:
                    list_da.append("")

                if j.get("firstIndustry", ""):
                    list_da.append(j.get("firstIndustry"))
                else:
                    list_da.append("")

                if j.get("firstIndustryCode", ""):
                    list_da.append(j.get("firstIndustryCode"))
                else:
                    list_da.append("")

                if j.get("secondIndustry", ""):
                    list_da.append(j.get("secondIndustry"))
                else:
                    list_da.append("")

                if j.get("secondIndustryCode", ""):
                    list_da.append(j.get("secondIndustryCode"))
                else:
                    list_da.append("")

                if j.get("threeIndustry", ""):
                    list_da.append(j.get("threeIndustry"))
                else:
                    list_da.append("")

                if j.get("threeIndustryCode", ""):
                    list_da.append(j.get("threeIndustryCode"))
                else:
                    list_da.append("")

                if j.get("firstLevelCode", ""):
                    list_da.append(j.get("firstLevelCode"))
                else:
                    list_da.append("")

                if j.get("firstLevelName", ""):
                    list_da.append(j.get("firstLevelName"))
                else:
                    list_da.append("")

                if j.get("secondLevelCode", ""):
                    list_da.append(j.get("secondLevelCode"))
                else:
                    list_da.append("")

                if j.get("secondLevelName", ""):
                    list_da.append(j.get("secondLevelName"))
                else:
                    list_da.append("")

                if j.get("threeLevelCode", ""):
                    list_da.append(j.get("threeLevelCode"))
                else:
                    list_da.append("")

                if j.get("threeLevelName", ""):
                    list_da.append(j.get("threeLevelName"))
                else:
                    list_da.append("")

                if j.get("fourLevelCode", ""):
                    list_da.append(j.get("fourLevelCode"))
                else:
                    list_da.append("")

                if j.get("fourLevelName", ""):
                    list_da.append(j.get("fourLevelName"))
                else:
                    list_da.append("")

                if j.get("eventCode", ""):
                    list_da.append(j.get("eventCode"))
                else:
                    list_da.append("")

                if j.get("eventName", ""):
                    list_da.append(j.get("eventName"))
                else:
                    list_da.append("")

                if j.get("emoScore", ""):
                    list_da.append(j.get("emoScore"))
                else:
                    list_da.append("")

                if j.get("emoLabel", ""):
                    list_da.append(j.get("emoLabel"))
                else:
                    list_da.append("")

                if j.get("emoConf", ""):
                    list_da.append(j.get("emoConf"))
                else:
                    list_da.append("")

                if j.get("impScore", ""):
                    list_da.append(j.get("impScore"))
                else:
                    list_da.append("")

                if j.get("impLabel", ""):
                    list_da.append(j.get("impLabel"))
                else:
                    list_da.append("")

                if j.get("srcType", ""):
                    list_da.append(j.get("srcType"))
                else:
                    list_da.append("")

                if j.get("srcUrl", ""):
                    list_da.append(j.get("srcUrl"))
                else:
                    list_da.append("")

                if j.get("pubTime", ""):
                    list_da.append(j.get("pubTime").strftime("%Y-%m-%d %H:%M:%S"))
                else:
                    list_da.append("")

                if j.get("getTime", ""):
                    list_da.append(j.get("getTime").strftime("%Y-%m-%d %H:%M:%S"))
                else:
                    list_da.append("")

                if j.get("isVaif", ""):
                    list_da.append(j.get("isVaif",""))
                else:
                    list_da.append("")

                if j.get("dataStatus", ""):
                    list_da.append(j.get("dataStatus"))
                else:
                    list_da.append("")
                list_es.append(list_da)
            return list_es
        except:
            print("查询无数据")
if __name__ == '__main__':
    es_select_where().es_count()


