from mysql_yi.mysql_pool import PymysqlPool

class mysq_his_k():
    def __init__(self):
        self.sq_time_d = '2017-01-01'
        self.sq_time_x = '2017-06-01'
        self.list_mysql = []
        self.num = 0
        self.num_z = 0
        self.num_page = 0
        self.um = 0
    def mysql_125(self):
        return PymysqlPool('125')

    def mysql_select(self):
        """
        获取不重复的 数据
        :return:
        """
        conn = self.mysql_125()
        sql = "SELECT * from sy_project_raw.dwa_me_gg_search_wgq_his_test_0001 WHERE pubTime >= '{}' and pubTime < '{}' ".format(self.sq_time_d,self.sq_time_x)
        count, infos = conn.getAll(sql)
        conn.dispose()
        self.infos = infos
        self.um = 0
        for k,i in enumerate(self.infos):
            self.num_z += 1
            list_da = self.mysql_list(i)
            self.list_mysql.append(list_da)
            self.num += 1
            if self.num >= 100 or self.num == self.num_page:
                print(len(self.list_mysql))
                self.mysql_insert_g_gao(self.list_mysql)
                self.list_mysql = []
                self.num = 0
                num_page = len(self.infos) - self.num_z
                print("++++++++++++++++++++++++++++++++")
                print(num_page)
                if num_page < 100 and self.um == 0:
                    self.num_page = num_page
                    self.um = 1
                print(self.num_page)
        self.num_z = 0
    def mysql_insert_g_gao(self,list_es):
        """
        储存数据  公告表 --125
        :param result:
        :return:
        """
        print("asdfasdfad+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        conn = self.mysql_125()
        sql = """INSERT IGNORE INTO sy_project_raw.dwa_me_gg_search_his  ( 
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
                                                                           ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        conn.insertMany(sql, list_es)
        conn.dispose()
        print("已存入--dwa_me_gg_search_wgq_his_test_555555--125")


    def mysql_list(self,j):
        """
        制作数据列表
        :return:
        """
        try:
            list_da = []
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
                if j.get("srcType") == "A股公告":
                    list_da.append(j.get("srcUrl").replace("www","static"))
                else:
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
            return tuple(list_da)
        except:
            print("查询无数据")


if __name__ == '__main__':
    mysq_his_k().mysql_select()