# -*-coding:utf-8-*-

import json
import codecs
import re
import pymysql
import time
import pymongo
import os
import ast
import redis
import datetime

# mongo上的爬取数据
mongo_name = 'api_csrc_RF_html'
# 要更新的数据
update_data_name = 'api_csrc_RF_union'
# Mysql日志名称
mysql_log_name = 'data_log'
# all:跑全量; add:跑增量
add_type_choice = 'all'
# add_type_choice = 'add'

# 读取连接数据库配置文件
load_config = '/home/seeyii/increase_nlp/db_config.json'
with open(load_config, 'r', encoding="utf-8") as f:
    reader = f.readlines()[0]
config_local = json.loads(reader)


# # 通过连接redis来获取是否有爬虫的标志
# if add_type_choice == 'add':
#     # conn = redis.StrictRedis(host = '127.0.0.1', port = 6379, password = 'shiye', db = 3)
#     conn = redis.StrictRedis(**config_local['local_redis'])
#     set_key = mongo_name
#     #conn.set(set_key,1)#test
#     index_redis = conn.get(set_key)
#     if index_redis == b'0':
#         print('#无爬虫操作退出')
#         os._exit(0)


# 将实际日期转换成int类型以便于比较
def convert_real_time(real_time):
    timeArray = datetime.datetime.strptime(real_time, '%Y-%m-%d %H:%M:%S')
    # print(timeArray)
    int_time = int(timeArray.timestamp())
    return int_time


# 打开数据库连接，从数据库获取当前最新数据的gtime
# db = pymysql.connect('127.0.0.1', 'root', 'shiye', 'EI_BDP', 3306, charset='utf8')
db = pymysql.connect(**config_local['local_sql'])
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()

# mongocli = pymongo.MongoClient('mongodb://seeyii:shiye@127.0.0.1:27017/admin')
mongocli = pymongo.MongoClient(config_local['cluster_mongo'])
# mongocli = pymongo.MongoClient('mongodb://192.168.1.63:27017')
print('connected to mongo')
db_name = mongocli['EI_BDP']

# 当前最新数据的gtime
last_gtime = db_name[mongo_name].find_one({"gtime": {"$exists": True}}, sort=[("gtime", -1)])["gtime"]

mongodb = mongocli['EI_BDP_DATA']
collection = mongodb[update_data_name + '_old']
collection2 = mongodb[update_data_name]
# collection = mongodb.api_csrc_RF_union_test


max_gtime = 0
if add_type_choice == 'add':
    # 如果增量处理，取log里的时间戳
    cursor.execute('SELECT * FROM ' + mysql_log_name)
    l1 = list(cursor.fetchall())
    cn = [desc[0] for desc in cursor.description]
    # 初始化max_gtime
    for i in l1:
        if i[cn.index('table_')] == update_data_name:
            num_gtime = convert_real_time(str(i[cn.index('end_gtime')]))
            if max_gtime < num_gtime:
                max_gtime = num_gtime
print('max_gtime:', max_gtime)

# 创建公司全称和简称列表
quancheng_list = []
jiancheng_list = []
code_list = []
cursor.execute('SELECT * FROM company_all_short_name')
co_names = list(cursor.fetchall())
cn = [desc[0] for desc in cursor.description]
for i in range(len(co_names)):
    quancheng_list.append(list(co_names[i])[cn.index('all_name')])
    jiancheng_list.append(list(co_names[i])[cn.index('short_name')])
    code_list.append(list(co_names[i])[cn.index('code')])

add_type = {'all': 0, 'add': max_gtime}
data = []
# data_list = db_name[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}})
# data_list_api = db_name[mongo_name].find({"api_time": {"$gt": add_type[add_type_choice]}})

# data_list = db_name[mongo_name].find({"ctime": {"$gt": 1546272000}})

data_list = db_name[mongo_name].find({"_id": '6e7ff69c2d97386eb352e1b04118d8861'})

no_dupli_id = []
for i in data_list:
    # if i.get('_id') == '1757c64f188d3f50840d912567e8bddd8':
    # print(type(i.get('gtime')))
    # if i.get('gtime') > 1552003200:
    no_dupli_id.append(i.get('_id'))
    data.append(i)

# for i in data_list_api:
#     if i.get('_id') not in no_dupli_id:
#         data.append(i)

# 如果没有增量数据则中止程序
if len(data) == 0:
    # conn.set(set_key, 0)
    print('There is no new data!')
    os._exit(0)
add_data_num = len(data)
print('add_data_num:', add_data_num)


# # 读取本地mongo文件（测试用）
# mongo_data = 'api_csrc_RF_html.json'
# data = []
# with open(mongo_data, 'rb') as f:
#     for line in f:
#         # print(json.loads(line).get('_id'))
#         # if json.loads(line).get('_id') == '0cc564500a0b3f65860e2b717fd9c7206':
#         data.append(json.loads(line))
# # print(len(data))
# # print(type(data))


# 字符串全角转半角
def DBC2SBC(ustring):
    rstring = ''
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 0x3000:
            inside_code = 0x0020
        else:
            inside_code -= 0xfee0
        # 转完之后不是半角字符返回原来的字符
        if inside_code < 0x0020 or inside_code > 0x7e:
            rstring += uchar
        else:
            rstring += chr(inside_code)
    return rstring


# 分割
def split_it(section, layer):
    pos_list = []
    new_section = []
    end_tmp = 0

    # if '目录' in section:
    #     layer = layer + layer

    # print([section])
    # print(layer)

    pos_tmp = 0
    for i in layer:
        # print(i)
        pos = section.find(i)
        # print([section[pos : pos + 10]])
        if pos == -1 or ('%' in section[pos: pos + 10] or '万元' in section[pos: pos + 10] or \
                         '亿元' in section[pos: pos + 10] or (
                                 is_Chinese(section[pos: pos + 10].replace(' ', '')) == False)):
            # print('continue',[section[pos : pos + 10]])
            continue
        elif pos != -1 and pos > pos_tmp:
            if not ('10' in section[pos: pos + 5] and '10' not in i):
                pos_tmp = pos
                if i[0] == ':' or i[0] == '。':
                    pos_list.append(pos + 1)
                else:
                    pos_list.append(pos)
    # print(pos_list)

    for p in range(len(pos_list)):
        # print(p)
        if p == 0:
            begin = 0
        else:
            begin = pos_list[p - 1]

        end = pos_list[p]
        new_section.append(section[begin:end])
        end_tmp = end
    new_section.append(section[end_tmp:])
    # print(new_section)
    if len(pos_list) != 0:
        return new_section
    else:
        return [section]


def is_Chinese(word):
    for ch in word:
        if '\u4e00' <= ch <= '\u9fff':
            return True
    return False


# 判断是问题还是回答
def judge_class(section):
    if ('说明' in section[:5] or '答' in section[:10] or '回复' in section[:10] or '回复:' in section[:10] \
            or '回复如下:' in section or '回复公告如下' in section):
        return '_a'
    elif '问题' in section[:10] or '你公司' in section or '请说明' in section or '请补充披露' in section or '详细说明' in section \
            or '请结合' in section or '请会计师对' in section or '请保荐机构' in section or '请申请人补充说明' in section or '请申请人说明' in section \
            or '请申请人' in section[:10]:
        return '_q'

    a_list = [type9, type18, type31]
    for al in a_list:
        for j in al:
            if j in section or j.replace('\n', '') in section or '答复' in section or '【回复】' in section:
                return '_a'

    q_list = [type2, type3, type4, type10, type11, type12, type13, type14, type15,
              type17, type19, type20, type21, type22, type23, type24, type25, type26,
              type27, type28, type29, type30, type32]
    for ql in q_list:
        for i in ql:
            if i == section[:len(i)] or i.replace('\n', '') == section[:len(i)]:
                # print(i)
                # print(section[:len(i)])
                return '_q'

    return '_N'


type1 = ['\n1、', '\n2、', '\n3、', '\n4、', '\n5、', '\n6、', '\n7、', '\n8、', '\n9、', '\n10、',
         '\n11、', '\n12、', '\n13、', '\n14、', '\n15、', '\n16、', '\n17、', '\n18、', '\n19、', '\n20、',
         '\n21、', '\n22、', '\n23、', '\n24、', '\n25、', '\n26、', '\n27、', '\n28、', '\n29、', '\n30、',
         '\n31、', '\n32、', '\n33、', '\n34、', '\n35、', '\n36、', '\n37、', '\n38、', '\n39、', '\n40、',
         '\n41、', '\n42、', '\n43、', '\n44、', '\n45、', '\n46、', '\n47、', '\n48、', '\n49、', '\n50、',
         '\n51、', '\n52、', '\n53、', '\n54、', '\n55、', '\n56、', '\n57、', '\n58、', '\n59、', '\n60、',
         '\n61、', '\n62、', '\n63、', '\n64、', '\n65、', '\n66、', '\n67、', '\n68、', '\n69、', '\n70、',
         '\n71、', '\n72、', '\n73、', '\n74、', '\n75、', '\n76、', '\n77、', '\n78、', '\n79、', '\n80、',
         '\n81、', '\n82、', '\n83、', '\n84、', '\n85、', '\n86、', '\n87、', '\n88、', '\n89、', '\n90、',
         '\n91、', '\n92、', '\n93、', '\n94、', '\n95、', '\n96、', '\n97、', '\n98、', '\n99、', '\n100、', ':1、']

# type2 = ['\n(1)','\n(2)','\n(3)','\n(4)','\n(5)','\n(6)','\n(7)','\n(8)','\n(9)','\n(10)',
#           '\n(11)','\n(12)','\n(13)','\n(14)','\n(15)','\n(16)','\n(17)','\n(18)']

type2 = ['。请你公司:', '。请你公司:', '。请你公司:', '。请你公司:', '。请你公司:', '。请你公司:', '。请你公司:', '。请你公司:', '。请你公司:', '。请你公司:']

type3 = ['\n事项1、', '\n事项2、', '\n事项3、', '\n事项3、', '\n事项4、', '\n事项5、', '\n事项6、', '\n事项7、', '\n事项8、', '\n事项9、', '\n事项10、']

type4 = ['\n问题1', '\n问题2', '\n问题3', '\n问题4', '\n问题5', '\n问题6', '\n问题7', '\n问题8', '\n问题9', '\n问题10',
         '\n问题11', '\n问题12', '\n问题13', '\n问题14', '\n问题15', '\n问题16', '\n问题17', '\n问题18', '\n问题19', '\n问题20',
         '\n问题21', '\n问题22', '\n问题23', '\n问题24', '\n问题25', '\n问题26', '\n问题27', '\n问题28', '\n问题29', '\n问题30',
         '\n问题31', '\n问题32', '\n问题33', '\n问题34', '\n问题35', '\n问题36', '\n问题37', '\n问题38', '\n问题39', '\n问题40',
         '\n问题41', '\n问题42', '\n问题43', '\n问题44', '\n问题45', '\n问题46', '\n问题47', '\n问题48', '\n问题49', '\n问题50',
         '\n问题51', '\n问题52', '\n问题53', '\n问题54', '\n问题55', '\n问题56', '\n问题57', '\n问题58', '\n问题59', '\n问题60']
# '\n问题','\n问题','\n问题','\n问题','\n问题','\n问题','\n问题','\n问题','\n问题','\n问题','\n问题','\n问题','\n问题']

#          '\n二十一、','\n二十二、','\n二十三、','\n二十四、','\n二十五、','\n二十六、','\n二十七、','\n二十八、','\n二十九、','\n三十、',
#          '\n三十一、','\n三十二、','\n三十三、','\n三十四、','\n三十五、','\n三十六、','\n三十七、','\n三十八、','\n三十九、','\n四十、',
#          '\n四十一、','\n四十二、','\n四十三、','\n四十四、','\n四十五、','\n四十六、','\n四十七、','\n四十八、','\n四十九、','\n五十、',
#          '\n五十一、','\n五十二、','\n五十三、','\n五十四、','\n五十五、','\n五十六、','\n五十七、','\n五十八、','\n五十九、','\n六十、',
#          '\n六十一、','\n六十二、','\n六十三、','\n六十四、','\n六十五、','\n六十六、','\n六十七、','\n六十八、','\n六十九、','\n七十、',
#          '\n七十一、','\n七十二、','\n七十三、','\n七十四、','\n七十五、','\n七十六、','\n七十七、','\n七十八、','\n七十九、','\n八十、',
#          '\n八十一、','\n八十二、','\n八十三、','\n八十四、','\n八十五、','\n八十六、','\n八十七、','\n八十八、','\n八十九、','\n九十、',
#          '\n九十一、','\n九十二、','\n九十三、','\n九十四、','\n九十五、','\n九十六、','\n九十七、','\n九十八、','\n九十九、','\n一百、']

type5 = ['\n一、重点问题', '\n二、一般问题']

type6 = ['\n(一)', '\n(二)', '\n(三)', '\n(四)', '\n(五)', '\n(六)', '\n(七)', '\n(八)', '\n(九)']

type7 = ['\n1.', '\n2.', '\n3.', '\n4.', '\n5.', '\n6.', '\n7.', '\n8.', '\n9.', '\n10.',
         '\n11.', '\n12.', '\n13.', '\n14.', '\n15.', '\n16.', '\n17.', '\n18.', '\n19.', '\n20.',
         '\n21.', '\n22.', '\n23.', '\n24.', '\n25.', '\n26.', '\n27.', '\n28.', '\n29.', '\n30.',
         '\n31.', '\n32.', '\n33.', '\n34.', '\n35.', '\n36.', '\n37.']

type8 = ['。一、', '。二、', '。三、', '。四、', '。五、', '。六、', '。七、', '。八、', '。九、', '。十、',
         '。十一、', '。十二、', '。十三、', '。十四、', '十五、', '十六、', '。十七、', '。十八、', '。十九、', '。二十、']

type9 = ['\n答复如下:', '【回复说明】', '\n问题答复:', '\n回复：' '公司回复', '公司的回', '\n回复如', '\n补充回', '\n说明:', '[回复说明]', '\n回复说明', '\n公司答复',
         '【公司回复】', '公司说明',
         '一、问题答复', '【问题回复】', '〔补充回复〕', '一、上市公司回复', '\n答:', '[说明]', '一、回复说明', '一、反馈问题回复', '\n问题回复', '\n【回复概述】',
         '\n会计师回复:', '本所回复:', '\n反馈意见回复:\n', '\n经核查,会计师认为,\n', '\n请独立财务顾问和律师核查并发表明确意见。回复:', '【答复】',
         '\n回复:\n', '回复:', '回复:\n', '\n【回复】', '回复:\n', '公司其他需要说明的事项', '。回复', '\n回复:', '\n答复:', '回复:', '\n回复', '[回复]',
         '【申请人回复】',
         '【会计师核查意见】', '【保荐机构核查意见】', '\n申请人回复说明:']

type10 = ['\n关注1:', '\n关注2:', '\n关注3:', '\n关注4:', '\n关注5:', '\n关注6:', '\n关注7:', '\n关注8:', '\n关注9:', '\n关注10:']

# type11 = ['\n一、申请材料显示','\n二、申请材料显示','\n三、申请材料显示','\n四、申请材料显示','\n五、申请材料显示','\n六、申请材料显示','\n七、申请材料显示',
#           '\n八、申请材料显示','\n九、申请材料显示','\n十、申请材料显示','\n十一、申请材料显示','\n十二、申请材料显示','\n十三、申请材料显示','\n十四、申请材料显示',
#           '\n十五、申请材料显示','\n十六、申请材料显示','\n十七、申请材料显示','\n十八、申请材料显示','\n十九、申请材料显示','\n二十、申请材料显示',
#           '\n二十一、申请材料显示','\n二十二、申请材料显示','\n二十三、申请材料显示','\n二十四、申请材料显示','\n二十五、申请材料显示','\n二十六、申请材料显示',
#           '\n二十七、申请材料显示','\n二十八、申请材料显示','\n二十九、申请材料显示','\n三十、申请材料显示','\n三十一、申请材料显示','\n三十二、申请材料显示',
#           '\n三十三、申请材料显示','\n三十四、申请材料显示','\n三十五、申请材料显示','\n三十六、申请材料显示','\n三十七、申请材料显示','\n三十八、申请材料显示',
#           '\n三十九、申请材料显示','\n四十、申请材料显示','\n四十一、申请材料显示','\n四十二、申请材料显示',]

type11 = ['\n反馈意见一', '\n反馈意见二', '\n反馈意见三', '\n反馈意见四', '\n反馈意见五', '\n反馈意见六', '\n反馈意见七', '\n反馈意见八', '\n反馈意见九', '\n反馈意见十',
          '\n反馈意见十一', '\n反馈意见十二', '\n反馈意见十三', '\n反馈意见十四', '\n反馈意见十五', '\n反馈意见十六', '\n反馈意见十七', '\n反馈意见十八', '\n反馈意见十九',
          '\n反馈意见二十']

type12 = ['\n问题一', '\n问题二', '\n问题三', '\n问题四', '\n问题五', '\n问题六', '\n问题七', '\n问题八', '\n问题九', '\n问题十',
          '\n问题十一', '\n问题十二', '\n问题十三', '\n问题十四', '\n问题十五', '\n问题十六', '\n问题十七', '\n问题十八', '\n问题十九', '\n问题二十',
          '\n问题二十一', '\n问题二十二', '\n问题二十三', '\n问题二十四', '\n问题二十五', '\n问题二十六', '\n问题二十七', '\n问题二十八', '\n问题二十九', '\n问题三十']

type13 = ['\n反馈问题1', '\n反馈问题2', '\n反馈问题3', '\n反馈问题4', '\n反馈问题5', '\n反馈问题6', '\n反馈问题7', '\n反馈问题8', '\n反馈问题9',
          '\n反馈问题10', '\n反馈问题11', '\n反馈问题12', '\n反馈问题13', '\n反馈问题14', '\n反馈问题15', '\n反馈问题16', '\n反馈问题17', '\n反馈问题18',
          '\n反馈问题19', '\n反馈问题20', '\n反馈问题21', '\n反馈问题22', '\n反馈问题23', '\n反馈问题24', '\n反馈问题25', '\n反馈问题26', '\n反馈问题27',
          '\n反馈问题28', '\n反馈问题29', '\n反馈问题30', '\n反馈问题31', '\n反馈问题32', '\n反馈问题33', '\n反馈问题34', '\n反馈问题35', '\n反馈问题36',
          '\n反馈问题37', '\n反馈问题38', '\n反馈问题39', '\n反馈问题40']

type14 = ['第1题', '第2题', '第3题', '第4题', '第5题', '第6题', '第7题', '第8题', '第9题', '第10题',
          '第11题', '第12题', '第13题', '第14题', '第15题', '第16题', '第17题', '第18题', '第19题', '第20题',
          '第21题', '第22题', '第23题', '第24题', '第25题', '第26题', '第27题', '第28题', '第29题', '第30题']

type15 = ['\n关注情况一:', '\n关注情况二:', '\n关注情况三:', '\n关注情况四:', '\n关注情况五:', '\n关注情况六:', '\n关注情况七:', '\n关注情况八:', '\n关注情况九:',
          '\n关注情况十:']

type16 = ['\n1)、', '\n2)、', '\n3)、', '\n4)、', '\n5)、', '\n6)、', '\n7)、', '\n8)、', '\n9)、', '\n10)、']

# type17 = ['\n问题','\n问题','\n问题','\n问题','\n问题','\n问题','\n问题','\n问题','\n问题','\n问题','\n问题','\n问题','\n问题']

type17 = ['\n【问题1】', '\n【问题2】', '\n【问题3】', '\n【问题4】', '\n【问题5】', '\n【问题6】', '\n【问题7】', '\n【问题8】', '\n【问题9】', '\n【问题10】',
          '\n【问题11】', '\n【问题12】', '\n【问题13】', '\n【问题14】', '\n【问题15】', '\n【问题16】', '\n【问题17】', '\n【问题18】', '\n【问题19】',
          '\n【问题20】']

type18 = ['问题1答复', '问题2答复', '问题3答复', '问题4答复', '问题5答复', '问题6答复', '问题7答复', '问题8答复', '问题9答复', '问题10答复',
          '问题11答复', '问题12答复', '问题13答复', '问题14答复', '问题15答复', '问题16答复', '问题17答复', '问题18答复', '问题19答复', '问题20答复',
          '问题21答复', '问题22答复', '问题23答复', '问题24答复', '问题25答复', '问题26答复', '问题27答复', '问题28答复', '问题29答复', '问题30答复']

type19 = ['一、反馈问题', '二、反馈问题', '三、反馈问题', '四、反馈问题', '五、反馈问题', '六、反馈问题', '七、反馈问题', '八、反馈问题', '九、反馈问题', '十、反馈问题',
          '十一、反馈问题', '十二、反馈问题', '十三、反馈问题', '十四、反馈问题', '十五、反馈问题', '十六、反馈问题', '十七、反馈问题', '十八、反馈问题', '十九、反馈问题', '二十、反馈问题',
          '二十一、反馈问题', '二十二、反馈问题', '二十三、反馈问题', '二十四、反馈问题', '二十五、反馈问题', '二十六、反馈问题', '二十七、反馈问题', '二十八、反馈问题', '二十九、反馈问题',
          '三十、反馈问题']

type20 = ['\n反馈意见1', '\n反馈意见2', '\n反馈意见3', '\n反馈意见4', '\n反馈意见5', '\n反馈意见6', '\n反馈意见7', '\n反馈意见8', '\n反馈意见9', '\n反馈意见10',
          '\n反馈意见11', '\n反馈意见12', '\n反馈意见13', '\n反馈意见14', '\n反馈意见15', '\n反馈意见16', '\n反馈意见17', '\n反馈意见18', '\n反馈意见19',
          '\n反馈意见20', \
          '\n反馈意见21', '\n反馈意见22', '\n反馈意见23', '\n反馈意见24', '\n反馈意见25', '\n反馈意见26', '\n反馈意见27', '\n反馈意见28', '\n反馈意见29',
          '\n反馈意见30']

type21 = ['\n[反馈意见1]', '\n[反馈意见2]', '\n[反馈意见3]', '\n[反馈意见4]', '\n[反馈意见5]', '\n[反馈意见6]', '\n[反馈意见7]',
          '\n[反馈意见8]', '\n[反馈意见9]', '\n[反馈意见10]', '\n[反馈意见11]', '\n[反馈意见12]', '\n[反馈意见13]', '\n[反馈意见14]',
          '\n[反馈意见15]', '\n[反馈意见16]', '\n[反馈意见17]', '\n[反馈意见18]', '\n[反馈意见19]', '\n[反馈意见20]', '\n[反馈意见21]',
          '\n[反馈意见22]', '\n[反馈意见23]', '\n[反馈意见24]', '\n[反馈意见25]', '\n[反馈意见26]', '\n[反馈意见27]', '\n[反馈意见28]',
          '\n[反馈意见29]', '\n[反馈意见30]', '\n[反馈意见31]', '\n[反馈意见32]', '\n[反馈意见33]', '\n[反馈意见34]', '\n[反馈意见35]']

# type22 = ['问题1)','问题2)','问题3)','问题4)','问题5)','问题6)','问题7)','问题8)','问题9)','问题10)']

type22 = ['\n审核意见1', '\n审核意见2', '\n审核意见3', '\n审核意见4', '\n审核意见5', '\n审核意见6', '\n审核意见7', '\n审核意见8', '\n审核意见9', '\n审核意见10',
          '\n审核意见11', '\n审核意见12', '\n审核意见13', '\n审核意见14', '\n审核意见15', '\n审核意见16', '\n审核意见17', '\n审核意见18', '\n审核意见19',
          '\n审核意见20']

type23 = ['\n1.申请文件显示', '\n2.申请文件显示', '\n3.申请文件显示', '\n4.申请文件显示', '\n5.申请文件显示', '\n6.申请文件显示', '\n7.申请文件显示',
          '\n8.申请文件显示', '\n9.申请文件显示', '\n10.申请文件显示', '\n11.申请文件显示', '\n12.申请文件显示', '\n13.申请文件显示', '\n14.申请文件显示',
          '\n15.申请文件显示', '\n16.申请文件显示', '\n17.申请文件显示', '\n18.申请文件显示', '\n19.申请文件显示', '\n20.申请文件显示', '\n21.申请文件显示',
          '\n22.申请文件显示', '\n23.申请文件显示', '\n24.申请文件显示', '\n25.申请文件显示', '\n26.申请文件显示', '\n27.申请文件显示', '\n28.申请文件显示',
          '\n29.申请文件显示', '\n30.申请文件显示']

type24 = ['\n反馈意见第1条', '\n反馈意见第2条', '\n反馈意见第3条', '\n反馈意见第4条', '\n反馈意见第5条', '\n反馈意见第6条', '\n反馈意见第7条', '\n反馈意见第8条',
          '\n反馈意见第9条', '\n反馈意见第10条', '\n反馈意见第11条', '\n反馈意见第12条', '\n反馈意见第13条', '\n反馈意见第14条', '\n反馈意见第15条', '\n反馈意见第16条',
          '\n反馈意见第17条', '\n反馈意见第18条', '\n反馈意见第19条', '\n反馈意见第20条', '\n反馈意见第21条', '\n反馈意见第22条', '\n反馈意见第23条',
          '\n反馈意见第24条',
          '\n反馈意见第25条', '\n反馈意见第26条', '\n反馈意见第27条', '\n反馈意见第28条', '\n反馈意见第29条', '\n反馈意见第30条', '\n反馈意见第31条',
          '\n反馈意见第32条',
          '\n反馈意见第33条', '\n反馈意见第34条', '\n反馈意见第35条', '\n反馈意见第36条', '\n反馈意见第37条', '\n反馈意见第38条', '\n反馈意见第39条',
          '\n反馈意见第40条']

type25 = ['题目1', '题目2', '题目3', '题目4', '题目5', '题目6', '题目7', '题目8', '题目9', '题目10',
          '题目11', '题目12', '题目13', '题目14', '题目15', '题目16', '题目17', '题目18', '题目19', '题目20',
          '题目21', '题目22', '题目23', '题目24', '题目25', '题目26', '题目27', '题目28', '题目29', '题目30',
          '题目31', '题目32', '题目33', '题目34', '题目35', '题目36', '题目37', '题目38', '题目39', '题目40',
          '题目41', '题目42', '题目43', '题目44', '题目45', '题目46', '题目47', '题目48', '题目49', '题目50']

type26 = ['反馈问题一', '反馈问题二', '反馈问题三', '反馈问题四', '反馈问题五', '反馈问题六', '反馈问题七', '反馈问题八', '反馈问题九', '反馈问题十',
          '反馈问题十一', '反馈问题十二', '反馈问题十三', '反馈问题十四', '反馈问题十五', '反馈问题十六', '反馈问题十七', '反馈问题十八', '反馈问题十九', '反馈问题二十',
          '反馈问题二十一', '反馈问题二十二', '反馈问题二十三', '反馈问题二十四', '反馈问题二十五', '反馈问题二十六', '反馈问题二十七', '反馈问题二十八', '反馈问题二十九',
          '反馈问题三十', '反馈问题三十一', '反馈问题三十二', '反馈问题三十三', '反馈问题三十四', '反馈问题三十五', '反馈问题三十六', '反馈问题三十七']

type27 = ['\n第一题', '\n第二题', '\n第三题', '\n第四题', '\n第五题', '\n第六题', '\n第七题', '\n第八题', '\n第九题', '\n第十题',
          '\n第十一题', '\n第十二题', '\n第十三题', '\n第十四题', '\n第十五题', '\n第十六题', '\n第十七题', '\n第十八题', '\n第十九题', '\n第二十题',
          '\n第二十一题', '\n第二十二题', '\n第二十三题', '\n第二十四题', '\n第二十五题', '\n第二十六题', '\n第二十七题', '\n第二十八题', '\n第二十九题', '\n第三十题']

# type28 = ['\n一重点问题','\n二一般问题‌']

type28 = ['\n[补充反馈意见1]', '\n[补充反馈意见2]', '\n[补充反馈意见3]', '\n[补充反馈意见4]', '\n[补充反馈意见5]', '\n[补充反馈意见6]']

type29 = ['\n[重点问题1]', '\n[重点问题2]', '\n[重点问题3]', '\n[重点问题4]', '\n[重点问题5]', '\n[重点问题6]', '\n[重点问题7]', '\n[重点问题8]',
          '\n[重点问题9]', '\n[重点问题10]', '\n[重点问题11]', '\n[重点问题12]', '\n[重点问题13]', '\n[一般问题1]', '\n[一般问题2]', '\n[一般问题3]',
          '\n[一般问题4]', '\n[一般问题5]', '\n[一般问题6]', '\n[一般问题7]', '\n[一般问题8]', '\n[一般问题9]', '\n[一般问题10]', '\n[一般问题11]']

type30 = ['\n第一部分重点问题', '\n重点问题一', '\n重点问题二', '\n重点问题三', '\n重点问题四', '\n重点问题五', '\n重点问题六', '\n重点问题七',
          '\n重点问题八', '\n重点问题九', '\n重点问题十', '\n重点问题十一', '\n重点问题十二', '\n重点问题十三', '\n重点问题十四', '\n重点问题十五',
          '\n重点问题1', '\n重点问题2', '\n重点问题3', '\n重点问题4', '\n重点问题5', '\n重点问题6',
          '\n重点问题7', '\n重点问题8', '\n重点问题9', '\n重点问题10', '\n重点问题11', '\n重点问题12', '\n重点问题13', '\n第二部分一般问题',
          '\n一般问题一', '\n一般问题二', '\n一般问题三', '\n一般问题四', '\n一般问题五', '\n一般问题六', '\n一般问题七', '\n一般问题八',
          '\n一般问题九', '\n一般问题十', '\n一般问题1', '\n一般问题2', '\n一般问题3', '\n一般问题4', '\n一般问题5', '\n一般问题6', '\n一般问题7',
          '\n一般问题8', '\n一般问题9', '\n一般问题10', '\n一般问题11']

type31 = ['\n回复说明正文:']

# type32 = ['\n一、','\n二、','\n三、','\n四、','\n五、','\n六、','\n七、','\n八、','\n九、','\n十、','\n十一、','\n十二、','\n十三、','\n十四、','\n十五、','\n十六、','\n十七、','\n十八、','\n十九、','\n二十、']
type32 = []

all_type_list = [type9, type18, type1, type2, type3, type4, type5, type6, type7, type8, type10, type11, type12, type13,
                 type14, type15, type16, type17, type19, type20, type21, type22, type23, type24, type25, type26, type27,
                 type28, type29, type30, type31, type32]

a_list = [type9, type18, type31]

api_count = 0
for i_num, line_de in enumerate(data):
    top = []
    print('i_num==============', i_num)
    # print(line_de)

    dict_con = ''
    hf_con = ''
    url_reply = ''
    title_reply = ''
    date_reply = ''

    api_msg_content = ''
    api_msg_table_list = []
    api_msg_table_dict = {}
    api_msg_api_url = ''
    api_msg_api_date = ''
    api_msg_api_title = ''

    black_flag = False

    _id = line_de.get('_id')
    url_wx = line_de.get('url_wx')
    title_wx = line_de.get('title_wx')
    content_wx = line_de.get('content_wx')
    gtime = line_de.get('gtime')
    channel = line_de.get('channel')
    department = line_de.get('department')
    ctime = line_de.get('ctime')
    real_ctime = line_de.get('real_ctime')
    company = line_de.get('company')
    accessory_name = line_de.get('accessory_name')
    accessory_url = line_de.get('accessory_url')
    gsdm = line_de.get('gsdm')
    gsjc = line_de.get('gsjc')

    # # 提取公司简称
    # gs = gsjc.replace(' ','')
    # # print(gs)
    # if gs in jiancheng_list:
    #     pos = jiancheng_list.index(gs)
    #     company = quancheng_list[pos]
    # elif gs.replace('*','') in jiancheng_list:
    #     pos = jiancheng_list.index(gs.replace('*',''))
    #     company = quancheng_list[pos]
    if len(str(gsdm)) > 5 and gsdm in code_list:
        pos = code_list.index(gsdm)
        company = quancheng_list[pos]
    print(company, '<<<<<<<<<<<<<<<<<<')

    if 'api_msg' in line_de.keys() and line_de.get('api_msg'):
        api_msg_content = ast.literal_eval(line_de.get('api_msg'))['content']
        # print(api_msg_content)
        api_msg_table_list = ast.literal_eval(line_de.get('api_msg'))['table_list']
        for item in api_msg_table_list:
            # print(item)
            # print(type(item))
            for i, j in item.items():
                # print(i,j)
                api_msg_table_dict[i.replace('\n', '')] = j.replace('\n', '')
        api_msg_api_url = ast.literal_eval(line_de.get('api_msg'))['api_url']
        url_reply = api_msg_api_url
        api_msg_api_date = ast.literal_eval(line_de.get('api_msg'))['api_date']
        date_reply = api_msg_api_date
        api_msg_api_title = ast.literal_eval(line_de.get('api_msg'))['api_title']
        title_reply = api_msg_api_title
        api_count += 1

        api_con = DBC2SBC(api_msg_content.replace('\u200c', '').replace('\uff61', '').replace('\uff64', ''))
        # print(api_con)
        # if '.....' in api_con:
        #     api_con_tmp = api_con
        #     pos = 0
        #     while pos != -1:
        #         pos = api_con_tmp.find('..........')
        #         api_con_tmp = api_con_tmp[pos + 10:]

        #     api_con = api_con_tmp
        # print(api_con[:400])
        # print(url_reply)

        # print([api_con])
        pos1 = api_con.find('[{')
        pos2 = api_con.find('}]', pos1)
        if pos1 != -1 and pos2 != -1 and api_con != []:
            # if ' ' == api_con[0]:
            # # if '[{' in api_con and '}]' in api_con:
            #     new_api_con = api_con[pos1:pos2+2]
            #     new_api_con_dict = ast.literal_eval(new_api_con)
            #     # print(type(new_api_con_dict))
            #     # print(new_api_con_dict)
            #     for dc in new_api_con_dict:
            #         dict_con += dc['title'] + '\n'
            hf_con = api_con[:pos1] + api_con[pos2 + 2:]
            # print(hf_con)
        else:
            hf_con = api_con
            # print(hf_con)
        # print(dict_con)
        # print(url_reply)

    if '.pdf' in url_reply[-10:] and hf_con != '':
        new_hf_con = ''
        for i in hf_con.split('\n'):
            if (len(i) < 100 and 'table_node' not in i and is_Chinese(i) == False) or len(i) < 2:
                # print([i])
                # new_hf_con += i
                continue
            else:
                new_hf_con += (i + '\n')
        hf_con = new_hf_con
        # print([hf_con])

    new_con = []
    if '目录' in hf_con[:1000] or '目 录' in hf_con[:1000]:
        new_con_tmp = hf_con.split('\n')
        # print(len(new_con_tmp[:120]))
        # print(new_con_tmp[:120])
        # new_dict_con = dict_con.split('\n')
        # print(new_dict_con)
        max_num = 0
        min_num = 1000000
        for num, item in enumerate(new_con_tmp[:120]):
            # print([item.replace(' ','')])

            flag = True
            for huifu in (type9 + type18 + type31):
                if huifu.replace('\n', '') == item[:len(huifu)].replace(' ', ''):
                    flag = False
                    # print(huifu)

            # # q_list = [type2, type3, type4, type10, type11, type12, type13, type14, type15, type17, type19,
            # #           type20, type21, type22, type23, type24, type25, type26]
            # q_list = [type12]
            # for t in q_list:
            #     for question in t:
            #         # print(question.replace('\n',''))
            #         # print(item[:len(question)].replace(' ',''))
            #         if question.replace('\n','') == item[:len(question.replace('\n',''))].replace(' ',''):
            #             stop_sign = t[0]
            #             print(stop_sign)

            # print(num)
            # print([item])
            if flag == False:
                continue
            item = item.replace('-', '')
            # print([item])
            if len(item) > 6 and (
                    ']' in item[-10:] or '。' in item[-10:] or ':' in item[-10:] or '...' in item[-10:] or '题' in item[
                                                                                                                 -10:]) and \
                    ((item[-1] == ' ' and item[-2].isdigit() == True and item[-3].isdigit() == True) or \
                     (item[-1].isdigit() == True and item[-2].isdigit() == True) or \
                     (item[-3].isdigit() == True and item[-4].isdigit() == True)) and \
                    '2017' not in item[-10:] and '2016' not in item[-10:] and '2015' not in item[-10:] and \
                    '2018' not in item[-10:] and not (item[-3].isdigit() and item[-2] == '。') and not (item.strip()[-4:].isdigit()) and \
                    '.' not in item[-5:] and ']' not in item[-8:]:
                print([item[-10:]])
                max_num = num
            if ('目录' in item[-5:] or '目 录' in item[-5:]) and min_num > num:
                min_num = num
                # print(item)

        # print(min_num)
        # print(max_num)

        top = new_con_tmp[:min_num]
        # print(top)

        hf_con = ''
        # for i in (new_con_tmp[:min_num] + new_con_tmp[max_num + 1:]):
        for i in new_con_tmp[max_num + 1:]:
            new_con.append(i.replace(' ', ''))
            hf_con += ('\n' + i)
        # print(new_con)

        # for d_num, menu in enumerate(new_dict_con):

        #     # if item and item[-1].isdigit() == True:
        #     #     new_con_tmp.remove(item)

        #     if item[:10] == menu[:10] and item in new_con_tmp and menu in new_dict_con:
        #         # print(num)
        #         # print('item',item)
        #         # print('menu',menu)
        #         # print(url_reply)
        #         # print('new_con_tmp[num]',item)
        #         # print('new_dict_con',new_dict_con[d_num])
        #         new_con_tmp.remove(item)

        #         for i in new_con_tmp:
        #             new_con.append(i.replace(' ',''))
        #         new_dict_con.remove(menu)
        # print(new_con)
    else:
        # print('dict_con================\n',dict_con)
        # print('hf_con==================\n',hf_con)
        # print(url_reply)
        new_con = hf_con.replace(' ', '').split('\n')
        # print(new_con)
        # print([hf_con])

    # for i in new_con:
    #     print(i)

    replace_flag_huifu = False
    replace_flag_wenti = False
    new_con_add = []
    for i in new_con:
        if '回复:' in i[-5:]:
            # print(item)
            pos = i.find('回复:')
            if pos != -1:
                new_con_add.append(i[:pos])
                new_con_add.append('回复:')
                replace_flag_huifu = True
                # hf_con.replace('回复','\n回复',1)
                # print(hf_con)
        elif '问题一:' in i[-5:] and '重点问题' not in i:
            # print(item)
            pos = i.find('问题一:')
            if pos != -1:
                new_con_add.append(i[:pos])
                new_con_add.append('问题一:')
                replace_flag_wenti = True
                # hf_con.replace('问题一','\n问题一',1)
                # print([hf_con])
        else:
            new_con_add.append(i)

    # print(new_con_add)

    # 拆分信息
    layer_list = []
    # if i % 15 == 0:
    # print(hf_con)
    for num, item in enumerate(new_con_add):
        # print([item])
        f = False
        for t_num, t in enumerate(all_type_list):
            for ti in t:
                new_t = ti.replace('\n', '').replace('\r', '')
                # print(new_t)
                # print(item[:len(new_t)])
                if new_t == item[:len(new_t)]:
                    layer_list.append(all_type_list[t_num])
                    # print(new_t)
                    # print(item[:len(new_t)])
                    # print(num)
                    # print(t_num)
                # if new_t == item[-(len(new_t)):]:
                #     layer_list.append(all_type_list[t_num])
                # print(new_t)
                # print(t_num)

        # print('----')

    l1 = []
    l2 = []
    l3 = []
    l4 = []
    huifu_flag = 4
    if len(layer_list) > 0:
        l1 = layer_list[0]
        if l1 == type9 or l1 == type18:
            # print('l1')
            huifu_flag = 1
        while l1 in layer_list:
            layer_list.remove(l1)

        huifu_flag -= 1
        if len(layer_list) > 0 and huifu_flag != 0:
            l2 = layer_list[0]
            if l2 == type9 or l2 == type18:
                # print('l2')
                huifu_flag = 1
            while l2 in layer_list:
                layer_list.remove(l2)

            huifu_flag -= 1
            if len(layer_list) > 0 and huifu_flag != 0:
                l3 = layer_list[0]
                if l3 == type9 or l3 == type18:
                    # print('l3')
                    huifu_flag = 1
                while l3 in layer_list:
                    layer_list.remove(l3)

                huifu_flag -= 1
                if len(layer_list) > 0 and huifu_flag != 0:
                    l4 = layer_list[0]
                    while l4 in layer_list:
                        layer_list.remove(l4)

        # if l1 == []:
        #     error_list.append(_id)

        # if _id == '0b6d0b38d8fe3f30942b1e374cabe090':
        # print([new_con])

        # if type9 != l1 and type9 != l2 and type9 != l3 and type9 != l4 and type18 != l1 and type18 != l2 and type18 != l3 and type18 != l4:
        # if 1:
    print(l1)
    print(l2)
    print(l3)
    print(l4)
    #     print(_id)
    #     print(api_msg_api_url)

    # print(_id)

    #     print(api_msg_api_url)
    #     print(gshf_content_url)
    #     print(hjnr_content_url)
    #     print('=========')

    # 拆分
    # layer1
    # print(type(hf_con))
    final_hf_con = hf_con.replace(' ', '')
    # print([hf_con])

    if replace_flag_huifu == True:
        pos = final_hf_con.find('回复:')
        final_hf_con = final_hf_con.replace('回复:', '\n回复:', 1)
    if replace_flag_wenti == True and '重点问题' not in final_hf_con:
        final_hf_con = final_hf_con.replace('问题一', '\n问题一', 1)
    # print([final_hf_con])

    layer1_content = split_it(final_hf_con, l1)
    # print(layer1_content)

    content_list = []
    for i in layer1_content:
        # print(i,'\n---------------')
        # content_list.append(i)
        layer2_content = split_it(i, l2)

        for j in layer2_content:
            # print(j,'\n----------------')
            layer3_content = split_it(j, l3)

            for k in layer3_content:
                # print(k,'\n----------------')
                layer4_content = split_it(k, l4)
                # content_list.append(k)

                for l in layer4_content:
                    if l != '' and l != ' ' and l != '\n':
                        content_list.append(l)
                        # print([l],'\n----------------')

    # print('=============')

    result_list = []
    # print(type(top))
    if top != []:
        top_tmp = ''
        for i in top:
            top_tmp += i.replace(' ', '')
        result_list = [('0_0', top_tmp)]

    # print(url)
    tmp_class = '_q'
    tmp_num = 0
    begin_tmp_num = 1
    class_flag = False
    for num, c in enumerate(content_list):
        # print([c[:100]])
        if judge_class(c) != '_N':
            tmp_class = judge_class(c)

        # 检查
        if tmp_class == '_a':
            class_flag = True

        # print(judge_class(c))
        # print(tmp_class)
        if tmp_class == '_q':
            tmp_num += 1

        if c[:2] in l1 or c[:3] in l1 or c[:4] in l1 or c[:5] in l1 or c[:6] in l1 or c[:7] in l1 or c[:8] in l1 or \
                c[:9] in l1 or c[:10] in l1 or c[:11] in l1 or c[:12] in l1 or '\n' + c[:1] in l1 or '\n' + c[
                                                                                                            :2] in l1 or \
                '\n' + c[:3] in l1 or '\n' + c[:4] in l1 or '\n' + c[:5] in l1 or '\n' + c[:6] in l1 or '\n' + c[
                                                                                                               :7] in l1 or \
                '\n' + c[:8] in l1 or '\n' + c[:9] in l1 or '\n' + c[:10] in l1 or '\n' + c[:11] in l1 or '\n' + c[
                                                                                                                 :12] in l1:
            if l1 == type5 or l1 == type29 or l1 == type30:
                result_list.append((str(num + begin_tmp_num) + tmp_class + '_' + str(tmp_num) + '_0', c))
            else:
                result_list.append((str(num + begin_tmp_num) + tmp_class + '_' + str(tmp_num) + '_' + str(tmp_num), c))

            # print(str(num) + tmp_class + '_0',[c])

        elif c[:2] in l2 or c[:3] in l2 or c[:4] in l2 or c[:5] in l2 or c[:6] in l2 or c[:7] in l2 or c[:8] in l2 or \
                c[:9] in l2 or c[:10] in l2 or c[:11] in l2 or c[:12] in l2 or '\n' + c[:1] in l2 or '\n' + c[
                                                                                                            :2] in l2 or \
                '\n' + c[:3] in l2 or '\n' + c[:4] in l2 or '\n' + c[:5] in l2 or '\n' + c[:6] in l2 or '\n' + c[
                                                                                                               :7] in l2 or \
                '\n' + c[:8] in l2 or '\n' + c[:9] in l2 or '\n' + c[:10] in l2 or '\n' + c[:11] in l2 or '\n' + c[
                                                                                                                 :12] in l2:
            result_list.append((str(num + begin_tmp_num) + tmp_class + '_' + str(tmp_num) + '_' + str(tmp_num), c))
            # print(str(num) + tmp_class + '_1',[c])

        elif c[:2] in l3 or c[:3] in l3 or c[:4] in l3 or c[:5] in l3 or c[:6] in l3 or c[:7] in l3 or c[:8] in l3 or \
                c[:9] in l3 or c[:10] in l3 or c[:11] in l3 or c[:12] in l3 or '\n' + c[:1] in l3 or '\n' + c[
                                                                                                            :2] in l3 or \
                '\n' + c[:3] in l3 or '\n' + c[:4] in l3 or '\n' + c[:5] in l3 or '\n' + c[:6] in l3 or '\n' + c[
                                                                                                               :7] in l3 or \
                '\n' + c[:8] in l3 or '\n' + c[:9] in l3 or '\n' + c[:10] in l3 or '\n' + c[:11] in l3 or '\n' + c[
                                                                                                                 :12] in l3:
            result_list.append((str(num + begin_tmp_num) + tmp_class + '_' + str(tmp_num) + '_' + str(tmp_num), c))
            # print(str(num) + tmp_class + '_2',[c])

        elif c[:2] in l4 or c[:3] in l4 or c[:4] in l4 or c[:5] in l4 or c[:6] in l4 or c[:7] in l4 or c[:8] in l4 or \
                c[:9] in l4 or c[:10] in l4 or c[:11] in l4 or c[:12] in l4 or '\n' + c[:1] in l4 or '\n' + c[
                                                                                                            :2] in l4 or \
                '\n' + c[:3] in l4 or '\n' + c[:4] in l4 or '\n' + c[:5] in l4 or '\n' + c[:6] in l4 or '\n' + c[
                                                                                                               :7] in l4 or \
                '\n' + c[:8] in l4 or '\n' + c[:9] in l4 or '\n' + c[:10] in l4 or '\n' + c[:11] in l4 or '\n' + c[
                                                                                                                 :12] in l4:
            result_list.append((str(num + begin_tmp_num) + tmp_class + '_' + str(tmp_num) + '_' + str(tmp_num), c))
            # print(str(num) + tmp_class + '_3',[c])
        elif num == 0:
            result_list.append(('0_0', c))
            begin_tmp_num = 0
            # print(str(num) + '_begin',[c])

            # 检查
            # print(_id)
    # 检查
    if class_flag == False and 'api_msg' in line_de.keys() and len(api_msg_content) > 100:
        print(_id)
        print(api_msg_api_url)
        print(l1)
        print(l2)
        print(l3)
        print(l4)

    if l1 == type1 or l2 == type1 or l3 == type1 or l4 == type1:
        print(_id)

    # print(result_list)
    content_dict = {}
    no_reply_flag = True
    for i in result_list:
        # print(i)
        content_dict[i[0]] = i[1]
        if '_a_' in i[0]:
            no_reply_flag = False
        print([i[0]], [i[1][:50]])
    # print(content_dict)
    if no_reply_flag or url_reply == 'http://analyze.doc.rongdasoft.com/null':
        content_dict = {}

    timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    try:
        collection.insert({'_id': _id,
                           'url_reply': url_reply,
                           'title_reply': title_reply,
                           'date_reply': date_reply,
                           'content_reply': content_dict,
                           'url_wx': url_wx,
                           'title_wx': title_wx,
                           'content_wx': content_wx,
                           'gtime': gtime,
                           'gsdm': gsdm,
                           'gsjc': gsjc,
                           'channel': channel,
                           'approve': '',
                           'financing_type': '',
                           'department': department,
                           'ctime': ctime,
                           'real_ctime': real_ctime,
                           'tables': api_msg_table_dict,
                           'company': company,
                           'accessory_url': accessory_url,
                           'accessory_name': accessory_name,
                           'i_time': timestamp
                           })

    except:
        collection.save({'_id': _id,
                         'url_reply': url_reply,
                         'title_reply': title_reply,
                         'date_reply': date_reply,
                         'content_reply': content_dict,
                         'url_wx': url_wx,
                         'title_wx': title_wx,
                         'content_wx': content_wx,
                         'gtime': gtime,
                         'gsdm': gsdm,
                         'gsjc': gsjc,
                         'channel': channel,
                         'approve': '',
                         'financing_type': '',
                         'department': department,
                         'ctime': ctime,
                         'real_ctime': real_ctime,
                         'tables': api_msg_table_dict,
                         'company': company,
                         'accessory_url': accessory_url,
                         'accessory_name': accessory_name,
                         'i_time': timestamp
                         })

    content_dict_2 = {}
    key_list = []
    # tmp_class = 0
    for i, con in enumerate(content_dict):
        # print(con)
        if l3 != [] and l4 == []:
            split_con = con.split('_')
            # print(split_con)
            if len(split_con) == 4 and split_con[3] == '0':
                # print(split_con)
                key_list.append(con)

            # if con == '0_0':
            #     content_dict_2[con] = content_dict[con]
            # elif split_con[2] == str(tmp_class):
            #     content_dict_2[con] = content_dict[con]
            #     tmp_class
    # print('key_list',key_list)

    # if l3 != [] and l4 == []:
    #     if len(key_list) == 2:
    #         tmp_class = 0
    #         tmp_key = ''
    #         for con in content_dict:
    #             print(con,tmp_class)
    #             split_con = con.split('_')
    #             split_tmp_key = tmp_key.split('_')
    #             if con == '0_0':
    #                 print('!0_0')
    #                 content_dict_2[con] = content_dict[con]

    #             elif con == key_list[0]:
    #                 print('!key_list[0]')
    #                 tmp_class = int(split_con[2])
    #                 tmp_key = key_list[0]
    #                 # print('tmp_class',tmp_class)
    #                 # content_dict_2[con] = content_dict[con]

    #             elif con == key_list[1]:
    #                 print('!key_list[1]')
    #                 tmp_class = int(split_con[2])
    #                 tmp_key = key_list[1]
    #                 # print('tmp_class',tmp_class)
    #                 # content_dict_2[con] = content_dict[con]

    #             elif len(split_con)>2 and tmp_class + 1 == int(split_con[2]) and tmp_key != '':
    #                 print('!tmp_class + 1')
    #                 content_dict_2[split_tmp_key[0]+'_c_'+split_con[3]+'_'+split_tmp_key[3]] = content_dict[tmp_key]
    #                 content_dict_2[con] = content_dict[con]
    #                 tmp_class += 1

    #             elif len(split_con)>2 and tmp_class == int(split_con[2]) and tmp_key != '':
    #                 print('!tmp_class')
    #                 content_dict_2[con] = content_dict[con]

    #     elif len(key_list) == 1:
    #         tmp_class = 0
    #         tmp_key = ''
    #         for con in content_dict:
    #             print(con,tmp_class)
    #             split_con = con.split('_')
    #             split_tmp_key = tmp_key.split('_')
    #             if con == '0_0':
    #                 print('!0_0')
    #                 content_dict_2[con] = content_dict[con]

    #             elif con == key_list[0]:
    #                 print('!key_list[0]')
    #                 tmp_class = int(split_con[2])
    #                 tmp_key = key_list[0]
    #                 # print('tmp_class',tmp_class)
    #                 # content_dict_2[con] = content_dict[con]

    #             elif len(split_con)>2 and tmp_class + 1 == int(split_con[2]) and tmp_key != '':
    #                 print('!tmp_class + 1')
    #                 content_dict_2[split_tmp_key[0]+'_c_'+split_con[3]+'_'+split_tmp_key[3]] = content_dict[tmp_key]
    #                 content_dict_2[con] = content_dict[con]
    #                 tmp_class += 1

    #             elif len(split_con)>2 and tmp_class == int(split_con[2]) and tmp_key != '':
    #                 print('!tmp_class')
    #                 content_dict_2[con] = content_dict[con]

    if l3 != [] and l4 == []:
        if len(key_list) == 2:
            tmp_class = 0
            tmp_key = ''
            tmp_num = 0
            for con in content_dict:
                print(con, tmp_class)
                split_con = con.split('_')
                split_tmp_key = tmp_key.split('_')
                if con == '0_0':
                    print('!0_0')
                    content_dict_2[con] = content_dict[con]
                    tmp_num += 1

                elif con == key_list[0]:
                    print('!key_list[0]')
                    tmp_class = int(split_con[2])
                    tmp_key = key_list[0]
                    # print('tmp_class',tmp_class)
                    # content_dict_2[con] = content_dict[con]

                elif con == key_list[1]:
                    print('!key_list[1]')
                    tmp_class = int(split_con[2])
                    tmp_key = key_list[1]
                    # print('tmp_class',tmp_class)
                    # content_dict_2[con] = content_dict[con]

                elif len(split_con) > 2 and tmp_class + 1 == int(split_con[2]) and tmp_key != '':
                    print('!tmp_class + 1')
                    content_dict_2[str(tmp_num) + '_q_' + split_con[3] + '_' + split_tmp_key[3]] = content_dict[tmp_key]
                    tmp_num += 1
                    content_dict_2[str(tmp_num) + '_' + split_con[1] + '_' + split_con[2] + '_' + split_con[3]] = \
                    content_dict[con]
                    tmp_class += 1
                    tmp_num += 1

                elif len(split_con) > 2 and tmp_class == int(split_con[2]) and tmp_key != '':
                    print('!tmp_class')
                    content_dict_2[str(tmp_num) + '_' + split_con[1] + '_' + split_con[2] + '_' + split_con[3]] = \
                    content_dict[con]
                    tmp_num += 1

        elif len(key_list) == 1:
            tmp_class = 0
            tmp_key = ''
            for con in content_dict:
                print(con, tmp_class)
                split_con = con.split('_')
                split_tmp_key = tmp_key.split('_')
                if con == '0_0':
                    print('!0_0')
                    content_dict_2[con] = content_dict[con]
                    tmp_num += 1

                elif con == key_list[0]:
                    print('!key_list[0]')
                    tmp_class = int(split_con[2])
                    tmp_key = key_list[0]
                    # print('tmp_class',tmp_class)
                    # content_dict_2[con] = content_dict[con]

                elif len(split_con) > 2 and tmp_class + 1 == int(split_con[2]) and tmp_key != '':
                    print('!tmp_class + 1')
                    content_dict_2[str(tmp_num) + '_q_' + split_con[3] + '_' + split_tmp_key[3]] = content_dict[tmp_key]
                    tmp_num += 1
                    content_dict_2[str(tmp_num) + '_' + split_con[1] + '_' + split_con[2] + '_' + split_con[3]] = \
                    content_dict[con]
                    tmp_class += 1
                    tmp_num += 1

                elif len(split_con) > 2 and tmp_class == int(split_con[2]) and tmp_key != '':
                    print('!tmp_class')
                    content_dict_2[str(tmp_num) + '_' + split_con[1] + '_' + split_con[2] + '_' + split_con[3]] = \
                    content_dict[con]
                    tmp_num += 1
    else:
        # print('!else')
        for con in content_dict:
            content_dict_2[con] = content_dict[con]
            tmp_num += 1

    # print(content_dict_2)

    # c_check_flag = False
    # for con in content_dict_2:
    #     print((con,content_dict_2[con][:50]))
    #     sc = con.split('_')
    #     print(sc[1])

    try:
        collection2.insert({'_id': _id,
                            'url_reply': url_reply,
                            'title_reply': title_reply,
                            'date_reply': date_reply,
                            'content_reply': content_dict,
                            'url_wx': url_wx,
                            'title_wx': title_wx,
                            'content_wx': content_wx,
                            'gtime': gtime,
                            'gsdm': gsdm,
                            'gsjc': gsjc,
                            'channel': channel,
                            'approve': '',
                            'financing_type': '',
                            'department': department,
                            'ctime': ctime,
                            'real_ctime': real_ctime,
                            'tables': api_msg_table_dict,
                            'company': company,
                            'accessory_url': accessory_url,
                            'accessory_name': accessory_name,
                            'i_time': timestamp
                            })
        if content_reply != {}:
            try:
                delete_id = collection2.find_one({'title_wx': title_wx, 'ctime': ctime, 'content_reply': {}})['_id']
                print('delete_id', delete_id)

                sql_ = "INSERT INTO delete_id_ES (table_name, _id, company, reao_ctime) VALUES ('" + update_data_name + "','" + delete_id + "','" + company + "','" + real_ctime + "')"
                print(sql_)
                cursor.execute(sql_)
                db.commit()

                collection2.remove({'_id': delete_id})
                print('reportdocs DELETE', title_wx, ctime)
                delete_count += 1
            except:
                continue

    except:
        collection2.save({'_id': _id,
                          'url_reply': url_reply,
                          'title_reply': title_reply,
                          'date_reply': date_reply,
                          'content_reply': content_dict,
                          'url_wx': url_wx,
                          'title_wx': title_wx,
                          'content_wx': content_wx,
                          'gtime': gtime,
                          'gsdm': gsdm,
                          'gsjc': gsjc,
                          'channel': channel,
                          'approve': '',
                          'financing_type': '',
                          'department': department,
                          'ctime': ctime,
                          'real_ctime': real_ctime,
                          'tables': api_msg_table_dict,
                          'company': company,
                          'accessory_url': accessory_url,
                          'accessory_name': accessory_name,
                          'i_time': timestamp
                          })

print('api_count', api_count)


def convert_num_time(num_time):
    timeArray = time.localtime(num_time)
    real_time = time.strftime('%Y-%m-%d %H:%M:%S', timeArray)
    return (str(real_time))


# 写log
# sql_command_log = '''CREATE TABLE IF NOT EXISTS {0} (table_ VARCHAR(255),
#                                                      add_number VARCHAR(255),
#                                                      start_gtime DATETIME,
#                                                      end_gtime DATETIME,
#                                                      run_time timestamp
#                                                      )'''
# cursor.execute(sql_command_log.format(mysql_log_name))
# # sql_ = '''INSERT INTO {0} (table_,add_number)VALUES(%s%s)'''
#
# sql_ = '''INSERT INTO {0} (table_,
#                            add_number,
#                            start_gtime,
#                            end_gtime
#                            )VALUES(%s,%s,%s,%s)'''
# cursor.execute(sql_.format(mysql_log_name), (update_data_name,
#                                              str(add_data_num),
#                                              convert_num_time(max_gtime),
#                                              convert_num_time(last_gtime)
#                                              ))
# db.commit()

db.close()

# # 自动将新增数据导入ES
# command='''
#         python3 mongo2es.py --gtime_low {0} --gtime_high {1} --mongo_data_name {2}
#         '''
# print(command.format(str(max_gtime),str(last_gtime),update_data_name))
# os.system(command.format(str(max_gtime),str(last_gtime),update_data_name))


# if add_type_choice == 'add':
#     conn.set(set_key,0)
