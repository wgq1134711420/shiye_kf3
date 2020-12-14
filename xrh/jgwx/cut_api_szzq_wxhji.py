#-*-coding:utf-8-*-

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

table_name = 'api_szzq_wxhj_union'

# mongo上的爬取数据
mongo_name = 'api_szzq_wxhj_html'
# 要更新的数据
update_data_name = 'api_szzq_wxhj_union'
# Mysql日志名称
mysql_log_name = 'data_log'
# all:跑全量; add:跑增量
add_type_choice = 'all'
# add_type_choice = 'add'

# 读取连接数据库配置文件
load_config='/home/seeyii/increase_nlp/db_config.json'
with open(load_config, 'r', encoding="utf-8") as f:
    reader = f.readlines()[0]
config_local = json.loads(reader)

# 通过连接redis来获取是否有爬虫的标志
if add_type_choice == 'add':
    conn = redis.StrictRedis(host = '127.0.0.1', port = 6379, password = 'shiye', db = 3)
    set_key = mongo_name
    #conn.set(set_key,1)#test
    index_redis = conn.get(set_key)
    if index_redis == b'0':
        print('#无爬虫操作退出')
        os._exit(0)


def is_Chinese(word):
    for ch in word:
        if '\u4e00' <= ch <= '\u9fff':
            return True
    return False


# 将实际日期转换成int类型以便于比较
def convert_real_time(real_time):
    timeArray = datetime.datetime.strptime(real_time, '%Y-%m-%d %H:%M:%S')
    #print(timeArray)   
    int_time = int(timeArray.timestamp())
    return int_time


# 打开数据库连接，从数据库获取当前最新数据的gtime
db = pymysql.connect('127.0.0.1', 'root', 'shiye', 'EI_BDP', 3306, charset='utf8')
# db = pymysql.connect('192.168.1.63', 'root', '', 'EI_BDP', 3306, charset='utf8')
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()


client = pymongo.MongoClient(config_local['cluster_mongo'])
# client = pymongo.MongoClient('mongodb://192.168.1.63:27017')
print('connected to mongo')
db_name = client['EI_BDP']

# 当前最新数据的gtime
last_gtime = db_name[mongo_name].find_one({"gtime": {"$exists": True}}, sort=[("gtime", -1)])["gtime"]

mongodb = client['EI_BDP_DATA']
collection = mongodb[update_data_name]
# collection = mongodb.api_szzq_wxhj_union_test


max_gtime=0
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


add_type={'all':0,'add':max_gtime}
data=[]
data_list = db_name[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}})
# data_list = db_name[mongo_name].find({"_id": "63531c07eae13420953000acc0b0538f"})

for i in data_list:
    if i.get('_id') == 'e20be0d0621f3d5e988f48a064ca7701':
        data.append(i)

# 如果没有增量数据则中止程序
if len(data)==0:
    conn.set(set_key, 0)
    print('There is no new data!')
    os._exit(0)
add_data_num = len(data)
print('add_data_num:',add_data_num)


# # 读取本地mongo文件（测试用）
# mongo_data = 'api_szzq_wxhj_html.json'
# data = []
# with open(mongo_data, 'rb') as f:
#     for line in f:
#         # print(json.loads(line).get('_id'))
#         # if json.loads(line).get('_id') == '00245b0bd98f3ae983ac562bef24fcca':
#         data.append(json.loads(line))
# # print(len(data))
# # print(type(data))


# 字符串全角转半角
def DBC2SBC(ustring):
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

# 分割
def split_it(section,layer):
    if layer == []:
        return [section]

    # print([section])
    # print(layer)
    
    pos_list=[]
    new_section=[]
    end_tmp=0

    pos_tmp = 0
    for i in layer:
        # print([i])
        pos = section.find(i)
        # print([section[pos : pos + 10]])
        if layer != type9 and ('%' in section[pos : pos + 10] or '=' in section[pos : pos + 5] or '万元' in section[pos : pos + 8] or\
                '亿元' in section[pos : pos + 10] or '欧元' in section[pos : pos + 10] or ',' in section[pos : pos + 5] or\
                '元/股' in section[pos : pos + 10] or '亿股' in section[pos : pos + 10] or '万吨' in section[pos : pos + 10] or\
                (is_Chinese(section[pos : pos + 20].replace(' ','')) == False)):
            # print('continue',[section[pos : pos + 10]])
            continue
        elif pos != -1 and pos > pos_tmp:
            # print(i)
            if not ('10' in section[pos : pos + 5] and '10' not in i):
                pos_tmp = pos
                if i[0]==':' or i[0]=='。':
                    pos_list.append(pos + 1)
                else:
                    pos_list.append(pos)

    # print(layer)
    # print(pos_list)

    for p in range(len(pos_list)):
        # print(p)
        if p==0:
            begin=0
        else:
            begin=pos_list[p-1]

        end=pos_list[p]
        new_section.append(section[begin:end])
        end_tmp=end
    new_section.append(section[end_tmp:])
    # print(new_section)
    if len(pos_list)!=0:
        return new_section
    else:
        return [section]

# 判断是问题还是回答
def judge_class(section):
    if '说明' in section[:5] or '答复' in section[:5] or '回复' in section[:5] or '回复:' in section\
    or '回复如下:' in section or '回复公告如下' in section or '本公司回复' in section or '答:' in section[:5]\
    or '上市公司回复' in section[:10]:
        return '_a'
    elif '问题' in section[:5] or '你公司' in section or '请说明' in section or '请补充披露' in section or '详细说明' in section\
    or '请结合' in section or '请会计师对' in section or '请保荐机构' in section or '请申请人补充说明' in section\
    or '请申请人说明' in section[:20] or '据披露' in section[:10] or '草案显示' in section[:10]:
        return '_q'

    q_list = [type1,type2,type3,type4,type5,type6,type7,type8,type10,type11,type12,type13,type14,type15,type16,type17,\
              type18,type19,type20,type21,type22,type23,type24,type25,type26,type27,type28,type29]
    a_list = [type9]
    for al in a_list:
        for j in al:
            if j in section or j.replace('\n','') in section or '答复' in section or '【回复】' in section:
                return '_a'
    for ql in q_list:
        for i in ql:
            if i in section or i.replace('\n','') in section or '是否' in section or '核查并发表意见' in section\
            or '发表核查意见' in section:
                return '_q'
    return '_N'

type1 = ['\n1、','\n2、','\n3、','\n4、','\n5、','\n6、','\n7、','\n8、','\n9、','\n10、',
         '\n11、','\n12、','\n13、','\n14、','\n15、','\n16、','\n17、','\n18、','\n19、','\n20、',
         '\n21、','\n22、','\n23、','\n24、','\n25、','\n26、','\n27、','\n28、','\n29、','\n30、',
         '\n31、','\n32、','\n33、','\n34、','\n35、','\n36、','\n37、','\n38、','\n39、','\n40、',
         '\n41、','\n42、','\n43、','\n44、','\n45、','\n46、','\n47、','\n48、','\n49、','\n50、',
         '\n51、','\n52、','\n53、','\n54、','\n55、','\n56、','\n57、','\n58、','\n59、','\n60、',
         '\n61、','\n62、','\n63、','\n64、','\n65、','\n66、','\n67、','\n68、','\n69、','\n70、',
         '\n71、','\n72、','\n73、','\n74、','\n75、','\n76、','\n77、','\n78、','\n79、','\n80、',
         '\n81、','\n82、','\n83、','\n84、','\n85、','\n86、','\n87、','\n88、','\n89、','\n90、',
         '\n91、','\n92、','\n93、','\n94、','\n95、','\n96、','\n97、','\n98、','\n99、','\n100、']

# type1 = []


type2 = ['\n问题:1','\n问题:2','\n问题:3','\n问题:4','\n问题:5','\n问题:6','\n问题:7','\n问题:8','\n问题:9','\n问题:10']

type3=['\n事项1','\n事项2','\n事项3','\n事项3','\n事项4','\n事项5','\n事项6','\n事项7','\n事项8','\n事项9','\n事项10','\n事项11','\n事项12']

type4=['\n问:1.','\n问:2.','\n问:3.','\n问:4.','\n问:5.','\n问:6.','\n问:7.','\n问:8.','\n问:9.','\n问:10.',
       '\n问:11.','\n问:12.','\n问:13.','\n问:14.','\n问:15.','\n问:16.','\n问:17.','\n问:18.','\n问:19.','\n问:20.',
       '\n问:21.','\n问:22.','\n问:23.','\n问:24.','\n问:25.','\n问:26.','\n问:27.','\n问:28.','\n问:29.','\n问:30.',
       '\n问:31.','\n问:32.','\n问:33.','\n问:34.','\n问:35.','\n问:36.','\n问:37.']

# type4 = ['\n问题1、','\n问题2、','\n问题3、','\n问题4、','\n问题5、','\n问题6、','\n问题7、','\n问题8、','\n问题9、','\n问题10、',
#        '\n问题11、','\n问题12、','\n问题13、','\n问题14、','\n问题15、','\n问题16、','\n问题17、','\n问题18、','\n问题19、','\n问题20、',
#        '\n问题21、','\n问题22、','\n问题23、','\n问题24、','\n问题25、','\n问题26、','\n问题27、','\n问题28、','\n问题29、','\n问题30、',
#        '\n问题31、','\n问题32、','\n问题33、','\n问题34、','\n问题35、','\n问题36、','\n问题37、','\n问题38、','\n问题39、','\n问题40、',
#        '\n问题41、','\n问题42、','\n问题43、','\n问题44、','\n问题45、','\n问题46、','\n问题47、','\n问题48、','\n问题49、','\n问题50、',
#        '\n问题51、','\n问题52、','\n问题53、','\n问题54、','\n问题55、','\n问题56、','\n问题57、','\n问题58、','\n问题59、','\n问题60']

type5 = ['\n一、','\n二、','\n三、','\n四、','\n五、','\n六、','\n七、','\n八、','\n九、','\n十、',
         '\n十一、','\n十二、','\n十三、','\n十四、','\n十五、','\n十六、','\n十七、','\n十八、','\n十九、','\n二十、',
         '\n二十一、','\n二十二、','\n二十三、','\n二十四、','\n二十五、','\n二十六、','\n二十七、','\n二十八、','\n二十九、','\n三十、',
         '\n三十一、','\n三十二、','\n三十三、','\n三十四、','\n三十五、','\n三十六、','\n三十七、','\n三十八、','\n三十九、','\n四十、',
         '\n四十一、','\n四十二、','\n四十三、','\n四十四、','\n四十五、','\n四十六、','\n四十七、','\n四十八、','\n四十九、','\n五十、',
         '\n五十一、','\n五十二、','\n五十三、','\n五十四、','\n五十五、','\n五十六、','\n五十七、','\n五十八、','\n五十九、','\n六十、',
         '\n六十一、','\n六十二、','\n六十三、','\n六十四、','\n六十五、','\n六十六、','\n六十七、','\n六十八、','\n六十九、','\n七十、',
         '\n七十一、','\n七十二、','\n七十三、','\n七十四、','\n七十五、','\n七十六、','\n七十七、','\n七十八、','\n七十九、','\n八十、',
         '\n八十一、','\n八十二、','\n八十三、','\n八十四、','\n八十五、','\n八十六、','\n八十七、','\n八十八、','\n八十九、','\n九十、',
         '\n九十一、','\n九十二、','\n九十三、','\n九十四、','\n九十五、','\n九十六、','\n九十七、','\n九十八、','\n九十九、','\n一百、']

# type6 = ['\n(一)','\n(二)','\n(三)','\n(四)','\n(五)','\n(六)','\n(七)','\n(八)','\n(九)']

type6 = ['\n问题1','\n问题2','\n问题3','\n问题4','\n问题5','\n问题6','\n问题7','\n问题8','\n问题9','\n问题10',
       '\n问题11','\n问题12','\n问题13','\n问题14','\n问题15','\n问题16','\n问题17','\n问题18','\n问题19','\n问题20',
       '\n问题21','\n问题22','\n问题23','\n问题24','\n问题25','\n问题26','\n问题27','\n问题28','\n问题29','\n问题30',
       '\n问题31','\n问题32','\n问题33','\n问题34','\n问题35','\n问题36','\n问题37','\n问题38','\n问题39','\n问题40',
       '\n问题41','\n问题42','\n问题43','\n问题44','\n问题45','\n问题46','\n问题47','\n问题48','\n问题49','\n问题50',
       '\n问题51','\n问题52','\n问题53','\n问题54','\n问题55','\n问题56','\n问题57','\n问题58','\n问题59','\n问题60']

type7 = ['\n1.','\n2.','\n3.','\n4.','\n5.','\n6.','\n7.','\n8.','\n9.','\n10.',
         '\n11.','\n12.','\n13.','\n14.','\n15.','\n16.','\n17.','\n18.','\n19.','\n20.',
         '\n21.','\n22.','\n23.','\n24.','\n25.','\n26.','\n27.','\n28.','\n29.','\n30.',
         '\n31.','\n32.','\n33.','\n34.','\n35.','\n36.','\n37.']

# type7 = []

type8 = ['。一、','。二、','。三、','。四、','。五、','。六、','。七、','。八、','。九、','。十、',
         '。十一、','。十二、','。十三、','。十四、','十五、','十六、','。十七、','。十八、','。十九、','。二十、']

type9 = ['公司和独立财务顾问说明:','\n【公司控股股东','\n公司说明','\n企业回复','\n问题答复:','\n公司回复','\n公司回复：','公司\n回复',
         '\n公司的回复','\n回复如下','。 回复\n', '\n回复——', '\n回复意见：', '\n公司及收购人回复：' '【泸天化回复】','【会计师说明】','【管理人回复】','\n回复说明如下','\n问题回复:',
         '\n问题\n回复','\n【回复意见】','【上市公司说明】','\n本公司回复','\n一、公司回复','【回复说明】','\n会计师说明',
         '\n补充回','[回复说明]','\n回复说明','\n公司答复','【公司回复】','一、问题答复','【问题回复】','〔补充回复〕',
         '一、上市公司回复','\n补充披露:','\n答:','[说明]','一、回复说明','一、反馈问题回复','\n【回复概述】','\n会计师回复:','本所回复:',
         '\n反馈意见回复:\n','\n修订说明:','\n回复:','\n经核查,会计师认为,\n','\n请独立财务顾问和律师核查并发表明确意见。回复:','\n【回复】',
         '【答复】','\n回复:\n','\n会计师意见:','\n【上市公司回复】','公司其他需要说明的事项','\n答复','[回复]','\n说明:','。回复:\n',
         '\n上市公司回复:','\n【回复如下】','\n【独立财务顾问回复】','公司回复:\n','\n回答:','\n1、上市公司回复','\n情况说明:','[上市公司说明]',
         '[独立财务顾问核查意见]','\n【说明】','\n相关说明:','\n【会计师回复】','\n报备:','\n独立财务顾问回复:','\n相关说明:',
         '\n回函说明:','\n公司对本问题回复如下:','\n回复内容','\n反馈回复:','\n【回复及补充披露】','\n公司监事会回复:',
         '\n【公司的回复】','\n说明及回复:','\n会计师事务所核查意见:','\n会计师核查意见:', '\n【上市公司回复】', '\n【回复说明：】']

type10=['\n关注1:','\n关注2:','\n关注3:','\n关注4:','\n关注5:','\n关注6:','\n关注7:','\n关注8:','\n关注9:','\n关注10:']

type11 = ['\n【问题1】','\n【问题2】','\n【问题3】','\n【问题4】','\n【问题5】','\n【问题6】','\n【问题7】','\n【问题8】','\n【问题9】','\n【问题10】',
          '\n【问题11】','\n【问题12】','\n【问题13】','\n【问题14】','\n【问题15】','\n【问题16】','\n【问题17】','\n【问题18】','\n【问题19】','\n【问题20】']

type12 = ['\n问题一','\n问题二','\n问题三','\n问题四','\n问题五','\n问题六','\n问题七','\n问题八','\n问题九','\n问题十',
          '\n问题十一','\n问题十二','\n问题十三','\n问题十四','\n问题十五','\n问题十六','\n问题十七','\n问题十八',
          '\n问题十九','\n问题二十','\n问题二十一','\n问题二十二','\n问题二十三','\n问题二十四','\n问题二十五','\n问题二十六',
          '\n问题二十七','\n问题二十八','\n问题二十九','\n问题三十','\n问题三十一','\n问题三十二','\n问题三十三','\n问题三十四','\n问题三十五','\n问题三十六','\n问题三十七','\n问题三十八','\n问题三十九']

type13 = ['\n问题:']

# type14 = ['\n问题1','\n问题2','\n问题3','\n问题4','\n问题5','\n问题6','\n问题7','\n问题8','\n问题9','\n问题10',
#           '\n问题11','\n问题12','\n问题13','\n问题14','\n问题15','\n问题16','\n问题17','\n问题18','\n问题19','\n问题20']

type14 = ['\n第一节','\n第二节','\n第三节','\n第四节','\n第五节']

type15 = ['\n关注情况一:','\n关注情况二:','\n关注情况三:','\n关注情况四:','\n关注情况五:','\n关注情况六:','\n关注情况七:','\n关注情况八:','\n关注情况九:','\n关注情况十:']

type16 = ['\n1)、','\n2)、','\n3)、','\n4)、','\n5)、','\n6)、','\n7)、','\n8)、','\n9)、','\n10)、']

type17 = ['\n1\n','\n2\n','\n3\n','\n4\n','\n5\n','\n6\n','\n7\n','\n8\n','\n9\n','\n10\n',
         '\n11\n','\n12\n','\n13\n','\n14\n','\n15\n','\n16\n','\n17\n','\n18\n','\n19\n','\n20\n',
         '\n21\n','\n22\n','\n23\n','\n24\n','\n25\n','\n26\n','\n27\n','\n28\n','\n29\n','\n30\n']

type18 = ['\n-1-\n','\n-2-\n','\n-3-\n','\n-4-\n','\n-5-\n','\n-6-\n','\n-7-\n','\n-8-\n','\n-9-\n','\n-10-\n',
         '\n-11-\n','\n-12-\n','\n-13-\n','\n-14-\n','\n-15-\n','\n-16-\n','\n-17-\n','\n-18-\n','\n-19-\n','\n-20-\n',
         '\n-21-\n','\n-22-\n','\n-23-\n','\n-24-\n','\n-25-\n','\n-26-\n','\n-27-\n','\n-28-\n','\n-29-\n','\n-30-\n']

type19 = ['\n【问题一】','\n【问题二】','\n【问题三】','\n【问题四】','\n【问题五】','\n【问题六】','\n【问题七】','\n【问题八】','\n【问题九】','\n【问题十】']

type20 = ['\n问询函第1条','\n问询函第2条','\n问询函第3条','\n问询函第4条','\n问询函第5条','\n问询函第6条','\n问询函第7条','\n问询函第8条']

type21 = ['\n《问询函》第1','\n《问询函》第2','\n《问询函》第3','\n《问询函》第4','\n《问询函》第5','\n《问询函》第6','\n《问询函》第7','\n《问询函》第8']

# type22 = ['\n(1)','\n(2)','\n(3)','\n(4)','\n(5)','\n(6)','\n(7)','\n(8)','\n(9)','\n(10)',
#           '\n(11)','\n(12)','\n(13)','\n(14)','\n(15)','\n(16)','\n(17)','\n(18)']

type22 = ['\n【关注问题1】','\n【关注问题2】','\n【关注问题3】']

# type23 = ['\n问询一:','\n问询二:','\n问询三:','\n问询四:','\n问询五:','\n问询六:','\n问询七:','\n问询八:','\n问询九:','\n问询十:']
type23 = ['\n重组问询函(一)','\n重组问询函(二)','\n重组问询函(三)','\n重组问询函(四)']

# type24 = ['\n(一)','\n(二)','\n(三)','\n(四)','\n(五)','\n(六)','\n(七)','\n(八)','\n(九)']
type24 = ['\n问询事项一','\n问询事项二','\n问询事项三','\n问询事项四','\n问询事项五','\n问询事项六','\n问询事项七','\n问询事项八',
          '\n问询事项九','\n问询事项十']

type25 = ['\n关注问题一','\n关注问题二','\n关注问题三','\n关注问题四','\n关注问题五']

type26 = ['\n问询函第1题','\n问询函第2题','\n问询函第3题','\n问询函第4题','\n问询函第5题',
          '\n问询函第6题','\n问询函第7题','\n问询函第8题','\n问询函第9题','\n问询函第10题',
          '\n问询函第11题','\n问询函第12题','\n问询函第13题','\n问询函第14题','\n问询函第15题'
          ,'\n问询函第16题']

type27 = ['\n问询一','\n问询二','\n问询三','\n问询四','\n问询五','\n问询六','\n问询七','\n问询八','\n问询九','\n问询十',
          '\n问询十一','\n问询十二','\n问询十三','\n问询十四','\n问询十五','\n问询十六','\n问询十七','\n问询十八',
          '\n问询十九','\n问询二十','\n问询二十一','\n问询二十二','\n问询二十三','\n问询二十四','\n问询二十五','\n问询二十六',
          '\n问询二十七','\n问询二十八','\n问询二十九','\n问询三十']

type28 = ['\n关注事项一','\n关注事项二','\n关注事项三','\n关注事项四','\n关注事项五']

type29 = ['\n事项一','\n事项二','\n事项三','\n事项四','\n事项五','\n事项六','\n事项七','\n事项八','\n事项九','\n事项十',
          '\n事项十一','\n事项十二','\n事项十三','\n事项十四','\n事项十五','\n事项十六','\n事项十七','\n事项十八',
          '\n事项十九','\n事项二十','\n事项二十一','\n事项二十二','\n事项二十三','\n事项二十四','\n事项二十五','\n事项二十六',
          '\n事项二十七','\n事项二十八','\n事项二十九','\n事项三十']

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

# print('quancheng_list:',len(quancheng_list))
# print('jiancheng_list:',len(jiancheng_list))
# print('code_list:',len(code_list))
# print(quancheng_list)
# print(jiancheng_list)
# print(code_list)


_id_noreply_list = []
url_reply_noreply_list = []
title_reply_noreply_list = []
date_reply_noreply_list = []
content_reply_noreply_list = []
url_wx_noreply_list = []
title_wx_noreply_list = []
content_wx_noreply_list = []
gtime_noreply_list = []
gsdm_noreply_list = []
gsjc_noreply_list = []
channel_noreply_list = []
approve_noreply_list = []
financing_type_list = []
department_noreply_list = []
ctime_noreply_list = []
real_ctime_noreply_list = []
tables_noreply_list = []
company_noreply_list = []
accessory_url_noreply_list = []
accessory_name_noreply_list = []


# 开始拆分
# error_list = []
url_reply_list = []
du_list = []
api_count = 0
count = 0
delete_count = 0

# # 读取当前数据库中的值用于去重
# old_data_list = mongodb[update_data_name].find({})
# for i in old_data_list:
#     if 'analyze' in i.get('url_reply') or 'reportdocs' in i.get('url_reply'):
#         du_list.append((i.get('title_wx'),i.get('ctime')))
#         url_reply_list.append(i.get('url_reply'))

# print(len(du_list))
# print(len(url_reply_list))

for i, line_de in enumerate(data):
    # print(i, line_de)
    print(i)
    

    api_msg_table_list = []
    api_msg_table_dict = {}
    api_msg_api_url = ''
    api_msg_api_date = ''
    api_msg_api_title = ''
    api_msg_api_file = ''
    api_msg_file_path = ''

    title_reply = ''
    title_name = ''
    url_reply = ''
    company = ''

    # 获取数据
    _id = line_de.get('_id')
    if 'url' in line_de.get('gshf_content').keys():
        gshf_content_url = line_de.get('gshf_content')['url']
        gshf_content_name = line_de.get('gshf_content')['name']
        gshf_content_file_name = line_de.get('gshf_content')['file_name']
        gshf_content_file_content = line_de.get('gshf_content')['file_content']
        # print(type(gshf_content_file_name))
    else:
        gshf_content_url = ''
        gshf_content_name = ''
        gshf_content_file_name = ''
        gshf_content_file_content = ''
    gtime = line_de.get('gtime')
    if 'url' in line_de.get('hjnr_content').keys():
        hjnr_content_url = line_de.get('hjnr_content')['url']
        hjnr_content_file_name = line_de.get('hjnr_content')['file_name']
        title_name = hjnr_content_file_name
        hjnr_content_file_content = line_de.get('hjnr_content')['file_content']
        # print(type(hjnr_content_file_content))
    else:
        hjnr_content_url = ''
        hjnr_content_file_name = ''
        hjnr_content_file_content = ''
    gsdm = line_de.get('gsdm')
    gsjc = line_de.get('gsjc')
    hjlb = line_de.get('hjlb')
    ctime = line_de.get('ctime')
    path = line_de.get('path')


    # 提取公司简称
    gs = gsjc.replace(' ','')
    # print(gs)
    if gs in jiancheng_list:
        pos = jiancheng_list.index(gs)
        company = quancheng_list[pos]
    elif gs.replace('*','') in jiancheng_list:
        pos = jiancheng_list.index(gs.replace('*',''))
        company = quancheng_list[pos]
    elif company == '' and gsdm in code_list:
        pos = code_list.index(gsdm)
        company = quancheng_list[pos]



    # 有api数据
    hf_con = ''
    deal_flag = False
    all_type_list = [type9, type1, type2, type3, type4, type5, type6, type7, type8,\
                     type10, type11, type12, type13, type14, type15, type16, type19,\
                     type20, type21, type22, type23, type24, type25, type26, type27,\
                     type28, type29]
    # print(line_de.keys())
    # print(line_de.get('api_msg')['content'][:100])
    # print(gshf_content_file_content)

    # if 'api_msg' in line_de.keys() and '会计师事务所' not in line_de.get('api_msg')['api_title'] and\
    # '法律意见书' not in line_de.get('api_msg')['content'][:200] and '核查意见' not in line_de.get('api_msg')['content'][:100] and\
    # '报告书' not in line_de.get('api_msg')['content'][:100] and '专项核查意见' not in line_de.get('api_msg')['content'][:100] and\
    # ('公告编号' not in line_de.get('api_msg')['content'][:100] or '律师' not in line_de.get('api_msg')['content'][:100]) and\
    # '证券有限公司' not in line_de.get('api_msg')['content'][:30] and\
    # '资产评估有限公司' not in line_de.get('api_msg')['content'][:30] and '证券股份有限公司' not in line_de.get('api_msg')['content'][:30] and\
    # '回复的补充说明' not in line_de.get('api_msg')['content'][:100] and\
    # '信会师函字' not in line_de.get('api_msg')['content'][:100] and '中信建投证券股份有限公司关于深圳证券交易所' not in line_de.get('api_msg')['content'][:100] and\
    # '会计师事务所' not in gshf_content_file_content[:100] and '法律意见书' not in gshf_content_file_content[:100] and\
    # '核查意见' not in gshf_content_file_content[:100] and ('公告编号' not in gshf_content_file_content[:100] or '律师' not in gshf_content_file_content[:100]) and\
    # '报告书' not in gshf_content_file_content[:100] and '专项核查意见' not in gshf_content_file_content[:100] and\
    # ('独立董事意见' not in line_de.get('api_msg')['content'][:100] or len(line_de.get('api_msg')['content']) > 1000):

    if 'api_msg' in line_de.keys():
        print('api_msg')
        api_msg_content = line_de.get('api_msg')['content']
        api_msg_table_list = line_de.get('api_msg')['table_list']
        for item in api_msg_table_list:
        #print(item)
        #print(type(item))
            for i, j in item.items():
                #print(i,j)
                api_msg_table_dict[i.replace('\n','')] = j.replace('\n','')
        api_msg_api_url = line_de.get('api_msg')['api_url']
        url_reply = api_msg_api_url
        api_msg_api_date = line_de.get('api_msg')['api_date']
        api_msg_api_title = line_de.get('api_msg')['api_title']
        title_reply = api_msg_api_title
        # api_msg_api_file = line_de.get('api_msg')['api_file']
        # api_msg_file_path = line_de.get('api_msg')['file_path']
        api_count += 1

        # print(api_msg_content)
        api_con = DBC2SBC(api_msg_content)
        if '问询函的公告' in api_con[:100]:
            api_con = ''

        # print(api_con)
        pos1 = api_con.find('[{')
        pos2 = api_con.find('}]', pos1)
        if pos1 != -1 and pos2 != -1 and api_con != []:
            hf_con = api_con[:pos1] + api_con[pos2 + 2:]
            # if ' ' == api_con[0]:
            # # if '[{' in api_con and '}]' in api_con:
            #     new_api_con = api_con[pos1:pos2+2]
            #     new_api_con_dict = ast.literal_eval(new_api_con)
            #     # print(type(new_api_con_dict))
            #     # print(new_api_con)
            #     tmp_con = ''
            #     for dc in new_api_con_dict:
            #         tmp_con += dc['title'] + '\n'
            #     # print(tmp_con)
            #     hf_con = api_con[:pos1] + tmp_con + api_con[pos2:].replace('}]','')
            #     # print(hf_con)
            # else:
            #     hf_con = api_con[:pos1] + api_con[pos2 + 2:]
            #     # print(hf_con)
        else:
            hf_con = api_con.replace('[]','')
            # print(hf_con)
            if len(hf_con) < 50:
                # print('hf_con:',hf_con)
                deal_flag = True

        for i in type17 + type18:
            # print([i])
            hf_con = hf_con.replace(' ', '').replace(i, '\n')

    # print([hf_con])
    if deal_flag == True or hf_con == '':
        # con默认为公司回复的文件内容
        hf_con = DBC2SBC(gshf_content_file_content)
        title_reply = gshf_content_name
        url_reply = gshf_content_url

    if '\n附表:\n' in hf_con:
        pos3 = hf_con.find('\n附表')
        if pos3 != -1:
            hf_con = hf_con[:pos3]
            # print(hf_con)
    elif '\n附件:' in hf_con[1000:]:
        pos3 = hf_con.find('\n附件:')
        if pos3 != -1:
            hf_con = hf_con[:pos3]
            # print(hf_con)

    if '.pdf' in url_reply[-10:] and hf_con != '':
        new_hf_con = ''
        for i in hf_con.split('\n'):
            if (len(i) < 100 and is_Chinese(i) == False) or len(i) < 2:
                # print([i])
                # new_hf_con += i
                continue
            else:
                new_hf_con += (i + '\n')
        hf_con = new_hf_con
        # print([hf_con])

    # 拆分信息
    layer_list = []
    # if i % 15 == 0:


    new_con = []
    if '目录' in hf_con[:1000].replace(' ',''):
        new_con_tmp = hf_con.split('\n')
        # print(new_con_tmp)
        max_num = 0
        min_num = 1000000
        for num, item in enumerate(new_con_tmp[:200]):
            # print([item][:100])
            flag = True
            for huifu in type9:
                if huifu.replace('\n','') == item[:len(huifu)].replace(' ',''):
                    flag = False
                    # print(huifu)

            if flag == False:
                break
            if len(item) > 6 and (']' in item[-10:] or '。' in item[-10:] or ':' in item[-10:] or\
            '...' in item[-10:] or '题' in item[-12:] or '…' in item[-10:] or '】' in item[-10:] or\
            '. ' in item[-10:] or '、' in item) and\
            ((item[-1] == ' ' and item[-2].isdigit() == True and item[-3].isdigit() == True) or\
            (item[-1].isdigit() == True and item[-2].isdigit() == True) or\
            (item[-1] == ' ' and item[-2] == ' ' and item[-3].isdigit() == True and item[-4].isdigit() == True)) and\
            '2017' not in item[-5:]:
                print([item[-10:]])
                max_num = num
            elif len(item.replace(' ','')) > 6 and item.replace(' ','')[-1].isdigit() == True and item.replace(' ','')[-2].isdigit() == True and\
            '.' in item.replace(' ','')[-5:]:
                print([item[-10:]])
                max_num = num

            if ('目录' in item or '目 录' in item) and min_num > num:
                min_num = num

        # print(min_num)
        # print(max_num)

        top = new_con_tmp[:min_num]
        # print(top)

        hf_con = ''
        # for i in (new_con_tmp[:min_num] + new_con_tmp[max_num + 1:]):
        for i in new_con_tmp[max_num + 1:]:
            new_con.append(i.replace(' ',''))
            hf_con += ('\n' + i)
        # print(new_con)

    else:
        new_con = hf_con.replace(' ','').split('\n')



    # print(hf_con)
    # new_con = hf_con.replace(' ','').split('\n')
    for num, item in enumerate(new_con):
        # print([item])

        if num < 20 and '请补充披露:' == item[-6:]:
            # print(item)
            continue

        f = False
        for t_num, t in enumerate(all_type_list):
            for ti in t:
                new_t = ti.replace('\n','').replace('\r','')
                # print(new_t)
                # print(item[:len(new_t)])
                if new_t == item[:len(new_t)]:
                    layer_list.append(all_type_list[t_num])
                    # print(new_t)
                    # print(t_num)
                # if new_t == item[-(len(new_t)):]:
                #     layer_list.append(all_type_list[t_num])
                #     # print(new_t)
                #     # print(t_num)

        # print('----')

    # print(layer_list)

    q_list = [type1,type2,type3,type4,type5,type6,type7,type8,type10,type11,
              type12,type13,type14,type15,type16,type17,type18,type19,type20,
              type21,type22,type23,type24,type25,type26,type27,type28,type29]
    a_list = [type9]

    mid_layer = []
    tmp_flag = False
    for n, k in enumerate(layer_list):
        for q in q_list:
            if k == q:
                tmp_flag = True

        for a in a_list:
            if k == a:
                tmp_flag = False

        if tmp_flag == True and k not in q_list and k not in a_list:
            # print(k)
            mid_layer = k
            break
    # print(mid_layer)


    l1 = []
    l2 = []
    l3 = []
    l4 = []
    huifu_flag = 0
    if len(layer_list) > 0:
        l1 = layer_list[0]
        if l1 == type9:
            # print('l1')
            huifu_flag = 1
        while l1 in layer_list:
            layer_list.remove(l1)

        if len(layer_list) > 0 and huifu_flag == 0:
            l2 = layer_list[0]
            if l2 == type9:
                # print('l2')
                huifu_flag = 1
            while l2 in layer_list:
                layer_list.remove(l2)

            if len(layer_list) > 0 and huifu_flag == 0:
                l3 = layer_list[0]
                if l3 == type9:
                    # print('l3')
                    huifu_flag = 1
                while l3 in layer_list:
                    layer_list.remove(l3)

                if len(layer_list) > 0 and huifu_flag == 0:
                    l4 = layer_list[0]
                    while l4 in layer_list:
                        layer_list.remove(l4)

        # 中间层
        # if mid_layer != []:
        #     if l1 in q_list and l2 in a_list and l3 == []:
        #         l3 = l2
        #         l2 = mid_layer
        #     if l2 in q_list and l3 in a_list and l4 == []:
        #         l4 = l3
        #         l3 = mid_layer


        # if l1 == []:
        #     error_list.append(_id)


        # if _id == '0b6d0b38d8fe3f30942b1e374cabe090':

        # if 1:

        # print([new_con])
        print(l1)
        print(l2)
        print(l3)
        print(l4)
        # print([hf_con])

        # print(_id)

        #     print(api_msg_api_url)
        #     print(gshf_content_url)
        #     print(hjnr_content_url)
        #     print('=========')

    # 拆分
    # layer1
    # print([hf_con])
    layer1_content = split_it(hf_con.replace(' ','').replace('\u200c',''),l1)
    # print(layer1_content)

    content_list = []
    for i in layer1_content:
        # print([i],'\n---------------')
        layer2_content = split_it(i,l2)

        for j in layer2_content:
            # print(j,'\n----------------')
            layer3_content = split_it(j,l3)

            for k in layer3_content:
                # print(k,'\n----------------')
                layer4_content = split_it(k,l4)
                # content_list.append(k)

                for l in layer4_content:
                    # print(l,'\n----------------')
                    if l != '' and l != ' ' and l != '\n':
                        content_list.append(l)

    print('=============')


    result_list=[]
    # print(url)
    tmp_class = '_q'
    tmp_num = 1
    a_flag = False
    begin_tmp_num = 1
    for num, c in enumerate(content_list):
        # print([c])
        if judge_class(c) != '_N':
            tmp_class = judge_class(c)

        # print(judge_class(c))
        # print(tmp_class)
        if c[:2] in l1 or c[:3] in l1 or c[:4] in l1 or c[:5] in l1 or c[:6] in l1 or c[:7] in l1 or c[:8] in l1 or\
        c[:9] in l1 or c[:10] in l1 or c[:11] in l1 or c[:12] in l1 or '\n' + c[:1] in l1 or '\n' + c[:2] in l1 or\
        '\n' + c[:3] in l1 or '\n' + c[:4] in l1 or '\n' + c[:5] in l1 or '\n' + c[:6] in l1 or '\n' + c[:7] in l1 or\
        '\n' + c[:8] in l1 or '\n' + c[:9] in l1 or '\n' + c[:10] in l1 or '\n' + c[:11] in l1 or '\n' + c[:12] in l1:
            result_list.append((str(num + begin_tmp_num) + '_q' + '_' + str(tmp_num) + '_'  + str(tmp_num),c))
            a_flag = False
            # print(str(num) + tmp_class + '_0',[c])

        elif c[:2] in l2 or c[:3] in l2 or c[:4] in l2 or c[:5] in l2 or c[:6] in l2 or c[:7] in l2 or c[:8] in l2 or\
        c[:9] in l2 or c[:10] in l2 or c[:11] in l2 or c[:12] in l2 or '\n' + c[:1] in l2 or '\n' + c[:2] in l2 or\
        '\n' + c[:3] in l2 or '\n' + c[:4] in l2 or '\n' + c[:5] in l2 or '\n' + c[:6] in l2 or '\n' + c[:7] in l2 or\
        '\n' + c[:8] in l2 or '\n' + c[:9] in l2 or '\n' + c[:10] in l2 or '\n' + c[:11] in l2 or '\n' + c[:12] in l2:
            if tmp_class == '_a' and a_flag == True:
                result_list.append((str(num + begin_tmp_num) + tmp_class + '_' + str(tmp_num - 1) + '_' + str(tmp_num - 1),c))
                continue
            result_list.append((str(num + begin_tmp_num) + tmp_class + '_' + str(tmp_num) + '_' + str(tmp_num),c))
            if tmp_class == '_a' and a_flag==False:
                tmp_num += 1
                a_flag = True
            else:
                a_flag = False
            # print(str(num) + tmp_class + '_1',[c])

        elif c[:2] in l3 or c[:3] in l3 or c[:4] in l3 or c[:5] in l3 or c[:6] in l3 or c[:7] in l3 or c[:8] in l3 or\
        c[:9] in l3 or c[:10] in l3 or c[:11] in l3 or c[:12] in l3 or '\n' + c[:1] in l3 or '\n' + c[:2] in l3 or\
        '\n' + c[:3] in l3 or '\n' + c[:4] in l3 or '\n' + c[:5] in l3 or '\n' + c[:6] in l3 or '\n' + c[:7] in l3 or\
        '\n' + c[:8] in l3 or '\n' + c[:9] in l3 or '\n' + c[:10] in l3 or '\n' + c[:11] in l3 or '\n' + c[:12] in l3:
            if tmp_class == '_a' and a_flag == True:
                result_list.append((str(num + begin_tmp_num) + tmp_class + '_' + str(tmp_num - 1) + '_' + str(tmp_num - 1),c))
                continue
            result_list.append((str(num + begin_tmp_num) + tmp_class + '_' + str(tmp_num) + '_' + str(tmp_num),c))
            if tmp_class == '_a' and a_flag==False:
                tmp_num += 1
                a_flag = True
            else:
                a_flag = False
            # print(str(num) + tmp_class + '_2',[c])

        elif c[:2] in l4 or c[:3] in l4 or c[:4] in l4 or c[:5] in l4 or c[:6] in l4 or c[:7] in l4 or c[:8] in l4 or\
        c[:9] in l4 or c[:10] in l4 or c[:11] in l4 or c[:12] in l4 or '\n' + c[:1] in l4 or '\n' + c[:2] in l4 or\
        '\n' + c[:3] in l4 or '\n' + c[:4] in l4 or '\n' + c[:5] in l4 or '\n' + c[:6] in l4 or '\n' + c[:7] in l4 or\
        '\n' + c[:8] in l4 or '\n' + c[:9] in l4 or '\n' + c[:10] in l4 or '\n' + c[:11] in l4 or '\n' + c[:12] in l4:
            if tmp_class == '_a' and a_flag == True:
                result_list.append((str(num + begin_tmp_num) + tmp_class + '_' + str(tmp_num - 1) + '_' + str(tmp_num - 1),c))
                continue
            result_list.append((str(num + begin_tmp_num) + tmp_class + '_' + str(tmp_num) + '_' + str(tmp_num),c))
            if tmp_class == '_a' and a_flag==False:
                tmp_num += 1
                a_flag = True
            else:
                a_flag = False
            # print(str(num) + tmp_class + '_3',[c])
        elif num == 0:
            result_list.append(('0_0',c))
            begin_tmp_num = 0
            # print(str(num) + '_begin',[c])

        # else:
        #     result_list.append((str(num + begin_tmp_num) + '_unknow_' + str(tmp_num),c))
            # print(str(num) + '_unknow:',[c])

    # print(gshf_content_file_content[:100])
    if (l2 == [] and l3 == [] and l4 ==[] and gshf_content_file_content != '') or\
    '会计师事务所' in gshf_content_file_content[:100] or\
    '法律意见书' in gshf_content_file_content[:100] or '专项意见' in gshf_content_file_content[:100] or\
    ('公告编号' in gshf_content_file_content[:100] and '律师' in gshf_content_file_content[:100]) or\
    '报告书' in gshf_content_file_content[:100] or '专项核查意见' in gshf_content_file_content[:100]:
        print('1111111111111111111',[gshf_content_file_content[:100]])
        result_list = [('0_0',gshf_content_file_content)]

    # print(result_list)
    content_dict={}
    for i in result_list:
        print(i[0],[i[1][:50]])
        content_dict[i[0]]=i[1]
    # print(content_dict)


    if '0_0' in content_dict.keys() and is_Chinese(content_dict['0_0']) == False:
        content_dict = {}
        print(_id)


    # 问询内容拆分
    wx_con = ''
    wx_con = hjnr_content_file_content
    # print([wx_con])

    layer_list = []

    # 拆分首段
    # pos = wx_con.find('号')
    # if pos != -1:
    #     begin_sec = wx_con[:pos + 1]
    #     new_wx_con_list = wx_con[pos + 1:].split('\n')
    #     new_wx_con = wx_con[pos + 1:]
    # else:
    #     begin_sec = ''
    #     new_wx_con_list = wx_con.split('\n')
    #     new_wx_con = wx_con
    # print([begin_sec])
    # print([new_wx_con_list])

    for i in type17 + type18:
        # print([i])
        wx_con = DBC2SBC(wx_con).replace(' ', '').replace(i, '\n')

    new_wx_con_list = wx_con.split('\n')

    for num, item in enumerate(new_wx_con_list):
        # print([item])
        for t_num, t in enumerate(all_type_list):
            for ti in t:
                new_t = ti.replace('\n','').replace('\r','')
                # print(new_t)
                # print(item[:len(new_t)])
                if new_t == item[:len(new_t)]:
                    layer_list.append(all_type_list[t_num])

    # print(layer_list)
    wx_l1 = []
    if len(layer_list) > 0:
        wx_l1 = layer_list[0]
    # print(wx_l1)

    # 拆分
    # layer1
    wx_layer1_content = split_it(wx_con, wx_l1)
    # print(wx_layer1_content)

    wx_content_list = []
    for i in wx_layer1_content:
        wx_content_list.append(i)
        # print('---------------',i)
    # print('=============')

    wx_result_list = []
    # print(url)
    tmp_num = 0
    begin_tmp_num = 1
    for num, c in enumerate(wx_content_list):
        # print([c])
        if num == 0:
            begin_tmp_num = 0
            wx_result_list.append(('0_0',c))
        else:
            wx_result_list.append((str(num + begin_tmp_num) + '_q' + '_0_'  + str(tmp_num),c))

        # if c[:2] in l1 or c[:3] in l1 or c[:4] in l1 or c[:5] in l1 or c[:6] in l1 or c[:7] in l1:
        #     # tmp_num += 1
        #     # wx_result_list.append((str(num) + '_',c))
        #     wx_result_list.append((str(num + begin_tmp_num) + '_q' + '_0_'  + str(tmp_num),c))
        #     # print(str(num) + '_',c)
        # else:
        #     wx_result_list.append((str(num + begin_tmp_num) + '_q' + '_0_'  + str(tmp_num),c))

    # print(result_list)
    wx_content_dict={}
    for i in wx_result_list:
        # print(i)
        wx_content_dict[i[0]]=i[1]
    # print(wx_content_dict)

    ctimeArray = time.localtime(ctime)
    real_ctime = time.strftime('%Y-%m-%d', ctimeArray)

    if company == '':
        tmp_sec = wx_content_dict['0_0']
        pos1 = tmp_sec.find('关于对')
        pos2 = tmp_sec.find('的', pos1)
        if pos1 != -1 and pos2 != -1:
            company = tmp_sec[pos1 + 3:pos2]
        # print(company)
        # print(gs)
        # print(wx_content_dict['0_'])
    # print(wx_content_dict.keys())
    # print(company)

    if is_Chinese(title_name) == False or '关于' not in title_name:
        pos1 = hjnr_content_file_content.find('关于对')
        pos2 = hjnr_content_file_content.find('函', pos1)
        if pos1 != -1 and pos2 != -1:
            title_name =  hjnr_content_file_content[pos1 : pos2 + 1]
        else:
            title_name = '关于对' + company + '的关注函'

    # print(url_reply)
    if url_reply not in url_reply_list or url_reply == '':
        url_reply_list.append(url_reply)

        title_name = title_name.replace('\n','').replace(' ','')
        # print(title_name.replace('\n',''))

        if 'analyze' in url_reply:
            du_list.append((title_name, ctime))

            timestamp = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            try:
                collection.insert({'_id': _id, 
                                   'url_reply': url_reply, 
                                   'title_reply': title_reply, 
                                   'date_reply': api_msg_api_date, 
                                   'content_reply': content_dict, 
                                   'url_wx': hjnr_content_url, 
                                   'title_wx': title_name, 
                                   'content_wx': wx_content_dict, 
                                   'gtime': gtime, 
                                   'gsdm': gsdm, 
                                   'gsjc': gsjc, 
                                   'channel': hjlb, 
                                   'approve': '',
                                   'financing_type': '',
                                   'department': '深交所',
                                   'ctime': ctime, 
                                   'real_ctime': real_ctime,
                                   'tables': api_msg_table_dict, 
                                   'company': company, 
                                   'accessory_url': '', 
                                   'accessory_name': '',
                                   'i_time':timestamp
                                   })
                count += 1
                try:
                    delete_id = collection.find_one({'title_wx':title_name, 'ctime':ctime, 'content_reply':{}})['_id']
                    print('delete_id',delete_id)

                    sql_ = '''INSERT INTO delete_id_ES (_id)VALUES(%s)'''
                    cursor.execute(sql_,(delete_id))
                    db.commit()

                    collection.remove({'_id':delete_id})
                    print('reportdocs DELETE', title_name, ctime)
                    delete_count += 1
                except:
                    continue

            except:
                collection.save({'_id': _id, 
                                 'url_reply': url_reply, 
                                 'title_reply': title_reply, 
                                 'date_reply': api_msg_api_date, 
                                 'content_reply': content_dict, 
                                 'url_wx': hjnr_content_url, 
                                 'title_wx': title_name, 
                                 'content_wx': wx_content_dict, 
                                 'gtime': gtime, 
                                 'gsdm': gsdm, 
                                 'gsjc': gsjc, 
                                 'channel': hjlb, 
                                 'approve': '',
                                 'financing_type': '',
                                 'department': '深交所',
                                 'ctime': ctime, 
                                 'real_ctime': real_ctime,
                                 'tables': api_msg_table_dict, 
                                 'company': company, 
                                 'accessory_url': '', 
                                 'accessory_name': '',
                                 'i_time':timestamp
                                 })
                try:
                    delete_id = collection.find_one({'title_wx':title_name, 'ctime':ctime, 'content_reply':{}})['_id']
                    print('delete_id',delete_id)

                    collection.remove({'_id':delete_id})
                    print('reportdocs DELETE', title_name, ctime)
                    delete_count += 1
                except:
                    continue

        elif 'reportdocs' in url_reply:
            # print((title_name, ctime))
            if (title_name, ctime) not in du_list:
                du_list.append((title_name, ctime))

                timestamp = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                try:
                    collection.insert({'_id': _id, 
                                       'url_reply': url_reply, 
                                       'title_reply': title_reply, 
                                       'date_reply': api_msg_api_date, 
                                       'content_reply': content_dict, 
                                       'url_wx': hjnr_content_url, 
                                       'title_wx': title_name, 
                                       'content_wx': wx_content_dict, 
                                       'gtime': gtime, 
                                       'gsdm': gsdm, 
                                       'gsjc': gsjc, 
                                       'channel': hjlb, 
                                       'approve': '',
                                       'financing_type': '',
                                       'department': '深交所',
                                       'ctime': ctime, 
                                       'real_ctime': real_ctime,
                                       'tables': api_msg_table_dict, 
                                       'company': company, 
                                       'accessory_url': '', 
                                       'accessory_name': '',
                                       'i_time':timestamp
                                       })
                    count += 1
                    if content_dict != {}:
                        try:
                            delete_id = collection.find_one({'title_wx':title_name, 'ctime':ctime, 'content_reply':{}})['_id']
                            print('delete_id',delete_id)

                            collection.remove({'_id':delete_id})
                            print('reportdocs DELETE', title_name, ctime)
                            delete_count += 1
                        except:
                            continue

                except:
                    collection.save({'_id': _id, 
                                     'url_reply': url_reply, 
                                     'title_reply': title_reply, 
                                     'date_reply': api_msg_api_date, 
                                     'content_reply': content_dict, 
                                     'url_wx': hjnr_content_url, 
                                     'title_wx': title_name, 
                                     'content_wx': wx_content_dict, 
                                     'gtime': gtime, 
                                     'gsdm': gsdm, 
                                     'gsjc': gsjc, 
                                     'channel': hjlb, 
                                     'approve': '',
                                     'financing_type': '',
                                     'department': '深交所',
                                     'ctime': ctime, 
                                     'real_ctime': real_ctime,
                                     'tables': api_msg_table_dict, 
                                     'company': company, 
                                     'accessory_url': '', 
                                     'accessory_name': '',
                                     'i_time':timestamp
                                     })
                    if content_dict != {}:
                        try:
                            delete_id = collection.find_one({'title_wx':title_name, 'ctime':ctime, 'content_reply':{}})['_id']
                            print('delete_id',delete_id)

                            sql_ = "INSERT INTO delete_id_ES (table_name, _id) VALUES ('"+table_name+"','"+delete_id+"')"
                            print(sql_)
                            cursor.execute(sql_)
                            db.commit()

                            collection.remove({'_id':delete_id})
                            print('reportdocs DELETE', title_name, ctime)
                            delete_count += 1
                        except:
                            continue
        else:
            _id_noreply_list.append(_id)
            url_reply_noreply_list.append(url_reply)
            title_reply_noreply_list.append(title_reply)
            date_reply_noreply_list.append(api_msg_api_date)
            content_reply_noreply_list.append(content_dict)
            url_wx_noreply_list.append(hjnr_content_url)
            title_wx_noreply_list.append(title_name)
            content_wx_noreply_list.append(wx_content_dict)
            gtime_noreply_list.append(gtime)
            gsdm_noreply_list.append(gsdm)
            gsjc_noreply_list.append(gsjc)
            channel_noreply_list.append(hjlb)
            approve_noreply_list.append('')
            financing_type_list.append('')
            department_noreply_list.append('深交所')
            ctime_noreply_list.append(ctime)
            real_ctime_noreply_list.append(real_ctime)
            tables_noreply_list.append(api_msg_table_dict)
            company_noreply_list.append(company)
            accessory_url_noreply_list.append('')
            accessory_name_noreply_list.append('')

for i in range(len(_id_noreply_list)):
    if (title_wx_noreply_list[i], ctime_noreply_list[i]) not in du_list:
        du_list.append((title_wx_noreply_list[i], ctime_noreply_list[i]))

        timestamp = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        try:
            collection.insert({'_id': _id_noreply_list[i], 
                               'url_reply': url_reply_noreply_list[i], 
                               'title_reply': title_reply_noreply_list[i], 
                               'date_reply': date_reply_noreply_list[i], 
                               'content_reply': content_reply_noreply_list[i], 
                               'url_wx': url_wx_noreply_list[i], 
                               'title_wx': title_wx_noreply_list[i], 
                               'content_wx': content_wx_noreply_list[i], 
                               'gtime': gtime_noreply_list[i], 
                               'gsdm': gsdm_noreply_list[i], 
                               'gsjc': gsjc_noreply_list[i], 
                               'channel': channel_noreply_list[i], 
                               'approve': approve_noreply_list[i],
                               'financing_type': financing_type_list[i],
                               'department': department_noreply_list[i],
                               'ctime': ctime_noreply_list[i], 
                               'real_ctime': real_ctime_noreply_list[i],
                               'tables': tables_noreply_list[i], 
                               'company': company_noreply_list[i], 
                               'accessory_url': accessory_url_noreply_list[i], 
                               'accessory_name': accessory_name_noreply_list[i],
                               'i_time':timestamp
                               })
            count += 1
        except:
            collection.save({'_id': _id_noreply_list[i], 
                             'url_reply': url_reply_noreply_list[i], 
                             'title_reply': title_reply_noreply_list[i], 
                             'date_reply': date_reply_noreply_list[i], 
                             'content_reply': content_reply_noreply_list[i], 
                             'url_wx': url_wx_noreply_list[i], 
                             'title_wx': title_wx_noreply_list[i], 
                             'content_wx': content_wx_noreply_list[i], 
                             'gtime': gtime_noreply_list[i], 
                             'gsdm': gsdm_noreply_list[i], 
                             'gsjc': gsjc_noreply_list[i], 
                             'channel': channel_noreply_list[i], 
                             'approve': approve_noreply_list[i],
                             'financing_type': financing_type_list[i],
                             'department': department_noreply_list[i],
                             'ctime': ctime_noreply_list[i], 
                             'real_ctime': real_ctime_noreply_list[i],
                             'tables': tables_noreply_list[i], 
                             'company': company_noreply_list[i], 
                             'accessory_url': accessory_url_noreply_list[i], 
                             'accessory_name': accessory_name_noreply_list[i],
                             'i_time':timestamp
                             })


print('api_count', api_count)
print('count', count)
print('delete_count', delete_count)


def convert_num_time(num_time):
    timeArray = time.localtime(num_time)
    real_time = time.strftime('%Y-%m-%d %H:%M:%S', timeArray)
    return(str(real_time))


# # 写log
# sql_command_log = '''CREATE TABLE IF NOT EXISTS {0} (table_ VARCHAR(255), 
#                                                      add_number VARCHAR(255), 
#                                                      start_gtime DATETIME, 
#                                                      end_gtime DATETIME, 
#                                                      run_time timestamp
#                                                      )'''
# cursor.execute(sql_command_log.format(mysql_log_name))
# #sql_ = '''INSERT INTO {0} (table_,add_number)VALUES(%s%s)'''

# sql_ = '''INSERT INTO {0} (table_,
#                            add_number, 
#                            start_gtime, 
#                            end_gtime
#                            )VALUES(%s,%s,%s,%s)'''
# cursor.execute(sql_.format(mysql_log_name),(update_data_name, 
#                                             str(add_data_num), 
#                                             convert_num_time(max_gtime),
#                                             convert_num_time(last_gtime)
#                                             ))
# db.commit()

db.close()


if add_type_choice == 'add':
    conn.set(set_key,0)
