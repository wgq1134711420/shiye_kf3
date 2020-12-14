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
import openpyxl
import csv
from lxml import etree

# mongo上的爬取数据
mongo_name = 'sy_feedback_raw'
# 要更新的数据
update_data_name = 'sse_kcb_xxpl_split_wgq_5815_1111'
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


# 将实际日期转换成int类型以便于比较
def convert_real_time(real_time):
    timeArray = datetime.datetime.strptime(real_time, '%Y-%m-%d %H:%M:%S')
    #print(timeArray)   
    int_time = int(timeArray.timestamp())
    return int_time


# 打开数据库连接，从数据库获取当前最新数据的gtime
# db = pymysql.connect('127.0.0.1', 'root', 'shiye', 'EI_BDP', 3306, charset='utf8')
db = pymysql.connect(**config_local['local_sql'])
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()


# 129mongo
# mongocli = pymongo.MongoClient(config_local['local_mongo'])

# 集群
mongocli = pymongo.MongoClient(config_local['cluster_mongo'])

print('connected to mongo')
db_name = mongocli['sy_risk_raw']
db_name_EI = mongocli['EI_BDP_DATA']
# 当前最新数据的gtime
last_gtime = db_name[mongo_name].find_one({"gtime":{"$exists":True}}, sort=[("gtime",-1)])["gtime"]

# 存数据的位置
collection = db_name_EI[update_data_name]

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
# data_list = db_name[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}})
# data_list = db_name[mongo_name].find({"url" : "http://static.sse.com.cn/stock/information/c/201905/0a4de2b2f3e841a5b51dd329d3498685.pdf"})
data_list = db_name[mongo_name].find({}).limit(5851)


for i in data_list:
    data.append(i)

# 如果没有增量数据则中止程序
if len(data)==0:
    # conn.set(set_key, 0)
    print('There is no new data!')
    os._exit(0)
add_data_num = len(data)
print('add_data_num:',add_data_num)


def convert_num_time(num_time):
    timeArray = time.localtime(num_time)
    real_time = time.strftime('%Y-%m-%d %H:%M:%S', timeArray)
    return(str(real_time))


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
    pos_list=[]
    new_section=[]
    end_tmp=0

    # if '目录' in section:
    #     layer = layer + layer

    # print([section])
    # print(layer)

    pos_tmp = 0
    for i in layer:      
        # print(i)
        pos = section.find(i)
        # print([section[pos : pos + 10]])
        if pos == -1 or ('%' in section[pos : pos + 10] or '万元' in section[pos : pos + 10] or\
        '万吨' in section[pos : pos + 10] or '元/股' in section[pos : pos + 10] or\
        (i == '\n问题二' and '\n问题二十' in section[pos : pos + 10]) or\
        '亿元' in section[pos : pos + 10] or (is_Chinese(section[pos : pos + 10].replace(' ','')) == False)):
            # print('continue',[section[pos : pos + 10]])
            continue
        elif pos != -1 and pos > pos_tmp:
            # print('split_it',[section[pos : pos + 10]])
            if not ('10' in section[pos : pos + 10] and '10' not in i):
                pos_tmp = pos
                if i[0]==':' or i[0]=='。':
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
        end_tmp=end
    new_section.append(section[end_tmp:])
    # print(new_section)
    if len(pos_list)!=0:
        return new_section
    else:
        return [section]

def is_Chinese(word):
    for ch in word:
        if '\u4e00' <= ch <= '\u9fff':
            return True
    return False




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

type4 = ['问题1','问题2','问题3','问题4','问题5','问题6','问题7','问题8','问题9','问题10',
       '问题11','问题12','问题13','问题14','问题15','问题16','问题17','问题18','问题19','问题20',
       '问题21','问题22','问题23','问题24','问题25','问题26','问题27','问题28','问题29','问题30',
       '问题31','问题32','问题33','问题34','问题35','问题36','问题37','问题38','问题39','问题40',
       '问题41','问题42','问题43','问题44','问题45','问题46','问题47','问题48','问题49','问题50',
       '问题51','问题52','问题53','问题54','问题55','问题56','问题57','问题58','问题59','问题60']


type5 = ['一、',':一、','\n一、','\n二、','\n三、','\n四、','\n五、','\n六、','\n七、','\n八、','\n九、','\n十、',
         '\n十一、','\n十二、','\n十三、','\n十四、','\n十五、','\n十六、','\n十七、','\n十八、','\n十九、','\n二十、',
         '\n二十一、','\n二十二、','\n二十三、','\n二十四、','\n二十五、','\n二十六、','\n二十七、','\n二十八、','\n二十九、','\n三十、',
         '\n三十一、','\n三十二、','\n三十三、','\n三十四、','\n三十五、','\n三十六、','\n三十七、','\n三十八、','\n三十九、','\n四十、',
         '\n四十一、','\n四十二、','\n四十三、','\n四十四、','\n四十五、','\n四十六、','\n四十七、','\n四十八、','\n四十九、','\n五十、',
         '\n五十一、','\n五十二、','\n五十三、','\n五十四、','\n五十五、','\n五十六、','\n五十七、','\n五十八、','\n五十九、','\n六十、',
         '\n六十一、','\n六十二、','\n六十三、','\n六十四、','\n六十五、','\n六十六、','\n六十七、','\n六十八、','\n六十九、','\n七十、',
         '\n七十一、','\n七十二、','\n七十三、','\n七十四、','\n七十五、','\n七十六、','\n七十七、','\n七十八、','\n七十九、','\n八十、',
         '\n八十一、','\n八十二、','\n八十三、','\n八十四、','\n八十五、','\n八十六、','\n八十七、','\n八十八、','\n八十九、','\n九十、',
         '\n九十一、','\n九十二、','\n九十三、','\n九十四、','\n九十五、','\n九十六、','\n九十七、','\n九十八、','\n九十九、','\n一百、']

type6 = ['\n(一)','\n(二)','\n(三)','\n(四)','\n(五)','\n(六)','\n(七)','\n(八)','\n(九)','\n(十)',
         '\n(十一)','\n(十二)','\n(十三)','\n(十四)','\n(十五)','\n(十六)','\n(十七)','\n(十八)','\n(十九)','\n(二十)',
         '\n(二一)','\n(二二)','\n(二三)','\n(二四)','\n(二五)','\n(二六)','\n(二七)','\n(二八)','\n(二九)','\n(三十)',
         '\n(三一)','\n(三二)','\n(三三)','\n(三四)','\n(三五)','\n(三六)','\n(三七)','\n(三八)','\n(三九)','\n(四十)',
         '\n(四一)','\n(四二)','\n(四三)','\n(四四)','\n(四五)','\n(四六)','\n(四七)','\n(四八)','\n(四九)','\n(五十)',
         '\n(五一)','\n(五二)','\n(五三)','\n(五四)','\n(五五)','\n(五六)','\n(五七)','\n(五八)','\n(五九)','\n(六十)']

type7 = ['\n(1)','\n(2)','\n(3)','\n(4)','\n(5)','\n(6)','\n(7)','\n(8)','\n(9)','\n(10)',
         '\n(11)','\n(12)','\n(13)','\n(14)','\n(15)','\n(16)','\n(17)','\n(18)','\n(19)','\n(20)',
         '\n(21)','\n(22)','\n(23)','\n(24)','\n(25)','\n(26)','\n(27)','\n(28)','\n(29)','\n(30)',
         '\n(31)','\n(32)','\n(33)','\n(34)','\n(35)','\n(36)','\n(37)','\n(38)','\n(39)','\n(40)',
         '\n(41)','\n(42)','\n(43)','\n(44)','\n(45)','\n(46)','\n(47)','\n(48)','\n(49)','\n(50)',
         '\n(51)','\n(52)','\n(53)','\n(54)','\n(55)','\n(56)','\n(57)','\n(58)','\n(59)','\n(60)']

type8 = ['\n1.','\n2.','\n3.','\n4.','\n5.','\n6.','\n7.','\n8.','\n9.','\n10.',
         '\n11.','\n12.','\n13.','\n14.','\n15.','\n16.','\n17.','\n18.','\n19.','\n20.',
         '\n21.','\n22.','\n23.','\n24.','\n25.','\n26.','\n27.','\n28.','\n29.','\n30.',
         '\n31.','\n32.','\n33.','\n34.','\n35.','\n36.','\n37.','\n38','\n39','\n40',
         '\n41.','\n42.','\n43.','\n44.','\n45.','\n46.','\n47.','\n48','\n49','\n50',
         '\n51.','\n52.','\n53.','\n54.','\n55.','\n56.','\n57.','\n58','\n59','\n60']


type12 = ['问题一','问题二','问题三','问题四','问题五','问题六','问题七','问题八','问题九','问题十',
          '问题十一','问题十二','问题十三','问题十四','问题十五','问题十六','问题十七','问题十八','问题十九','问题二十',
          '问题二十一','问题二十二','问题二十三','问题二十四','问题二十五','问题二十六','问题二十七','问题二十八','问题二十九','问题三十',
          '问题三十一','问题三十二','问题三十三','问题三十四','问题三十五','问题三十六','问题三十七','问题三十八','问题三十九','问题四十',
          '问题四十一','问题四十二','问题四十三','问题四十四','问题四十五','问题四十六','问题四十七','问题四十八','问题四十九','问题五十',
          '问题五十一','问题五十二','问题五十三','问题五十四','问题五十五','问题五十六','问题五十七','问题五十八','问题五十九','问题六十']

type13 = ['\n反馈问题:','\n反馈问题1','\n反馈问题2','\n反馈问题3','\n反馈问题4','\n反馈问题5','\n反馈问题6','\n反馈问题7','\n反馈问题8','\n反馈问题9','\n反馈问题10',
          '\n反馈问题11','\n反馈问题12','\n反馈问题13','\n反馈问题14','\n反馈问题15','\n反馈问题16','\n反馈问题17','\n反馈问题18','\n反馈问题19','\n反馈问题20',
          '\n反馈问题21','\n反馈问题22','\n反馈问题23','\n反馈问题24','\n反馈问题25','\n反馈问题26','\n反馈问题27','\n反馈问题28','\n反馈问题29','\n反馈问题30',
          '\n反馈问题31','\n反馈问题32','\n反馈问题33','\n反馈问题34','\n反馈问题35','\n反馈问题36','\n反馈问题37','\n反馈问题38','\n反馈问题39','\n反馈问题40',
          '\n反馈问题41','\n反馈问题42','\n反馈问题43','\n反馈问题44','\n反馈问题45','\n反馈问题46','\n反馈问题47','\n反馈问题48','\n反馈问题49','\n反馈问题50',
          '\n反馈问题51','\n反馈问题52','\n反馈问题53','\n反馈问题54','\n反馈问题55','\n反馈问题56','\n反馈问题57','\n反馈问题58','\n反馈问题59','\n反馈问题60']

type14 = ['第1题','第2题','第3题','第4题','第5题','第6题','第7题','第8题','第9题','第10题',
          '第11题','第12题','第13题','第14题','第15题','第16题','第17题','第18题','第19题','第20题',
          '第21题','第22题','第23题','第24题','第25题','第26题','第27题','第28题','第29题','第30题',
          '第31题','第32题','第33题','第34题','第35题','第36题','第37题','第38题','第39题','第40题',
          '第41题','第42题','第43题','第44题','第45题','第46题','第47题','第48题','第49题','第50题',
          '第51题','第52题','第53题','第54题','第55题','第56题','第57题','第58题','第59题','第60题']

type15 = ['\n关注情况一:','\n关注情况二:','\n关注情况三:','\n关注情况四:','\n关注情况五:','\n关注情况六:','\n关注情况七:','\n关注情况八:','\n关注情况九:','\n关注情况十:']

type16 = ['问询函第1题','问询函第2题','问询函第3题','问询函第4题','问询函第5题','问询函第6题','问询函第7题','问询函第8题','问询函第9题','问询函第10题',
          '问询函第11题','问询函第12题','问询函第13题','问询函第14题','问询函第15题','问询函第16题','问询函第17题','问询函第18题','问询函第19题','问询函第20题',
          '问询函第21题','问询函第22题','问询函第23题','问询函第24题','问询函第25题','问询函第26题','问询函第27题','问询函第28题','问询函第29题','问询函第30题',
          '问询函第31题','问询函第32题','问询函第33题','问询函第34题','问询函第35题','问询函第36题','问询函第37题','问询函第38题','问询函第39题','问询函第40题',
          '问询函第41题','问询函第42题','问询函第43题','问询函第44题','问询函第45题','问询函第46题','问询函第47题','问询函第48题','问询函第49题','问询函第50题',
          '问询函第51题','问询函第52题','问询函第53题','问询函第54题','问询函第55题','问询函第56题','问询函第57题','问询函第58题','问询函第69题','问询函第60题']

type17 = ['\n【问题1】','\n【问题2】','\n【问题3】','\n【问题4】','\n【问题5】','\n【问题6】','\n【问题7】','\n【问题8】','\n【问题9】','\n【问题10】',
          '\n【问题11】','\n【问题12】','\n【问题13】','\n【问题14】','\n【问题15】','\n【问题16】','\n【问题17】','\n【问题18】','\n【问题19】','\n【问题20】']

type19 = ['一、反馈问题','二、反馈问题','三、反馈问题','四、反馈问题','五、反馈问题','六、反馈问题','七、反馈问题','八、反馈问题','九、反馈问题','十、反馈问题',
          '十一、反馈问题','十二、反馈问题','十三、反馈问题','十四、反馈问题','十五、反馈问题','十六、反馈问题','十七、反馈问题','十八、反馈问题','十九、反馈问题','二十、反馈问题',
          '二十一、反馈问题','二十二、反馈问题','二十三、反馈问题','二十四、反馈问题','二十五、反馈问题','二十六、反馈问题','二十七、反馈问题','二十八、反馈问题','二十九、反馈问题','三十、反馈问题']

type20 = ['\n反馈意见1','\n反馈意见2','\n反馈意见3','\n反馈意见4','\n反馈意见5','\n反馈意见6','\n反馈意见7',
          '\n反馈意见8','\n反馈意见9','\n反馈意见10','\n反馈意见11','\n反馈意见12','\n反馈意见13','\n反馈意见14',
          '\n反馈意见15','\n反馈意见16','\n反馈意见17','\n反馈意见18','\n反馈意见19','\n反馈意见20',
          '\n反馈意见21','\n反馈意见22','\n反馈意见23','\n反馈意见24','\n反馈意见25','\n反馈意见26',
          '\n反馈意见27','\n反馈意见28','\n反馈意见29','\n反馈意见30']

type21 = ['\n[反馈意见1]','\n[反馈意见2]','\n[反馈意见3]','\n[反馈意见4]','\n[反馈意见5]','\n[反馈意见6]','\n[反馈意见7]',
          '\n[反馈意见8]','\n[反馈意见9]','\n[反馈意见10]','\n[反馈意见11]','\n[反馈意见12]','\n[反馈意见13]','\n[反馈意见14]',
          '\n[反馈意见15]','\n[反馈意见16]','\n[反馈意见17]','\n[反馈意见18]','\n[反馈意见19]','\n[反馈意见20]','\n[反馈意见21]',
          '\n[反馈意见22]','\n[反馈意见23]','\n[反馈意见24]','\n[反馈意见25]','\n[反馈意见26]','\n[反馈意见27]','\n[反馈意见28]',
          '\n[反馈意见29]','\n[反馈意见30]','\n[反馈意见31]','\n[反馈意见32]','\n[反馈意见33]','\n[反馈意见34]','\n[反馈意见35]']

type22 = ['\n审核意见1','\n审核意见2','\n审核意见3','\n审核意见4','\n审核意见5','\n审核意见6','\n审核意见7','\n审核意见8','\n审核意见9','\n审核意见10',
          '\n审核意见11','\n审核意见12','\n审核意见13','\n审核意见14','\n审核意见15','\n审核意见16','\n审核意见17','\n审核意见18','\n审核意见19','\n审核意见20']

type23 = ['\n1.申请文件显示','\n2.申请文件显示','\n3.申请文件显示','\n4.申请文件显示','\n5.申请文件显示','\n6.申请文件显示','\n7.申请文件显示',
          '\n8.申请文件显示','\n9.申请文件显示','\n10.申请文件显示','\n11.申请文件显示','\n12.申请文件显示','\n13.申请文件显示','\n14.申请文件显示',
          '\n15.申请文件显示','\n16.申请文件显示','\n17.申请文件显示','\n18.申请文件显示','\n19.申请文件显示','\n20.申请文件显示','\n21.申请文件显示',
          '\n22.申请文件显示','\n23.申请文件显示','\n24.申请文件显示','\n25.申请文件显示','\n26.申请文件显示','\n27.申请文件显示','\n28.申请文件显示',
          '\n29.申请文件显示','\n30.申请文件显示']

type24 = ['\n反馈意见第1条','\n反馈意见第2条','\n反馈意见第3条','\n反馈意见第4条','\n反馈意见第5条','\n反馈意见第6条','\n反馈意见第7条','\n反馈意见第8条',
          '\n反馈意见第9条','\n反馈意见第10条','\n反馈意见第11条','\n反馈意见第12条','\n反馈意见第13条','\n反馈意见第14条','\n反馈意见第15条','\n反馈意见第16条',
          '\n反馈意见第17条','\n反馈意见第18条','\n反馈意见第19条','\n反馈意见第20条','\n反馈意见第21条','\n反馈意见第22条','\n反馈意见第23条','\n反馈意见第24条',
          '\n反馈意见第25条','\n反馈意见第26条','\n反馈意见第27条','\n反馈意见第28条','\n反馈意见第29条','\n反馈意见第30条','\n反馈意见第31条','\n反馈意见第32条',
          '\n反馈意见第33条','\n反馈意见第34条','\n反馈意见第35条','\n反馈意见第36条','\n反馈意见第37条','\n反馈意见第38条','\n反馈意见第39条','\n反馈意见第40条']

type25 = ['题目1','题目2','题目3','题目4','题目5','题目6','题目7','题目8','题目9','题目10',
          '题目11','题目12','题目13','题目14','题目15','题目16','题目17','题目18','题目19','题目20',
          '题目21','题目22','题目23','题目24','题目25','题目26','题目27','题目28','题目29','题目30',
          '题目31','题目32','题目33','题目34','题目35','题目36','题目37','题目38','题目39','题目40',
          '题目41','题目42','题目43','题目44','题目45','题目46','题目47','题目48','题目49','题目50']

type26 = ['反馈问题一','反馈问题二','反馈问题三','反馈问题四','反馈问题五','反馈问题六','反馈问题七','反馈问题八','反馈问题九','反馈问题十',
          '反馈问题十一','反馈问题十二','反馈问题十三','反馈问题十四','反馈问题十五','反馈问题十六','反馈问题十七','反馈问题十八','反馈问题十九','反馈问题二十',
          '反馈问题二十一','反馈问题二十二','反馈问题二十三','反馈问题二十四','反馈问题二十五','反馈问题二十六','反馈问题二十七','反馈问题二十八','反馈问题二十九',
          '反馈问题三十','反馈问题三十一','反馈问题三十二','反馈问题三十三','反馈问题三十四','反馈问题三十五','反馈问题三十六','反馈问题三十七']

type27 = ['\n第一题','\n第二题','\n第三题','\n第四题','\n第五题','\n第六题','\n第七题','\n第八题','\n第九题','\n第十题',
          '\n第十一题','\n第十二题','\n第十三题','\n第十四题','\n第十五题','\n第十六题','\n第十七题','\n第十八题','\n第十九题','\n第二十题',
          '\n第二十一题','\n第二十二题','\n第二十三题','\n第二十四题','\n第二十五题','\n第二十六题','\n第二十七题','\n第二十八题','\n第二十九题','\n第三十题']

type28 = ['\n[补充反馈意见1]','\n[补充反馈意见2]','\n[补充反馈意见3]','\n[补充反馈意见4]','\n[补充反馈意见5]','\n[补充反馈意见6]']

type29 = ['\n[重点问题1]','\n[重点问题2]','\n[重点问题3]','\n[重点问题4]','\n[重点问题5]','\n[重点问题6]','\n[重点问题7]','\n[重点问题8]',
          '\n[重点问题9]','\n[重点问题10]','\n[重点问题11]','\n[重点问题12]','\n[重点问题13]','\n[一般问题1]','\n[一般问题2]','\n[一般问题3]',
          '\n[一般问题4]','\n[一般问题5]','\n[一般问题6]','\n[一般问题7]','\n[一般问题8]','\n[一般问题9]','\n[一般问题10]','\n[一般问题11]']

type30 = ['\n第一部分重点问题','\n重点问题一','\n重点问题二','\n重点问题三','\n重点问题四','\n重点问题五','\n重点问题六','\n重点问题七',
          '\n重点问题八','\n重点问题九','\n重点问题十','\n重点问题1','\n重点问题2','\n重点问题3','\n重点问题4','\n重点问题5','\n重点问题6',
          '\n重点问题7','\n重点问题8','\n重点问题9','\n重点问题10','\n重点问题11','\n重点问题12','\n重点问题13','\n第二部分一般问题',
          '\n一般问题一','\n一般问题二','\n一般问题三','\n一般问题四','\n一般问题五','\n一般问题六','\n一般问题七','\n一般问题八',
          '\n一般问题九','\n一般问题十','\n一般问题1','\n一般问题2','\n一般问题3','\n一般问题4','\n一般问题5','\n一般问题6','\n一般问题7',
          '\n一般问题8','\n一般问题9','\n一般问题10','\n一般问题11']

type32 = ['【反馈意见1】','【反馈意见2】','【反馈意见3】','【反馈意见4】','【反馈意见5】','【反馈意见6】','【反馈意见7】',
          '【反馈意见8】','【反馈意见9】','【反馈意见10】','【反馈意见11】','【反馈意见12】','【反馈意见13】','【反馈意见14】',
          '【反馈意见15】','【反馈意见16】','【反馈意见17】','【反馈意见18】','【反馈意见19】','【反馈意见20】','【反馈意见21】',
          '【反馈意见22】','【反馈意见23】','【反馈意见24】','【反馈意见25】','【反馈意见26】','【反馈意见27】','【反馈意见28】']

type33 = ['反馈意见第1题:','反馈意见第2题:','反馈意见第3题:','反馈意见第4题:','反馈意见第5题:','反馈意见第6题:','反馈意见第7题:']

type34 = ['\n反馈意见(一)','\n反馈意见(二)','\n反馈意见(三)','\n反馈意见(四)','\n反馈意见(五)','\n反馈意见(六)','\n反馈意见(七)',
          '\n反馈意见(八)','\n反馈意见(九)','\n反馈意见(十)','\n反馈意见(十一)','\n反馈意见(十二)','\n反馈意见(十三)','\n反馈意见(十四)',
          '\n反馈意见(十五)','\n反馈意见(十六)','\n反馈意见(十七)','\n反馈意见(十八)','\n反馈意见(十九)','\n反馈意见(二十)',
          '\n反馈意见(二十一)','\n反馈意见(二十二)','\n反馈意见(二十三)','\n反馈意见(二十四)','\n反馈意见(二十五)','\n反馈意见(二十六)',
          '\n反馈意见(二十七)','\n反馈意见(二十八)','\n反馈意见(二十九)','\n反馈意见(三十)','\n反馈意见(三十一)']

type35 = ['问询问题1','问询问题2','问询问题3','问询问题4','问询问题5','问询问题6','问询问题7','问询问题8','问询问题9','问询问题10',
          '问询问题11','问询问题12','问询问题13','问询问题14','问询问题15','问询问题16','问询问题17','问询问题18','问询问题19','问询问题20',
          '问询问题21','问询问题22','问询问题23','问询问题24','问询问题25','问询问题26','问询问题27','问询问题28','问询问题29','问询问题30',
          '问询问题31','问询问题32','问询问题33','问询问题34','问询问题35','问询问题36','问询问题37','问询问题38','问询问题39','问询问题40',
          '问询问题41','问询问题42','问询问题43','问询问题44','问询问题45','问询问题46','问询问题47','问询问题48','问询问题49','问询问题50',
          '问询问题51','问询问题52','问询问题53','问询问题54','问询问题55','问询问题56','问询问题57','问询问题58','问询问题59','问询问题60',
          '问询问题61','问询问题62','问询问题63','问询问题64','问询问题65','问询问题66','问询问题67','问询问题68','问询问题69','问询问题70']

type36 = ['《落实函》第1题','《落实函》第2题','《落实函》第3题','《落实函》第4题','《落实函》第5题','《落实函》第6题','《落实函》第7题','《落实函》第8题','《落实函》第9题','《落实函》第10题',
          '《落实函》第11题','《落实函》第12题','《落实函》第13题','《落实函》第14题','《落实函》第15题','《落实函》第16题','《落实函》第17题','《落实函》第18题','《落实函》第19题','《落实函》第20题',
          '《落实函》第21题','《落实函》第22题','《落实函》第23题','《落实函》第24题','《落实函》第25题','《落实函》第26题','《落实函》第27题','《落实函》第28题','《落实函》第29题','《落实函》第30题',
          '《落实函》第31题','《落实函》第32题','《落实函》第33题','《落实函》第34题','《落实函》第35题','《落实函》第36题','《落实函》第37题','《落实函》第38题','《落实函》第39题','《落实函》第40题',
          '《落实函》第41题','《落实函》第42题','《落实函》第43题','《落实函》第44题','《落实函》第45题','《落实函》第46题','《落实函》第47题','《落实函》第48题','《落实函》第49题','《落实函》第50题',
          '《落实函》第51题','《落实函》第52题','《落实函》第53题','《落实函》第54题','《落实函》第55题','《落实函》第56题','《落实函》第57题','《落实函》第58题','《落实函》第59题','《落实函》第60题']

type37 = ['问题1:','问题2:','问题3:','问题4:','问题5:','问题6:','问题7:','问题8:','问题9:','问题10:',
       '问题11:','问题12:','问题13:','问题14:','问题15:','问题16:','问题17:','问题18:','问题19:','问题20:',
       '问题21:','问题22:','问题23:','问题24:','问题25:','问题26:','问题27:','问题28:','问题29:','问题30:',
       '问题31:','问题32:','问题33:','问题34:','问题35:','问题36:','问题37:','问题38:','问题39:','问题40:',
       '问题41:','问题42:','问题43:','问题44:','问题45:','问题46:','问题47:','问题48:','问题49:','问题50:',
       '问题51:','问题52:','问题53:','问题54:','问题55:','问题56:','问题57:','问题58:','问题59:','问题60:']

type38 = ['\n\n\n问题']

type39 = ['《问询意见》之问题1','《问询意见》之问题2','《问询意见》之问题3','《问询意见》之问题4','《问询意见》之问题5','《问询意见》之问题6','《问询意见》之问题7','《问询意见》之问题8','《问询意见》之问题9','《问询意见》之问题10',
          '《问询意见》之问题11','《问询意见》之问题12','《问询意见》之问题13','《问询意见》之问题14','《问询意见》之问题15','《问询意见》之问题16','《问询意见》之问题17','《问询意见》之问题18','《问询意见》之问题19','《问询意见》之问题20',
          '《问询意见》之问题21','《问询意见》之问题22','《问询意见》之问题23','《问询意见》之问题24','《问询意见》之问题25','《问询意见》之问题26','《问询意见》之问题27','《问询意见》之问题28','《问询意见》之问题29','《问询意见》之问题30',
          '《问询意见》之问题31','《问询意见》之问题32','《问询意见》之问题33','《问询意见》之问题34','《问询意见》之问题35','《问询意见》之问题36','《问询意见》之问题37','《问询意见》之问题38','《问询意见》之问题39','《问询意见》之问题40',
          '《问询意见》之问题41','《问询意见》之问题42','《问询意见》之问题43','《问询意见》之问题44','《问询意见》之问题45','《问询意见》之问题46','《问询意见》之问题47','《问询意见》之问题48','《问询意见》之问题49','《问询意见》之问题50',
          '《问询意见》之问题51','《问询意见》之问题52','《问询意见》之问题53','《问询意见》之问题54','《问询意见》之问题55','《问询意见》之问题56','《问询意见》之问题57','《问询意见》之问题58','《问询意见》之问题59','《问询意见》之问题60',]

type40 = ['第二轮审核问询第1题','第二轮审核问询第2题','第二轮审核问询第3题','第二轮审核问询第4题','第二轮审核问询第5题','第二轮审核问询第6题','第二轮审核问询第7题','第二轮审核问询第8题','第二轮审核问询第9题','第二轮审核问询第10题',
          '第二轮审核问询第11题','第二轮审核问询第12题','第二轮审核问询第13题','第二轮审核问询第14题','第二轮审核问询第15题','第二轮审核问询第16题','第二轮审核问询第17题','第二轮审核问询第18题','第二轮审核问询第19题','第二轮审核问询第20题',
          '第二轮审核问询第21题','第二轮审核问询第22题','第二轮审核问询第23题','第二轮审核问询第24题','第二轮审核问询第25题','第二轮审核问询第26题','第二轮审核问询第27题','第二轮审核问询第28题','第二轮审核问询第29题','第二轮审核问询第30题',
          '第二轮审核问询第31题','第二轮审核问询第32题','第二轮审核问询第33题','第二轮审核问询第34题','第二轮审核问询第35题','第二轮审核问询第36题','第二轮审核问询第37题','第二轮审核问询第38题','第二轮审核问询第39题','第二轮审核问询第40题',
          '第二轮审核问询第41题','第二轮审核问询第42题','第二轮审核问询第43题','第二轮审核问询第44题','第二轮审核问询第45题','第二轮审核问询第46题','第二轮审核问询第47题','第二轮审核问询第48题','第二轮审核问询第49题','第二轮审核问询第50题']

type41 = ['\n一.','\n二.','\n三.','\n四.','\n五.','\n六.','\n七.','\n八.','\n九.','\n十.',
          '\n十一.','\n十二.','\n十三.','\n十一四.','\n十五.','\n十六.','\n十七.','\n十八.','\n十九.','\n二十.',
          '\n二十一.','\n二十二.','\n二十三.','\n二十四.','\n二十五.','\n二十六.','\n二十七.','\n二十八.','\n二十九.','\n三十.',
          '\n三十一.','\n三十二.','\n三十三.','\n三十四.','\n三十五.','\n三十六.','\n三十七.','\n三十八.','\n三十九.','\n四十.',
          '\n四十一.','\n四十二.','\n四十三.','\n四十四.','\n四十五.','\n四十六.','\n四十七.','\n四十八.','\n四十九.','\n五十.']

type42 = ["\n《第二轮审核问询函》之问题1.1","\n《第二轮审核问询函》之问题1.2","\n《第二轮审核问询函》之问题1.3","\n《第二轮审核问询函》之问题1.4","\n《第二轮审核问询函》之问题1.5","\n《第二轮审核问询函》之问题1.6","\n《第二轮审核问询函》之问题1.7","\n《第二轮审核问询函》之问题1.8","\n《第二轮审核问询函》之问题1.9","\n《第二轮审核问询函》之问题2.1",
          "\n《第二轮审核问询函》之问题2.1","\n《第二轮审核问询函》之问题2.2","\n《第二轮审核问询函》之问题2.3","\n《第二轮审核问询函》之问题2.4","\n《第二轮审核问询函》之问题2.5","\n《第二轮审核问询函》之问题2.6","\n《第二轮审核问询函》之问题2.7","\n《第二轮审核问询函》之问题2.8","\n《第二轮审核问询函》之问题2.9","\n《第二轮审核问询函》之问题3.1",
          "\n《第二轮审核问询函》之问题3.1","\n《第二轮审核问询函》之问题3.2","\n《第二轮审核问询函》之问题3.3","\n《第二轮审核问询函》之问题3.4","\n《第二轮审核问询函》之问题3.5","\n《第二轮审核问询函》之问题3.6","\n《第二轮审核问询函》之问题3.7","\n《第二轮审核问询函》之问题3.8","\n《第二轮审核问询函》之问题3.9","\n《第二轮审核问询函》之问题4.1",
          "\n《第二轮审核问询函》之问题4.1","\n《第二轮审核问询函》之问题4.2","\n《第二轮审核问询函》之问题4.3","\n《第二轮审核问询函》之问题4.4","\n《第二轮审核问询函》之问题4.5","\n《第二轮审核问询函》之问题4.6","\n《第二轮审核问询函》之问题4.7","\n《第二轮审核问询函》之问题4.8","\n《第二轮审核问询函》之问题4.9","\n《第二轮审核问询函》之问题5.1",
          "\n《第二轮审核问询函》之问题5.1","\n《第二轮审核问询函》之问题5.2","\n《第二轮审核问询函》之问题5.3","\n《第二轮审核问询函》之问题5.4","\n《第二轮审核问询函》之问题5.5","\n《第二轮审核问询函》之问题5.6","\n《第二轮审核问询函》之问题5.7","\n《第二轮审核问询函》之问题5.8","\n《第二轮审核问询函》之问题5.9","\n《第二轮审核问询函》之问题6.1",
          "\n《第二轮审核问询函》之问题6.1","\n《第二轮审核问询函》之问题6.2","\n《第二轮审核问询函》之问题6.3","\n《第二轮审核问询函》之问题6.4","\n《第二轮审核问询函》之问题6.5","\n《第二轮审核问询函》之问题6.6","\n《第二轮审核问询函》之问题6.7","\n《第二轮审核问询函》之问题6.8","\n《第二轮审核问询函》之问题6.9","\n《第二轮审核问询函》之问题7.1",
          "\n《第二轮审核问询函》之问题7.1","\n《第二轮审核问询函》之问题7.2","\n《第二轮审核问询函》之问题7.3","\n《第二轮审核问询函》之问题7.4","\n《第二轮审核问询函》之问题7.5","\n《第二轮审核问询函》之问题7.6","\n《第二轮审核问询函》之问题7.7","\n《第二轮审核问询函》之问题7.8","\n《第二轮审核问询函》之问题7.9","\n《第二轮审核问询函》之问题8.1",
          ]

type43 = ["\n《落实函》一、","\n《落实函》二、","\n《落实函》三、","\n《落实函》四、","\n《落实函》五、","\n《落实函》六、","\n《落实函》七、","\n《落实函》八、","\n《落实函》九、","\n《落实函》十、",
          "\n《落实函》十一、","\n《落实函》十二、","\n《落实函》十三、","\n《落实函》十四、","\n《落实函》十五、","\n《落实函》十六、","\n《落实函》十七、","\n《落实函》十八、","\n《落实函》十九、","\n《落实函》十、",
          "\n《落实函》二十一、","\n《落实函》二十二、","\n《落实函》二十三、","\n《落实函》二十四、","\n《落实函》二十五、","\n《落实函》二十六、","\n《落实函》二十七、","\n《落实函》二十八、","\n《落实函》二十九、","\n《落实函》三十、",
          "\n《落实函》三十一、","\n《落实函》三十二、","\n《落实函》三十三、","\n《落实函》三十四、","\n《落实函》三十五、","\n《落实函》三十六、","\n《落实函》三十七、","\n《落实函》三十八、","\n《落实函》三十九、","\n《落实函》四十、",
          "\n《落实函》四十一、","\n《落实函》四十二、","\n《落实函》四十三、","\n《落实函》四十四、","\n《落实函》四十五、","\n《落实函》四十六、","\n《落实函》四十七、","\n《落实函》四十八、","\n《落实函》四十九、","\n《落实函》五十、",
          "\n《落实函》五十一、","\n《落实函》五十二、","\n《落实函》五十三、","\n《落实函》五十四、","\n《落实函》五十五、","\n《落实函》五十六、","\n《落实函》五十七、","\n《落实函》五十八、","\n《落实函》五十九、","\n《落实函》六十、",
          "\n《落实函》六十一、","\n《落实函》六十二、","\n《落实函》三、","\n《落实函》六十四、","\n《落实函》六十五、","\n《落实函》六十六、","\n《落实函》六十七、","\n《落实函》六十八、","\n《落实函》六十九、","\n《落实函》七十、",
          ]


all_type_list = [type4, type12, type13, type14, type15, type16, type17, type19, type20, type21, type22, type23, type24, type25, type26, type27, type28,type29, type30, type32, type33, type34,type35,type36,type37,type38,type39,type40]
all_type_list_s = [type1, type5, type6,type7,type8,type41,type42,type43]


def _label_d_v(value):
    re = etree.HTML(value)
    res = re.xpath("//text()")
    string = '\n\n\n'
    for i in res:
        string = string + i
    return string

def _label_(web_contents,label):
    for j in label:
        for k_i, k in enumerate(web_contents):
            try:
                if k.split() == j.split():
                    value_s = _label_d_v(label[j])
                    web_contents[k_i] = value_s
            except Exception as e:
                print(e)
    return web_contents
for i_num, line_de in enumerate(data):
    top = []
    print('i_num==============', i_num)
    dict_con = ''
    black_flag = False
    _id = line_de.get('_id')
    url = line_de.get('url')
    with open("hif_log.log", "r", encoding='utf-8') as a_hif:
        k_hif = a_hif.read()
    if str(url) in k_hif.split(","):
        print("本条记录已存在----{}".format(url))
        continue
    print('url+++++++++++++++++++++++++++++++++++++++++++++++++++',url)
    title = line_de.get('title')
    gtime = line_de.get('gtime')
    channel = line_de.get('channel')
    department = line_de.get('department')
    ctime = line_de.get('ctime')
    company = line_de.get('company')
    label = line_de.get('label')
    web_contents = line_de.get("web_contents")
    if label:
        web_contents = _label_(web_contents,label)
    try:
      hf_con = ''.join(line_de.get('web_contents'))
    except Exception as e:
      print(e)
      with open("log_relation_result_list.log","a+") as w:
        w.write(url+","+title+"\n")
    


    new_con = []
    if '目录' in hf_con[:1000] or '目 录' in hf_con[:1000] or '目  录' in hf_con[:1000] or '目   录' in hf_con[:1000] or '目    录' in hf_con[:1000] or '目     录' in hf_con[:1000] or '目      录' in hf_con[:1000]:
        print('去除目录')
        new_con_tmp = hf_con.split('\n\n\n')
        # print(new_con_tmp[:10])
        # new_dict_con = dict_con.split('\n')
        # print(new_dict_con)
        max_num = 0
        min_num = 1000000
        for num, item in enumerate(new_con_tmp[:120]):
            # print([item][:100])
            print("item这是个啥",item[-10:])
            if len(item) > 6 and '%。' not in item[-10:] and '年' not in item[-10:] and ('正文' in item[-10:] or '《问询意见》' in item[-10:]) and (']' in item[-10:]  or ':' in item[-10:] or '...' in item[-10:] or '题' in item[-10:] ) and ((item[-1] == ' ' and item[-2].isdigit() == True and item[-3].isdigit() == True) or (item[-1].isdigit() == True and item[-2].isdigit() == True) or (item[-3].isdigit() == True and item[-4].isdigit() == True)) and '2017' not in item[-7:] and not (item[-3].isdigit() and item[-2] == '。') and '.' not in item[-5:] and not (item.strip()[-4:].isdigit()):
                print('去除目录',[item[-10:]])
                max_num = num
            if ('目录' in item or '目 录' in item) and min_num > num:
                min_num = num

        # print('min_num',min_num)
        # print('max_num',max_num)

        # print('hf_con min max',[new_con_tmp[min_num:max_num]])

        top = new_con_tmp[:min_num]
        # print(top)

        hf_con = ''
        # for i in (new_con_tmp[:min_num] + new_con_tmp[max_num + 1:]):
        for i in new_con_tmp[max_num + 1:]:
            new_con.append(i.replace(' ',''))
            hf_con += ('\n' + i)
        # print('new_con',new_con)

    else:

        new_con = hf_con.replace(' ','').split('\n')

    # print('hf_con',hf_con[:100])


    # 拆分信息
    layer_list = []
    for num, item in enumerate(new_con):
        f = False
        for t_num, t in enumerate(all_type_list):
            for ti in t:
                new_t = ti.replace('\n','').replace('\r','')
                if new_t == item[:len(new_t)]:
                    layer_list.append(all_type_list[t_num])



    print(layer_list,"拆分信息")
    if layer_list:
        pass
    else:
        for num, item in enumerate(new_con):
            f = False
            for t_num, t in enumerate(all_type_list_s):
                for ti in t:
                    new_t = ti.replace('\n', '').replace('\r', '')
                    if new_t == item[:len(new_t)]:
                        layer_list.append(all_type_list_s[t_num])

    if layer_list:
        pass
    else:
        with open("hif_结果.csv", "a", encoding='utf-8', newline="") as f:
            k = csv.writer(f, dialect="excel")
            with open("hif_目录.csv", "r", encoding='utf-8', newline="") as f:
                reader = csv.reader(f)
                if not [row for row in reader]:
                    k.writerow(["url", "title", "department"])
                    k.writerow([url, title, department])
                else:
                    k.writerow([url, title, department])
        continue

    with open("hif_log.log","a+",encoding='utf-8') as f_hif:
        f_hif.write(str(url)+",")
    # 日志文件



    l1 = []
    l2 = []
    l3 = []
    l4 = []
    if len(layer_list) > 0:
        l1 = layer_list[0]
        while l1 in layer_list:
            layer_list.remove(l1)

        if len(layer_list) > 0:
            l2 = layer_list[0]
            while l2 in layer_list:
                layer_list.remove(l2)

            if len(layer_list) > 0:
                l3 = layer_list[0]
                while l3 in layer_list:
                    layer_list.remove(l3)

                if len(layer_list) > 0:
                    l4 = layer_list[0]
                    while l4 in layer_list:
                        layer_list.remove(l4)




        print(l1)
        print(l2)
        print(l3)
        print(l4)


    # 拆分
    # layer1
    # print(type(hf_con))
    hf_con = hf_con.replace(' ','')
    print('hf_con',[hf_con[:1000]])
    # if '目录' in hf_con[:1000] or '目 录' in hf_con[:1000] or '目  录' in hf_con[:1000] or '目   录' in hf_con[:1000] or '目    录' in hf_con[:1000] or '目     录' in hf_con[:1000] or '目      录' in hf_con[:1000]:
    #     with open("hif_目录.csv", "a", encoding='utf-8', newline="") as f:
    #         k = csv.writer(f, dialect="excel")
    #         with open("hif_目录.csv", "r", encoding='utf-8', newline="") as f:
    #             reader = csv.reader(f)
    #             if not [row for row in reader]:
    #                 k.writerow(["url", "title", "department"])
    #                 k.writerow([url,title,department])
    #             else:
    #                 k.writerow([url,title,department])
    #     continue
    layer1_content = split_it(hf_con,l1)
    print('layer1_content',len(layer1_content))
    content_list = layer1_content
    if '问题1' not in content_list[0]:
        content_list = content_list[1:]

    # content_list = []
    # for i in layer1_content:
    #     # print([i],'\n---------------')
    #     # content_list.append(i)
    #     layer2_content = split_it(i,l2)

    #     for j in layer2_content:
    #         # print(j,'\n----------------')
    #         layer3_content = split_it(j,l3)

    #         for k in layer3_content:
    #             # print(k,'\n----------------')
    #             layer4_content = split_it(k,l4)
    #             # content_list.append(k)

    #             for l in layer4_content:
    #                 if l != '' and l != ' ' and l != '\n':
    #                     content_list.append(l)
    #                     # print([l],'\n----------------')


    # print('=============')

    result_list = []
    # # print(type(top))
    # if top != []:
    #     top_tmp = ''
    #     for i in top:
    #         top_tmp += i.replace(' ','')
    #     result_list = [('0_0',top_tmp)]


    # print(url)
    class_flag = False
    tmp_num = 0
    begin_tmp_num = 1
    for num, c in enumerate(content_list):
        result_list.append((str(num + begin_tmp_num),c))



    # print(result_list)
    content_dict={}
    no_reply_flag = True
    for i in result_list:
        content_dict[i[0]]=i[1]
        if '_a_' in i[0]:
            no_reply_flag = False
        print([i[0]],[i[1][:50]])


    timestamp = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))


    try:
        collection.insert({'_id': _id,
                           'content_result': content_dict, 
                           'url':url,
                           'title':title,
                           'gtime':gtime,
                           'channel': channel,
                           'department': department,
                           'ctime':ctime,
                           'company': company, 
                           'i_time':timestamp
                           })
        print('insert mongo')
        if content_dict != {}:
            try:
                delete_id = collection.find_one({'title':title, 'ctime':ctime, 'content_result':{}})['_id']
                print('delete_id',delete_id)

                sql_ = "INSERT INTO delete_id_ES (table_name, _id) VALUES ('"+update_data_name+"','"+delete_id+"')"
                print(sql_)
                cursor.execute(sql_)
                db.commit()

                collection.remove({'_id':delete_id})
                print('reportdocs DELETE', title, ctime)
                delete_count += 1
            except:
                continue

    except:
        collection.save({'_id': _id,
                         'content_result': content_dict, 
                         'url':url,
                         'title':title,
                         'gtime':gtime,
                         'channel': channel,
                         'department': department,
                         'ctime':ctime,
                         'company': company, 
                         'i_time':timestamp
                         })
        print('save mongo')


# # 写log
# if add_type_choice == 'add':
#     print('写log!!!')
#     sql_command_log = '''CREATE TABLE IF NOT EXISTS {0} (table_ VARCHAR(255), 
#                                                          add_number VARCHAR(255), 
#                                                          start_gtime DATETIME, 
#                                                          end_gtime DATETIME, 
#                                                          run_time timestamp
#                                                          )'''
#     cursor.execute(sql_command_log.format(mysql_log_name))
#     #sql_ = '''INSERT INTO {0} (table_,add_number)VALUES(%s%s)'''

#     sql_ = '''INSERT INTO {0} (table_,
#                                add_number, 
#                                start_gtime, 
#                                end_gtime
#                                )VALUES(%s,%s,%s,%s)'''
#     cursor.execute(sql_.format(mysql_log_name),(update_data_name, 
#                                                 str(add_data_num), 
#                                                 convert_num_time(max_gtime),
#                                                 convert_num_time(last_gtime)
#                                                 ))

# db.commit()
db.close()
