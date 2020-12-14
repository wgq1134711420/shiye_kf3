# conding:utf-8
# http://www.sse.com.cn/disclosure/credibility/supervision/inquiries/
'''
上交所问询答复拆分
'''
import json
import re
import sys
from pymongo import MongoClient
import pymongo
import time
import pymysql
import os
import argparse
import redis
from datetime import datetime

load_config = '/home/seeyii/increase_nlp/db_config.json'
with open(load_config, 'r', encoding="utf-8") as f:
    reader = f.readlines()[0]
config_local = json.loads(reader)

parse = argparse.ArgumentParser()
# mongo数据存两个，一个为mongo_data_name1，mongo_data_name

parse.add_argument("--data_gtime", type=str, default='add', help="默认取增量数据，如果‘all’则为全部处理,如果‘add’则为全部处理")
# parse.add_argument("--data_gtime",type=str,default='all',help="默认取增量数据，如果‘all’则为全部处理,如果‘add’则为全部处理")

parse.add_argument("--mongo_data_name1", type=str, default='api_shzq_wxhj_data', help="mysql数据存储位置_原始数据开发用")
# parse.add_argument("--mongo_data_name",type=str,default='api_shzq_wxhj_data',help="mysql数据存储位置_实际使用")
parse.add_argument("--log_name", type=str, default='data_log', help="处理数据存储位置")
parse.add_argument("--mongo_html_name", type=str, default='api_shzq_wxhj_html', help="html源数据存储位置")

FLAGS, unparsed = parse.parse_known_args(sys.argv[1:])
# redis
update_data_name = 'api_shzq_wxhj_data'
conn = redis.StrictRedis(**config_local['local_redis'])
set_key = FLAGS.mongo_html_name

mongocli = pymongo.MongoClient(config_local['cluster_mongo'])
print('connected to mongo')
table = mongocli['EI_BDP_DATA'][FLAGS.mongo_data_name1]
# table2 = mongocli['EI_BDP_DATA'][FLAGS.mongo_data_name]

# 打开数据库连接
db = pymysql.connect(**config_local['local_sql'])
print('contented to mysql')
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()

# flag d对问题和回答做标记。这里默认问题是最多有20个！！！
flag_s1 = ['\n一、', '\n二、', '\n三、', '\n四、', '\n五、', '\n六、', '\n七、', '\n八、', '\n九、', '\n十、', '\n十一、', '\n十二、', '\n十三、',
           '\n十四、', '\n十五、', '\n十六、', '\n十七、', '\n十八、', '\n十九、', '\n二十、']  # 一级段落#（一）
flag_s2 = ['\n1、', '\n2、', '\n3、', '\n4、', '\n5、', '\n6、', '\n7、', '\n8、', '\n9、', '\n10、', '\n11、', '\n12、', '\n13、',
           '\n14、', '\n15、', '\n16、', '\n17、', '\n18、', '\n19、', '\n20、']  # 二级段落
# flag_s3=['\n(1)','\n(2)','\n(3)','\n(4)','\n(5)','\n(6)','\n(7)','\n(8)','\n(9)','\n(10)','\n(11)','\n(12)','\n(13)','\n(14)','\n(15)','\n(16)','\n(17)','\n(18)','\n(19)','\n(20)']#三级段落
flag_s5 = ['\n问题1、', '\n问题2、', '\n问题3、', '\n问题4、', '\n问题5、', '\n问题6、', '\n问题7、', '\n问题8、', '\n问题9、', '\n问题10、',
           '\n问题11、', \
           '\n问题12、', '\n问题13、', '\n问题14、', '\n问题15、', '\n问题16、', '\n问题17、', '\n问题18、', '\n问题19、', '\n问题20、']
flag_s3 = ['\n(一)', '\n(二)', '\n(三)', '\n(四)', '\n(五)', '\n(六)', '\n(七)', '\n(八)', '\n(九)', '\n(十)', '\n(十一)', '\n(十二)',
           '\n(十三)', '\n(十四)', '\n(十五)', '\n(十六)', '\n(十七)', '\n(十八)', '\n(十九)', '\n(二十)']
# flag_s4 = ['\n1.', '\n2.', '\n3.', '\n4.', '\n5.', '\n6.', '\n7.', '\n8.', '\n9.', '\n10.', '\n11.', '\n12.', '\n13.',
#            '\n14.', '\n15.', '\n16.', '\n17.', '\n18.', '\n19.', '\n20.', '\n21.', '\n22.', '\n23.', '\n24.', '\n25.',
#            '\n26.', '\n27.', '\n28.', '\n29.', '\n30.', '\n31.']  # 二级段落
flag_s4 = []
for i in range(1000):
    flag_s4.append('\n'+str(i+1)+'.')
flag_s6 = ['\n问题1:', '\n问题2:', '\n问题3:', '\n问题4:', '\n问题5:', '\n问题6:', '\n问题7:', '\n问题8:', '\n问题9:', '\n问题10:',
           '\n问题11:', \
           '\n问题12:', '\n问题13:', '\n问题14:', '\n问题15:', '\n问题16', '\n问题17:', '\n问题18:', '\n问题19:', '\n问题20:']
flag_s7 = ['\n问题一', '\n问题二', '\n问题三', '\n问题四', '\n问题五', '\n问题六', '\n问题七', '\n问题八', '\n问题九', '\n问题十', '\n问题十一', \
           '\n问题十二', '\n问题十三', '\n问题十四', '\n问题十五', '\n问题十六', '\n问题十七', '\n问题十八', '\n问题十九', '\n问题二十']
flag_s8 = ['\n问题 1.', '\n问题 2.', '\n问题 3.', '\n问题 4.', '\n问题 5.', '\n问题 6.', '\n问题 7.', '\n问题 8.', '\n问题 9.',
           '\n问题 10.', '\n问题 11.', \
           '\n问题 12.', '\n问题 13.', '\n问题 14.', '\n问题 15.', '\n问题 16.', '\n问题 17.', '\n问题 18.', '\n问题 19.', '\n问题 20.']
flag_s9 = ['\n问询 1．', '\n问询 2．', '\n问询 3．', '\n问询 4．', '\n问询 5．', '\n问询 6.', '\n问询 7.', '\n问询 8.', '\n问询 9.',
           '\n问询 10.', '\n问询 11.', \
           '\n问询 12.', '\n问询 13.', '\n问询 14.', '\n问询 15.', '\n问询 16.', '\n问询 17.', '\n问询 18.', '\n问询 19.', '\n问询 20.']

flag_s10 = ['\n问询 1.', '\n问询 2.', '\n问询 3.', '\n问询 4.', '\n问询 5.', '\n问询 6.', '\n问询 7.', '\n问询 8.', '\n问询 9.',
           '\n问询 10.', '\n问询 11.', \
           '\n问询 12.', '\n问询 13.', '\n问询 14.', '\n问询 15.', '\n问询 16.', '\n问询 17.', '\n问询 18.', '\n问询 19.', '\n问询 20.']

flag_a = ['\n答复:', '\n回复:','\n说明：', '\n回复如下:', '\n【回复如下】','\n【公司回复】', '。回复', '【回复】', '回复说明:', ' \n回复说明：', '答:', '审核意见回复:', '\n我公司回复:', '\n公司答复:',
          '\n公司回复:', '\n【公司说明】', '\n核查情况如下', '【回复说明】', \
          '\n一、上市公司回复内容', '\n二、上市公司回复内容', '\n三、上市公司回复内容', '\n四、上市公司回复内容', \
          '\n五、上市公司回复内容', '\n六、上市公司回复内容', '\n七、上市公司回复内容', '\n八、上市公司回复内容', '\n九、上市公司回复内容', '\n十、上市公司回复内容', \
          '\n公司对问题一(1)回复如下:', '\n公司对问题一(2)回复如下:', '\n公司对问题一(3)回复如下:', '\n公司对问题一(4)回复如下:', '\n公司对问题一(5)回复如下:', \
          '\n公司对问题二(1)回复如下:', '\n公司对问题二(2)回复如下:', '\n公司对问题二(3)回复如下:', '\n公司对问题二(4)回复如下:', '\n公司对问题二(5)回复如下:', \
          '\n公司对问题三(1)回复如下:', '\n公司对问题三(2)回复如下:', '\n公司对问题三(3)回复如下:', '\n公司对问题三(4)回复如下:', '\n公司对问题三(5)回复如下:', \
          '\n公司对问题四(1)回复如下:', '\n公司对问题四(2)回复如下:', '\n公司对问题四(3)回复如下:', '\n公司对问题四(4)回复如下:', '\n公司对问题四(5)回复如下:', \
          '\n公司对问题五(1)回复如下:', '\n公司对问题五(2)回复如下:', '\n公司对问题五(3)回复如下:', '\n公司对问题五(4)回复如下:', '\n公司对问题五(5)回复如下:', \
          '\n公司对问题六(1)回复如下:', '\n公司对问题六(2)回复如下:', '\n公司对问题六(3)回复如下:', '\n公司对问题六(4)回复如下:', '\n公司对问题六(5)回复如下:', \
          '\n公司对问题七(1)回复如下:', '\n公司对问题七(2)回复如下:', '\n公司对问题七(3)回复如下:', '\n公司对问题七(4)回复如下:', '\n公司对问题七(5)回复如下:', \
          '\n公司对问题八(1)回复如下:', '\n公司对问题八(2)回复如下:', '\n公司对问题八(3)回复如下:', '\n公司对问题八(4)回复如下:', '\n公司对问题八(5)回复如下:', \
          '\n公司对问题九(1)回复如下:', '\n公司对问题九(2)回复如下:', '\n公司对问题九(3)回复如下:', '\n公司对问题九(4)回复如下:', '\n公司对问题九(5)回复如下:', '核实并披露情况'
          ]
follow_marker = ['回复公告如下:', '回复说明如下:',
                 '内容公告如下:']  # 处理这种问题：http://analyze.doc.rongdasoft.com/file/2018/2018-06-09/0b05d8235628f7953e7e2c61ddb3a122.html
flags = flag_s1 + flag_s2 + flag_s3 + flag_s4 + flag_s5 + flag_s6 + flag_s7 + flag_s8 + flag_s9 + flag_s10 +  flag_a
flags1 = flag_s1 + flag_s2 + flag_s3 + flag_s4 + flag_s5 + flag_s6 + flag_s7 + flag_s8 + flag_s9 + flag_s10


#
def DBC2SBC(ustring):
    """把字符串全角转半角"""
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 0x3000:
            inside_code = 0x0020
        else:
            inside_code -= 0xfee0
        if inside_code < 0x0020 or inside_code > 0x7e:  # 转完之后不是半角字符返回原来的字符
            rstring += uchar
        else:
            rstring += chr(inside_code)
    return rstring


def dl(text):
    list_rm = ['\u3000', '􀳁', '■']
    for i in list_rm:
        while (i in text):
            text = text.replace(i, '')
    if len(text) == 0:
        return text
    elif ('。' in text[0]):
        text = text[1:]
    return text


def company_extractoin(text):
    nane = ''
    name = ex(text)
    return name


# 对于公司名称的提取
def ex(s):
    s = s.replace('\n', 'Q$0')
    # print(s)
    obj1 = ['关于对', 'Q$0']
    obj2 = ['公司', '务所']
    len1 = 2
    pos1_list = []
    for item in obj1:
        # print('it1:',item)
        pos1 = s.find(item)
        if pos1 != -1:
            pos1_list.append(pos1)
    nPos1 = min(pos1_list)
    pos2_list = []
    for item in obj2:
        # 确保第二段内容与第一段没有重叠
        pos2 = s.find(item, nPos1)
        if pos2 != -1:
            # print('pos@@',pos2)
            pos2_list.append(pos2)
    # print('pos##',pos2_list)
    if pos2_list:
        nPos2 = min(pos2_list)
    else:
        nPos2 = -1

    if nPos1 != -1 and nPos2 != -1 and nPos2 > nPos1 + 4:
        # new_con = s[:(nPos1+4)]+'==============分割============='+s[nPos2:-1]
        new_title = s[(nPos1 + 3):nPos2 + len1]
        new_title = new_title.replace(' Q$0', '')
    else:
        new_title = ''
    # print(new_title)
    return new_title


def findall(p, s):
    '''Yields all the positions of the pattern p in the string s.'''
    i = s.find(p)
    while i != -1:
        yield i
        i = s.find(p, i + 1)


def extraction_framework(content, class_=1):  # class在这里是兼容有回复的，深证证券
    postions = []
    sections = []
    postions_a = []
    dict_ = {}
    acc = 0
    content = DBC2SBC(content)
    # content =content.replace(' ','').replace('   ','')#
    # if '\n附表********' in content:acc=1
    # content = content.split('\n附表********')[0]
    content = content.replace(' ', '').replace('   ', '')
    # content 只截取‘回答、回复’等标记前的段落。marker_start(开始)来判断需要分段的标记
    for item in flag_a:
        [postions_a.append(i) for i in findall(item, content)]
    content_start = content
    if postions_a != []: content_start = content[:postions_a[0]]
    for i in follow_marker:
        if i in content_start:
            content_start = content_start.split(i)[1]
    for item in flags:
        pos1 = content.find(item)
        [postions.append((i, item)) for i in findall(item, content) if len(re.findall(r'{}\.\d+\%*'.format(item.replace('.','')), content)) == 0]  # a tuple list
    postions.sort()  # ranging list
    if postions != []:
        content_0 = content[:postions[0][0]]
    else:
        content_0 = content
    print(postions)
    postions, level = judge_level(postions, content, content_start)
    print(postions)
    part_index = '0'
    qa_index = '0'
    label_ = '0_0'
    mark = 0
    for j, it in enumerate(postions):

        section_level = ''
        # label_=''
        i = it[0]
        if it[1] in flag_a:
            section_level = 'a'  # 对回答的key做标记
        else:
            section_level = 'q'  # 对

        for n, m in enumerate(
                level):  # level为二维数组#level为：[['\n一、', '\n二、', '\n三、', '\n四 、', '\n五、', '\n六、', '\n七、', '\n八、', '\n九、', '\n十、', '\n十一、', '\n十二、', '\n十三、', '\n十四、', '\n十五、', '\n十六、', '\n十七、', '\n十八、', '\n十九、', '\n二十、'], ['\n1.', '\n2.', '\n3.', '\n4.', '\n5.', '\n6.', '\n7.', '\n8.', '\n9.', '\n10.', '\n11.', '\n12.', '\n13.', '\n14.', '\n15.', '\n16.', '\n17.', '\n18.', '\n19.', '\n20.']]
            if n == 0 and it[1] in m and len(level) > 1:  # level大于1说明才有大的标题部分，否则就是问题，大的标题只能在n=0时。此时不记录问题对。
                # part_index=m.index(it[1])+1
                # label_=str(part_index)+'_'+'0'
                label_ = str(n) + '_' + '0'
            if (n >= 1 and it[1] in m and len(level) > 1) or (
                    n == 0 and it[1] in m and len(level) == 1):  # level大于1说明在大于1的才是问题对，level=0只有一种说明是问题对
                qa_index = m.index(it[1]) + 1
                # label_=str(part_index)+'_'+str(qa_index)
                label_ = str(n) + '_' + str(qa_index)
                # label_=str(qa_index)+'_'+str(qa_index)
        # if label_=='0_0':
        section_level = str(j + 1) + '_' + str(section_level) + '_' + label_  # 主要是对字典里内容（value）对应的key做标签，方便在ES里拆分。
        if j < len(postions) - 1:
            k = postions[j + 1][0]
            content_i = dl(content[i:k])
            if '[{' in content_i:
                content_i = content_i.split('[{')[0]
            dict_[section_level] = content_i
        if j == len(postions) - 1:
            end_content = dl(content[i:])
            if '[{' in end_content:
                end_content = end_content.split('[{')[0]
            dict_[section_level] = end_content
    section_begin = '0_0' + content_0
    # print('\n' in (content[:postions[0][0]]))
    if class_ == 0:
        dict_['0_0'] = company_extractoin((content_0))
    ####sections.insert(0,section_begin)
    dict_['0_0'] = content_0
    for i_name, i_value in dict_.items():
        print([i_name,i_value[:30]])
    # if acc==1:dict_['section_acc'] = '表格省略'
    return sections, dict_


# 判断段落标志并把错误的remove，
def judge_level(data, text, content_start):
    # print('text:',text)
    s1 = []
    s2 = []
    s3 = []
    s4 = []
    s5 = []
    s6 = []
    s7 = []
    s8 = []
    s9 = []
    s10 = []
    d1 = []
    d2 = []
    d3 = []
    d4 = []
    d5 = []
    d6 = []
    d7 = []
    d8 = []
    d9 = []
    d10 = []
    level = []
    redata = []
    # d对flags的归类
    for p, it in enumerate(data):
        idf = flags.index(it[1])
        # print(idf)
        if len(text[it[0]:]) < 8: continue
        if idf < 20:  # 在flag_s1中
            if (idf != 0) and s1 == []:  # 第一次出现非1的标记去除
                redata.append(it)
                continue
            if s1 != [] and idf - 1 != s1[-1]:
                redata.append(it)
                continue
            if (idf == 0 or idf - 1 == s1[-1]) and (flag_s1[0] in content_start):
                # print('##',it)
                s1.append(idf)
                d1.append(it[0])
                if (d1[0], flag_s1) not in level:
                    level.append((d1[0], flag_s1))
            else:
                redata.append(it)
        if idf >= 20 and idf < 40:
            if (idf != 20 and s2 == []):
                redata.append(it)
                continue
            if s2 != [] and idf - 1 != s2[-1]:
                redata.append(it)
                continue
            if (idf == 20 or idf - 1 == s2[-1]) and flag_s2[0] in content_start:
                s2.append(idf)
                d2.append(it[0])
                if (d2[0], flag_s2) not in level:
                    level.append((d2[0], flag_s2))
            else:
                redata.append(it)
        if idf >= 40 and idf < 60:
            if (idf != 40 and s3 == []):
                redata.append(it)
                continue
            if s3 != [] and idf - 1 != s3[-1]:
                redata.append(it)
                continue
            if (idf == 40 or idf - 1 == s3[-1]) and flag_s3[0] in content_start:
                s3.append(idf)
                d3.append(it[0])
                if (d3[0], flag_s3) not in level:
                    level.append((d3[0], flag_s3))
            else:
                redata.append(it)

        if idf >= 60 and idf < 80:
            if idf != 60 and s4 == []:
                redata.append(it)
                continue
            if s4 != [] and idf - 1 != s4[-1]:
                redata.append(it)
                continue
            if (text[it[0] + 3].isdigit() or text[it[0] + 4].isdigit() or "%" in text[it[0] + 3:it[
                                                                                                    0] + 7]) and '201' not in text[
                                                                                                                              it[
                                                                                                                                  0] + 3:
                                                                                                                              it[
                                                                                                                                  0] + 7]:
                redata.append(it)
                continue
            if (idf == 60 or idf - 1 == s4[-1]) and flag_s4[0] in content_start:  # 去除1.2这种，只支持1.不加数字
                s4.append(idf)
                d4.append(it[0])
                if (d4[0], flag_s4) not in level:
                    level.append((d4[0], flag_s4))
            else:
                redata.append(it)
        if idf >= 80 and idf < 100:
            if (idf != 80 and s5 == []):
                redata.append(it)
                continue
            if s5 != [] and idf - 1 != s5[-1]:
                redata.append(it)
                continue
            if (idf == 80 or idf - 1 == s5[-1]) and flag_s5[0] in content_start:
                s5.append(idf)
                d5.append(it[0])
                if (d5[0], flag_s5) not in level:
                    level.append((d5[0], flag_s5))
            else:
                redata.append(it)
        if idf >= 100 and idf < 120:
            if (idf != 100 and s6 == []):
                redata.append(it)
                continue
            if s6 != [] and idf - 1 != s6[-1]:
                redata.append(it)
                continue
            if (idf == 100 or idf - 1 == s6[-1]) and flag_s6[0] in content_start:
                s6.append(idf)
                d6.append(it[0])
                if (d6[0], flag_s6) not in level:
                    level.append((d6[0], flag_s6))
            else:
                redata.append(it)
        if idf >= 120 and idf < 140:
            if (idf != 120 and s7 == []):
                redata.append(it)
                continue
            if s7 != [] and idf - 1 != s7[-1]:
                redata.append(it)
                continue
            if (idf == 120 or idf - 1 == s7[-1]) and flag_s7[0] in content_start:
                s7.append(idf)
                d7.append(it[0])
                if (d7[0], flag_s7) not in level:
                    level.append((d7[0], flag_s7))
            else:
                redata.append(it)
        if idf >= 140 and idf < 160:
            if (idf != 140 and s8 == []):
                redata.append(it)
                continue
            if s8 != [] and idf - 1 != s8[-1]:
                redata.append(it)
                continue
            if (idf == 140 or idf - 1 == s8[-1]) and flag_s8[0] in content_start:
                s8.append(idf)
                d8.append(it[0])
                if (d8[0], flag_s8) not in level:
                    level.append((d8[0], flag_s8))
            else:
                redata.append(it)
        if idf >= 160 and idf < 180:
            if (idf != 160 and s9 == []):
                redata.append(it)
                continue
            if s9 != [] and idf - 1 != s9[-1]:
                redata.append(it)
                continue
            if (idf == 160 or idf - 1 == s9[-1]) and flag_s9[0] in content_start:
                s9.append(idf)
                d9.append(it[0])
                if (d9[0], flag_s9) not in level:
                    level.append((d9[0], flag_s9))
            else:
                redata.append(it)

        if idf >= 160 and idf < 180:
            if (idf != 160 and s10 == []):
                redata.append(it)
                continue
            if s10 != [] and idf - 1 != s10[-1]:
                redata.append(it)
                continue
            if (idf == 160 or idf - 1 == s10[-1]) and flag_s10[0] in content_start:
                s10.append(idf)
                d10.append(it[0])
                if (d10[0], flag_s10) not in level:
                    level.append((d10[0], flag_s10))
            else:
                redata.append(it)

    # level =[(d1[0],flag_s1),(d2[0],flag_s2),(d3[0],flag_s3),(d4[0],flag_s4)]
    level.sort()
    level = [i[1] for i in level]
    level = level
    return [i for i in data if i not in redata], level


# 对非结构的拆分
def extraction_no_marker(content, class_=1):
    postions = []
    sections = []
    dict_ = {}
    acc = 0
    content = DBC2SBC(content)
    if '\n附表********' in content: acc = 1
    content = content.split('\n附表********')[0]
    # content =content.replace(' ','').replace('   ','')
    flags = ['。 \n', ': \n', ':\n', '。\n']
    # section_level ='section_'+str(j)+'_'
    for item in flags:
        [postions.append((i, item)) for i in findall(item, content)]
    if len(postions) == 0:
        # dict_['section_begin']= dl(content)
        dict_['0_0'] = dl(content)
        return sections, dict_
    postions.sort()
    for item in postions:
        if content[item[0] + 3:item[0] + 7] == '特此函告':
            postions.remove(item)
    for j, it in enumerate(postions):
        section_level = ''
        i = it[0]
        if it[1] in flag_a:
            section_level = 'reply'
        section_level = str(j + 1) + '_' + 'q_0_0'  # 对于五段落标记的默认为都1
        if j < len(postions) - 1:
            k = postions[j + 1][0]
            content_i = dl(content[i + 3:k])
            if '[{' in content_i:
                content_i = content_i.split('[{')[0]
            dict_[section_level] = content_i
            ####sections.append(section_level+content[i+3:k])
        if j == len(postions) - 1:
            content_end = dl(content[i + 3:])
            if '[{' in content_end:
                content_end = content_end.split('[{')[0]
            dict_[section_level] = content_end

    section_begin = 'section_begin' + dl(content[:postions[0][0]])
    if class_ == 0:
        dict_['section_0_0'] = company_extractoin((content[:postions[0][0]]))
    # dict_['section_0_0']=''
    # dict_['section_0_0'] =company_extractoin((content[:postions[0][0]]))
    ####sections.insert(0,section_begin)
    dict_['0_0'] = dl(content[:postions[0][0]])
    if acc == 1: dict_['section_acc'] = '表格省略'
    for i_name, i_value in dict_.items():
        print([i_name,i_value[:30]])
    return sections, dict_


def company_extractoin(text):
    nane = ''
    name = ex(text)
    return name


def ex(s):
    s = s.replace('\n', 'Q$0')
    obj1 = ['关于', 'Q$0']
    obj2 = ['公司', '务所']
    len1 = 2
    pos1_list = []
    for item in obj1:
        pos1 = s.find(item)
        if pos1 != -1:
            pos1_list.append(pos1)
    if pos1_list:
        nPos1 = min(pos1_list)
    else:
        nPos1 = -1
    pos2_list = []
    for item in obj2:
        # 确保第二段内容与第一段没有重叠
        pos2 = s.find(item, nPos1)
        if pos2 != -1:
            pos2_list.append(pos2)
    if pos2_list:
        nPos2 = min(pos2_list)
    else:
        nPos2 = -1

    if nPos1 != -1 and nPos2 != -1 and nPos2 > nPos1 + 4:
        # new_con = s[:(nPos1+4)]+'==============分割============='+s[nPos2:-1]
        new_title = s[(nPos1 + 2):nPos2 + len1]
        new_title = new_title.replace(' Q$0', '')
    else:
        new_title = ''
    if new_title:
        if new_title[0] == '对':
            new_title = new_title[1:]
    return new_title


dbname_html = mongocli['EI_BDP']
dbname_data = mongocli['EI_BDP_DATA']

max_gtime_html = 0
min_gtime_html = 100000000000000


def convert_time(real_time):  # datetime.strptime(start, "%Y-%m-%d")
    timeArray = datetime.strptime(real_time, "%Y-%m-%d %H:%M:%S")
    ctime = int(timeArray.timestamp())
    return ctime


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

if FLAGS.data_gtime == 'add':
    try:
        # 如果增量处理，取log里的时间戳
        cursor.execute('SELECT * FROM ' + FLAGS.log_name)
        l1 = list(cursor.fetchall())
        cn = [desc[0] for desc in cursor.description]
        for i in l1:
            if i[cn.index('table_')] == FLAGS.mongo_data_name1:  # log里的表名为mongo_data名
                time_ = convert_time(str(i[cn.index('end_gtime')]))
                if max_gtime_html < time_:
                    max_gtime_html = time_
        print('last_time in log:', max_gtime_html)
        add_type = {'all': 0, 'add': max_gtime_html}
    except:
        print('error')
        FLAGS.data_gtime = 'all'
        add_type = {'all': 0, 'add': max_gtime_html}
        pass
else:  # 全量处理
    print(11, FLAGS.data_gtime)
    add_type = {'all': 0, 'add': max_gtime_html}

# def get_mongo(dbname,col_name):
#     # data=[]
#     # data_list = dbname[col_name].find({"gtime": {"$gt": add_type[FLAGS.data_gtime]}})#.limit(200)
#     # # data_list = dbname[col_name].find({"_id": "b298df8693683e1bbd4af90f64068891"})
#     # for item in data_list:
#     #     data.append(item)
#     # return data
#     data=[]
#     data_list = dbname[col_name].find({"gtime": {"$gt": add_type[FLAGS.data_gtime]}})#.limit(200)
#     data_list_api = db_name[col_name].find({"api_time": {"$gt": add_type[FLAGS.data_gtime]}})

#     # data_list = db_name[mongo_name].find({"ctime": {"$gt": 1546272000}})

#     no_dupli_id = []
#     for i in data_list:
#         # if i.get('_id') == '1757c64f188d3f50840d912567e8bddd8':
#         # print(type(i.get('gtime')))
#         # if i.get('gtime') > 1552003200:
#         no_dupli_id.append(i.get('_id'))
#         data.append(i)

#     for i in data_list_api:
#         if i.get('_id') not in no_dupli_id:
#             data.append(i)
#     return data

# data =get_mongo(dbname_html,FLAGS.mongo_html_name)#mongo_data
# # data=data[:10]#test!
# add_num=len(data)
# print('add_num',add_num)
# if add_num==0:
#     conn.set(set_key,0)
#     print('there is no new data to get')
#     os._exit(0)


data = []
# data_list = dbname_html[FLAGS.mongo_html_name].find({"gtime": {"$gt": add_type[FLAGS.data_gtime]}})
# data_list_api = dbname_html[FLAGS.mongo_html_name].find({"api_time": {"$gt": add_type[FLAGS.data_gtime]}})

data_list = dbname_html[FLAGS.mongo_html_name].find({"_id": "6d8779dbfe5131ebb6156d6d6ac6434f"})

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
add_num = len(data)
print('add_num:', add_num)

for i in data:
    if max_gtime_html < i['gtime']:
        max_gtime_html = i['gtime']
    if min_gtime_html > i['gtime']:
        min_gtime_html = i['gtime']
print(max_gtime_html, min_gtime_html)
time_now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
replace_list = [('现就《问询函》的相关问题补充披露如下： \n \n问题三、2018 年 6 月 7 日，公司董事会审议通过《关于投资广州东湛房地',
                 '现就《问询函》的相关问题补充披露如下： \n \n问题一、2018 年 6 月 7 日，公司董事会审议通过《关于投资广州东湛房地'), \
                ('情况。 二、请公司补充披露交易具体情况：（1）本次交易的定价依据，是否存在溢价情 \n况及具体原因；',
                 '情况。 \n二、请公司补充披露交易具体情况：（1）本次交易的定价依据，是否存在溢价情 \n况及具体原因；'), \
                ('）2600号）（以下简称“《问询函》”），现回 复如下。', '）2600号）（以下简称“《问询函》”），现回 复如下。\n现回复如下。'), ('\n回复： \n \n', '\n回复：'),
                ('\n回复： \n', '\n回复：'), ('\n回复：\n', '\n回复：'), ('。 回复： \n', '。 \n回复：')]  #


def replace_special(content):
    for item in replace_list:
        if item[0] in content:
            content = content.replace(item[0], item[1])
    return content


mululist = [('\n目 录 \n一、', '证预 案的完整性及严谨性，易于投资者阅读、理解。 58'),
            ('\n目 录 \n关于对佳都新太科技股份有', '2015 年度 \n标的资产、标的公司、华之源 指 广东华之源信息工程有限公司'), \
            ('\n目 录 \n一、关于本次重组的主要风险 3 \n问题一 3 ', '41 \n问题九 44 \n问题十 47 \n问题十一 50'),
            ('\n目录 \n问题一：标的资产控股股东为何云鹏', '产品有一项停运，是否存在前后不一致。请财务顾问核查并发表意见。 231') \
    , ('\n目 录 \n \n \ntable_node_1\n \ntable_node_2\n', '\n目 录 \n \n \ntable_node_1\n \ntable_node_2\n'),
            ('\n目 录 \n一、关于本次重组可能构成重组上市的问题 9 \n问题一', '39 万元应为交易金额，而非资产总额， 请财务顾问予以更正。 167'), \
            ('\n（如无特殊说明，本回复中简称与报告书中的简称具有相同含义） 本回', '问题 10 及回复 44'), (' \n目 录 \n目 录', '层及核心技术人员流失对公司产生的具体影响及其应 对措施。 18'),
            ('\n目 录 \n一', '是否有利于保护公司和投资者的利益 70'), \
            ('\n目录 \n问题 1： 3 \n问题 ', '38 \n问题 7： 41 \n问题 8： 47 \n问题 9： 69 \n问题 10： 80 \n问题 11： 83 \n问题 12： 89'),
            (' \n目录 \n \n一、关于标的资产业务的独立性', '\n问题 11： 55 \n问题 12： 71 '), ('\n目 录 \n一、问题 1 及回复 3 \n', '\n五、问题 5 及回复 14'), \
            ('\n目录 \n一、关于标的资产持续盈利能力与后续整合 ', '\n上年同期数据不一致，请公司披露具体差异并说明原因。 51'),
            ('\n目录 \n一、关于标的资产的财务状况 4 \n问', '请补充披露该担保产生的具体原因，与被担保方是否存在关联关系。 请财务顾问及律师发表意见。 46'), \
            ('\n目录 \n问题一：预案披露，万华实业于 2010 年、2', '向公司及万华化工主张提前清偿或另行提供担保的债务金额。 64'),
            ('\n目录 \n问题一 3 \n问题', '十三 69 \n问题十四 72 \n问题十五 74'), \
            ('\n目 录 \n目 录 2 \n问题一：公告', '，并结合目前相关锁定股份的市值说明其是否足够保障上市公司利益。 .13')]


def delete_mulu(text):
    for item in mululist:
        # print(item,(item[0] in text),(item[1] in text))
        if item[0] in text and item[1] in text:
            locate1 = text.index(item[0])
            locate2 = text.index(item[1])
            # cut_text=text.replace()
            text = text[:locate1] + text[locate2:].replace(item[1], '')
    return text


for i, line in enumerate(data):
    print(i)
    # if i>=40:continue
    table_list = {}
    dict_wx = {}
    id_ = line.get('_id')
    gsjc = line.get('gsjc')
    gsdm = line.get('gsdm')
    jgwxlx = line.get('jgwxlx')
    ctime = line.get('ctime')
    gtime = line.get('gtime')
    wjxz_path = line.get('wjxz_path')
    title = line.get('title')
    # if title!='关于江苏索普化工股份有限公司的重大资产重组草案审核意见函':
    # print(22)or title!='关于对远东智慧能源股份有限公司向控股股东收购股权事项的问询函' \
    # or title!='关于江苏索普化工股份有限公司的重大资产重组草案审核意见函':
    #    continue
    company = company_extractoin(title)

    # 提取公司简称
    gs = gsjc.replace(' ', '')
    # print(gs)
    if gs in jiancheng_list:
        pos = jiancheng_list.index(gs)
        company = quancheng_list[pos]
    elif gs.replace('*', '') in jiancheng_list:
        pos = jiancheng_list.index(gs.replace('*', ''))
        company = quancheng_list[pos]
    elif company == '' and gsdm in code_list:
        pos = code_list.index(gsdm)
        company = quancheng_list[pos]

    hjnr_url = line['title_href']
    timeArray = time.localtime(ctime)
    real_ctime = time.strftime('%Y-%m-%d', timeArray)
    content_wx = line.get('file_content')
    ####问询
    if (len(content_wx) > 150):
        is_extract = 0
        for i in flags1:
            if i in DBC2SBC(content_wx).replace(' ', '').replace('   ', ''):
                is_extract = is_extract + 1
        a = re.sub("[A-Za-z0-9\!\%\[\]\,\。\*]", "", dl(content_wx))
        if is_extract <= 1:
            _, dict_wx = extraction_no_marker(content_wx)
        else:
            _, dict_wx = extraction_framework(content_wx)
    else:
        print('问询少', id_, title)
    print('******'*5)
    if 'api_msg' not in line.keys():
        for k in dict_wx.keys():

            print([k, dict_wx[k][:30]])
        # print(dict_wx)
        table.save(
            {'_id': id_, 'real_ctime': real_ctime, 'ctime': ctime, 'gtime': gtime, 'i_time': time_now, 'gsjc': gsjc,
             'gsdm': gsdm, 'title_wx': title, 'company': company, 'department': '上交所',
             'channel': jgwxlx, 'url_wx': hjnr_url, 'approve': '', 'finacing_type': '', 'title_reply': '',
             'url_reply': '', 'date_reply': '',
             'content_wx': dict_wx, 'content_reply': {}, 'tables': table_list, 'accessory_url': '',
             'accessory_title': ''})
        continue

    api_all = line.get('api_msg')
    content_hj = api_all['content']
    title_reply = api_all['api_title']
    re_url = api_all['api_url']
    re_date = api_all['api_date']
    tables = api_all['table_list']
    content_hj = replace_special(content_hj)
    if ('\n目 录 \n' in content_hj or '\n目录' in content_hj or '\n目 录' in content_hj) and len(content_hj) > 2000:
        content_hj = delete_mulu(content_hj)
    for item in tables:
        for i, j in item.items():
            table_list[i.replace('\n', '')] = j  # .replace('\n','')
    is_extract = 0
    for i in flags1:
        try:
            content_hj = DBC2SBC(content_hj)
        except:
            pass
        if i in content_hj.replace(' ', '').replace('   ', ''):  # copy
            is_extract = is_extract + 1
    a = re.sub("[A-Za-z0-9\!\%\[\]\,\。\*]", "", dl(content_hj))
    if (len(a) < 150):  #

        dict_re = {}
    if '[{' in content_hj:
        content_hj = content_hj.split('[{')[0]

    section, dict_re = extraction_framework(content_hj, 1)  # 有结构化的段落标志处理
    table.save({'_id': id_, 'real_ctime': real_ctime, 'ctime': ctime, 'gtime': gtime, 'i_time': time_now, 'gsjc': gsjc,
                'gsdm': gsdm, 'title_wx': title, 'company': company, 'department': '上交所', 'channel': jgwxlx,
                'url_wx': hjnr_url, 'ipo_approve': '', 'ipo_prospectus': '', 'finacing_type': '',
                'title_reply': title_reply, 'url_reply': re_url, 'date_reply': re_date,
                'content_wx': dict_wx, 'content_reply': dict_re, 'tables': table_list, 'accessory_url': '',
                'accessory_name': ''})
    # 去重
    try:

        delete_id = table.find_one({'title_wx': title_wx, 'ctime': ctime, 'content_reply': {}})['_id']
        print('delete_id', delete_id)
        sql_ = "INSERT INTO delete_id_ES (table_name, _id) VALUES ('" + update_data_name + "','" + delete_id + "')"
        cursor.execute(sql_)
        db.commit()
        table.remove({'_id': delete_id})
        # print('reportdocs DELETE', title_wx, ctime)
        # delete_count += 1
        #  table.remove({'title_wx':title, 'ctime':ctime, 'content_reply':{}})
    except:
        pass

sql_command_log = '''CREATE TABLE IF NOT EXISTS {0} (table_ VARCHAR(255),add_number VARCHAR(255),start_gtime datetime,end_gtime datetime,run_time timestamp)'''
cursor.execute(sql_command_log.format(FLAGS.log_name))
# sql_ = '''INSERT INTO {0} (table_,add_number)VALUES(%s%s)'''

timeArray = time.localtime(max_gtime_html)
end_gtime = time.strftime('%Y-%m-%d %H:%M:%S', timeArray)

timeArray = time.localtime(min_gtime_html)
start_gtime = time.strftime('%Y-%m-%d %H:%M:%S', timeArray)
# sql_ = '''INSERT INTO {0} (table_,add_number,start_gtime,end_gtime)VALUES(%s,%s,%s,%s)'''
# cursor.execute(sql_.format(FLAGS.log_name), (FLAGS.mongo_data_name1, str(add_num), start_gtime, end_gtime))

# db.commit()
db.close()
# os._exit(0)
# command='''
#         python3 /home/seeyii/increase_nlp/mongo2es.py --gtime_low {0} --gtime_high {1} --mongo_data_name1 {2}
#         '''
# print(command.format(str(min_gtime_html),str(max_gtime_html),FLAGS.mongo_data_name1))
# os.system(command.format(str(min_gtime_html),str(max_gtime_html),FLAGS.mongo_data_name1))

# conn.set(set_key,0)
