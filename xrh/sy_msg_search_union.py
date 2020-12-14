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
import sys
import math
import argparse
import htmlparser
import jieba
from itertools import islice
from PIL import Image
import base64
import subprocess

table_name = 'sy_msg_search_union'

# mongo上的反馈数据
mongo_name_list = ['csrc_ann_aud_union', 'csrc_IPOF_union']
mongo_name_list2 = ['api_csrc_RF_html', 'api_csrc_mer_rec_html', 'api_shzq_wxhj_html', 'api_szzq_wxhj_html']
# 要更新的数据
update_data_name = 'sy_msg_search_union'
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

# # 打开数据库连接，从数据库获取当前最新数据的gtime
# db = pymysql.connect('127.0.0.1', 'root', 'shiye', 'EI_BDP', 3306, charset='utf8')
# # db = pymysql.connect('192.168.1.63', 'root', '', 'EI_BDP', 3306, charset='utf8')
# # 使用 cursor() 方法创建一个游标对象 cursor
# cursor = db.cursor()

db = pymysql.connect(**config_local['local_sql'])
# db = pymysql.connect('192.168.1.63', 'root', '', 'EI_BDP', 3306, charset='utf8')
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()

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

# client = pymongo.MongoClient('mongodb://seeyii:shiye@127.0.0.1:27017/admin')
client = pymongo.MongoClient(config_local['cluster_mongo'])
print('connected to mongo')
db_name = client['EI_BDP_DATA']
db_name2 = client['EI_BDP']

# client2 = pymongo.MongoClient('mongodb://root:shiye1805A@192.168.1.125:10011,192.168.1.126:10011,192.168.1.127:10011/admin')
mongodb = client['sy_multi_raw']

collection = mongodb[update_data_name]


def is_Chinese(word):
    for ch in word:
        if '\u4e00' <= ch <= '\u9fff':
            return True
    return False

def get_max_gtime(mongo_name):
    max_gtime=0
    if add_type_choice == 'add':
        # 如果增量处理，取log里的时间戳
        cursor.execute('SELECT * FROM ' + mysql_log_name)
        l1 = list(cursor.fetchall())
        cn = [desc[0] for desc in cursor.description]
        # 初始化max_gtime
        for i in l1:
            if i[cn.index('table_')] == mongo_name:
                num_gtime = convert_real_time(str(i[cn.index('start_gtime')]))
                if max_gtime < num_gtime:
                    max_gtime = num_gtime
    else:
        max_gtime= 0
    print('max_gtime:', max_gtime)
    return max_gtime

def get_max_gtime2(mongo_name):
    max_gtime=0
    if add_type_choice == 'add':
        # 如果增量处理，取log里的时间戳
        cursor.execute('SELECT * FROM ' + mysql_log_name)
        l1 = list(cursor.fetchall())
        cn = [desc[0] for desc in cursor.description]
        # 初始化max_gtime
        for i in l1:
            if i[cn.index('table_')] == mongo_name:
                num_gtime = convert_real_time(str(i[cn.index('start_gtime')]))
                if max_gtime < num_gtime:
                    max_gtime = num_gtime
    else:
        max_gtime= 0
    print('max_gtime:', max_gtime)
    return max_gtime

# table和img转换
class Rewrite2Monogo():
    """docstring for rewrite2monogo"""
    def __init__(self, dir_html='',dir_pic='',name=''):
        super(Rewrite2Monogo, self).__init__()
        self.dir_html = dir_html
        self.dir_pic = dir_pic
        self.name = name
        self.suffix = '.htm'
    def getFiles(self): # 查找根目录，文件后缀 
        paths = []

        for root, directory, files in os.walk(self.dir_html):  # =>当前根,根下目录,目录下的文件
            for filename in files:
                name, suf = os.path.splitext(filename) # =>文件名,文件后缀
                if suf == self.suffix and name == self.name:
                    paths.append(os.path.join(root, filename)) # =>吧一串字符串组合成路径

        return paths

    def base_64_fn(self,pic_path):
        #print(pic_path)
        with open(pic_path, 'rb') as f:
            base64_data = base64.b64encode(f.read())
        s = base64_data.decode()
        return s
    def imgsize1_fn(self,path):
        img = Image.open(path)
        return img.size
    def imgsize_fn(self,path):
        #返回图片大小
        size_ = os.path.getsize(path)
        return size_
    def get_pic_fn(self,path):
        #图片 替换为% 或反回图片base64格式
        pic_dict={}
        pic_small=[]
        pic_path=self.dir_pic+ path.split('/')[-1].replace('.htm','')
        #print(222222,self.dir_pic+ path.split('/')[-1].replace('.htm',''))
        img_list=[]
        root_path=''
        for root, directory, files in os.walk(pic_path):
            root_path=root
            img_list=files
        for filename in img_list:
            pic_path=os.path.join(root_path, filename)

            imgsize = self.imgsize_fn(pic_path)

            if imgsize<=300:
                pic_small.append(filename)
            else:
                #将大的图片转为base64
                pic_dict[filename]=self.base_64_fn(pic_path)
        return pic_small,pic_dict
    def process(self,html_path):
        pic_small,pic_dict =self.get_pic_fn(html_path)
        #print(pic_small)
        with open(html_path, 'r', encoding='utf8') as F:
            html_output = F.read()
############################################
#替换所有满足条件的%
        all_imgs = re.findall('<img.*?\>', html_output, re.S | re.I | re.M)#先取出所有的包干img部分
        for i in all_imgs:
            for j in pic_small:
                if j  in i:
                    html_output = html_output.replace(i,'%')
#########################################
#先处理table

        table_list_parttern = '(<table.*?</table>)'
        table_list = re.findall(table_list_parttern, html_output, re.S | re.I | re.M)
        label = {}
        for count, table_item in enumerate(table_list):
            index_table_num = count
            node = 'table_node_{}'.format(index_table_num) + '\n'
            html_output = html_output.replace(table_item, '<p>' + node + '</p>')
            table_item = re.sub('<img.*?>', '', table_item, re.S | re.I | re.M)
            label[node] = table_item

##########################################
#处理非lable的image

        for i in all_imgs:

            for k in pic_dict.keys():
                if k  in i:
                    node = '#img_0_0_'+k.replace('Image_','').replace('.png','').replace('.jpg','').replace('.gif','')+'#'
                    html_output = html_output.replace(i,node)
        pic_dict_new={}
        dict2sort=sorted(pic_dict.items(), key = lambda item : len(item[1]), reverse=True)#从大到小排序，保留最大。去除最小
        for j,k in enumerate(dict2sort):
            if j<=50:#图片大于50不保留
                node = '#img_0_0_'+k[0].replace('Image_','').replace('.png','').replace('.jpg','').replace('.gif','')+'#'
                pic_dict_new[node]=k[1]
            else:
                html_output= html_output.replace(node,'')
        web_contents = ''
        if html_output == '':
            pass
        else:
            xml = htmlparser.Parser(html_output)
            try:
                content_list = xml.xpathall('''//p | //table | //h1 | //h2 | //h3 | //h4 | //h5 | //h6''')
                for con in content_list:
                    con = con.text().strip()
                    if type(con) == bytes:
                        con = str(con, encoding='utf8')
                    if con and con!='%':#只有%忽略

                        web_contents += ('\n' + con + '\n')
            except:
                pass
        return label,web_contents,pic_dict_new

    def main_execute(self):

        paths = self.getFiles()

        for path in paths:
            # table_list = []
            # label1={}
            # label2={}
            # path_pdf=path.split('/')[-1].replace('.htm','.pdf')
            label,web_contents,pic_dict = self.process(path)
            # table_list.append(label)
            return label,web_contents,pic_dict
    def chunks(self,data,SIZE=2):
        it = iter(data)
        for i in range(0, len(data), SIZE):
            yield {k:data[k] for k in islice(it, SIZE)}

_id_list = []
department_list = []
company_list = []
gsdm_list = []
title_list = []
ctime_list = []
gtime_list = []
url_list = []
url_wx_list = []
accessory_list = []
contents_list = []
tables_list = []
imgs_list = []
content_xml_list = []
api_flag_list = []

for mongo_name in mongo_name_list:
    max_gtime = get_max_gtime(mongo_name)
    add_type={'all': 0, 'add':max_gtime}
    data = []
    data_list = db_name[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}})
    print('mongo_name:', mongo_name)
    for i in data_list:
        data.append(i)
    if len(data) == 0:
        print('There is no new data!')
        continue

    if mongo_name == 'csrc_ann_aud_union':
        for i, line_de in enumerate(data):
            print(i)
            contents = ''
            accessory = []
            tables = {}
            imgs = {}
            _id = line_de.get('_id')
            department = 'IPO审核意见'
            company = line_de.get('company')
            gsdm = line_de.get('gsdm')
            title = line_de.get('title_wx')
            ctime = line_de.get('ctime')
            gtime = line_de.get('gtime')
            url = line_de.get('url_wx')
            url_wx = line_de.get('url_wx')
            accessory_url = line_de.get('accessory_url')
            accessory_name = line_de.get('accessory_name')
            accessory_dict = {'url': accessory_url, 'file_name': accessory_name, 'file_path': 'csrc/csrc_ann_aud'}
            accessory.append(accessory_dict)
            content_dict = line_de.get('content_wx')
            content_xml = ''
            api_flag = 0
            for k, v in content_dict.items():
                contents += v + '\n'
            _id_list.append(_id)
            department_list.append(department)
            company_list.append(company)
            gsdm_list.append(gsdm)
            title_list.append(title)
            ctime_list.append(ctime)
            gtime_list.append(gtime)
            url_list.append(url)
            url_wx_list.append(url_wx)
            accessory_list.append(accessory)
            contents_list.append(contents)
            tables_list.append(tables)
            imgs_list.append(imgs)
            content_xml_list.append(content_xml)
            api_flag_list.append(api_flag)

    if mongo_name == 'csrc_IPOF_union':
        for i, line_de in enumerate(data):
            print(i)
            contents = ''
            accessory = []
            tables = {}
            imgs = {}
            _id = line_de.get('_id')
            department = 'IPO申报文件反馈'
            company = line_de.get('company')
            gsdm = line_de.get('gsdm')
            title = line_de.get('title_wx')
            ctime = line_de.get('ctime')
            gtime = line_de.get('gtime')
            url = line_de.get('url_wx')
            url_wx = line_de.get('url_wx')
            accessory_url = line_de.get('accessory_url')
            accessory_name = line_de.get('accessory_name')
            accessory_dict = {'url': accessory_url, 'file_name': accessory_name, 'file_path': 'csrc/IPOF'}
            accessory.append(accessory_dict)
            content_dict = line_de.get('content_wx')
            content_xml = ''
            api_flag = 0
            for k, v in content_dict.items():
                contents += v + '\n'
            _id_list.append(_id)
            department_list.append(department)
            company_list.append(company)
            gsdm_list.append(gsdm)
            title_list.append(title)
            ctime_list.append(ctime)
            gtime_list.append(gtime)
            url_list.append(url)
            url_wx_list.append(url_wx)
            accessory_list.append(accessory)
            contents_list.append(contents)
            tables_list.append(tables)
            imgs_list.append(imgs)
            content_xml_list.append(content_xml)
            api_flag_list.append(api_flag)

for mongo_name in mongo_name_list2:
    if mongo_name == 'api_csrc_RF_html':
        max_gtime = get_max_gtime('api_csrc_RF_union')
    elif mongo_name == 'api_csrc_mer_rec_html':
        max_gtime = get_max_gtime('api_csrc_mer_rec_union')
    elif mongo_name == 'api_shzq_wxhj_html':
        max_gtime = get_max_gtime('api_shzq_wxhj_data')
    elif mongo_name == 'api_szzq_wxhj_html':
        max_gtime = get_max_gtime('api_szzq_wxhj_union')
    print('mongo_name:', mongo_name)
    add_type={'all': 0, 'add':max_gtime}
    data=[]
    no_dupli_id = []
    data_list = db_name2[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}})
    data_list_api = db_name2[mongo_name].find({"api_time": {"$gt": add_type[add_type_choice]}})
    for i in data_list:
        no_dupli_id.append(i.get('_id'))
        data.append(i)
    for i in data_list_api:
        if i.get('_id') not in no_dupli_id:
            data.append(i)
    if len(data) == 0:
        print('There is no new data!')
        continue

    if mongo_name == 'api_csrc_RF_html':
        for i, line_de in enumerate(data):
            print(i)
            contents = ''
            accessory = []
            tables = {}
            imgs = {}
            _id = line_de.get('_id')
            department = '再融资反馈'
            company = line_de.get('company')
            gsdm = line_de.get('gsdm')
            title = line_de.get('title_wx')
            ctime = line_de.get('ctime')
            gtime = line_de.get('gtime')
            url = line_de.get('url_wx')
            url_wx = line_de.get('url_wx')
            accessory_url = line_de.get('accessory_url')
            accessory_name = line_de.get('accessory_name')
            accessory_dict = {'url': accessory_url, 'file_name': accessory_name, 'file_path': 'csrc/RF'}
            accessory.append(accessory_dict)
            content_dict = line_de.get('content_wx')
            content_xml = ''
            api_flag = 0
            if 'api_msg' in line_de.keys() and line_de.get('api_msg'):
                contents = ast.literal_eval(line_de.get('api_msg'))['content']
                table_list = ast.literal_eval(line_de.get('api_msg'))['table_list']
                for item in table_list:
                    for i, j in item.items():
                        # print(i,j)
                        tables[i.replace('\n', '')] = j.replace('\n', '')
                title = ast.literal_eval(line_de.get('api_msg'))['api_title']
                real_time = ast.literal_eval(line_de.get('api_msg'))['api_date']
                ctime = int(time.mktime(time.strptime(real_time, '%Y-%m-%d %H:%M:%S')))
                url = ast.literal_eval(line_de.get('api_msg'))['api_url']
                api_flag = 1
            else:
                for k, v in content_dict.items():
                    contents += v + '\n'
            _id_list.append(_id)
            department_list.append(department)
            company_list.append(company)
            gsdm_list.append(gsdm)
            title_list.append(title)
            ctime_list.append(ctime)
            gtime_list.append(gtime)
            url_list.append(url)
            url_wx_list.append(url_wx)
            accessory_list.append(accessory)
            contents_list.append(contents)
            tables_list.append(tables)
            imgs_list.append(imgs)
            content_xml_list.append(content_xml)
            api_flag_list.append(api_flag)

    if mongo_name == 'api_csrc_mer_rec_html':
        for i, line_de in enumerate(data):
            print(i)
            contents = ''
            accessory = []
            tables = {}
            imgs = {}
            _id = line_de.get('_id')
            department = '并购重组反馈'
            company = line_de.get('company')
            gsdm = line_de.get('gsdm')
            title = line_de.get('title_wx')
            ctime = line_de.get('ctime')
            gtime = line_de.get('gtime')
            url = line_de.get('url_wx')
            url_wx = line_de.get('url_wx')
            accessory_url = line_de.get('accessory_url')
            accessory_name = line_de.get('accessory_name')
            accessory_dict = {'url': accessory_url, 'file_name': accessory_name, 'file_path': 'csrc/mer_rec'}
            accessory.append(accessory_dict)
            content_dict = line_de.get('content_wx')
            content_xml = ''
            api_flag = 0
            if 'api_msg' in line_de.keys() and line_de.get('api_msg'):
                contents = ast.literal_eval(line_de.get('api_msg'))['content']
                if contents != '':
                    table_list = ast.literal_eval(line_de.get('api_msg'))['table_list']
                    for item in table_list:
                        for i, j in item.items():
                            # print(i,j)
                            tables[i.replace('\n', '')] = j.replace('\n', '')
                    title = ast.literal_eval(line_de.get('api_msg'))['api_title']
                    real_time = ast.literal_eval(line_de.get('api_msg'))['api_date']
                    ctime = int(time.mktime(time.strptime(real_time, '%Y-%m-%d %H:%M:%S')))
                    url = ast.literal_eval(line_de.get('api_msg'))['api_url']
                    api_flag = 1
                else:
                    for k, v in content_dict.items():
                        contents += v + '\n'
            else:
                for k, v in content_dict.items():
                    contents += v + '\n'
            _id_list.append(_id)
            department_list.append(department)
            company_list.append(company)
            gsdm_list.append(gsdm)
            title_list.append(title)
            ctime_list.append(ctime)
            gtime_list.append(gtime)
            url_list.append(url)
            url_wx_list.append(url_wx)
            accessory_list.append(accessory)
            contents_list.append(contents)
            tables_list.append(tables)
            imgs_list.append(imgs)
            content_xml_list.append(content_xml)
            api_flag_list.append(api_flag)

    if mongo_name == 'api_shzq_wxhj_html':
        for i, line_de in enumerate(data):
            print(i)
            contents = ''
            accessory = []
            tables = {}
            imgs = {}
            _id = line_de.get('_id')
            department = '上交所'
            gsjc = line_de.get('gsjc')
            gsdm = line_de.get('gsdm')
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
            title = line_de.get('title')
            ctime = line_de.get('ctime')
            gtime = line_de.get('gtime')
            url = line_de.get('title_href')
            url_wx = line_de.get('title_href')
            accessory_url = url
            accessory_name = url.split('/')[-1]
            accessory_dict = {'url': accessory_url, 'file_name': accessory_name, 'file_path': 'sse/shzq_jgwx'}
            accessory.append(accessory_dict)
            content_xml = ''
            api_flag = 0
            if 'api_msg' in line_de.keys() and line_de.get('api_msg'):
                contents = line_de.get('api_msg')['content']
                if contents != '':
                    table_list = line_de.get('api_msg')['table_list']
                    for item in table_list:
                        for i, j in item.items():
                            # print(i,j)
                            tables[i] = j
                    title = line_de.get('api_msg')['api_title']
                    real_time = line_de.get('api_msg')['api_date']
                    ctime = int(time.mktime(time.strptime(real_time, '%Y-%m-%d %H:%M:%S')))
                    url = line_de.get('api_msg')['api_url']
                    api_flag = 1
                else:
                    contents = line_de.get('file_content')
            else:
                contents = line_de.get('file_content')
            _id_list.append(_id)
            department_list.append(department)
            company_list.append(company)
            gsdm_list.append(gsdm)
            title_list.append(title)
            ctime_list.append(ctime)
            gtime_list.append(gtime)
            url_list.append(url)
            url_wx_list.append(url_wx)
            accessory_list.append(accessory)
            contents_list.append(contents)
            tables_list.append(tables)
            imgs_list.append(imgs)
            content_xml_list.append(content_xml)
            api_flag_list.append(api_flag)

    if mongo_name == 'api_szzq_wxhj_html':
        file_name_list = []
        for i, line_de in enumerate(data):
            print(i)
            contents = ''
            accessory = []
            tables = {}
            imgs = {}
            _id = line_de.get('_id')
            department = '深交所'
            gsjc = line_de.get('gsjc')
            gsdm = line_de.get('gsdm')
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
            title = line_de.get('hjnr_content')['file_name']
            ctime = line_de.get('ctime')
            gtime = line_de.get('gtime')
            url = line_de.get('hjnr_content')['url']
            url_wx = url
            accessory_url = ''
            accessory_name = ''
            content_xml = ''
            api_flag = 0
            if 'api_msg' in line_de.keys() and line_de.get('api_msg'):
                contents = line_de.get('api_msg')['content']
                table_list = line_de.get('api_msg')['table_list']
                for item in table_list:
                    for i, j in item.items():
                        # print(i,j)
                        tables[i] = j
                title = line_de.get('api_msg')['api_title']
                real_time = line_de.get('api_msg')['api_date']
                ctime = int(time.mktime(time.strptime(real_time, '%Y-%m-%d %H:%M:%S')))
                url = line_de.get('api_msg')['api_url']
                api_flag = 1
            elif 'gshf_content' in line_de.keys() and line_de.get('gshf_content'):
                file_name = line_de.get('gshf_content')['file_name']
                name = file_name.split('.')[0]
                rewrite2monogo=Rewrite2Monogo(dir_html='/home/seeyii/szzq_html/htmlfile/',dir_pic='/home/seeyii/szzq_html/picfile/',name=name)
                if rewrite2monogo.main_execute():
                    tables,contents,imgs = rewrite2monogo.main_execute()
                    if contents == '':
                        contents = line_de.get('gshf_content')['file_content']
                else:
                    contents = line_de.get('gshf_content')['file_content']
                    file_name_list.append(file_name)
                title = line_de.get('gshf_content')['name']
                url = line_de.get('gshf_content')['url']
                accessory_url = url
                accessory_name = file_name
                api_flag = 1
            else:
                contents = line_de.get('hjnr_content')['file_content']
                accessory_url = line_de.get('hjnr_content')['url']
                accessory_name = line_de.get('hjnr_content')['file_name']
                if is_Chinese(title) == False or '关于' not in title:
                    pos1 = contents.find('关于对')
                    pos2 = contents.find('函', pos1)
                    if pos1 != -1 and pos2 != -1:
                        title =  contents[pos1 : pos2 + 1]
                    else:
                        title = '关于对' + company + '的关注函'
            if contents == '':
                contents = line_de.get('hjnr_content')['file_content']
                accessory_url = line_de.get('hjnr_content')['url']
                accessory_name = line_de.get('hjnr_content')['file_name']
                api_flag = 0
                if is_Chinese(title) == False or '关于' not in title:
                    pos1 = contents.find('关于对')
                    pos2 = contents.find('函', pos1)
                    if pos1 != -1 and pos2 != -1:
                        title =  contents[pos1 : pos2 + 1]
                    else:
                        title = '关于对' + company + '的关注函'
            _id_list.append(_id)
            accessory_dict = {'url': accessory_url, 'file_name': accessory_name, 'file_path': 'szse/wxhj_szzq'}
            accessory.append(accessory_dict)
            department_list.append(department)
            company_list.append(company)
            gsdm_list.append(gsdm)
            title_list.append(title)
            ctime_list.append(ctime)
            gtime_list.append(gtime)
            url_list.append(url)
            url_wx_list.append(url_wx)
            accessory_list.append(accessory)
            contents_list.append(contents)
            tables_list.append(tables)
            imgs_list.append(imgs)
            content_xml_list.append(content_xml)
            api_flag_list.append(api_flag)

add_data_num = 0
for i in range(len(_id_list)):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    data = collection.find_one({'_id': _id_list[i]})
    data2 = collection.find_one({'department': '深交所', 'url_wx': url_wx_list[i]})
    if data2 and data2['api_flag'] == 1:
        continue
    if api_flag_list[i] == 0 and data2 and data2['api_flag'] == 0:
        continue
    if api_flag_list[i] == 1 and data2 and data2['api_flag'] == 0:
        collection.remove({'_id': data2['_id']})
    if data and data['api_flag'] == 1:
        continue
    if api_flag_list[i] == 0 and data and data['api_flag'] == 0:
        continue
    if api_flag_list[i] == 1 and data and data['api_flag'] == 0:
        collection.remove({'_id': _id_list[i]})
    try:
        collection.insert({'_id': _id_list[i], 
                   'department': department_list[i], 
                   'company': company_list[i], 
                   'gsdm': gsdm_list[i],
                   'title': title_list[i], 
                   'ctime': ctime_list[i],
                   'gtime': gtime_list[i],
                   'url': url_list[i],
                   'url_wx': url_wx_list[i],
                   'content_xml': content_xml_list[i], 
                   'accessory': accessory_list[i],
                   'contents': contents_list[i],
                   'tables': tables_list[i],
                   'imgs': imgs_list[i],
                   'content_xml': content_xml_list[i],
                   'api_flag' : api_flag_list[i],
                   'i_time':timestamp
                   })
        add_data_num += 1
    except Exception as r:
        print(r)
        print(department_list[i], _id_list[i])
        continue

# 写log
# sql_command_log = '''CREATE TABLE IF NOT EXISTS {0} (table_ VARCHAR(255),
#                                                      add_number VARCHAR(255),
#                                                      start_gtime DATETIME,
#                                                      end_gtime DATETIME,
#                                                      run_time timestamp
#                                                      )'''
# cursor.execute(sql_command_log.format(mysql_log_name))
# # sql_ = '''INSERT INTO {0} (table_,add_number)VALUES(%s%s)'''

# sql_ = '''INSERT INTO {0} (table_,
#                            add_number,
#                            start_gtime,
#                            end_gtime
#                            )VALUES(%s,%s,%s,%s)'''
# cursor.execute(sql_.format(mysql_log_name), (update_data_name,
#                                              str(add_data_num),
#                                              convert_num_time(max_gtime),
#                                              timestamp
#                                              ))
db.commit()

db.close()

# 查找pdf转html失败的文件名
file_name_list2 = []
for file_name in file_name_list:
    print('未解析成html的文件：', file_name)
    file_name += '\n'
    file_name_list2.append(file_name)
with open('fileNameData.txt','w', encoding='utf-8') as f:
    f.writelines(file_name_list2)