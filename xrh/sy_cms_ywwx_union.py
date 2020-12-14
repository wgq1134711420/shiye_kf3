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
import uuid

table_name = 'sy_multi_feedback_union'

# mongo上的反馈数据
mongo_name_list = ['csrc_ann_aud_union', 'csrc_IPOF_union', 'sse_kcb_xxpl_raw']
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

# 生成企业唯一识别码
def add_uuid(data):

    """
    对字符串进行加密
    :return:
    """
    data = uuid.uuid3(uuid.NAMESPACE_DNS, data)
    data = str(data)
    result_data = data.replace('-', '')
    return result_data

def conn_mysql():
    mysql_conn = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        passwd='shiye',
        db='sy_project_raw',
        charset='utf8'
    )

    return mysql_conn

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

client = pymongo.MongoClient('mongodb://seeyii:shiye@127.0.0.1:27017/admin')
print('connected to mongo')
db_name = client['EI_BDP_DATA']
db_name2 = client['EI_BDP']

client2 = pymongo.MongoClient('mongodb://root:shiye1805A@192.168.1.125:10011,192.168.1.126:10011,192.168.1.127:10011/admin')
mongodb = client2['sy_multi_raw']

collection = mongodb[update_data_name]


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
                num_gtime = convert_real_time(str(i[cn.index('end_gtime')]))
                if max_gtime < num_gtime:
                    max_gtime = num_gtime
    else:
        max_gtime= 0
    print('max_gtime:', max_gtime)
    return max_gtime

_id_list = []
company_list = []  # 企业名称
uid_list = []  # 企业唯一识别码
scn_list = []  # 目标主体统一社会信用代码
org_num_list = []  # 组织机构代码
reg_num_list = []  # 工商注册号
tax_reg_num_list = []  # 纳税人识别号
institution_num_list = []  # 事业单位证书号
social_org_num_list = []  # 社会组织登记证号
ctime_date_list = []  # 发函日期
department_list = []  # 发函单位
contents_list = []  # 函件内容
event_num_list = []  # 文号
project_name_list = []  # 项目名称
source_type_list = []  # 函件类型(来源类型)
issue_type_list = []  # 是否对反馈问题进行分类(现有问题是否有分类信息)
file_type_list = []  # 文件类型
if_reply_list = []  # 是否已回函(有回复的，表示是，否则否)
reply_date_list = []  # 回函日期
url_list = []  # 原链接
data_source_list = []  # 数据来源，同department
gtime_date_list = []  # 数据更新时间
_id_list2 = []


for mongo_name in mongo_name_list:
    max_gtime = get_max_gtime(mongo_name)
    print(mongo_name)
    add_type={'all': 0, 'add':max_gtime}
    data = []
    data_list = db_name[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}})
    # data_list = db_name[mongo_name].find({"gtime": {"$gt": 1582868478}})
    for i in data_list:
        data.append(i)
    if len(data) == 0:
        print('There is no new data!')
        continue
    print('mongo_name:', mongo_name)

    if mongo_name == 'csrc_ann_aud_union':
        for i, line_de in enumerate(data):
            print(i)
            contents = ''
            accessory = []
            tables = {}
            _id = line_de.get('_id')
            company = line_de.get('company')
            uid = add_uuid(company)
            ctime = line_de.get('ctime')
            ctimeArray = time.localtime(ctime)
            ctimeDate = time.strftime('%Y-%m-%d %H:%M:%S', ctimeArray)
            department = line_de.get('department')
            source_type = 'IPO审核意见'
            accessory_url = line_de.get('accessory_url')
            if accessory_url:
                file_type = accessory_url.split('.')[-1]
            else:
                file_type = 'html'
            issue_type = 'Y'
            if_reply = 'N'
            reply_date = None
            url = line_de.get('url_wx')
            gtime = line_de.get('gtime')
            gtimeArray = time.localtime(gtime)
            gtimeDate = time.strftime('%Y-%m-%d %H:%M:%S', gtimeArray)
            content_dict = line_de.get('content_wx')
            for k, v in content_dict.items():
                contents += v + '\n'
            if len(contents) < 4:
                contents = '未提出需企业进一步说明的询问'
            _id_list.append(_id)
            company_list.append(company)
            uid_list.append(uid)
            ctime_date_list.append(ctimeDate)
            department_list.append(department)
            contents_list.append(contents)
            event_num_list.append('')
            project_name_list.append('')
            source_type_list.append(source_type)
            file_type_list.append(file_type)
            issue_type_list.append(issue_type)
            if_reply_list.append(if_reply)
            reply_date_list.append(reply_date)
            url_list.append(url)
            data_source_list.append(department)
            gtime_date_list.append(gtimeDate)

    if mongo_name == 'csrc_IPOF_union':
        for i, line_de in enumerate(data):
            print(i)
            contents = ''
            accessory = []
            tables = {}
            _id = line_de.get('_id')
            company = line_de.get('company')
            uid = add_uuid(company)
            ctime = line_de.get('ctime')
            ctimeArray = time.localtime(ctime)
            ctimeDate = time.strftime('%Y-%m-%d %H:%M:%S', ctimeArray)
            department = line_de.get('department')
            source_type = 'IPO申报文件反馈'
            accessory_url = line_de.get('accessory_url')
            if accessory_url:
                file_type = accessory_url.split('.')[-1]
            else:
                file_type = 'html'
            issue_type = 'Y'
            if_reply = 'N'
            reply_date = None
            url = line_de.get('url_wx')
            gtime = line_de.get('gtime')
            gtimeArray = time.localtime(gtime)
            gtimeDate = time.strftime('%Y-%m-%d %H:%M:%S', gtimeArray)
            content_dict = line_de.get('content_wx')
            for k, v in content_dict.items():
                contents += v + '\n'
            _id_list.append(_id)
            company_list.append(company)
            uid_list.append(uid)
            ctime_date_list.append(ctimeDate)
            department_list.append(department)
            contents_list.append(contents)
            event_num_list.append('')
            project_name_list.append('')
            source_type_list.append(source_type)
            file_type_list.append(file_type)
            issue_type_list.append(issue_type)
            if_reply_list.append(if_reply)
            reply_date_list.append(reply_date)
            url_list.append(url)
            data_source_list.append(department)
            gtime_date_list.append(gtimeDate)

    if mongo_name == 'sse_kcb_xxpl_raw':
        for i, line_de in enumerate(data):
            print(i)
            contents = ''
            accessory = []
            tables = {}
            _id = line_de.get('_id')
            company = line_de.get('company')
            uid = add_uuid(company)
            ctime = line_de.get('ctime')
            ctimeArray = time.localtime(ctime)
            ctimeDate = time.strftime('%Y-%m-%d %H:%M:%S', ctimeArray)
            department = line_de.get('department')
            source_type = '科创板问询答复'
            issue_type = 'N'
            if_reply = 'Y'
            reply_date = ctimeDate
            url = line_de.get('url')
            file_type = url.split('.')[-1]
            gtime = line_de.get('gtime')
            gtimeArray = time.localtime(gtime)
            gtimeDate = time.strftime('%Y-%m-%d %H:%M:%S', gtimeArray)
            _id_list.append(_id)
            company_list.append(company)
            uid_list.append(uid)
            ctime_date_list.append(ctimeDate)
            department_list.append(department)
            contents_list.append(contents)
            event_num_list.append('')
            project_name_list.append('')
            source_type_list.append(source_type)
            file_type_list.append(file_type)
            issue_type_list.append(issue_type)
            if_reply_list.append(if_reply)
            reply_date_list.append(reply_date)
            url_list.append(url)
            data_source_list.append(department)
            gtime_date_list.append(gtimeDate)

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
    data_list = db_name2[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}})
    # data_list = db_name2[mongo_name].find({"_id": 'ba6877dd75a63441b12c5743dc517b02'})
    for i in data_list:
        data.append(i)
    if len(data) == 0:
        print('There is no new data!')
        continue

    if mongo_name == 'api_csrc_RF_html':
        for i, line_de in enumerate(data):
            print(i)
            contents = ''
            table_list = []
            _id = line_de.get('_id')
            company = line_de.get('company')
            uid = add_uuid(company)
            ctime = line_de.get('ctime')
            ctimeArray = time.localtime(ctime)
            ctimeDate = time.strftime('%Y-%m-%d %H:%M:%S', ctimeArray)
            department = line_de.get('department')
            source_type = '再融资反馈'
            accessory_url = line_de.get('accessory_url')
            if accessory_url:
                file_type = accessory_url.split('.')[-1]
            else:
                file_type = 'html'
            issue_type = 'N'
            if_reply = 'N'
            reply_date = None
            url = line_de.get('url_wx')
            gtime = line_de.get('gtime')
            gtimeArray = time.localtime(gtime)
            gtimeDate = time.strftime('%Y-%m-%d %H:%M:%S', gtimeArray)
            content_dict = line_de.get('content_wx')
            if 'api_msg' in line_de.keys() and line_de.get('api_msg'):
                if_reply = 'Y'
                reply_date = ast.literal_eval(line_de.get('api_msg'))['api_date']
            for key in content_dict.keys():
                dict_2 = {}  # 将拆分的段落存到dict在写进list
                dict_2['table'] = content_dict[key]
                dict_2['table_no'] = str(key).split('_')[0]
                table_list.append(dict_2)
            content_list = sorted(table_list, key=lambda x: int(x['table_no']))
            for content in content_list:
                contents += (content['table'] + '\n')
            _id_list.append(_id)
            company_list.append(company)
            uid_list.append(uid)
            ctime_date_list.append(ctimeDate)
            department_list.append(department)
            contents_list.append(contents)
            event_num_list.append('')
            project_name_list.append('')
            source_type_list.append(source_type)
            file_type_list.append(file_type)
            issue_type_list.append(issue_type)
            if_reply_list.append(if_reply)
            reply_date_list.append(reply_date)
            url_list.append(url)
            data_source_list.append(department)
            gtime_date_list.append(gtimeDate)

    if mongo_name == 'api_csrc_mer_rec_html':
        for i, line_de in enumerate(data):
            print(i)
            contents = ''
            _id = line_de.get('_id')
            company = line_de.get('company')
            uid = add_uuid(company)
            ctime = line_de.get('ctime')
            ctimeArray = time.localtime(ctime)
            ctimeDate = time.strftime('%Y-%m-%d %H:%M:%S', ctimeArray)
            department = line_de.get('department')
            source_type = '并购重组反馈'
            accessory_url = line_de.get('accessory_url')
            if accessory_url:
                file_type = accessory_url.split('.')[-1]
            else:
                file_type = 'html'
            issue_type = 'Y'
            if_reply = 'N'
            reply_date = None
            url = line_de.get('url_wx')
            gtime = line_de.get('gtime')
            gtimeArray = time.localtime(gtime)
            gtimeDate = time.strftime('%Y-%m-%d %H:%M:%S', gtimeArray)
            content_dict = line_de.get('content_wx')
            if 'api_msg' in line_de.keys() and line_de.get('api_msg'):
                if_reply = 'Y'
                reply_date = ast.literal_eval(line_de.get('api_msg'))['api_date']
            for k, v in content_dict.items():
                contents += v + '\n'
            _id_list.append(_id)
            company_list.append(company)
            uid_list.append(uid)
            ctime_date_list.append(ctimeDate)
            department_list.append(department)
            contents_list.append(contents)
            event_num_list.append('')
            project_name_list.append('')
            source_type_list.append(source_type)
            file_type_list.append(file_type)
            issue_type_list.append(issue_type)
            if_reply_list.append(if_reply)
            reply_date_list.append(reply_date)
            url_list.append(url)
            data_source_list.append(department)
            gtime_date_list.append(gtimeDate)

    if mongo_name == 'api_shzq_wxhj_html':
        for i, line_de in enumerate(data):
            print(i)
            contents = ''
            company = ''
            _id = line_de.get('_id')
            # 提取公司简称
            gsjc = line_de.get('gsjc')
            gs = gsjc.replace(' ','')
            gsdm = line_de.get('gsdm')
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
            uid = add_uuid(company)
            ctime = line_de.get('ctime')
            ctimeArray = time.localtime(ctime)
            ctimeDate = time.strftime('%Y-%m-%d %H:%M:%S', ctimeArray)
            department = '上交所'
            source_type = '交易所问询答复'
            issue_type = 'N'
            if_reply = 'N'
            reply_date = None
            url = line_de.get('title_href')
            file_type = url.split('.')[-1]
            gtime = line_de.get('gtime')
            gtimeArray = time.localtime(gtime)
            gtimeDate = time.strftime('%Y-%m-%d %H:%M:%S', gtimeArray)
            if 'api_msg' in line_de.keys() and line_de.get('api_msg'):
                if_reply = 'Y'
                reply_date = line_de.get('api_msg')['api_date']
            contents = line_de.get('file_content')
            _id_list.append(_id)
            company_list.append(company)
            uid_list.append(uid)
            ctime_date_list.append(ctimeDate)
            department_list.append(department)
            contents_list.append(contents)
            event_num_list.append('')
            project_name_list.append('')
            source_type_list.append(source_type)
            file_type_list.append(file_type)
            issue_type_list.append(issue_type)
            if_reply_list.append(if_reply)
            reply_date_list.append(reply_date)
            url_list.append(url)
            data_source_list.append(department)
            gtime_date_list.append(gtimeDate)


    if mongo_name == 'api_szzq_wxhj_html':
        for i, line_de in enumerate(data):
            print(i)
            contents = ''
            company = ''
            _id = line_de.get('_id')
            # 提取公司简称
            gsjc = line_de.get('gsjc')
            gs = gsjc.replace(' ','')
            gsdm = line_de.get('gsdm')
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
            uid = add_uuid(company)
            ctime = line_de.get('ctime')
            ctimeArray = time.localtime(ctime)
            ctimeDate = time.strftime('%Y-%m-%d %H:%M:%S', ctimeArray)
            department = '深交所'
            source_type = '交易所问询答复'
            issue_type = 'N'
            if_reply = 'N'
            reply_date = None
            url = line_de.get('hjnr_content')['url']
            file_type = url.split('.')[-1]
            gtime = line_de.get('gtime')
            gtimeArray = time.localtime(gtime)
            gtimeDate = time.strftime('%Y-%m-%d %H:%M:%S', gtimeArray)
            content_dict = line_de.get('content_wx')
            if 'api_msg' in line_de.keys() and line_de.get('api_msg'):
                if_reply = 'Y'
                reply_date = line_de.get('api_msg')['api_date']
            else:
                if 'gshf_content' in line_de.keys() and line_de.get('gshf_content'):
                    if_reply = 'Y'
            contents = line_de.get('hjnr_content')['file_content']
            if company == '':
                tmp_sec = contents[0:100]
                pos1 = tmp_sec.find('关于对')
                pos2 = tmp_sec.find('的', pos1)
                if pos1 != -1 and pos2 != -1:
                    company = tmp_sec[pos1 + 3:pos2]
            _id_list.append(_id)
            if reply_date and ctimeDate > reply_date:
                _id_list2.append(_id)
            company_list.append(company)
            uid_list.append(uid)
            ctime_date_list.append(ctimeDate)
            department_list.append(department)
            contents_list.append(contents)
            event_num_list.append('')
            project_name_list.append('')
            source_type_list.append(source_type)
            file_type_list.append(file_type)
            issue_type_list.append(issue_type)
            if_reply_list.append(if_reply)
            reply_date_list.append(reply_date)
            url_list.append(url)
            data_source_list.append(department)
            gtime_date_list.append(gtimeDate)


# 取公司的工商数据
for company in company_list:
    company = company.replace('(', '（').replace(')', '）')
    select_sql = '''SELECT * from cms_company_info where company = "{}" '''.format(company)
    cursor.execute(select_sql)
    infos = cursor.fetchone()
    if infos:
        scn_list.append(infos[2])
        org_num_list.append(infos[3])
        reg_num_list.append(infos[4])
        tax_reg_num_list.append(infos[5])
        institution_num_list.append(infos[6])
        social_org_num_list.append(infos[7])
    else:
        scn_list.append(None)
        org_num_list.append(None)
        reg_num_list.append(None)
        tax_reg_num_list.append(None)
        institution_num_list.append(None)
        social_org_num_list.append(None)


mysql_conn = conn_mysql()
cur = mysql_conn.cursor()
try:
    insert_sql = '''insert into sy_cms_ywwx(company,uid,scn,orgNum,regNum,taxRegNum,institutionNum,socialOrgNum,ctime,department,content,eventNum,projectName,sourceType,fileType,issueType,ifReply,replyDate,url,dataSource,gtime)
                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    for i in range(len(_id_list)):
        if company_list[i] == '':
            continue
        if if_reply_list[i] == 'Y':
            select_sql2 = '''SELECT count(*) from sy_cms_ywwx where company = "{}" and url = "{}" and ifReply = "{}" '''.format(company_list[i], url_list[i], 'N')
            cur.execute(select_sql2)
            infos2 = cur.fetchone()
            if infos2 and infos2[0] == 1:
                delete_sql = '''delete from sy_cms_ywwx where company = "{}" and url = "{}" and ifReply = "{}" '''.format(company_list[i], url_list[i], 'N')
                cur.execute(delete_sql)
                mysql_conn.commit()
        if if_reply_list[i] == 'N':
            select_sql3 = '''SELECT count(*) from sy_cms_ywwx where company = "{}" and url = "{}" and ifReply = "{}" '''.format(company_list[i], url_list[i], 'Y')
            cur.execute(select_sql3)
            infos3 = cur.fetchone()
            if infos3 and infos3[0] >= 1:
                continue
        if department_list[i] == '深交所' and reply_date_list[i]:
            select_sql4 = '''SELECT count(*) from sy_cms_ywwx where company = "{}" and url = "{}" and ifReply = "{}" '''.format(company_list[i], url_list[i], 'Y')
            cur.execute(select_sql4)
            infos4 = cur.fetchone()
            if infos4 and infos4[0] == 1:
                delete_sql2 = '''delete from sy_cms_ywwx where company = "{}" and url = "{}" and ifReply = "{}" '''.format(company_list[i], url_list[i], 'Y')
                cur.execute(delete_sql2)
                mysql_conn.commit()
        if department_list[i] == '深交所' and not reply_date_list[i]:
            select_sql5 = '''SELECT count(*) from sy_cms_ywwx where company = "{}" and url = "{}" and ifReply = "{}" and replyDate is not Null '''.format(company_list[i], url_list[i], 'Y')
            cur.execute(select_sql5)
            infos5 = cur.fetchone()
            if infos5 and infos5[0] >= 1:
                continue
        cur.execute(insert_sql,(company_list[i],
                                uid_list[i],
                                scn_list[i],
                                org_num_list[i],
                                reg_num_list[i],
                                tax_reg_num_list[i],
                                institution_num_list[i],
                                social_org_num_list[i],
                                ctime_date_list[i],
                                department_list[i],
                                contents_list[i],
                                event_num_list[i],
                                project_name_list[i],
                                source_type_list[i],
                                file_type_list[i],
                                issue_type_list[i],
                                if_reply_list[i],
                                reply_date_list[i],
                                url_list[i],
                                data_source_list[i],
                                gtime_date_list[i],
                                ))
        mysql_conn.commit()
except Exception as e:
    print(e)
finally:
    mysql_conn.close()


# 导出公司名称
# company_list2 = []
# for company in company_list:
#     company += '\n'
#     company_list2.append(company)
# with open('data.txt','w', encoding='utf-8') as f:
#     f.writelines(company_list2)

# 导出问函时间大于回复时间的mongo_id
# _id_list3 = []
# for sid in _id_list2:
#     sid += '\n'
#     _id_list3.append(sid)
# with open('data2.txt','w', encoding='utf-8') as f:
#     f.writelines(_id_list3)

db.close()