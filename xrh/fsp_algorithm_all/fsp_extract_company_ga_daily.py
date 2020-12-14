# -*-coding:utf-8-*-

'''
文件名： fsp_extract_company_third_all.py
功能： 提取中信建投项目第三批数据（全量）中受罚公司、被罚金额、处罚类型等信息

代码历史：
2019-10-31：郭蓟申 创建代码
'''

import pymysql
import pymongo
import datetime
import os
import re
import json
import time
import sys
import decimal
import xlrd

sys.path.append("/home/seeyii/zhangcongcong/package/num_extract")
import num_extract

sys.path.append("/home/seeyii/algorithm_project/fsp_algorithm_all/")
import han_2_num
from fsp_extract_helper import *
from DBUtils.PooledDB import PooledDB

# 总表名
# sy_csc_fsp_department_daily 正式表
# sy_csc_fsp_department_all_tmp_xrh 实验表
final_mysql_name = 'sy_csc_fsp_department_daily'

# mongo上的爬取数据
mongo_name = 'fsp_department_ga_raw'
# mongo_name_xyzg = 'fsp_department_xyzg_raw'

# 写日志的表名
mysql_log_name = 'sy_csc_data_log'

# all:跑全量; add:跑增量
# add_type_choice = 'all'
add_type_choice = 'add'

# 是否清空表
clean_flag = False

# 是否取撞库数据
get_stakeholderName = True

# 是否记录最新gtime到日志表
logging = True

# 读取连接数据库配置文件
load_config = '/home/seeyii/increase_nlp/db_config.json'
with open(load_config, 'r', encoding="utf-8") as f:
    reader = f.readlines()[0]
config_local = json.loads(reader)


def add_company_name_tmp(c_name):
    '''
    过滤并添加直接被处罚公司名称  ###########################可更改###########################
    '''
    # print('c_name',c_name)
    if len(c_name) > 3 and '“' not in c_name and '谈话' not in c_name and '协会' not in c_name and \
            '人民银行' not in c_name and '姓名' not in c_name and '法定' not in c_name and '审计局' not in c_name and \
            '号' not in c_name and '、' not in c_name and '公开表' not in c_name and '相关责任' not in c_name and \
            '以下' not in c_name and '成立' not in c_name and '曾用名' not in c_name and '处分' not in c_name and \
            '更名' not in c_name and '代表人' not in c_name and '代码' not in c_name and '主营业务' not in c_name and \
            '财务顾问' not in c_name and '控制人' not in c_name and '涉案' not in c_name and '推荐' not in c_name and \
            '〔' not in c_name and '〕' not in c_name and '”' not in c_name and ',' not in c_name and '冒名' not in c_name and \
            '买卖' not in c_name and '规定' not in c_name and '我局' not in c_name and '政府' not in c_name and \
            '国资' not in c_name and '自然人' not in c_name and '其他' not in c_name and '公众' not in c_name and \
            '性别' not in c_name and '公积金' not in c_name and '客户' not in c_name and '处罚' not in c_name and \
            '监罚' not in c_name and '问题' not in c_name and '政府' not in c_name and '财政' not in c_name and \
            '违法' not in c_name and '汇总表' not in c_name and '手续' not in c_name and '必填' not in c_name and \
            '发展和改革委员会' not in c_name and '水利局' not in c_name and '管委会' not in c_name and \
            '市委员会' not in c_name and '分局' not in c_name and '申报' != c_name[-2:] and '公示截止期' not in c_name and \
            '商户' != c_name[-2:] and '日期' != c_name[-2:] and '名称' != c_name[-2:] and '管理' != c_name[-2:] and \
            '注销' != c_name[-2:] and '时间' != c_name[-2:] and '正常户' not in c_name and '工商户' not in c_name and \
            '管理局' != c_name[-3:] and '有限' != c_name[-2:] and '公安局' not in c_name and '统计局' not in c_name and \
            '航空局' != c_name[-2:] and '花生油' != c_name[-3:] and '橄榄油' != c_name[-3:] and '葵花籽油' != c_name[-4:] and \
            '写字楼' != c_name[-3:] and '运动项目' not in c_name and '法院' != c_name[-2:] and '社区' != c_name[:2] and \
            '接纳未成年人进入营业场所' not in c_name and '产权局' not in c_name and '执法' not in c_name and \
            '扫黄办' not in c_name and '旅体局' != c_name[-3:] and '体育局' != c_name[-3:] and '旅游局' != c_name[-3:] and \
            '人民法' not in c_name and '汽车站' not in c_name and '中' != c_name[-1] and '水产局' not in c_name and \
            (checkword(c_name) or checkword_en(c_name)) and '公示截止期' not in c_name and '亏损' not in c_name and \
            '酒驾' not in c_name and '个人' not in c_name:
        # print('直接关系名称',c_name)
        c_name = c_name.replace(' ', '').replace(':', '').replace('。', '').strip(',').strip('Y') \
            .strip('企业自然人名称').strip('对原')
        if c_name not in company_name_tmp:
            company_name_tmp.append(c_name)
            return True
    return False


def add_person_company_tmp(c_name):
    '''
    过滤并添加被处罚人所在公司名称  ###########################可更改###########################
    '''
    # print('pc_name',pc_name)
    # print('c_name',c_name)
    if len(c_name) > 3 and '“' not in c_name and '谈话' not in c_name and '协会' not in c_name and \
            '人民银行' not in c_name and '姓名' not in c_name and '法定' not in c_name and \
            '号' not in c_name and '、' not in c_name and '公开表' not in c_name and '相关责任' not in c_name and \
            '以下' not in c_name and '成立' not in c_name and '曾用名' not in c_name and '处分' not in c_name and \
            '更名' not in c_name and '代表人' not in c_name and '代码' not in c_name and '主营业务' not in c_name and \
            '财务顾问' not in c_name and '控制人' not in c_name and '涉案' not in c_name and '推荐' not in c_name and \
            '〔' not in c_name and '〕' not in c_name and '”' not in c_name and ',' not in c_name and '冒名' not in c_name and \
            '买卖' not in c_name and '规定' not in c_name and '我局' not in c_name and '政府' not in c_name and \
            '国资' not in c_name and '自然人' not in c_name and '其他' not in c_name and '公众' not in c_name and \
            '性别' not in c_name and '公积金' not in c_name and '客户' not in c_name and '处罚' not in c_name and \
            '监罚' not in c_name and '问题' not in c_name and '政府' not in c_name and '财政' not in c_name and \
            '违法' not in c_name and '汇总表' not in c_name and '手续' not in c_name and '必填' not in c_name and \
            '发展和改革委员会' not in c_name and '水利局' not in c_name and '管委会' not in c_name and \
            '市委员会' not in c_name and '分局' not in c_name and '申报' != c_name[-2:] and '公示截止期' not in c_name and \
            '商户' != c_name[-2:] and '日期' != c_name[-2:] and '名称' != c_name[-2:] and '管理' != c_name[-2:] and \
            '注销' != c_name[-2:] and '时间' != c_name[-2:] and '正常户' not in c_name and '工商户' not in c_name and \
            '管理局' != c_name[-3:] and '有限' != c_name[-2:] and '公安局' not in c_name and \
            '航空局' != c_name[-2:] and '花生油' != c_name[-3:] and '橄榄油' != c_name[-3:] and '葵花籽油' != c_name[-4:] and \
            '写字楼' != c_name[-3:] and '运动项目' not in c_name and '法院' != c_name[-2:] and \
            '接纳未成年人进入营业场所' not in c_name and '产权局' not in c_name and '执法' not in c_name and \
            '扫黄办' not in c_name and '旅体局' != c_name[-3:] and '体育局' != c_name[-3:] and '旅游局' != c_name[-3:] and \
            '人民法' not in c_name and '汽车站' not in c_name and '中' != c_name[-1] and \
            (checkword(c_name) or checkword_en(c_name)) and '公示截止期' not in c_name and '亏损' not in c_name and \
            '酒驾' not in c_name and '个人' not in c_name:
        # print('直接关系名称',c_name)
        c_name = c_name.replace(' ', '').replace('。', '').strip(',').strip('Y').strip('企业自然人名称').strip('对原')
        if c_name not in person_company_tmp and c_name not in company_name_tmp:
            person_company_tmp.append(c_name)



# 从Mongo数据库中的爬取数据中获得当前最新数据的信息
# mongocli = pymongo.MongoClient('mongodb://seeyii:shiye@127.0.0.1:27017/admin')
try:

    mongocli = pymongo.MongoClient(config_local['cluster_mongo'])
    dbname = mongocli['ei_bdp_raw']
    # 当前最新数据的gtime
    last_gtime = dbname[mongo_name].find_one({"gtime": {"$exists": True}}, sort=[("gtime", -1)])["gtime"]
    print('last_gtime当前最新爬取时间:', last_gtime, type(last_gtime))

    # 连接mysql
    # db = pymysql.connect(**config_local['local_sql_csc'])
    # cursor = db.cursor()


    pool = PooledDB(creator=pymysql, **config_local['local_sql_csc_pool'])
    db = pool.connection()
    cursor = db.cursor()

    db1 = pymysql.connect(**config_local['local_sql'])
    cursor1 = db1.cursor()

    # 取log中记录的上次数据的时间戳
    max_gtime = 0
    if add_type_choice == 'add':
        max_gtime = get_max_gtime(db,
                                  mongo_name,
                                  mysql_log_name)
    print('max_gtime上次爬取截止时间:', max_gtime)

    # max_gtime = 1559664000
    # 获取30天前的时间戳，用来过滤历史数据
    # db30_time = datetime.datetime.now().date() + datetime.timedelta(days=-30)
    # c30_time = int(time.mktime(db30_time.timetuple()))
    # print('ctime30过滤时间',c30_time)


    # 获取增量数据或全量数据
    add_type = {'all': 0, 'add': max_gtime}
    # ===========================================
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]},"ctime": {"$gt": c30_time}})
    # data_list = dbname[mongo_name].find({'ctime': 1559491200})
    # data_list = dbname[mongo_name].find({'ctime': {'$gt': 1559491200}})
    # data_list = dbname[mongo_name].find({'gtime': {'$gte': 1559664000}})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}, 'branch_tree.0':'信用中国'})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": 1566403200}, 'branch_tree.0':'信用中国'})

    # 信用中国数据量巨大，未加入合并表，从mongo_name_xyzg中单独取数据
    data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}})

    # data_list = dbname[mongo_name].find({"gtime": {"$gt":1565539200}, 'branch_tree.0':'信用中国', 'article_info.类型':'联合惩戒'}).limit(200)
    # data_list = dbname[mongo_name].find({"gtime": {"$gt":1565625600}, 'branch_tree.0':'信用中国', 'article_info.类型':'行政处罚'}).limit(500)

    # 其余部委从合并表mongo_name中取数据
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]},"ctime": {"$gt": 1325347200}, 'branch_tree.0':'中华人民共和国海关总署'}).limit(100)

    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}, 'branch_tree.0':'中华人民共和国商务部'})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": 0}, 'branch_tree.0':'中华人民共和国商务部'})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}, 'branch_tree.0':'国家能源局'})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}, 'branch_tree.0':'国家铁路局'})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}, 'branch_tree.0':'中华人民共和国交通运输部'})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}, 'branch_tree.0':'中华人民共和国国家邮政局'})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}, 'branch_tree.0':'中华人民共和国公安部'})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}, 'branch_tree.0':'国家外汇管理局'})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}, 'branch_tree.0':'中国民用航空局'})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}, 'branch_tree.0':'国家知识产权局'})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}, 'branch_tree.0':'证券期货市场'})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}, 'branch_tree.0':'国家市场监督管理总局'})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}, 'branch_tree.0':'国家广播电视总局'})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}, 'branch_tree.0':'国家体育总局'})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}, 'branch_tree.0':'中华人民共和国文化和旅游部'})
    # data_list = dbname[mongo_name].find({"gtime": {"$gt": add_type[add_type_choice]}}).limit(100).skip(0)efad0ae4e2203ac29398eda48d052cd4
    # data_list = dbname[mongo_name].find({"title":'2017年第一批严重违法超限超载运输失信当事人名单'})
    # data_list = dbname[mongo_name].find({"uid":'3cac8975789e3a13ba25cda52b0b6180'})
    # data_list = dbname[mongo_name_xyzg].find({"url": 'http://www.xyyc.org.cn/xygs/hmd/2019/06/13095236011.html'})

    # 取单条数据用于调试
    # data_list = dbname[mongo_name].find({"uid":''})
    # data_list = dbname[mongo_name_xyzg].find({"uid":''})

    # 获取2天前的时间戳，用来过滤历史数据
    # db2_time = datetime.datetime.now().date() + datetime.timedelta(days=-2)
    # c2_time = convert_real_time(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(int(time.mktime(db2_time.timetuple())))))
    # print('ctime2过滤时间',c2_time)

    data = []
    for i in data_list:
        # if i.get('branch_tree')['0'] in ['信用中国','中华人民共和国交通运输部','国家市场监督管理总局'] and\
        # convert_num_time(i.get('ctime')) < c2_time:
        #     # print('两天以上的历史数据跳过')
        #     continue

        if i.get('branch_tree')['0'] in ['信用中国', '中华人民共和国海关总署', '中华人民共和国商务部',
                                         '国家能源局', '国家铁路局', '中华人民共和国交通运输部',
                                         '中华人民共和国国家邮政局', '中华人民共和国公安部',
                                         '国家外汇管理局', '中国民用航空局', '国家知识产权局',
                                         '证券期货市场', '国家广播电视总局', '国家体育总局',
                                         '国家市场监督管理总局', '中华人民共和国文化和旅游部',
                                         '国家广播电视总局']:
            # print(i.get('gtime'))
            data.append(i)

    # 如果没有增量数据则中止程序
    if len(data) == 0:
        print('There is no new data!')
        os._exit(0)
    add_data_num = len(data)
    print('add_data_num:', add_data_num)

    # 创建公司全称和简称列表
    quancheng_list = []
    jiancheng_list = []
    quancheng_list, jiancheng_list = get_quancheng_jiancheng(db1)
    print('quancheng_list:', len(quancheng_list))
    print('jiancheng_list:', len(jiancheng_list))

    # 创建目标公司名称列表，用于撞库匹配
    all_company_list = []
    if get_stakeholderName:
        all_company_list = get_stakeholder_name(db)
    print('all_company_list:', len(all_company_list))

    # 开始提取 ##########################################################################

    # 关键词列表
    obj0 = ['支行', '联社']

    obj1 = ['个人姓名']
    obj2 = ['单位', '所属', '所在', '案由', '主要违法']

    # 找单位名称
    obj5 = ['单位名称', '位名称：']
    obj6 = ['法定代表人', '主要负责人', '主要违法', '地址']

    # 找当事人单位名称
    obj7 = ['当事人任', '人在担任', '(案由)', '当事人对', '事人担任']
    obj8 = ['监事长', '期间', '副总经理', '经理', '零售信贷部', '授信管理部', '风险管理部', '营业部',
            '办公室', '副行长', '行长', '主任', '个人金融部', '贷款资金', '违规', '滚动签发', '转让',
            '个人消费', '董事', '员工', '未能通过', '案由)任', '严重违反', '理事长', '副主任',
            '原行长', '股东', '监事', ',', '借冒名']

    obj9 = ['银行', '分行', '公司', '联社', '支行', '厂', '中心', '部']

    obj12 = ['公司', '中心', '务所']

    obj13 = ['证券', '营业部', '公司', '事务所', '纺织', '电力', '股份', '电器', '期货',
             '控股', '化纤', '科技', '电信', '商社', '集团', '财经', '房产', '药业',
             'Ltd.', 'Limited']

    obj14 = ['住所', '公司注册地址', '法定代表人', '地址', '经查', '营业场所', '住址', '工作单位地址', '主要负责人']

    obj15 = ['(以', ',以下简称', '(原', '(现', ',(以', '(特殊', '(有限合伙', ',为推荐', '(简称“']

    obj16 = ['公司', '事务所', '合作联社', '分行']

    obj17 = ['公司', '分行', '支行', '联社', '中心', '合社', '商行', '赁站', '务所']

    # 生成分割文章关键词列表  ###########################可更改###########################
    split_key = []
    for i in range(5000):
        split_key.append('\t' + str(i + 1) + '\t')

    split_key_1 = []
    for i in range(5000):
        split_key_1.append('\n\n' + str(i + 1) + '\n')

    split_key_3 = []
    for i in range(5000):
        split_key_3.append('\n\n\t' + str(i + 1) + '\n')

    split_key_4 = []
    for i in range(5000):
        split_key_4.append(' \n\n' + str(i + 1) + ' ')
        # split_key_4.append('\r\n' + str(i) + ' ')

    split_key_5 = []
    for i in range(5000):
        split_key_5.append('\n' + str(i + 1) + '.0 ')

    split_key_6 = []
    for i in range(5000):
        split_key_6.append('\n\n' + str(i + 1) + ' ')

    split_key_7 = []
    for i in range(5000):
        split_key_7.append('\n' + str(i + 1) + '.0  ')

    split_key_8 = []
    for i in range(5000):
        split_key_8.append('\n' + str(i + 1))

    # 连接模型预测服务
    bclient3 = model3()
    # try:
    #     if bclient1.process_seq('') == []:
    #         print('测试服务1成功')
    # except:
    #     print('服务未启动')

    # bclient2 = model2()
    # try:
    #     if bclient2.process_seq('') == []:
    #         print('测试服务2成功')
    # except:
    #     print('服务未启动')

    # 初始化用于存放全部文章提取信息的列表
    person_company_list = []
    company_name_list = []
    attention_list = []
    etime_list = []
    punish_type_list = []
    event_num_list = []
    punish_detail_list = []
    penalty_amount_list = []
    involved_amount_list = []
    skip_flag_list = []
    end_time_list = []

    # 初始化用于存储单篇文章提取信息数据
    company_name_tmp = []  # 直接处罚
    person_company_tmp = []  # 间接处罚
    etime_tmp = []  # 事件发生时间
    punish_type_tmp = []  # 处罚类型
    punish_detail_tmp = []  # 处罚详情
    penalty_amount_tmp = []  # 处罚金额
    involved_amount_tmp = []  # 涉案金额
    event_num_tmp = []  # 事件号
    end_time_tmp = []  # 事件结束时间

    # 初始化存放大文件文章uid列表
    large_file_uid_list = []

    # 开始遍历每篇文章 ####################################################################
    for i, line_de in enumerate(data):
        if i % 100 == 0:
            print('序号', i)
        # print(i)
        # print(line_de)
        # print(i, line_de.get('uid'))

        # 初始化跳过撞库和模型提取标志，主要用于提取表格内容时
        skip_flag = False

        # 初始化每篇文章的提取信息内容
        company_name_tmp = []
        person_company_tmp = []
        etime_tmp = []
        end_time_tmp = []
        punish_type_tmp = []
        punish_detail_tmp = []
        penalty_amount_tmp = []
        involved_amount_tmp = []
        event_num_tmp = []
        attention_tmp = ''
        tmp_person = ''

        # 获取标题
        t = ''
        if line_de.get('title') != None:
            t = DBC2SBC(line_de.get('title')).replace(' ', '').replace('–', '') \
                .replace('\u3000', '').replace('\xa0', '').replace('-', '') \
                .replace('—', '').replace('.', '').replace('_', '').replace('/', '') \
                .replace(':', '').replace('\n', '').replace('\r', '').replace('】', '') \
                .replace('【', '')

        # 获取文章
        contents = [str(i) for i in line_de.get('contents')]

        # 针对table_list情况进行处理
        contents_table_list = []
        if len(contents) > 0 and 'table_list' == contents[0]:
            contents_table_list = contents[2:]
        print(len(contents_table_list))

        # 初始化分别用于规则和模型提取的str
        s = ''
        s_p = ''

        if '我委无招标投标违法违规行为行政处罚案件' in t or '公共服务事项清单' in s or '本月未发生行政处罚事项' in s:
            s = ''
        elif len(contents) > 0 and isinstance(contents[0], dict):
            print('dict!!!')
            s = ''
        elif len(contents) > 0 and '.pdf' not in contents[0] and \
                '.doc' not in contents[0] and '.docx' not in contents[0] and \
                'xls' not in contents[0] and 'xlsx' not in contents[0] and \
                '点击下载' not in contents[0] and \
                '附件' not in DBC2SBC(''.join(contents)).replace('\xa0', '').replace('\t', '')[:100] and \
                len(DBC2SBC(''.join(contents))) > 10:
            # print('s1')
            s = DBC2SBC(''.join(contents)).replace(' ', '').replace('–', '') \
                .replace('\u3000', '').replace('\xa0', '').replace('-', '') \
                .replace('—', '').replace('.', '').replace('_', '').replace('/', '') \
                .replace(':', '').replace('\n', '').replace('\r', '').replace('\t', '')
            s_p = DBC2SBC('。'.join(contents)).replace(' ', '').replace('–', '') \
                .replace('\u3000', '').replace('\xa0', '').replace('-', '') \
                .replace('—', '').replace('.', '').replace('_', '').replace('/', '') \
                .replace('\n', '').replace('\r', '').replace('有限。公司', '有限公司') \
                .replace('有。限公司', '有限公司').replace('有限公。司', '有限公司')
            # print(s,'\n')
        if len(line_de.get('accessory')) > 0:
            # print('s3')
            # print(range(len(line_de.get('accessory'))))
            # and 'file_content' in line_de.get('accessory')[0].keys()
            for num in range(len(line_de.get('accessory'))):
                # print(num)
                if 'file_content' in line_de.get('accessory')[num].keys():
                    s += DBC2SBC(line_de.get('accessory')[num]['file_content']).replace(' ', '').replace('–', '') \
                             .replace('\u3000', '').replace('\xa0', '').replace('-', '').replace('\n\t', '\t') \
                             .replace('—', '').replace('.', '').replace('_', '').replace('/', '') \
                             .replace(':', '').replace('\n', '').replace('\r', '') + '\n'
                    s_p += DBC2SBC(line_de.get('accessory')[num]['file_content']).replace('–', '') \
                               .replace('\u3000', '').replace('\xa0', '').replace('-', '').replace('\n\t', '\t') \
                               .replace('—', '').replace('.', '').replace('_', '').replace('/', '') \
                               .replace('\n', '').replace('\r', '') + '\n'
                    if len(contents) > 0 and (
                            'xls' in ''.join(contents) or 'doc' in ''.join(contents) or '名单附后' in ''.join(contents)):
                        s_p = '\t'.join(s_p.split())

        # mongo中如果已经有被处罚单位信息，或是一些非处罚单位类的信息，跳过撞库和模型提取，将skip_flag设置为True
        # 需根据各个部委的具体数据调整该部分

        # 针对标题为公司的数据进行处理  ###########################可更改###########################
        if '0' in line_de.get('branch_tree').keys() and line_de.get('branch_tree')['0'] == '信用中国' and len(t) > 4 and \
                ('公司' == t[-2:] or '厂' == t[-1] or '商号' == t[-2:] or '商行' == t[-2:] or '医院' == t[-2:] or '酒店' == t[-2:]):
            person_company_tmp = []
            company_name_tmp = [t]
            skip_flag = True

        if '0' in line_de.get('branch_tree').keys() and line_de.get('branch_tree')['0'] == '信用中国' and \
                (line_de.get('contents')[0] == 'table_list' or (
                        len(line_de.get('accessory')) > 0 and 'file_name' in line_de.get('accessory')[0].keys())):
            person_company_tmp = []
            company_name_tmp = []
            skip_flag = True

        if line_de.get('branch') in ['宿迁市公安局', '泰州市公安局', '汕尾市公安局', '洛阳市公安局'] and len(t) > 4:
            person_company_tmp = []
            company_name_tmp = [t]
            skip_flag = True

        if line_de.get('branch') in ['洛阳市公安局'] and len(t) <= 4:
            person_company_tmp = []
            company_name_tmp = ['针对个人处罚']
            skip_flag = True

        if line_de.get('branch') == '四平市公安局' and len(t) >= 4 and '对' in t and '行政处罚' in t:
            person_company_tmp = []
            company_name_tmp = [(t.split('对')[-1]).split('行政处罚')[0]]
            skip_flag = True

        if line_de.get('branch') == '鹤壁市公安局':
            if '行政相对人名称' in contents:
                p1 = contents.index('行政相对人名称')
                person_company_tmp = []
                company_name_tmp.append(contents[p1 + 1])
                skip_flag = True
        if line_de.get('branch') == '绵阳市公安局':
            if '行政相对人' in contents:
                tmp_penalty = '0.0'
                p1 = contents.index('行政相对人')
                p2 = contents.index('处罚文书号')
                p3 = contents.index('处罚类别2')
                p4 = contents.index('处罚名称')
                p5 = contents.index('处罚生效期')
                p6 = contents.index('处罚结果')
                if '计' in contents[p6 + 1] and '元' in contents[p6 + 1]:
                    tmp_penalty = contents[p6 + 1].split('计')[-1]
                    tmp_penalty = tmp_penalty.split('元')[0]
                company_name_tmp.append(contents[p1 + 1])
                event_num_tmp.append(contents[p2 + 1])
                punish_type_tmp.append(contents[p3 + 1])
                penalty_amount_tmp.append(tmp_penalty)
                involved_amount_tmp.append('0.0')
                punish_detail_tmp.append(contents[p4 + 1].replace('\n', '\t'))
                etime_tmp.append(contents[p5 + 1])

        if line_de.get('branch_tree.0') == '中华人民共和国公安部' and ('裁量权' in t or '流程图' in t):
            person_company_tmp = []
            company_name_tmp = []
            skip_flag = True
        # else:
        #     print(line_de.get('branch_tree')['0'],t,line_de.get('uid'))

        if '0' in line_de.get('branch_tree').keys() and line_de.get('branch_tree')['0'] == '中华人民共和国公安部' and \
                ('打架' in s or '盗窃' in s or '赌博' in s or '持刀' in s or '毒品' in s or '私自阻挡' in s or '吸食了一小包' in s or \
                 '机动车驾驶证' in s or '盗走' in s or '发生口角' in s or '发生撕扯' in s or '打伤' in s or '非法占为己有' in s or \
                 '散装汽油' in s or '赌资' in s or '殴打' in s or '寻衅滋事' in s or '非法侵入' in s or '砸损' in s or '买卖户口' in s or \
                 '砸坏' in s or '冰毒' in s or '无证车辆' in s or '吸毒' in s or '迫害' in s or '海洛因' in s or '多次干扰' in s or \
                 '蹬碎' in s or '拒不配合' in s or '醉酒' in s or '私自储存' in s or '擅自经营' in s or '欲伤害' in s or '身份证' in s or \
                 '行政拘留' in s or '非法居留' in s or '违法事实可能涉及敏感内容' in s or '醉酒后驾驶机动车' in s, '涉嫌非法入境' in s) and \
                line_de.get('branch_tree')['1'] != '江苏省公安厅' and \
                line_de.get('branch_tree')['1'] != '内蒙古自治区公安厅' and line_de.get('branch_tree')['1'] != '安徽省公安厅' and \
                line_de.get('branch_tree')['1'] != '湖北省公安厅' and line_de.get('branch_tree')['1'] != '广东省公安厅' and \
                line_de.get('branch_tree')['1'] != '河南省公安厅' and line_de.get('branch_tree')['1'] != '四川省公安厅':
            person_company_tmp = []
            company_name_tmp = ['针对个人处罚']
            skip_flag = True
        elif '2' in line_de.get('branch_tree').keys() and line_de.get('branch_tree')['2'] == '烟台市公安局' and (
                '违法行为人' in s or '违法单位' in s):
            if '违法行为人' in s:
                company_name = (s.split('违法行为人')[-1]).split(',')[0]
                if len(company_name) > 4:
                    person_company_tmp = []
                    company_name_tmp = [company_name]
                    skip_flag = True
                else:
                    person_company_tmp = []
                    company_name_tmp = ['针对个人处罚']
                    skip_flag = True
            elif '违法单位' in s:
                company_name = (s.split('违法单位')[-1]).split(',')[0]
                company_name = company_name.replace('：', '').replace(':', '')
                if len(company_name) > 4:
                    person_company_tmp = []
                    company_name_tmp = [company_name]
                    skip_flag = True
                else:
                    person_company_tmp = []
                    company_name_tmp = ['针对个人处罚']
                    skip_flag = True

        # 记录每篇文章的skip_flag状态
        print('skip_flag', skip_flag)
        skip_flag_list.append(skip_flag)

        # print(s_p)
        print('整理后文章长度', len(s_p))

        if len(s_p) < 5000000 and skip_flag == False:

            # 匹配公司库名称
            print('匹配公司库名称')
            for co in all_company_list:
                if co in t + s:
                    print('匹配公司库名称', co)
                    add_company_name_tmp(co)

            text = t + '。' + s_p
            # 模型1提取
            print('模型3提取')
            # print([text])
            if text != '':
                try:
                    result_list = bclient3.process_seq(text)
                    # print('模型',result_list)
                    if result_list != []:
                        for co in result_list:
                            if len(co) > 4 and '路' != co[-1:] and co not in company_name_tmp and co.replace('。',
                                                                                                            '') not in company_name_tmp:
                                print('模型提取', co, line_de.get('uid'))
                                add_company_name_tmp(co.replace('。', ''))
                except:
                    print('预测服务中断')
            else:
                print('数据为空')

            # # 模型2提取
            # print('模型2提取')
            # # print([text])
            # if text != '':
            #     try:
            #         result_list = bclient2.process_seq(text)
            #         # print('模型',result_list)
            #         if result_list != []:
            #             for co in result_list:
            #                 if len(co) > 4 and '路' != co[-1:] and co not in company_name_tmp and co.replace('。',
            #                                                                                                 '') not in company_name_tmp:
            #                     print('模型提取', co, line_de.get('uid'))
            #                     add_company_name_tmp(co.replace('。', ''))
            #     except:
            #         print('预测服务中断')
            # else:
            #     print('数据为空')

        if len(s_p) > 100000 or len(company_name_tmp) > 30:
            print('超出字数!!!!!!!!!!!', line_de.get('uid'), t)
            large_file_uid_list.append(line_de.get('uid'))

        # print([line_de.get('web_contents')[0]])
        # print(t, company_name_tmp)

        # 添加对各个部委的处理 ###########################需要更改###########################

        acce_content_xls = ''
        # 提取附件表格中的信息
        if len(line_de.get('accessory')) > 0 and 'file_name' in line_de.get('accessory')[0].keys() and \
                '.xls' in line_de.get('accessory')[0]['file_name']:
            print('xls')
            for num in range(len(line_de.get('accessory'))):
                # print(num)
                if 'file_content' in line_de.get('accessory')[num] and 'name' in line_de.get('accessory')[num] and \
                        '许可' not in line_de.get('accessory')[num]['name'] and '认可' not in line_de.get('accessory')[num][
                    'name'] and \
                        '个体工商户' not in line_de.get('accessory')[num]['name']:
                    acce_content_xls += DBC2SBC(
                        line_de.get('accessory')[num]['file_content']) + '**different_acce**'
            # print('acce_content_xls',[acce_content_xls[:1000]])

            if '\n' in acce_content_xls and '2' in line_de.get('branch_tree').keys() and line_de.get('branch_tree')[
                '2'] in ['广安市公安局', '江门市公安局']:
                s_row_split = acce_content_xls.split('\n')
            else:
                if '\n1' in acce_content_xls:
                    s_row_split = split_it(acce_content_xls, split_key_8)
                else:
                    s_row_split = ['']

            # print(s_row_split,len(s_row_split))

            # 提取附件表格中的信息
            company_name_tmp = []
            for s_row in s_row_split:
                s_row = s_row.replace('**different_acce**', '')
                # print([s_row],len(s_row))
                if '  ' in s_row and s_row.count('  ') > 3:
                    info_split = s_row.split('  ')
                    while '' in info_split:
                        info_split.remove('')
                    while '\n' in info_split:
                        info_split.remove('\n')
                    print([info_split], len(info_split), 'xls nt')

                    if line_de.get('branch') in ['广安市公安局'] and \
                            len(info_split) >= 12 and len(info_split[0]) > 4 and \
                            len(info_split[0]) < 50 and is_Chinese(info_split[0][:4]) and \
                            '行政相对人名称' not in info_split[0] and '必填项' not in info_split[0]:
                        skip_flag_list[i] = True

                        company_name_tmp.append(info_split[0])
                        punish_type_tmp.append('')
                        penalty_amount_tmp.append('0.0')
                        involved_amount_tmp.append('0.0')
                        punish_detail_tmp.append(s_row[:1000])
                        event_num_tmp.append(info_split[3])
                        etime_tmp.append('0001-01-01')
                    elif line_de.get('branch') in ['盐城市公安局'] and \
                            len(info_split) == 6 and len(info_split[1]) >= 4 and \
                            len(info_split[1]) < 50 and is_Chinese(info_split[1][:4]):
                        skip_flag_list[i] = True
                        tmp_penalty = '0.0'
                        company = info_split[1]
                        extract_term = num_extract.NumberExtract().dosegment_all(s_row, [company])
                        if company in extract_term.keys():
                            # print([s_row],[company])
                            if 'penaltyAmount' in extract_term[company].keys() and \
                                    extract_term[company]['penaltyAmount'] != '':
                                tmp_penalty = str(round(float(extract_term[company]['penaltyAmount']), 2))
                        print('tmp_penalty', tmp_penalty)
                        punish_type = ''
                        if '通报批评' in s_row and '通报批评' not in punish_type:
                            punish_type += ',通报批评'
                        if '书面警示' in s_row and '书面警示' not in punish_type:
                            punish_type += ',书面警示'
                        if '诫勉谈话' in s_row and '诫勉谈话' not in punish_type:
                            punish_type += ',诫勉谈话'
                        if '约见谈话' in s_row and '约见谈话' not in punish_type:
                            punish_type += ',约见谈话'
                        if '要求提交书面承诺' in s_row and '要求提交书面承诺' not in punish_type:
                            punish_type += ',要求提交书面承诺'
                        if '警示函' in s_row or '誉示函' in s_row:
                            punish_type += ',警示函'
                        if '公开谴责' in s_row and '公开谴责' not in punish_type:
                            punish_type += ',公开谴责'
                        if '罚款' in s_row.replace('以下的罚款', '') and '罚款' not in punish_type:
                            punish_type += ',罚款'
                        if '警告' in s_row and '警告' not in punish_type:
                            punish_type += ',警告'
                        if '没收' in s_row and '没收' not in punish_type:
                            punish_type += ',没收'
                        if '暂停执行业务' in s_row and '暂停执行业务' not in punish_type:
                            punish_type += ',暂停执行业务'
                        if '暂停经营' in s_row and '暂停经营' not in punish_type:
                            punish_type += ',暂停经营'
                        if '失信被执行人' in s_row and '失信被执行人' not in punish_type:
                            punish_type += ',失信被执行人'
                        if '严重违法失信企业' in s_row and '严重违法失信企业' not in punish_type:
                            punish_type += ',严重违法失信企业'
                        company_name_tmp.append(company)
                        punish_type_tmp.append(punish_type.strip(','))
                        penalty_amount_tmp.append(tmp_penalty)
                        involved_amount_tmp.append('0.0')
                        punish_detail_tmp.append(s_row[:1000])
                        event_num_tmp.append('')
                        if len(info_split[4]) == 8:
                            etime_tmp.append(info_split[4][0:4] + '-' + info_split[4][4:6] + '-' + info_split[4][6:8])
                        else:
                            etime_tmp.append('0001-01-01')
                    elif line_de.get('branch') in ['荆门市公安局'] and \
                            len(info_split) == 16 and len(info_split[1]) > 4 and \
                            len(info_split[1]) < 50 and is_Chinese(info_split[1][:4]):
                        skip_flag_list[i] = True
                        company_name_tmp.append(info_split[1])
                        punish_type_tmp.append('罚款')
                        penalty_amount_tmp.append('400.0')
                        involved_amount_tmp.append('0.0')
                        punish_detail_tmp.append(s_row[:1000])
                        event_num_tmp.append(info_split[6])
                        etime_tmp.append('2016-03-02')
                        end_time_tmp.append('2016-03-17')
                    elif line_de.get('branch') in ['江门市公安局'] and \
                            len(info_split) == 18 and len(info_split[0]) > 4 and \
                            len(info_split[0]) < 50 and is_Chinese(info_split[0][:4]):
                        skip_flag_list[i] = True
                        tmp_penalty = '0.0'
                        if info_split[10].count('.') == 1:
                            left, right = info_split[10].split(".")
                            if left.isdigit() and right.isdigit():
                                tmp_penalty = str(float(info_split[10]) * 10000)
                        company_name_tmp.append(info_split[0])
                        punish_type_tmp.append(info_split[8])
                        penalty_amount_tmp.append(tmp_penalty)
                        involved_amount_tmp.append('0.0')
                        punish_detail_tmp.append(s_row[:1000])
                        event_num_tmp.append('')
                        etime_tmp.append(info_split[11])
                    elif line_de.get('branch') in ['江门市公安局'] and \
                            len(info_split) == 20 and len(info_split[0]) > 4 and \
                            len(info_split[0]) < 50 and is_Chinese(info_split[0][:4]):
                        skip_flag_list[i] = True
                        tmp_penalty = '0.0'
                        if info_split[12].count('.') == 1:
                            left, right = info_split[12].split(".")
                            if left.isdigit() and right.isdigit():
                                tmp_penalty = str(float(info_split[12]) * 10000)
                        company_name_tmp.append(info_split[0])
                        punish_type_tmp.append(info_split[10])
                        penalty_amount_tmp.append(tmp_penalty)
                        involved_amount_tmp.append('0.0')
                        punish_detail_tmp.append(s_row[:1000])
                        event_num_tmp.append('')
                        etime_tmp.append(info_split[13])
                    elif line_de.get('branch') in ['江门市公安局'] and \
                            len(info_split) == 17 and len(info_split[0]) > 4 and \
                            len(info_split[0]) < 50 and is_Chinese(info_split[0][:4]):
                        skip_flag_list[i] = True
                        tmp_penalty = '0.0'
                        if info_split[9].count('.') == 1:
                            left, right = info_split[9].split(".")
                            if left.isdigit() and right.isdigit():
                                tmp_penalty = str(float(info_split[9]) * 10000)
                        company_name_tmp.append(info_split[0])
                        punish_type_tmp.append(info_split[7])
                        penalty_amount_tmp.append(tmp_penalty)
                        involved_amount_tmp.append('0.0')
                        punish_detail_tmp.append(s_row[:1000])
                        event_num_tmp.append('')
                        etime_tmp.append(info_split[10])
                    elif line_de.get('branch') in ['宁夏回族自治区公安厅'] and \
                            len(info_split) == 7 and len(info_split[1]) > 4 and \
                            len(info_split[1]) < 50 and is_Chinese(info_split[1][:4]):
                        skip_flag_list[i] = True
                        tmp_penalty = '0.0'
                        company = info_split[1]
                        extract_term = num_extract.NumberExtract().dosegment_all(s_row, [company])
                        if company in extract_term.keys():
                            # print([s_row],[company])
                            if 'penaltyAmount' in extract_term[company].keys() and \
                                    extract_term[company]['penaltyAmount'] != '':
                                tmp_penalty = str(round(float(extract_term[company]['penaltyAmount']), 2))
                        print('tmp_penalty', tmp_penalty)
                        punish_type = ''
                        if '通报批评' in s_row and '通报批评' not in punish_type:
                            punish_type += ',通报批评'
                        if '书面警示' in s_row and '书面警示' not in punish_type:
                            punish_type += ',书面警示'
                        if '诫勉谈话' in s_row and '诫勉谈话' not in punish_type:
                            punish_type += ',诫勉谈话'
                        if '约见谈话' in s_row and '约见谈话' not in punish_type:
                            punish_type += ',约见谈话'
                        if '要求提交书面承诺' in s_row and '要求提交书面承诺' not in punish_type:
                            punish_type += ',要求提交书面承诺'
                        if '警示函' in s_row or '誉示函' in s_row:
                            punish_type += ',警示函'
                        if '公开谴责' in s_row and '公开谴责' not in punish_type:
                            punish_type += ',公开谴责'
                        if '罚款' in s_row.replace('以下的罚款', '') and '罚款' not in punish_type:
                            punish_type += ',罚款'
                        if '警告' in s_row and '警告' not in punish_type:
                            punish_type += ',警告'
                        if '没收' in s_row and '没收' not in punish_type:
                            punish_type += ',没收'
                        if '暂停执行业务' in s_row and '暂停执行业务' not in punish_type:
                            punish_type += ',暂停执行业务'
                        if '暂停经营' in s_row and '暂停经营' not in punish_type:
                            punish_type += ',暂停经营'
                        if '失信被执行人' in s_row and '失信被执行人' not in punish_type:
                            punish_type += ',失信被执行人'
                        if '严重违法失信企业' in s_row and '严重违法失信企业' not in punish_type:
                            punish_type += ',严重违法失信企业'
                        company_name_tmp.append(company)
                        punish_type_tmp.append(punish_type.strip(','))
                        penalty_amount_tmp.append(tmp_penalty)
                        involved_amount_tmp.append('0.0')
                        punish_detail_tmp.append(s_row[:1000])
                        event_num_tmp.append('')
                        etime_tmp.append(info_split[4])

        # 信用中国 失信黑名单==================================================================
        if line_de.get('branch_tree')['0'] in ['中华人民共和国公安部']:
            # 针对数据特点进行处理

            if contents_table_list != []:
                print('网页内容为表格!!!!!!')
                # print(contents_table_list)
                company_name_tmp = []
                for row in contents_table_list:
                    row = row.strip('\t')
                    info_split = row.replace('\n', '').replace('\u3000', '').replace('\xa0', '').replace(' 号', '号').split(
                        '\t')
                    # while '' in info_split:
                    #     info_split.remove('')
                    # while '\xa0' in info_split:
                    #     info_split.remove('\xa0')

                    print([info_split], len(info_split))

                    if line_de.get('branch') in ['铜陵市公安局'] and \
                            len(info_split) >= 9 and len(info_split[1]) > 4 and \
                            len(info_split[1]) < 50 and is_Chinese(info_split[1][:4]) and info_split[1][-1] != '号' and \
                            info_split[0].isdigit():
                        skip_flag_list[i] = True
                        tmp_penalty = '0.0'
                        company = info_split[1]
                        extract_term = num_extract.NumberExtract().dosegment_all(row, [company])
                        if company in extract_term.keys():
                            # print([s_row],[company])
                            if 'penaltyAmount' in extract_term[company].keys() and \
                                    extract_term[company]['penaltyAmount'] != '':
                                tmp_penalty = str(round(float(extract_term[company]['penaltyAmount']), 2))
                        print('tmp_penalty', tmp_penalty)
                        punish_type = ''
                        if '通报批评' in row and '通报批评' not in punish_type:
                            punish_type += ',通报批评'
                        if '书面警示' in row and '书面警示' not in punish_type:
                            punish_type += ',书面警示'
                        if '诫勉谈话' in row and '诫勉谈话' not in punish_type:
                            punish_type += ',诫勉谈话'
                        if '约见谈话' in row and '约见谈话' not in punish_type:
                            punish_type += ',约见谈话'
                        if '要求提交书面承诺' in row and '要求提交书面承诺' not in punish_type:
                            punish_type += ',要求提交书面承诺'
                        if '警示函' in row or '誉示函' in row:
                            punish_type += ',警示函'
                        if '公开谴责' in row and '公开谴责' not in punish_type:
                            punish_type += ',公开谴责'
                        if '罚款' in row.replace('以下的罚款', '') and '罚款' not in punish_type:
                            punish_type += ',罚款'
                        if '警告' in row and '警告' not in punish_type:
                            punish_type += ',警告'
                        if '没收' in row and '没收' not in punish_type:
                            punish_type += ',没收'
                        if '暂停执行业务' in row and '暂停执行业务' not in punish_type:
                            punish_type += ',暂停执行业务'
                        if '暂停经营' in row and '暂停经营' not in punish_type:
                            punish_type += ',暂停经营'
                        if '失信被执行人' in row and '失信被执行人' not in punish_type:
                            punish_type += ',失信被执行人'
                        if '严重违法失信企业' in row and '严重违法失信企业' not in punish_type:
                            punish_type += ',严重违法失信企业'
                        company_name_tmp.append(info_split[1])
                        punish_type_tmp.append(punish_type.strip(','))
                        penalty_amount_tmp.append(tmp_penalty)
                        involved_amount_tmp.append('0.0')
                        punish_detail_tmp.append(row[:1000])
                        event_num_tmp.append(info_split[2])
                        etime_tmp.append(info_split[7])
                    if line_de.get('branch') in ['南充市公安局'] and \
                            len(info_split) == 21 and len(info_split[4]) > 4 and \
                            len(info_split[4]) < 50 and is_Chinese(info_split[4][:4]):
                        skip_flag_list[i] = True
                        punish_type = ''
                        if '整顿' in row and '整顿' not in punish_type:
                            punish_type += ',整顿'
                        company_name_tmp.append(info_split[4])
                        punish_type_tmp.append(punish_type.strip(','))
                        penalty_amount_tmp.append('0.0')
                        involved_amount_tmp.append('0.0')
                        punish_detail_tmp.append(row[:1000])
                        event_num_tmp.append('')
                        etime_tmp.append(info_split[18])
                    if line_de.get('branch') in ['南充市公安局'] and \
                            len(info_split) == 31 and len(info_split[11]) > 4 and \
                            len(info_split[11]) < 50 and is_Chinese(info_split[11][:4]):
                        skip_flag_list[i] = True
                        punish_type = ''
                        tmp_penalty = '0.0'
                        if '罚款' in info_split[21] and '元' in info_split[21]:
                            tmp_penalty = info_split[21].split('罚款')[-1]
                            tmp_penalty = tmp_penalty.split('元')[0]
                            if tmp_penalty.isdigit():
                                tmp_penalty = tmp_penalty
                        if '罚款' in row and '罚款' not in punish_type:
                            punish_type += ',罚款'
                        company_name_tmp.append(info_split[11])
                        punish_type_tmp.append(punish_type.strip(','))
                        penalty_amount_tmp.append(tmp_penalty)
                        involved_amount_tmp.append('0.0')
                        punish_detail_tmp.append(row[:1000])
                        event_num_tmp.append(info_split[0])
                        e_time = info_split[23]
                        if len(e_time) == 8 and '/' not in e_time and '.' not in e_time and '-' not in e_time:
                            e_time = e_time[0:4] + '-' + e_time[4:6] + '-' + e_time[6:8]
                        etime_tmp.append(e_time)
                    if line_de.get('branch') in ['宁夏回族自治区公安厅'] and \
                            len(info_split) == 21 and len(info_split[6]) > 4 and \
                            len(info_split[6]) < 50 and is_Chinese(info_split[6][:4]) and '：' in info_split[6]:
                        skip_flag_list[i] = True
                        punish_type = ''
                        tmp_penalty = '0.0'
                        event_num = ''
                        e_time = '0001-01-01'
                        end_time = '0001-01-01'
                        company = info_split[6].split('：')[-1]
                        extract_term = num_extract.NumberExtract().dosegment_all(row, [company])
                        if company in extract_term.keys():
                            # print([s_row],[company])
                            if 'penaltyAmount' in extract_term[company].keys() and \
                                    extract_term[company]['penaltyAmount'] != '':
                                tmp_penalty = str(round(float(extract_term[company]['penaltyAmount']), 2))
                        print('tmp_penalty', tmp_penalty)
                        if '罚款' in row and '罚款' not in punish_type:
                            punish_type += ',罚款'
                        if '行政处罚决定书文号：' in info_split[1]:
                            event_num = info_split[1].split('：')[-1]
                        if '处罚生效期：' in info_split[14]:
                            e_time = info_split[14].split('：')[-1]
                        if '处罚截止期：' in info_split[15]:
                            end_time = info_split[15].split('：')[-1]
                        company_name_tmp.append(company)
                        punish_type_tmp.append(punish_type.strip(','))
                        penalty_amount_tmp.append(tmp_penalty)
                        involved_amount_tmp.append('0.0')
                        punish_detail_tmp.append(row[:1000])
                        event_num_tmp.append(event_num)
                        etime_tmp.append(e_time)
                        end_time_tmp.append(end_time)

        # 针对原文为图片的文章进行处理
        if line_de.get('web_contents') != [] and '#image0#' == line_de.get('web_contents')[0] and \
                '此文包含图片无法解析' not in company_name_tmp and t not in company_name_tmp and company_name_tmp == [] and \
                person_company_tmp == []:
            if len(company_name_tmp) > 0 and company_name_tmp[0] not in t:
                # print('图片')
                company_name_tmp.append('此文包含图片无法解析')

        # print(company_name_tmp)
        if '此文包含图片无法解析' in company_name_tmp or company_name_tmp == []:
            attention_tmp = 'C'

        # 过滤行政许可类文章
        if '行政许可' in t:
            person_company_tmp = []
            company_name_tmp = []

        # 将每篇文章的提取信息添加到总列表
        attention_list.append(attention_tmp)
        person_company_list.append(person_company_tmp)
        # company_name_list.append(company_name_tmp)
        company_name_list.append(convert_2_quanjiao(company_name_tmp))
        etime_list.append(etime_tmp)
        end_time_list.append(end_time_tmp)
        punish_type_list.append(punish_type_tmp)
        punish_detail_list.append(punish_detail_tmp)
        penalty_amount_list.append(penalty_amount_tmp)
        involved_amount_list.append(involved_amount_tmp)
        event_num_list.append(event_num_tmp)

    # 匹配别名  ========================不用动==============================
    time_start = time.time()
    # load_config='/home/seeyii/increase_nlp/db_config.json'
    # with open(load_config, 'r', encoding="utf-8") as f:
    #     reader = f.readlines()[0]
    # local_config = json.loads(reader)
    # db2 = pymysql.connect(**local_config['remote_sql'])

    pool = PooledDB(creator=pymysql, **config_local['remote_sql_pool'])
    db2 = pool.connection()

    sqlalias = SySqlApi(db=db2)

    # 别名列表
    alias_dict_list = {}

    for num, cnl_split in enumerate(company_name_list):
        # print(cnl_split)
        if cnl_split != []:
            com_new = sqlalias.aliae_name(com_list=cnl_split)
            print('别名company_name_list')
            # print(com_new.keys())
            # print(com_new.values())
            # print(event_num_list[num])
            for k_num, key in enumerate(com_new.keys()):
                val = com_new[key]
                if val not in company_name_list[num]:
                    company_name_list[num].append(val)
                    alias_dict_list[val] = key
                    # 为别名添加案件号
                    if event_num_list[num] != []:
                        event_num_list[num].append(event_num_list[num][k_num])
                    # 为别名添加处罚详情
                    # print(key,val,len(punish_detail_list[num]),punish_detail_list[num])
                    if len(punish_detail_list[num]) > k_num:
                        punish_detail_list[num].append(punish_detail_list[num][k_num])
                    # 为别名添加处罚金额
                    if len(penalty_amount_list[num]) > k_num:
                        penalty_amount_list[num].append(penalty_amount_list[num][k_num])
                    else:
                        print('!!!!!!!!!!!!!处罚详情：', key, val)
        print(len(punish_detail_list[num]))

        # print(company_name_list[num])

    for num, cnl_split in enumerate(person_company_list):
        # print(cnl_split)
        if cnl_split != []:
            com_new = sqlalias.aliae_name(com_list=cnl_split)
            print('别名person_company_list')
            print(com_new.keys())
            print(com_new.values())
            for val in com_new.values():
                if val not in person_company_list[num]:
                    person_company_list[num].append(val)
        # print(person_company_list[num])
    db2.close()
    time_end = time.time()
    print('match_alias_totally_cost', time_end - time_start)

    # 统计提取数  ========================不用动==============================
    print('person_company_list')
    # print(len(person_company_list))
    print(none_empty_account(person_company_list))
    # print(person_company_list)

    print('company_name_list')
    # print(len(company_name_list))
    print(none_empty_account(company_name_list))
    # print(company_name_list)

    print('attention_list')
    # print(len(attention_list))
    print(none_empty_account(attention_list))
    # print(attention_list)

    print('etime_list')
    # print(len(etime_list))
    print(none_empty_account(etime_list))
    # print(etime_list)

    print('end_time_list')
    # print(len(end_time_list))
    print(none_empty_account(end_time_list))

    print('punish_type_list')
    # print(len(punish_type_list))
    print(none_empty_account(punish_type_list))
    # print(punish_type_list)

    print('punish_detail_list')
    # print(len(punish_detail_list))
    print(none_empty_account(punish_detail_list))
    # print(punish_detail_list)

    print('penalty_amount_list')
    # print(len(penalty_amount_list))
    print(none_empty_account(penalty_amount_list))
    # print(penalty_amount_list)

    print('involved_amount_list')
    # print(len(involved_amount_list))
    print(none_empty_account(involved_amount_list))
    # print(involved_amount_list)

    print('event_num_list')
    # print(len(event_num_list))
    print(none_empty_account(event_num_list))
    # print(len(event_num_list))
    # print(event_num_list)


    # 根据标记清空目标表
    # if clean_flag:
    #     clean_table(db, final_mysql_name)

    # 去重列表(按title和ctimeDate)
    no_dupli = []

    # 遍历结果列表并将数据写入数据库
    for i, item in enumerate(data):
        print('uid', item['uid'])

        # 如果文章没有直接或间接处罚的公司,则会单独处理
        black_flag = True
        # print(i)

        # 文章内容
        contents_set = ''
        contents_set = DBC2SBC(''.join(item['contents'])).replace('\n', '').replace(' ', '').replace('徫', '律')
        if len(item['accessory']) > 0:
            for acc in item['accessory']:
                if 'file_content' in acc.keys() and 'name' in acc.keys() and \
                        type(acc['name']) is str and '许可' not in acc['name'] and '征求意见稿' not in acc['name'] and '认可' not in \
                        acc['name']:
                    contents_set += DBC2SBC(acc['file_content']).replace('\n', '').replace(' ', '').replace('徫', '律')
        # print([contents_set])

        # 标题内容
        title = ''
        if isinstance(item['title'], str):
            title = DBC2SBC(item['title']).replace('企业/自然人名称:', '').replace(' ', '').replace('–', '') \
                .replace('\u3000', '').replace('\xa0', '').replace('-', '') \
                .replace('—', '').replace('.', '').replace('_', '').replace('/', '') \
                .replace(':', '').replace('\n', '').replace('\r', '')

        # 初始化分支机构信息
        branch0 = ''
        branch1 = ''
        branch2 = ''
        branch3 = ''
        if '0' in item['branch_tree'].keys():
            branch0 = item['branch_tree']['0']
        if '1' in item['branch_tree'].keys():
            branch1 = item['branch_tree']['1']
        if '2' in item['branch_tree'].keys():
            branch2 = item['branch_tree']['2']
        if '3' in item['branch_tree'].keys():
            branch3 = item['branch_tree']['3']

        branch_tree = (branch0 + '_' + branch1 + '_' + branch2 + '_' + branch3).strip('_')

        # eventType初始化  ========================不用动==============================
        eventType = '各部委监管处罚'
        # 针对信用中国不同类型给eventType赋值
        if item.get('branch_tree')['0'] == '信用中国' and item.get('article_info')['类型'] == '失信黑名单':
            eventType = '失信黑名单'
        elif item.get('branch_tree')['0'] == '信用中国' and item.get('article_info')['类型'] == '联合惩戒':
            eventType = '信用惩戒'
        elif item.get('branch_tree')['0'] == '信用中国' and item.get('article_info')['类型'] == '行政处罚':
            eventType = '信用中国公示处罚'
        elif item.get('branch_tree')['0'] == '证券期货市场':
            eventType = '证券市场失信主体'

        ###########################可更改###########################
        # 初始化reviewed '':默认，'1':人工修改过的已复核数据，'0':没问题的已复核数据，'22'：删除
        reviewed = ''
        # 根据实际情况来将无效数据标记删除或自动复核
        if eventType == '失信黑名单' and title != '查看' and ('超限运输' in title or '堵塞交通' in title or \
                                                       '统计局' in title or '图片解读' in title or '酒驾' in title or '个人' in title or '无证行医' in title):
            reviewed = '22'
        elif item.get('branch') == '宿迁市公安局' and len(title) < 4:
            reviewed = '22'

        if company_name_list[i] == ['针对个人处罚']:
            reviewed = '22'

        # if item['uid'] in large_file_uid_list and branch0 in\
        # ['信用中国','中华人民共和国交通运输部','国家市场监督管理总局','国家知识产权局']:
        #     reviewed = '0'

        # if branch0 in ['信用中国','中华人民共和国交通运输部'] and title in company_name_list[i]:
        #     reviewed = '0'

        # if branch0 == '中华人民共和国交通运输部' and ('车辆案' in title or '扬撒案' in title or '飘散案' in title or\
        # '计价器案' in title):
        #     reviewed = '0'

        # if eventType == '信用中国公示处罚' and (is_Chinese(title) == False or len(title) < 4):
        #     reviewed = '22'

        # 提取ctime  ========================不用动==============================
        ctimeStamp = item['ctime']
        ctimeArray = time.localtime(ctimeStamp)
        ctimeDate = time.strftime('%Y-%m-%d', ctimeArray)

        # 提取gtime  ========================不用动==============================
        gtimeStamp = item['gtime']
        gtimeArray = time.localtime(gtimeStamp)
        gtimeDatetime = time.strftime('%Y-%m-%d %H:%M:%S', gtimeArray)

        # 如有附件连接则提取  ========================不用动==============================
        urlAcce = ''
        if 'accessory' in item.keys() and item['accessory'] != [] and isinstance(item['accessory'][0], dict) and \
                'url' in item['accessory'][0].keys():
            urlAcce = item['accessory'][0]['url']

        # 提取链接  ========================不用动==============================
        url = item['url']
        if url == 'http://':
            url = ''

        # 如果有涉及对象信息则提取  ========================不用动==============================
        relatedObj = ''
        if 'article_info' in item.keys() and item['article_info'] != None and '涉及对象' in item['article_info'].keys():
            relatedObj = item['article_info']['涉及对象']

        # 初始化事件发生时间 ###########################可更改###########################
        real_etime = '0001-01-01 00:00:00'
        if 'ptime' in item.keys():
            etimeStamp = item['ptime']
            # print('etimeStamp',etimeStamp)
            if is_valid_date(etimeStamp):
                real_etime = etimeStamp
            elif etimeStamp != -1:
                etimeArray = time.localtime(etimeStamp)
                real_etime = time.strftime('%Y-%m-%d %H:%M:%S', etimeArray)

        if real_etime == '0001-01-01 00:00:00' and '发布日期' in item['article_info'].keys():
            if is_valid_date(item['article_info']['发布日期'].replace('.', '-')):
                real_etime = item['article_info']['发布日期'].replace('.', '-')

        if real_etime == '0001-01-01 00:00:00' and '发布时间' in item['article_info'].keys():
            if is_valid_date(item['article_info']['发布时间'].replace('.', '-')):
                real_etime = item['article_info']['发布时间'].replace('.', '-')

        if real_etime == '0001-01-01 00:00:00' and '处罚决定日期' in item['article_info'].keys():
            if is_valid_date(item['article_info']['处罚决定日期'].replace('.', '-')):
                real_etime = item['article_info']['处罚决定日期'].replace('.', '-')

        if real_etime == '0001-01-01 00:00:00' and '采取监管措施日期' in item['article_info'].keys() and \
                item['article_info']['采取监管措施日期'] != '':
            real_etime = item['article_info']['采取监管措施日期']
        # print(real_etime)

        if real_etime == '0001-01-01 00:00:00' and len(item['contents']) > 10 and '处罚决定日期' in item['contents']:
            tmp_pos1 = item['contents'].index('处罚决定日期') + 1
            if len(item['contents']) > tmp_pos1 and is_valid_date(item['contents'][tmp_pos1].replace('.', '-')):
                real_etime = item['contents'][tmp_pos1].replace('.', '-')

        # 初始化事件结束时间 ###########################可更改###########################
        endTime = '0001-01-01 00:00:00'
        if 'etime' in item.keys() and item['etime'] != -1:
            endtimeArray = time.localtime(item['etime'])
            endTime = time.strftime('%Y-%m-%d %H:%M:%S', endtimeArray)

        # 初始化处罚类型 ###########################可更改###########################
        punishType1 = ''
        punishType2 = ''
        punishType3 = ''

        tmp_punish_type = ''
        if 'punish_type' in item.keys():
            tmp_punish_type = item['punish_type']
        if '取消任职资格' in tmp_punish_type:
            tmp_punish_type.remove('取消任职资格')
        if '禁止进入相关行业' in tmp_punish_type:
            tmp_punish_type.remove('禁止进入相关行业')
        # print(tmp_punish_type,type(tmp_punish_type))
        punishType_all = DBC2SBC(','.join(tmp_punish_type))
        if punishType_all == '':
            if '通报批评' in contents_set and '通报批评' not in punishType_all:
                punishType_all += ',通报批评'
            if '书面警示' in contents_set and '书面警示' not in punishType_all:
                punishType_all += ',书面警示'
            if '诫勉谈话' in contents_set and '诫勉谈话' not in punishType_all:
                punishType_all += ',诫勉谈话'
            if '约见谈话' in contents_set and '约见谈话' not in punishType_all:
                punishType_all += ',约见谈话'
            if '要求提交书面承诺' in contents_set and '要求提交书面承诺' not in punishType_all:
                punishType_all += ',要求提交书面承诺'
            if '警示函' in contents_set or '誉示函' in contents_set:
                punishType_all += ',警示函'
            if '公开谴责' in contents_set and '公开谴责' not in punishType_all:
                punishType_all += ',公开谴责'
            if '罚款' in contents_set.replace('以下的罚款', '') and '罚款' not in punishType_all:
                punishType_all += ',罚款'
            if '警告' in contents_set and '警告' not in punishType_all:
                punishType_all += ',警告'
            if '没收' in contents_set and '没收' not in punishType_all:
                punishType_all += ',没收'
            if '暂停执行业务' in contents_set and '暂停执行业务' not in punishType_all:
                punishType_all += ',暂停执行业务'
            if '暂停经营' in contents_set and '暂停经营' not in punishType_all:
                punishType_all += ',暂停经营'
            if '失信被执行人' in contents_set and '失信被执行人' not in punishType_all:
                punishType_all += ',失信被执行人'
            if '严重违法失信企业' in contents_set and '严重违法失信企业' not in punishType_all:
                punishType_all += ',严重违法失信企业'
        punishType1 = punishType_all.strip(',')
        # print('punishType1',punishType1)

        # 初始化处罚详情  ###########################可更改###########################
        punishDetail = ''
        if 'punish_info' in item.keys():
            punishDetail = DBC2SBC(item['punish_info'])

        if punishDetail == '' and '函件内容' in item['article_info'].keys():
            punishDetail = item['article_info']['函件内容']

        if punishDetail == '' and '处罚事由' in item['article_info'].keys() and item['article_info']['处罚事由'] != '':
            punishDetail = item['article_info']['处罚事由']

        if punishDetail == '' and len(item['contents']) > 10 and '处罚事由' in item['contents']:
            tmp_pos1 = item['contents'].index('处罚事由') + 1
            if len(item['contents']) > tmp_pos1 and item['contents'][tmp_pos1] != '处罚依据':
                punishDetail = item['contents'][tmp_pos1]

        if punishDetail == '' and len(item['contents']) > 10 and '处罚事由：' in item['contents']:
            tmp_pos1 = item['contents'].index('处罚事由：') + 1
            if len(item['contents']) > tmp_pos1 and item['contents'][tmp_pos1] != '处罚依据':
                punishDetail = item['contents'][tmp_pos1]

        # 提取分支机构名称  ========================不用动==============================
        branch = item['branch']

        # 调用提取金额模块 ###########################可更改###########################
        extract_term = {}
        if skip_flag_list[i] == False or (
                '2' in line_de.get('branch_tree').keys() and line_de.get('branch_tree')['2'] in ['烟台市公安局', '鹤壁市公安局',
                                                                                                 '洛阳市公安局'] and
                company_name_list[i][0] != '针对个人处罚'):
            numextract = num_extract.NumberExtract()
            # print([contents_set],[company_name_list[i]])
            extract_term = numextract.dosegment_all(contents_set, company_name_list[i])
            print('extract_term', extract_term)

        # 案件编号 ###########################可更改###########################
        eventNum1 = ''
        # 优先取mongo中已有的信息
        if '函号' in item['article_info']:
            eventNum1 = item['article_info']['函号']
        elif '行政处罚决定书文号' in item['article_info']:
            eventNum1 = item['article_info']['行政处罚决定书文号']
        elif '文　　号' in item['article_info']:
            eventNum1 = item['article_info']['文　　号']

        if eventNum1 == '' and 'punish_number' in item.keys() and len(item['punish_number']) < 100:
            eventNum1 = item['punish_number']

        # 从标题中提取
        if eventNum1 == '' and '(' in title and '号)' in title:
            tmp_pos1 = title.find('(')
            tmp_pos2 = title.find('号)', tmp_pos1)
            if len(title[tmp_pos1 + 1:tmp_pos2 + 1]) < 20 and len(title[tmp_pos1 + 1:tmp_pos2 + 1]) > 5:
                eventNum1 = title[tmp_pos1 + 1:tmp_pos2 + 1]

        if eventNum1 == '' and item['branch'] in ['福建省公安厅', '广安市公安局'] and '号' in title:
            eventNum1 = title

        # 从contents list中截取文号
        if eventNum1 == '' and len(item['contents']) > 10 and '行政处罚决定书文号' in item['contents']:
            tmp_pos1 = item['contents'].index('行政处罚决定书文号') + 1
            if len(item['contents']) > tmp_pos1 and len(item['contents'][tmp_pos1]) < 20 and '号' in item['contents'][
                tmp_pos1]:
                eventNum1 = item['contents'][tmp_pos1]
        if eventNum1 == '' and len(item['contents']) > 10 and '行政处罚决定书文号：' in item['contents']:
            tmp_pos1 = item['contents'].index('行政处罚决定书文号：') + 1
            if len(item['contents']) > tmp_pos1 and len(item['contents'][tmp_pos1]) < 20 and '号' in item['contents'][
                tmp_pos1]:
                eventNum1 = item['contents'][tmp_pos1]

        # 从文章内容中截取文号
        if eventNum1 == '' and item['department'] == '中国发展和改革委员会' and '-' in title and \
                '号' in title:
            tmp_pos1 = title.find('-')
            if len(title[tmp_pos1 + 1:]) < 20: eventNum1 = title[tmp_pos1 + 1:]
        elif eventNum1 == '' and item['department'] == '中国发展和改革委员会' and '(' in title and \
                ')' in title and '号' in title:
            tmp_pos1 = title.find('(')
            tmp_pos2 = contents_set.find(')', tmp_pos1)
            if len(title[tmp_pos1 + 1:tmp_pos2]) < 20 and '号' in title[tmp_pos1 + 1:tmp_pos2]: eventNum1 = title[
                                                                                                           tmp_pos1 + 1:tmp_pos2]
        elif eventNum1 == '' and item['department'] == '中国发展和改革委员会' and '巴区发改处字' in contents_set and \
                '号' in contents_set:
            tmp_pos1 = contents_set.find('巴区发改处字')
            tmp_pos2 = contents_set.find('号', tmp_pos1)
            if len(contents_set[tmp_pos1:tmp_pos2 + 1]) < 20: eventNum1 = contents_set[tmp_pos1:tmp_pos2 + 1]

        # 用正则提取文中括号内文号,该方法容易误提取法规文号,需注意过滤
        if eventNum1 == '':
            num_list = re.findall(r'[(](.*?)[)]', contents_set[:500])
            for nl in num_list:
                pos = contents_set.find(nl)
                if '〔' in nl and '号' in nl and len(nl) < 20 and '《' not in contents_set[pos - 10:pos]:
                    eventNum1 = nl
                    break

        # 去重(按title和ctimeDate) ###########################可更改###########################
        if (title, ctimeDate, eventNum1) in no_dupli:
            reviewed = '22'
        else:
            no_dupli.append((title, ctimeDate, eventNum1))

        # 初始化其余信息  ========================不用动==============================
        eventNum2 = ''
        eventNum3 = ''

        party1 = ''
        party2 = ''
        party3 = ''

        priority = ''

        involvedAmount2 = 0

        # 开始遍历某篇文章中提取出的直接被处罚对象
        # print('company_name_list', company_name_list[i])
        if company_name_list[i] != [''] and company_name_list[i] != '' and company_name_list[i] != []:
            black_flag = False
            for j, company in enumerate(company_name_list[i]):
                # print(company)
                # 优先取mongo中的罚款金额和涉及金额 ###########################可更改###########################
                penaltyAmount = 0
                if 'relate_amount' in item.keys() and 'punish_amount' in item['relate_amount'].keys() and \
                        item['relate_amount']['punish_amount'] != '' and item['relate_amount']['punish_amount'][
                    0].isdigit():
                    penaltyAmount = round(float(han_2_num.cn2digTransformer(item['relate_amount']['punish_amount'])), 2)
                elif 'relate_amount' in item.keys() and 'punish_amount' in item['relate_amount'].keys() and \
                        item['relate_amount']['punish_amount'] != '' and item['branch_tree']['2'] == '泰州市公安局':
                    penaltyAmount = round(float(item['relate_amount']['punish_amount'].split('）')[-1]) * 10000, 2)

                # print('penaltyAmount',penaltyAmount)

                involvedAmount1 = 0
                if 'relate_amount' in item.keys() and 'other_amount' in item['relate_amount'].keys() and \
                        item['relate_amount']['other_amount'] != '':
                    involvedAmount1 = round(float(han_2_num.cn2digTransformer(item['relate_amount']['other_amount'])), 2)
                if involvedAmount1 == 0 and 'relate_amount' in item.keys() and 'owing_amount' in item[
                    'relate_amount'].keys() and item['relate_amount']['owing_amount'] != '':
                    involvedAmount1 = round(float(han_2_num.cn2digTransformer(item['relate_amount']['owing_amount'])), 2)

                # 直接被处罚
                punishRelation = '1'

                # 如从有提取到每个公司对应的详细信息,则优先采用,通常用于表格 ###########################可更改###########################
                if skip_flag_list[i]:
                    if len(punish_type_list) > i and len(punish_type_list[i]) > j:
                        punishType1 = punish_type_list[i][j]
                    if len(penalty_amount_list) > i and len(penalty_amount_list[i]) > j and penalty_amount_list[i][
                        j].replace('.', '').isdigit() and penalty_amount_list[i][j].count('.') <= 1:
                        penaltyAmount = penalty_amount_list[i][j]
                    if len(involved_amount_list) > i and len(involved_amount_list[i]) > j and involved_amount_list[i][
                        j].replace('.', '').isdigit() and involved_amount_list[i][j].count('.') <= 1:
                        involvedAmount1 = involved_amount_list[i][j]
                    if len(punish_detail_list) > i and len(punish_detail_list[i]) > j:
                        punishDetail = punish_detail_list[i][j]
                    if len(event_num_list) > i and len(event_num_list[i]) > j:
                        eventNum1 = event_num_list[i][j]
                    if len(etime_list) > i and len(etime_list[i]) > j:
                        real_etime = etime_list[i][j].replace('年', '-').replace('月', '-').replace('日', '')
                    if len(end_time_list) > i and len(end_time_list[i]) > j:
                        endTime = end_time_list[i][j].replace('年', '-').replace('月', '-').replace('日', '')

                # 如果处罚详情依然为空且文章长度较短,则用文章当作处罚详情内容 ###########################可更改###########################
                if punishDetail == '' and len(contents_set) < 1000:
                    punishDetail = contents_set

                # 在取处罚金额和涉及金额时对于别名取原名数据  ========================不用动==============================
                if company in alias_dict_list.keys():
                    tmp_company = alias_dict_list[company]
                else:
                    tmp_company = company
                # print('tmp_company',tmp_company)

                if tmp_company in extract_term.keys():
                    if penaltyAmount == 0 and 'penaltyAmount' in extract_term[tmp_company].keys() and \
                            extract_term[tmp_company]['penaltyAmount'] != '':
                        penaltyAmount = round(float(extract_term[tmp_company]['penaltyAmount']), 2)

                    if involvedAmount1 == 0 and 'involvedAmount' in extract_term[tmp_company].keys() and \
                            extract_term[tmp_company]['involvedAmount'] != '':
                        involvedAmount1 = round(float(extract_term[tmp_company]['involvedAmount']), 2)

                # 写入数据  ========================不用动==============================
                insert_mysql(db, final_mysql_name, company, '', '', item['uid'], punishRelation, real_etime, endTime,
                             eventNum1, eventNum2, eventNum3, party1, party2, party3,
                             punishType1, '', '', priority, punishDetail,
                             penaltyAmount, involvedAmount1, involvedAmount2,
                             ctimeDate, gtimeDatetime, title, url, urlAcce, relatedObj,
                             eventType, branch0, branch1, branch2, branch3, branch_tree,
                             branch, item['channel'], reviewed, attention_list[i])

                # 针对分公司情况添加其母公司  ###########################可更改###########################
                if company[-3:] == '支公司' or company[-3:] == '分公司' or company[-2:] == '分行' or company[-2:] == '支行' or \
                        '分行(' in company or '支行(' in company:
                    pos1 = company.find('公司')
                    pos2 = company.find('银行')
                    if pos1 != -1:
                        company = company[:pos1 + 2]
                        if company in jiancheng_list:
                            pos = jiancheng_list.index(company)
                            company = quancheng_list[pos]

                        if company in company_name_list[i]:
                            continue
                        insert_mysql(db, final_mysql_name, company, '', '', item['uid'], punishRelation, real_etime,
                                     endTime,
                                     eventNum1, eventNum2, eventNum3, party1, party2, party3,
                                     punishType1, '', '', priority, punishDetail,
                                     penaltyAmount, involvedAmount1, involvedAmount2,
                                     ctimeDate, gtimeDatetime, title, url, urlAcce, relatedObj,
                                     eventType, branch0, branch1, branch2, branch3, branch_tree,
                                     branch, item['channel'], reviewed, attention_list[i])

        # 开始遍历某篇文章中提取出的间接被处罚对象
        if person_company_list[i] != [''] and person_company_list[i] != [] and person_company_list[i] != '':
            black_flag = False
            # print(person_company_list[i])
            for j, company in enumerate(person_company_list[i]):
                # print(company)

                # 间接被处罚
                punishRelation = '2'

                # 如从有提取到每个公司对应的详细信息,则优先采用,通常用于表格
                if skip_flag_list[i]:
                    if len(punish_type_list) > i and len(punish_type_list[i]) > j:
                        punishType1 = punish_type_list[i][j]
                    if len(penalty_amount_list) > i and len(penalty_amount_list[i]) > j and penalty_amount_list[i][
                        j].replace('.', '').isdigit() and penalty_amount_list[i][j].count('.') <= 1:
                        penaltyAmount = penalty_amount_list[i][j]
                    if len(involved_amount_list) > i and len(involved_amount_list[i]) > j and involved_amount_list[i][
                        j].replace('.', '').isdigit() and involved_amount_list[i][j].count('.') <= 1:
                        involvedAmount1 = involved_amount_list[i][j]
                    if len(punish_detail_list) > i and len(punish_detail_list[i]) > j:
                        punishDetail = punish_detail_list[i][j]
                    if len(etime_list) > i and len(etime_list[i]) > j:
                        real_etime = etime_list[i][j].replace('年', '-').replace('月', '-').replace('日', '')

                # 如果处罚详情依然为空且文章长度较短,则用文章当作处罚详情内容
                if punishDetail == '' and len(contents_set) < 1000:
                    punishDetail = contents_set

                # 对于别名数据取原名
                if company in alias_dict_list.keys():
                    tmp_company = alias_dict_list[company]
                else:
                    tmp_company = company
                print('tmp_company', tmp_company)

                if tmp_company in extract_term.keys():
                    if penaltyAmount == 0 and 'penaltyAmount' in extract_term[tmp_company].keys() and \
                            extract_term[tmp_company]['penaltyAmount'] != '':
                        penaltyAmount = round(float(extract_term[tmp_company]['penaltyAmount']), 2)

                    if involvedAmount1 == 0 and 'involvedAmount' in extract_term[tmp_company].keys() and \
                            extract_term[tmp_company]['involvedAmount'] != '':
                        involvedAmount1 = round(float(extract_term[tmp_company]['involvedAmount']), 2)
                # print('involvedAmount1',type(involvedAmount1),involvedAmount1)

                # 写入数据
                insert_mysql(db, final_mysql_name, company, '', '', item['uid'], punishRelation, real_etime, endTime,
                             eventNum1, eventNum2, eventNum3, party1, party2, party3,
                             punishType1, '', '', priority, punishDetail,
                             penaltyAmount, involvedAmount1, involvedAmount2,
                             ctimeDate, gtimeDatetime, title, url, urlAcce, relatedObj,
                             eventType, branch0, branch1, branch2, branch3, branch_tree,
                             branch, item['channel'], reviewed, attention_list[i])

                # 针对分公司情况添加其母公司
                if company[-3:] == '支公司' or company[-3:] == '分公司' or company[-2:] == '分行' or company[-2:] == '支行' or \
                        '分行(' in company or '支行(' in company:
                    pos1 = company.find('公司')
                    pos2 = company.find('银行')
                    if pos1 != -1:
                        company = company[:pos1 + 2]
                        if company in jiancheng_list:
                            pos = jiancheng_list.index(company)
                            company = quancheng_list[pos]

                        if company in company_name_list[i]:
                            continue

                        insert_mysql(db, final_mysql_name, company, '', '', item['uid'], punishRelation, real_etime,
                                     endTime,
                                     eventNum1, eventNum2, eventNum3, party1, party2, party3,
                                     punishType1, '', '', priority, punishDetail,
                                     penaltyAmount, involvedAmount1, involvedAmount2,
                                     ctimeDate, gtimeDatetime, title, url, urlAcce, relatedObj,
                                     eventType, branch0, branch1, branch2, branch3, branch_tree,
                                     branch, item['channel'], reviewed, attention_list[i])

        # 没有提取到任何处罚对象情况
        if black_flag:
            # print(company)
            # try:
            insert_mysql(db, final_mysql_name, '', '', '', item['uid'], '', real_etime, endTime,
                         eventNum1, eventNum2, eventNum3, party1, party2, party3,
                         punishType1, '', '', priority, punishDetail,
                         0, 0, involvedAmount2,
                         ctimeDate, gtimeDatetime, title, url, urlAcce, relatedObj,
                         eventType, branch0, branch1, branch2, branch3, branch_tree,
                         branch, item['channel'], reviewed, attention_list[i])

            # except:
            #     print(item['_id'])
        if i % 100 == 0:
            print(i)

    # 记录gtime数据log
    if logging:
        write_log(db,
                  mysql_log_name,
                  mongo_name,
                  add_data_num,
                  max_gtime,
                  last_gtime)


    # 打印大文件的uid
    if large_file_uid_list != []:
        print('大文件uid集合', large_file_uid_list, datetime.datetime.now().date())
    else:
        print('大文件uid集合为空', datetime.datetime.now().date())

    db.commit()
    db.close()
    db1.close()
except:
    print('error')
    with open('/data1/fsp_extract_company_ga_daily.log','a') as f:
        f.write('error\n')