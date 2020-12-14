# coding:utf-8

'''
文件名：sy_ctxt_main_basic.py
功能：长投学堂证券主表增量数据处理
代码历史：20200421，徐荣华
'''

import pymysql
import datetime
import time
import os
import json
from DBUtils.PooledDB import PooledDB




# 读取连接数据库配置文件
load_config='/home/seeyii/increase_nlp/db_config.json'
with open(load_config, 'r', encoding="utf-8") as f:
    reader = f.readlines()[0]
config_local = json.loads(reader)

# 129csc_risk，写log
pool = PooledDB(creator=pymysql, **config_local['local_sql_csc_pool'])
db = pool.connection()
cursor = db.cursor()

# 172sy_project_raw，写数据
pool2 = PooledDB(creator=pymysql, **config_local['local_sql_project_raw_172pool'])
mysql_conn = pool2.connection()
cur = mysql_conn.cursor()

# 192sy_project_raw，写数据
pool192 = PooledDB(creator=pymysql, **config_local['local_sql_project_raw_pool'])
mysql_conn192 = pool192.connection()
cur192= mysql_conn192.cursor()

# 连接一部db_seeyii，取数据
pool3 = PooledDB(creator=pymysql, **config_local['fsp_event_pool'])
mysql_conn2 = pool3.connection()
cur2 = mysql_conn2.cursor()

isValid = 1  # 是否有效，0：否，1：是
createTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))  # sql操作时间
dataState = 1  # 数据状态:1->新增;2->更新;3->删除
logging = False
table_name = 'sy_ctxt_main_assess_value'

# 写日志的表名
mysql_log_name = 'sy_ctxt_data_log'

# 第一步：取净利润，净资产，用于计算pe，pb值
# innerCode 内码
# endDate 报表日期
# reportType 报表类型
# netProfit 净利润


# promain_sql = '''SELECT innerCode,enDdate,reportType,(case when industryType in ('1','4') then netPareCompProf when industryType in ('2', '5') then parenetp when industryType in ('3') then NETPARESHARPROF else null end) as netProfit from sq_fin_proincnew WHERE reportType = '3' '''

# cur2.execute(promain_sql)
# promain_infos = cur2.fetchall()
# truncate_table_sql = '''truncate table sq_fin_promain'''
# cur.execute(truncate_table_sql)
# mysql_conn.commit()
# print('清空sq_fin_promain数据成功')
# promain_insert_sql = '''insert into sq_fin_promain(innerCode,endDate,reportType,netProfit)
#                 value (%s,%s,%s,%s)'''
# cur.executemany(promain_insert_sql, promain_infos)
# mysql_conn.commit()
# print('插入sq_fin_promain数据成功')


# # # 取商誉值用于计算pb不含商誉值
# probalsheetnew_sql = '''SELECT innerCode,endDate,reportType,
# (case when industryType in ('1','4') then pareCompSharRight when industryType in ('2')
# then PARECOMPTOTEQUI when industryType in ('5','3') then pareSharRigh else null end) as totsharEqui,goodWill from sq_fin_probalsheetnew where innerCode is not null and reportType = '3' '''
# cur2.execute(probalsheetnew_sql)
# probalsheetnew_infos = cur2.fetchall()
# truncate_table_sql2 = '''truncate table sq_fin_probalsheetnew'''
# cur.execute(truncate_table_sql2)
# mysql_conn.commit()
# print('清空sq_fin_probalsheetnew数据成功')
# probalsheetnew_insert_sql = '''insert into sq_fin_probalsheetnew(innerCode,endDate,reportType,totsharEqui,goodWill)
#                 value (%s,%s,%s,%s,%s)'''
# cur.executemany(probalsheetnew_insert_sql, probalsheetnew_infos)
# mysql_conn.commit()
# print('插入sq_fin_probalsheetnew数据成功')

# goodWill 商誉值
# totsharEqui 净资产

# 获取数据源最大交易日期
max_trade_date_sql = 'select max(tradeDate) from sq_qt_skdailyprice'
cur2.execute(max_trade_date_sql)
max_trade_date_info = cur2.fetchone()
max_trade_date = max_trade_date_info[0]
max_trade_date = '20201113'

# 获取目标表最大交易日期
totmktcap_max_trade_date_sql = 'select max(tradeDate) from sy_ctxt_main_totmktcap '
cur.execute(totmktcap_max_trade_date_sql)
totmktcap_max_trade_date_info = cur.fetchone()
totmktcap_max_trade_date = totmktcap_max_trade_date_info[0]
totmktcap_max_trade_date = '20191231'

# # 查询增量数据
# select_sql = '''SELECT c.ChiName,c.SecuAbbr,c.SecuCode,c.innerCode,c.companyCode,d.tradeDate,d.totmktcap from (SELECT a.*,b.innerCode,b.companyCode from 
#         (SELECT ChiName,SecuAbbr,SecuCode
#            from secumain WHERE SecuCategory = 1 and SecuMarket in (83,90) and ListedSector in (1,2,6,7) and ListedState in (1,3,5)) as a INNER JOIN
#         (SELECT secuCode,innerCode, companyCode from sq_sk_basicinfo) as b on a.SecuCode = b.secuCode GROUP BY a.SecuCode) as c INNER JOIN
#         (SELECT tradeDate, companyCode, totmktcap from sq_qt_skdailyprice where tradeDate <= '{}' and tradeDate > '{}') as d on c.companyCode = d.companyCode'''.format(
#         max_trade_date,totmktcap_max_trade_date)

# cur2.execute(select_sql)
# add_infos = cur2.fetchall()
# add_data_num = len(add_infos)

# print(max_trade_date,totmktcap_max_trade_date,add_data_num)

# # 判断是否有新数据
# if add_data_num == 0:
#     print('There is no new data!')
#     os._exit(0)
# # 写入129数据库
# insert_sql = '''insert into sy_ctxt_main_totmktcap(chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap)
#                             value (%s,%s,%s,%s,%s,%s,%s)'''
# cur.executemany(insert_sql, add_infos)
# mysql_conn.commit()
# print('插入表sy_ctxt_main_totmktcap数据成功')

# secuCodeList = []
# with open('/home/seeyii/xrh/ctxt/secuCode.txt', 'r', encoding="utf-8") as f:
#     reader = f.readlines()
#     for i in reader:
#         secu_code = i.replace('\n', '')
#         secuCodeList.append(secu_code)

# print(secuCodeList[4000:4001], len(secuCodeList))
# # 计算增量数据pe值
# for secuCode in secuCodeList[4000:]:
    # pe_sql = '''SELECT E.chiName,E.secuAbbr,E.secuCode,E.innerCode,E.companyCode,E.tradeDate,E.totmktcap, 
    #         round((CASE WHEN E.totmktcap is not NULL and F.netProfit is NOT NULL THEN (E.totmktcap/F.netProfit)*10000 ELSE NULL END),4) as pe from 
    # (
    # select chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,min(daynum) as daymin from 
    # (
    # select A.*,B.*,DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d')) as daynum from 
    # (
    # SELECT chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap FROM sy_ctxt_main_totmktcap
    # WHERE tradeDate <= '{}' and tradeDate > '{}' and secuCode='{}'
    # ) 
    # as A
    # ,
    # (
    # select   
    #     innerCode as innerCode1,
    #   enDdate,
    #     netProfit   
    #      from sq_fin_promain where substring(endDate,5,4) = '1231'
    #      ) as B 
    # where    A.innerCode=B.innerCode1 
    #          and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))>=0
    #                  and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))<500 # 这个数值是交易日与报表截止日期的差值，可以变得
    # ) as C   
    # group by chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap

    # ) as E ,

    # (
    # select A.innerCode,A.tradeDate,B.*,DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d')) as daynum from 
    # (
    # SELECT * FROM sy_ctxt_main_totmktcap WHERE tradeDate <= '{}' and tradeDate > '{}' and secuCode='{}'
    # ) 
    # as A
    # ,
    # (
    # select   
    #     innerCode as innerCode1,
    #   enDdate,
    #     netProfit   
    #      from sq_fin_promain where substring(endDate,5,4) = '1231'
    #      ) as B 
    # where    A.innerCode=B.innerCode1 
    #         and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))>=0
    #                 and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))<500
    # )       
    # as F 
    # where E.innerCode=F.innerCode and E.tradeDate=F.tradeDate  and E.daymin=F.daynum ORDER BY secuCode'''.format(
    #         max_trade_date,totmktcap_max_trade_date,secuCode, max_trade_date,totmktcap_max_trade_date, secuCode)

    # cur.execute(pe_sql)
    # pe_infos = cur.fetchall()
    # pe_insert_sql = '''insert into sy_ctxt_main_pe(chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,pe)
    #                             value (%s,%s,%s,%s,%s,%s,%s,%s)'''
    # cur.executemany(pe_insert_sql, pe_infos)
    # mysql_conn.commit()
    # print('{}-插入表sy_ctxt_main_pe数据成功'.format(secuCode))

    # # 计算增量数据pb值
    # pb_sql = '''SELECT E.chiName,E.secuAbbr,E.secuCode,E.innerCode,E.companyCode,E.tradeDate,E.totmktcap,E.pe,F.totsharEqui,
    #         round((CASE WHEN E.totmktcap is not NULL and F.totsharEqui is NOT NULL THEN (E.totmktcap/F.totsharEqui)*10000 ELSE NULL END),4) as pb from 
    # (
    # select chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,pe,min(daynum) as daymin from 
    # (
    # select A.*,B.*,DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d')) as daynum from 
    # (
    # SELECT chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,pe FROM sy_ctxt_main_pe
    # WHERE tradeDate <= '{}' and tradeDate > '{}' and secuCode='{}'
    # ) 
    # as A
    # ,
    # (
    # select   
    #     innerCode as innerCode1,
    #   enDdate,
    #     totsharEqui   
    #      from sq_fin_probalsheetnew
    #      ) as B 
    # where    A.innerCode=B.innerCode1 
    #          and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))>=0
    #                  and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))<5000
    # ) as C   
    # group by chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,pe

    # ) as E ,

    # (
    # select A.innerCode,A.tradeDate,B.*,DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d')) as daynum from 
    # (
    # SELECT * FROM sy_ctxt_main_pe WHERE tradeDate <= '{}' and tradeDate > '{}' and secuCode='{}'
    # ) 
    # as A
    # ,
    # (
    # select   
    #     innerCode as innerCode1,
    #   enDdate,
    #     totsharEqui   
    #      from sq_fin_probalsheetnew
    #      ) as B 
    # where    A.innerCode=B.innerCode1 
    #         and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))>=0
    #                 and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))<5000
    # )       
    # as F 
    # where E.innerCode=F.innerCode and E.tradeDate=F.tradeDate  and E.daymin=F.daynum ORDER BY secuCode'''.format(
    #         max_trade_date,totmktcap_max_trade_date,secuCode, max_trade_date,totmktcap_max_trade_date, secuCode)

    # cur.execute(pb_sql)
    # pb_infos = cur.fetchall()
    # pb_insert_sql = '''insert into sy_ctxt_main_pb(chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,pe,totsharEqui,pb)
    #                             value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    # cur.executemany(pb_insert_sql, pb_infos)
    # mysql_conn.commit()
    # print('{}-插入表sy_ctxt_main_pb数据成功'.format(secuCode))

    # # 计算增量数据pb不含商誉值
    # pbb_sql = '''SELECT E.chiName,E.secuAbbr,E.secuCode,E.innerCode,E.companyCode,E.tradeDate,E.totmktcap,E.pe,E.totsharEqui,E.pb,
    #         round((CASE WHEN E.totmktcap is not NULL and E.totsharEqui is NOT NULL and F.goodWill2 is NOT NULL THEN (E.totmktcap/(E.totsharEqui - F.goodWill2))*10000
    #         WHEN F.goodWill2 is NULL THEN E.pb ELSE NULL END),4) as pbb from 
    # (
    # select chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,pe,totsharEqui,pb,min(daynum) as daymin from 
    # (
    # select A.*,B.*,
    # (case WHEN DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d')) is null then 10000 else 
    # DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d')) end) as daynum from 
    # (
    # SELECT chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,pe,totsharEqui,pb FROM sy_ctxt_main_pb
    # WHERE tradeDate <= '{}' and tradeDate > '{}' and secuCode ='{}'
    # ) 
    # as A
    # ,
    # (
    # select   
    #     innerCode as innerCode1,
    #   enDdate,
    #     goodWill   
    #      from sq_fin_probalsheetnew
    #      ) as B 
    # WHERE    A.innerCode=B.innerCode1 
    #          and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))>=0
    #                  and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))<5000
    # ) as C   
    # group by chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,pe,totsharEqui,pb

    # ) as E ,

    # (
    # select A.innerCode,A.tradeDate,B.*,(case when B.goodwill is  null then 0 else B.goodwill end ) as goodwill2,
    # (case WHEN DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d')) is null then 10000 else 
    # DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d')) end) as daynum from 
    # (
    # SELECT * FROM sy_ctxt_main_pb WHERE tradeDate <= '{}' and tradeDate > '{}' and secuCode='{}'
    # ) 
    # as A
    # ,
    # (
    # select   
    #     innerCode as innerCode1,
    #   enDdate,
    #     goodWill   
    #      from sq_fin_probalsheetnew
    #      ) as B 
    # WHERE     A.innerCode=B.innerCode1 
    #         and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))>=0
    #                 and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))<5000
    # )       
    # as F 
    # where E.innerCode=F.innerCode and E.tradeDate=F.tradeDate  and E.daymin=F.daynum ORDER BY secuCode'''.format(
    #         max_trade_date,totmktcap_max_trade_date,secuCode,max_trade_date,totmktcap_max_trade_date,secuCode)

    # cur.execute(pbb_sql)
    # pbb_infos = cur.fetchall()
    # pbb_insert_sql = '''insert into sy_ctxt_main_pbb_daily(chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,pe,totsharEqui,pb,pbb)
    #                             value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    # cur.executemany(pbb_insert_sql, pbb_infos)
    # mysql_conn.commit()
    # print('{}-插入表sy_ctxt_main_pbb_daily数据成功'.format(secuCode))


all_secu_code_list = []
with open('/home/seeyii/xrh/ctxt/secuCode.txt', 'r', encoding="utf-8") as f:
    reader = f.readlines()
    for i in reader:
        secu_code = i.replace('\n', '')
        all_secu_code_list.append(secu_code)

print(all_secu_code_list[2500:2501], len(all_secu_code_list))

# 分为点计算
i = 2500
for secuCode in all_secu_code_list[2500:3000]:
    quantile_sql = '''SELECT chiName,innerCode,secuCode,tradeDate,totmktcap,pe,pb,pbb
    from sy_ctxt_main_pbb_daily where tradeDate <= '{}' and tradeDate > '{}' and secuCode='{}' '''.format(max_trade_date,totmktcap_max_trade_date,secuCode)
    try:
        # 历史所有分为点计算
        peFiveQuantile = None
        peTenQuantile = None
        pbFiveQuantile = None
        pbTenQuantile = None
        pbbFiveQuantile = None
        pbbTenQuantile = None
        cur.execute(quantile_sql)
        quantile_infos = cur.fetchall()
        for info in quantile_infos:
            chiName = info[0]
            # secuAbbr = info[1]
            innerCode = info[1]
            secuCode = info[2]
            tradeDate = info[3]
            totmktcap = info[4]
            if totmktcap:
                totmktcap = round(totmktcap*10000, 4)
            pe = info[5]
            pb = info[6]
            pbb = info[7]
            count_sql = '''SELECT count(*) as count,secuCode from sy_ctxt_main_pbb_daily
            WHERE tradeDate <= '{}' and secuCode = '{}' '''.format(tradeDate, secuCode)
            cur.execute(count_sql)
            count_info = cur.fetchone()
            count = count_info[0]
            pe_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb_daily WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
    pe
    from
    ( 
        select * from `sy_ctxt_main_pbb_daily` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and secuCode = '{}' order by pe  

    ) as A) as c on b.secuCode = c.secuCode and b.pe = c.pe '''.format(tradeDate, secuCode, tradeDate, secuCode)
            cur.execute(pe_rn_sql)
            pe_rn_info = cur.fetchone()
            if pe_rn_info:
                pe_rn = pe_rn_info[0]
                if count > 1:
                    peQuantile = (pe_rn - 1) / (count - 1)
                    peQuantile = round(peQuantile * 100, 4)
                else:
                    peQuantile = 0
            else:
                peQuantile = None
            pb_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb_daily WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
    pb
    from
    ( 
        select * from `sy_ctxt_main_pbb_daily` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and secuCode = '{}' order by pb  

    ) as A) as c on b.secuCode = c.secuCode and b.pb = c.pb '''.format(tradeDate, secuCode, tradeDate, secuCode)
            cur.execute(pb_rn_sql)
            pb_rn_info = cur.fetchone()
            if pb_rn_info:
                pb_rn = pb_rn_info[0]
                if count > 1:
                    pbQuantile = (pb_rn - 1) / (count - 1)
                    pbQuantile = round(pbQuantile * 100, 4)
                else:
                    pbQuantile = 0
            else:
                pbQuantile = None
            pbb_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb_daily WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
    pbb
    from
    ( 
        select * from `sy_ctxt_main_pbb_daily` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and secuCode = '{}' order by pbb  

    ) as A) as c on b.secuCode = c.secuCode and b.pbb = c.pbb '''.format(tradeDate, secuCode, tradeDate, secuCode)
            cur.execute(pbb_rn_sql)
            pbb_rn_info = cur.fetchone()
            if pbb_rn_info:
                pbb_rn = pbb_rn_info[0]
                if count > 1:
                    pbbQuantile = (pbb_rn - 1) / (count - 1)
                    pbbQuantile = round(pbbQuantile * 100, 4)
                else:
                    pbbQuantile = 0
            else:
                pbbQuantile = None
            
            # 五年分为点计算
            five_trade_date = str(int(tradeDate[0:4]) - 5) + tradeDate[4:8]
            five_count_sql = '''SELECT count(*) as count,secuCode from sy_ctxt_main_pbb_daily
            WHERE tradeDate >= '{}' and tradeDate <= '{}' and secuCode = '{}' '''.format(five_trade_date,tradeDate, secuCode)
            cur.execute(five_count_sql)
            five_count_info = cur.fetchone()
            five_count = five_count_info[0]
            five_pe_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb_daily WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
    pe
    from
    ( 
        select * from `sy_ctxt_main_pbb_daily` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and tradeDate>= '{}' and secuCode = '{}' order by pe  

    ) as A) as c on b.secuCode = c.secuCode and b.pe = c.pe '''.format(tradeDate, secuCode, tradeDate,five_trade_date, secuCode)
            cur.execute(five_pe_rn_sql)
            five_pe_rn_info = cur.fetchone()
            if five_pe_rn_info:
                five_pe_rn = five_pe_rn_info[0]
                if five_count > 1:
                    peFiveQuantile = (five_pe_rn - 1) / (five_count - 1)
                    peFiveQuantile = round(peFiveQuantile * 100, 4)
                else:
                    peFiveQuantile = 0
            else:
                peFiveQuantile = None
            five_pb_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb_daily WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
    pb
    from
    ( 
        select * from `sy_ctxt_main_pbb_daily` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and tradeDate>= '{}' and secuCode = '{}' order by pb  

    ) as A) as c on b.secuCode = c.secuCode and b.pb = c.pb '''.format(tradeDate, secuCode, tradeDate,five_trade_date, secuCode)
            cur.execute(five_pb_rn_sql)
            five_pb_rn_info = cur.fetchone()
            if five_pb_rn_info:
                five_pb_rn = five_pb_rn_info[0]
                if five_count > 1:
                    pbFiveQuantile = (five_pb_rn - 1) / (five_count - 1)
                    pbFiveQuantile = round(pbFiveQuantile * 100, 4)
                else:
                    pbFiveQuantile = 0
            else:
                pbFiveQuantile = None
            five_pbb_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb_daily WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
    pbb
    from
    ( 
        select * from `sy_ctxt_main_pbb_daily` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and tradeDate>= '{}' and secuCode = '{}' order by pbb  

    ) as A) as c on b.secuCode = c.secuCode and b.pbb = c.pbb '''.format(tradeDate, secuCode, tradeDate,five_trade_date, secuCode)
            cur.execute(five_pbb_rn_sql)
            five_pbb_rn_info = cur.fetchone()
            if five_pbb_rn_info:
                five_pbb_rn = five_pbb_rn_info[0]
                if five_count > 1:
                    pbbFiveQuantile = (five_pbb_rn - 1) / (five_count - 1)
                    pbbFiveQuantile = round(pbbFiveQuantile * 100, 4)
                else:
                    pbbFiveQuantile = 0
            else:
                pbbFiveQuantile = None

            # 十年分为点计算
            ten_trade_date = str(int(tradeDate[0:4]) - 10) + tradeDate[4:8]
            ten_count_sql = '''SELECT count(*) as count,secuCode from sy_ctxt_main_pbb_daily
            WHERE tradeDate >= '{}' and tradeDate <= '{}' and secuCode = '{}' '''.format(ten_trade_date,tradeDate, secuCode)
            cur.execute(ten_count_sql)
            ten_count_info = cur.fetchone()
            ten_count = ten_count_info[0]
            ten_pe_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb_daily WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
    pe
    from
    ( 
        select * from `sy_ctxt_main_pbb_daily` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and tradeDate>= '{}' and secuCode = '{}' order by pe  

    ) as A) as c on b.secuCode = c.secuCode and b.pe = c.pe '''.format(tradeDate, secuCode, tradeDate,ten_trade_date, secuCode)
            cur.execute(ten_pe_rn_sql)
            ten_pe_rn_info = cur.fetchone()
            if ten_pe_rn_info:
                ten_pe_rn = ten_pe_rn_info[0]
                if ten_count > 1:
                    peTenQuantile = (ten_pe_rn - 1) / (ten_count - 1)
                    peTenQuantile = round(peTenQuantile * 100, 4)
                else:
                    peTenQuantile = 0
            else:
                peTenQuantile = None
            ten_pb_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb_daily WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
    pb
    from
    ( 
        select * from `sy_ctxt_main_pbb_daily` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and tradeDate>= '{}' and secuCode = '{}' order by pb  

    ) as A) as c on b.secuCode = c.secuCode and b.pb = c.pb '''.format(tradeDate, secuCode, tradeDate,ten_trade_date, secuCode)
            cur.execute(ten_pb_rn_sql)
            ten_pb_rn_info = cur.fetchone()
            if ten_pb_rn_info:
                ten_pb_rn = ten_pb_rn_info[0]
                if ten_count > 1:
                    pbTenQuantile = (ten_pb_rn - 1) / (ten_count - 1)
                    pbTenQuantile = round(pbTenQuantile * 100, 4)
                else:
                    pbTenQuantile = 0
            else:
                pbTenQuantile = None
            ten_pbb_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb_daily WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
    (select 
    secuCode,
    case when @gid=secuCode then @rn:=@rn+1
        when @gid:=secuCode then @rn:=1 
     else @rn:=1 end rn,
    pbb
    from
    ( 
        select * from `sy_ctxt_main_pbb_daily` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and tradeDate>= '{}' and secuCode = '{}' order by pbb  

    ) as A) as c on b.secuCode = c.secuCode and b.pbb = c.pbb '''.format(tradeDate, secuCode, tradeDate,ten_trade_date, secuCode)
            cur.execute(ten_pbb_rn_sql)
            ten_pbb_rn_info = cur.fetchone()
            if ten_pbb_rn_info:
                ten_pbb_rn = ten_pbb_rn_info[0]
                if ten_count > 1:
                    pbbTenQuantile = (ten_pbb_rn - 1) / (ten_count - 1)
                    pbbTenQuantile = round(pbbTenQuantile * 100, 4)
                else:
                    pbbTenQuantile = 0
            else:
                pbbTenQuantile = None
            insert_sql = '''insert into sy_ctxt_main_assess_value_daily(chiName,innerCode,secuCode,tradeDate,totmktcap,
                            pe,peFiveQuantile,peTenQuantile,peQuantile,pb,pbFiveQuantile,pbTenQuantile,pbQuantile,
                            pbb,pbbFiveQuantile,pbbTenQuantile,pbbQuantile,isValid,createTime,dataState)
                            value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            cur.execute(insert_sql, (chiName,innerCode,secuCode,tradeDate,totmktcap,
                            pe,peFiveQuantile,peTenQuantile,peQuantile,pb,pbFiveQuantile,pbTenQuantile,pbQuantile,
                            pbb,pbbFiveQuantile,pbbTenQuantile,pbbQuantile,isValid,createTime,dataState))
            mysql_conn.commit()

    except Exception as e:
        print(e)
        print('插入表数据失败：',secuCode,tradeDate)

    print('插入表sy_ctxt_main_assess_value_daily数据成功',str(i), secuCode)
    i +=1

# secuCode = '600101'
# max_trade_date = '20200408'
# totmktcap_max_trade_date = '20200407'
# quantile_sql = '''SELECT chiName,innerCode,secuCode,tradeDate,totmktcap,pe,pb,pbb
# from sy_ctxt_main_pbb_daily where tradeDate <= '{}' and tradeDate > '{}' and secuCode='{}' '''.format(max_trade_date,totmktcap_max_trade_date,secuCode)

# # 历史所有分为点计算
# peFiveQuantile = None
# peTenQuantile = None
# pbFiveQuantile = None
# pbTenQuantile = None
# pbbFiveQuantile = None
# pbbTenQuantile = None
# cur.execute(quantile_sql)
# quantile_infos = cur.fetchall()
# for info in quantile_infos:
#     chiName = info[0]
#     # secuAbbr = info[1]
#     innerCode = info[1]
#     secuCode = info[2]
#     tradeDate = info[3]
#     totmktcap = info[4]
#     if totmktcap:
#         totmktcap = round(totmktcap*10000, 4)
#     pe = info[5]
#     pb = info[6]
#     pbb = info[7]
#     count_sql = '''SELECT count(*) as count,secuCode from sy_ctxt_main_pbb_daily
#     WHERE tradeDate <= '{}' and secuCode = '{}' '''.format(tradeDate, secuCode)
#     cur.execute(count_sql)
#     count_info = cur.fetchone()
#     count = count_info[0]
#     pbb_rn_sql = '''SELECT c.rn from (SELECT * FROM sy_ctxt_main_pbb_daily WHERE tradeDate = '{}' and secuCode = '{}') AS b INNER JOIN
#         (select 
#         secuCode,
#         case when @gid=secuCode then @rn:=@rn+1
#             when @gid:=secuCode then @rn:=1 
#          else @rn:=1 end rn,
#         pbb
#         from
#         ( 
#             select * from `sy_ctxt_main_pbb_daily` ,(select @gid:=null,@rn:=0 ) vars WHERE tradeDate <= '{}' and secuCode = '{}' order by pbb  

#         ) as A) as c on b.secuCode = c.secuCode and b.pbb = c.pbb '''.format(tradeDate, secuCode, tradeDate, secuCode)
#     cur.execute(pbb_rn_sql)
#     pbb_rn_info = max(cur.fetchall())
#     if pbb_rn_info:
#         pbb_rn = pbb_rn_info[0]
#         if count > 1:
#             pbbQuantile = (pbb_rn - 1) / (count - 1)
#             pbbQuantile = round(pbbQuantile * 100, 4)
#         else:
#             pbbQuantile = 0
#     else:
#         pbbQuantile = None
#     print(pbb_rn)
#     print(count)
#     print(pbbQuantile)
mysql_conn2.close()
mysql_conn192.close()
mysql_conn.close()
db.close()
