# coding:utf-8

'''
文件名：sy_ctxt_pettm.py
功能：长投学堂扣非利润
代码历史：20200707，邢冬梅
'''

import pymysql
import datetime
import time
import os
import json
from DBUtils.PooledDB import PooledDB
import traceback 
# 读取连接数据库配置文件
load_config='/home/seeyii/increase_nlp/db_config.json'
with open(load_config, 'r', encoding="utf-8") as f:
    reader = f.readlines()[0]
config_local = json.loads(reader)

# 129csc_risk，写log
pool = PooledDB(creator=pymysql, **config_local['local_sql_csc_pool'])
db = pool.connection()
cursor = db.cursor()

# 129sy_project_raw，写数据
pool2 = PooledDB(creator=pymysql, **config_local['local_sql_project_raw_pool'])
mysql_conn = pool2.connection()
cur = mysql_conn.cursor()

# 连接一部db_seeyii，取数据
pool3 = PooledDB(creator=pymysql, **config_local['fsp_event_pool'])
mysql_conn2 = pool3.connection()
cur2 = mysql_conn2.cursor()

isValid = 1  # 是否有效，0：否，1：是
createTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))  # sql操作时间
dataState = 1  # 数据状态:1->新增;2->更新;3->删除
logging = True
table_name = 'sy_ctxt_pettm'

# 写日志的表名
mysql_log_name = 'sy_ctxt_data_log'

# 插入数据语句
insert_sql = "insert into sy_ctxt_pettm (chiName, secuCode,innerCode,tradeDate,npCut,lastYearNpCut1231,lastYearNpCut,totmktcap,newNpCut,pettm,endDate,isValid,createTime,dataState) value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

# ==================================================================================================================== #
# 交易日
def fun_tradeDate():
    trade_sql = "SELECT max(tradeDate) FROM sy_ctxt_main_totmktcap"
    cur.execute(trade_sql)
    tradeDate = cur.fetchone()[0]
    return tradeDate

# ==================================================================================================================== #
# 更新数据源
def seeyii_to_project():
    """从数据源表取数据导入本地"""
    # promain_sql = '''SELECT * FROM sq_fin_promain WHERE EndDate=(SELECT max(EndDate) FROM sq_fin_promain)'''
    promain_sql = '''SELECT * FROM sq_fin_promain'''
    cur2.execute(promain_sql)
    promain_infos = cur2.fetchall()

    truncate_sql = "truncate table sq_fin_promain_npcut"
    cur.execute(promain_sql)
    mysql_conn.commit()

    promain_insert_sql = '''insert into sq_fin_promain_npcut(id,innerCode,publishDate,endDate,reportType,accstaCode,industryType,netCapLamt,netWgtRiskyAsset,capAdeRate,badLoanRto5,badLoanProvRto5,badLoanCovRto5,avgRoe1,avgRoa,totDebt,shareHolderEqt,bizIncome,netProfit,nPParComOwners,costIncRto,netIntrdiff,netAssGrowRate,mainbusIncGrowRate,netIncGrowRate,totAssGrowRate,mainBusiIncome,totalAssets,totalLiab,operRevenue,ncoret1cap2013,nt1cap2013,coret1car2013,t1car2013,lendAndLoanCust,totAsset,clieDepo,totsharEqui,epsFullDiluted,epsBasic,naps,ebitdasCover,currentRt,assliabRt,ndebt,accRecgTurnRt,invTurnRt,taTurnRt,nonIntrInc,ncap,na,ncaptona,ncaptodebt,natodebt,npcut,roeWeighted,roeWeightedCut,sgpmargin,saleGrossProfitRto,operriskres,quickRt,roeDiluted,roeDilutedCut,cashEquFinBal,cashNetr,finNetCflow,invNetCashFlow,snpMargInconms,opProRT,mgtexpRT,opExpRT,finlExpRT,tdebt,peropeCashPerShare,fcff,insertTime,_MASK_FROM_V2
    )
                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    try:
        cur.executemany(promain_insert_sql, promain_infos)
        mysql_conn.commit()
        print('插入sq_fin_promain_npcut数据成功')

#         select_sql = """select count(*) FROM sq_fin_promain_npcut WHERE (id, innerCode) in (
# select A.id, A.innerCode from  (
# select id, innerCode FROM sq_fin_promain_npcut WHERE innerCode in (Select innerCode From sq_fin_promain_npcut Group By innerCode, endDate Having Count(*)>1) and id in (Select id From sq_fin_promain_npcut Group By innerCode, endDate Having Count(*)>1))  as A) 
# """
#         cur.execute(select_sql)
#         select_num = cur.fetchone()[0]
#         if int(select_num) >=1:
#             delete_sql = """delete FROM sq_fin_promain_npcut WHERE (id, innerCode) in (
# select A.id, A.innerCode from  (
# select id, innerCode FROM sq_fin_promain_npcut WHERE innerCode in (Select innerCode From sq_fin_promain_npcut Group By innerCode, endDate Having Count(*)>1) and id in (Select id From sq_fin_promain_npcut Group By innerCode, endDate Having Count(*)>1))  as A) 
# """
#             cur.execute(delete_sql)
#             mysql_conn.commit()
#             print("删除重复数据{}条".format(select_num))

    except Exception as e:
        print('错误信息---',e)

# ==================================================================================================================== #
# 增量数据
def daily_data():
    """每日增量数据"""
    tradeDate = fun_tradeDate()
    trade_sql = "SELECT max(tradeDate) FROM sy_ctxt_pettm"
    cur.execute(trade_sql)
    pettm_tradeDate = cur.fetchone()[0]
    # 判断pettm的交易是否是交易表的最新交易日
    if pettm_tradeDate == tradeDate:
        get_time()
        exit()

    # 日增量 # 当年或者去年第一三季度扣非利润非空（含有2019年年报为空）

    select_sql = """
    select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.npCut, e.lastYearnpCut1231, e.lastYearnpCut,   (d.totmktcap*10000) as totmktcap ,e.newNpCut,((d.totmktcap*10000)/e.newNpCut)as pettm, e.endDate from (SELECT A.chiName, A.secuAbbr, A.secuCode, A.innerCode, A.companyCode,A.tradeDate,A.totmktcap, B.min, B.max  FROM (select * from sy_ctxt_main_totmktcap WHERE tradeDate >= '{}') AS A INNER JOIN (
    select  aa.innerCode,MAX(aa.publishDate) AS max, MIN(aa.publishDate) AS min FROM (select * from sq_fin_promain_npcut where endDate >= '20200331') as aa where (
            SELECT count(1) FROM (select * from sq_fin_promain_npcut  where endDate >= '20200331') as bb WHERE aa.innerCode=bb.innerCode and bb.endDate >= aa.endDate
    )<=2  GROUP BY aa.innerCode) AS B  ON A.innerCode = B.innerCode WHERE A.tradeDate >= '{}') as d inner join (select h.innerCode,h.endDate,h.npCut, h.lastYearnpCut1231,  h.lastYearnpCut, h.newNpCut from (select a.innerCode, a.endDate, a.npCut as npCut, (case when substring(a.endDate, 5,8)='1231' then 0.0 else b.npCut end) as lastYearnpCut1231, cnpCut as lastYearnpCut,
    (a.npCut + (case when substring(a.endDate, 5,8)='1231' then 0.0 else b.npCut end) - cnpCut)as newNpCut, (case  when  month(a.endDate)in ('03','09') and (a.npCut is not NULL and  a.npCut != 0.0 and  cnpCut is not NULL and  cnpCut != 0.0) THEN 'Full' when month(a.endDate)in ('12','06') THEN 'Full' ELSE 'Null' END) as SendDate  from 
            
            (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = (SELECT max(endDate) FROM sq_fin_promain_npcut where innerCode=A.innerCode))as a,
            
            (SELECT B.innerCode, B.endDate, B.npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1','1231') FROM sq_fin_promain_npcut where innerCode=B.innerCode))as b,
            
            (SELECT C.innerCode, C.endDate, (case when substring(C.endDate, 5, 8)='1231' then 0.0 else C.npCut end) as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1',substring(max(endDate),5,8)) FROM sq_fin_promain_npcut where innerCode=C.innerCode))as c 
            
            WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode) as h WHERE SendDate = 'Full')as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null) as g on f.innerCode=g.innerCode
    """.format(isValid, createTime, dataState, tradeDate, tradeDate)
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    cur.executemany(insert_sql, select_infos)
    mysql_conn.commit()
    print('20191231至当正常数据已完成写入')

    # 当年或者去年第一三季度扣非利润有空（含有2019年年报为空）
    select_sql = """
        select aa.innerCode from  (select innerCode from sy_ctxt_main_totmktcap group by  innerCode)    as aa inner join  (select h.innerCode  from (select a.innerCode, a.endDate, a.npCut as npCut, (case when substring(a.endDate, 5,8)='1231' then 0.0 else b.npCut end) as lastYearnpCut1231, cnpCut as lastYearnpCut,
        (a.npCut + (case when substring(a.endDate, 5,8)='1231' then 0.0 else b.npCut end) - cnpCut)as newNpCut, (case  when  month(a.endDate)in ('03','09') and (a.npCut is not NULL and  a.npCut != 0.0 and  cnpCut is not NULL and  cnpCut != 0.0) THEN 'Full' when month(a.endDate)in ('12','06') THEN 'Full' ELSE 'Null' END) as SendDate  from 
                
                (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = (SELECT max(endDate) FROM sq_fin_promain_npcut where innerCode=A.innerCode))as a,
                
                (SELECT B.innerCode, B.endDate, B.npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1','1231') FROM sq_fin_promain_npcut where innerCode=B.innerCode))as b,
                
                (SELECT C.innerCode, C.endDate, (case when substring(C.endDate, 5, 8)='1231' then 0.0 else C.npCut end) as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1',substring(max(endDate),5,8)) FROM sq_fin_promain_npcut where innerCode=C.innerCode))as c 
                
                WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode) as h WHERE SendDate = 'Null') as bb on aa.innerCode=bb.innerCode
        """
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    print('2020有特殊数据',str(len(select_infos)))
    if len(select_infos) >=1:
        # 判断上年年报是否存在
        NULL_20191231_innerCode_daily(tuple([','.join(_) for _ in select_infos]))
    # 去重
    # delete_sql = """DELETE FROM sy_ctxt_pettm where id in (   select A.id FROM (select * from sy_ctxt_pettm where tradeDate >='{}')as A WHERE A.id NOT IN (SELECT  dt.minno FROM ( SELECT MIN(id) AS minno FROM sy_ctxt_pettm where tradeDate>='{}'group BY innerCode) dt))""".format(tradeDate, tradeDate)
    delete_sql = """DELETE FROM sy_ctxt_pettm where id in (   select A.id FROM (select * from sy_ctxt_pettm where tradeDate ='{}')as A WHERE A.id NOT IN (SELECT  dt.minno FROM ( SELECT MIN(id) AS minno FROM sy_ctxt_pettm where tradeDate='{}'group BY innerCode) dt))""".format(tradeDate, tradeDate)
    cur.execute(delete_sql)
    mysql_conn.commit()
    print("删除重复数据")
    num_sql = "select count(*) from sy_ctxt_pettm where tradeDate >='{}' ".format(tradeDate)
    cur.execute(num_sql)
    add_data_num = cur.fetchone()[0]

    # 判断是否有新数据
    if add_data_num == 0:
        print('There is no new data!')
        os._exit(0)

    if 3500 > add_data_num or add_data_num > 4500:
        print('ERORR数据入库数量异常')

    if logging:
        sql_ = '''INSERT INTO {0} (table_,
                            add_date, 
                           add_number
                           )VALUES(%s,%s,%s)'''
        cursor.execute(sql_.format(mysql_log_name),(table_name,
                                                    tradeDate,
                                                    str(add_data_num)
                                                    ))
        db.commit()

def NULL_data_daily(innerCodes, tradeDate):
    # 用于季报出现不存在时的特殊数据处理增量
    if len(innerCodes) <= 1:
        select_sql = """
    select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.npCut, e.lastYearnpCut1231, e.lastYearnpCut, (d.totmktcap*10000)as totmktcap,e.newNpCut,((d.totmktcap*10000)/e.newNpCut)as pettm, 
        e.endDate from (SELECT B.* from  (select BB.* from  (select * from sy_ctxt_main_totmktcap where tradeDate >='{}' and innerCode = '{}') as BB INNER JOIN
                
                        (select  aa.innerCode,MAX(aa.publishDate) AS max, MIN(aa.publishDate) AS min FROM (select * from sq_fin_promain_npcut where endDate >= '20200331') as aa where (
                SELECT count(1) FROM (select * from sq_fin_promain_npcut  where endDate >= '20200331' and innerCode = '{}') as bb WHERE aa.innerCode=bb.innerCode and bb.endDate >= aa.endDate
        )<=2  GROUP BY aa.innerCode)as CC ON BB.innerCode=CC.innerCode WHERE BB.tradeDate >= '{}'
                )as B 
        INNER JOIN (select a.innerCode  from (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = (SELECT max(endDate) FROM sq_fin_promain_npcut where innerCode=A.innerCode))as a,(SELECT B.innerCode, B.endDate, B.npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1','1231') FROM sq_fin_promain_npcut where innerCode=B.innerCode))as b,
         (SELECT C.innerCode, C.endDate, (case when substring(C.endDate, 5, 8)='1231' then 0.0 else C.npCut end) as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1',substring(max(endDate),5,8)) FROM sq_fin_promain_npcut where innerCode=C.innerCode))as c 
         WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode and (a.npCut is NULL or  a.npCut = 0.0 or  c.cnpCut is NULL or  c.cnpCut = 0.0 )) as A 
         ON A.innerCode=B.innerCode) as d left join (select a.innerCode, a.endDate, a.npCut as npCut, (case when substring(a.endDate, 5,8)='1231' then 0.0 else b.npCut end) as  lastYearnpCut1231, cnpCut as lastYearnpCut,
        ((case when month(a.endDate)='03' then 0.0 ELSE a.npCut end ) + b.npCut - (case when month(a.endDate)='03' then 0.0 ELSE cnpCut end))as newNpCut  from 
                
                (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = (SELECT (case when month(max(endDate))='09' then CONCAT(substring(max(endDate),1,4),'0930') else max(endDate) end)  FROM sq_fin_promain_npcut where innerCode=A.innerCode) and A.innerCode = '{}')as a,
                
                (SELECT B.innerCode, B.endDate, B.npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1','1231') FROM sq_fin_promain_npcut where innerCode=B.innerCode))as b,
                
                (SELECT C.innerCode, C.endDate, (case when substring(C.endDate, 5, 8)='1231' then 0.0 else C.npCut end) as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1',substring((case when month(max(endDate))='09' then '00000930' else max(endDate) end),5,8)) FROM sq_fin_promain_npcut where innerCode=C.innerCode))as c WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode
)as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null) as g on f.innerCode=g.innerCode""".format(isValid, createTime, dataState,tradeDate, str(innerCodes[0]),str(innerCodes[0]),tradeDate,str(innerCodes[0]))
    else:
        select_sql = """
        select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.npCut, e.lastYearnpCut1231, e.lastYearnpCut, (d.totmktcap*10000)as totmktcap,e.newNpCut,((d.totmktcap*10000)/e.newNpCut)as pettm, 
            e.endDate from (SELECT B.* from  (select BB.* from  (select * from sy_ctxt_main_totmktcap where tradeDate >='{}' and innerCode in {}) as BB INNER JOIN
                    
                            (select  aa.innerCode,MAX(aa.publishDate) AS max, MIN(aa.publishDate) AS min FROM (select * from sq_fin_promain_npcut where endDate >= '20200331') as aa where (
                    SELECT count(1) FROM (select * from sq_fin_promain_npcut  where endDate >= '20200331' and innerCode in {}) as bb WHERE aa.innerCode=bb.innerCode and bb.endDate >= aa.endDate
            )<=2  GROUP BY aa.innerCode)as CC ON BB.innerCode=CC.innerCode WHERE BB.tradeDate >= '{}'
                    )as B 
            INNER JOIN (select a.innerCode  from (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = (SELECT max(endDate) FROM sq_fin_promain_npcut where innerCode=A.innerCode))as a,(SELECT B.innerCode, B.endDate, B.npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1','1231') FROM sq_fin_promain_npcut where innerCode=B.innerCode))as b,
             (SELECT C.innerCode, C.endDate, (case when substring(C.endDate, 5, 8)='1231' then 0.0 else C.npCut end) as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1',substring(max(endDate),5,8)) FROM sq_fin_promain_npcut where innerCode=C.innerCode))as c 
             WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode and (a.npCut is NULL or  a.npCut = 0.0 or  c.cnpCut is NULL or  c.cnpCut = 0.0 )) as A 
             ON A.innerCode=B.innerCode) as d left join (select a.innerCode, a.endDate, a.npCut as npCut, (case when substring(a.endDate, 5,8)='1231' then 0.0 else b.npCut end) as  lastYearnpCut1231, cnpCut as lastYearnpCut,
            ((case when month(a.endDate)='03' then 0.0 ELSE a.npCut end ) + b.npCut - (case when month(a.endDate)='03' then 0.0 ELSE cnpCut end))as newNpCut  from 
                    
                    (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = (SELECT (case when month(max(endDate))='09' then CONCAT(substring(max(endDate),1,4),'0930') else max(endDate) end)  FROM sq_fin_promain_npcut where innerCode=A.innerCode) and A.innerCode in {})as a,
                    
                    (SELECT B.innerCode, B.endDate, B.npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1','1231') FROM sq_fin_promain_npcut where innerCode=B.innerCode))as b,
                    
                    (SELECT C.innerCode, C.endDate, (case when substring(C.endDate, 5, 8)='1231' then 0.0 else C.npCut end) as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1',substring((case when month(max(endDate))='09' then '00000930' else max(endDate) end),5,8)) FROM sq_fin_promain_npcut where innerCode=C.innerCode))as c WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode
    )as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null) as g on f.innerCode=g.innerCode

        """.format(isValid, createTime, dataState,tradeDate, str(innerCodes),str(innerCodes),tradeDate,str(innerCodes))
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    cur.executemany(insert_sql, select_infos)
    mysql_conn.commit()
    print('{0}特殊数据已完成写入'.format(tradeDate))
    return len(select_infos)

def NULL_20191231_innerCode_daily(innerCodes):
    # 确定20191231扣非为空
    # 查询2019年年报为空的公司
    tradeDate = fun_tradeDate()
    select_sql = """
        SELECT B.innerCode FROM sq_fin_promain_npcut AS B WHERE B.endDate ='20191231' and B.npCut is NULL and B.innerCode in (select innerCode from sy_ctxt_main_totmktcap GROUP BY innerCode) 
    """
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    # 删掉库里2019年年报为空的数据
    pettm_sql = """delete from sy_ctxt_pettm where innerCode in {} and tradeDate='{}' """.format(tuple([','.join(_) for _ in select_infos]), tradeDate)
    cur.execute(pettm_sql)
    mysql_conn.commit()
    null_list = tuple([','.join(_) for _ in select_infos])  # 2019年年报是null数据
    nums = quarterly_reports_20200331_daily(null_list)
    # -----------------------------
    # 有2019年年报,无当年或去年1季度或3季度数据
    select_sql = """
        SELECT B.innerCode FROM sq_fin_promain_npcut AS B WHERE B.endDate ='20191231' and B.npCut is not NULL and B.innerCode in (select innerCode from sy_ctxt_main_totmktcap where innerCode in {} GROUP BY innerCode) 
    """.format(str(innerCodes))
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    innerCodes = tuple([','.join(_) for _ in select_infos])
    data_num = NULL_data_daily(innerCodes,tradeDate)

    nums +=data_num
    return nums

def quarterly_reports_20200331_daily(innerCodes):
    """2019年年报为空,2020年第一季度特殊处理"""
    endDate = '20200331'
    # 2019年年报为空，2019年3季报存在的公司
    select_sql = """SELECT B.innerCode FROM sq_fin_promain_npcut AS B WHERE B.endDate ='20190930' and B.npCut is not NULL and B.innerCode in {} """.format(str(innerCodes))
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    full_list = tuple([','.join(_) for _ in select_infos])
    null_list = tuple([i for i in innerCodes if i not in full_list])
    # 无20190930 扣非
    num = quarterly_reports_lastyear0930_daily(null_list)
    # 含有20190930 扣非
    remove_NULL_20191231(endDate,str(full_list))
    select_sql = """
            select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.npCut as npCut, e.lastYearnpCut1231 as lastYearnpCut1231, e.lastYearnpCut as lastYearnpCut,  (d.totmktcap*10000)as totmktcap, e.newNpCut, ((d.totmktcap*10000)/e.newNpCut)as pettm, e.endDate from (SELECT A.chiName, A.secuAbbr, A.secuCode, A.innerCode, A.companyCode,A.tradeDate,A.totmktcap, B.min, B.max  FROM (select * from sy_ctxt_main_totmktcap where innerCode in {}) AS A INNER JOIN (
        select  aa.innerCode,MAX(aa.publishDate) AS max, MIN(aa.publishDate) AS min FROM (select * from sq_fin_promain_npcut where endDate = '20200331') as aa where (
                SELECT count(1) FROM (select * from sq_fin_promain_npcut where endDate = '20200331') as bb WHERE aa.innerCode=bb.innerCode and bb.endDate >= aa.endDate
        )<=2  GROUP BY aa.innerCode ) AS B  ON A.innerCode = B.innerCode WHERE A.tradeDate >= '{}') as d inner join (select a.innerCode, '20200331' as endDate, a.npCut as npCut, (a.npCut+b.npCut) as lastYearnpCut1231, cnpCut as lastYearnpCut,
        (a.npCut + (a.npCut+b.npCut)- cnpCut)as newNpCut  from 
                
                (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = '20200331' and
                            
                            innerCode in {})as a,
                
                (SELECT B.innerCode, B.endDate, (case when B.npCut is NULL then 0.0 ELSE B.npCut END)as npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate ='20190930')as b,
                
                (SELECT C.innerCode, C.endDate, (case when C.npCut is NULL then 0.0 ELSE C.npCut END)as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate ='20190331')as c 
                
                WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode)as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null) as g on f.innerCode=g.innerCode
            """.format(isValid, createTime, dataState,str(full_list),fun_tradeDate(), str(full_list))

    cur.execute(select_sql)
    select_infos = cur.fetchall()
    cur.executemany(insert_sql, select_infos)
    mysql_conn.commit()
    print('2019年年报为空,2020年第一季度特殊处理,临时更改数据已经完成写入，有{0}'.format(len(select_infos)))
    num += len(select_infos)
    return num

def quarterly_reports_lastyear0930_daily(innerCodes):
    # 针对2019年 没有2019年年报且没有2019年3季报，2018年半年报存在
    select_sql = "SELECT B.innerCode FROM sq_fin_promain_npcut AS B WHERE B.endDate ='20180630' and B.npCut is not NULL and B.innerCode in {} ".format(str(innerCodes))
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    full_list = tuple([','.join(_) for _ in select_infos])
    null_list = tuple([i for i in innerCodes if i not in full_list])
    # 含有2018年半年报
    tradeDate = fun_tradeDate()
    remove_NULL_20190930(tradeDate, full_list)
    select_sql = """ select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.npCut as npCut, e.lastYearnpCut1231 as lastYearnpCut1231, e.lastYearnpCut as lastYearnpCut,  (d.totmktcap*10000)as totmktcap, e.newNpCut, ((d.totmktcap*10000)/e.newNpCut)as pettm, e.endDate from (SELECT A.chiName, A.secuAbbr, A.secuCode, A.innerCode, A.companyCode,A.tradeDate,A.totmktcap, B.min, B.max  FROM (select * from sy_ctxt_main_totmktcap where innerCode in {}) AS A INNER JOIN (
                select  aa.innerCode,MAX(aa.publishDate) AS max, MIN(aa.publishDate) AS min FROM (select * from sq_fin_promain_npcut where endDate = '20200331') as aa where (
                        SELECT count(1) FROM (select * from sq_fin_promain_npcut where endDate = '20200331') as bb WHERE aa.innerCode=bb.innerCode and bb.endDate >= aa.endDate
                )<=2  GROUP BY aa.innerCode ) AS B  ON A.innerCode = B.innerCode WHERE A.tradeDate >= '{}') as d inner join (select a.innerCode, '20200331' as endDate, (case when cnpCut = 0 then 0.0 else a.npCut end) as npCut, b.npCut as lastYearnpCut1231, cnpCut as lastYearnpCut,
                (a.npCut + b.npCut- cnpCut)as newNpCut  from 
                        
                        (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = '20190630' and
                                    
                                    innerCode in {})as a,
                        
                        (SELECT B.innerCode, B.endDate, (case when B.npCut is NULL then 0.0 ELSE B.npCut END)as npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate ='20181231')as b,
                        
                        (SELECT C.innerCode, C.endDate, (case when C.npCut is NULL then 0.0 ELSE C.npCut END)as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate ='20180630')as c 
                        
                WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode)as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null) as g on f.innerCode=g.innerCode
                """.format(isValid, createTime, dataState, str(full_list), tradeDate, str(full_list))

    cur.execute(select_sql)
    select_infos = cur.fetchall()
    cur.executemany(insert_sql, select_infos)
    mysql_conn.commit()
    print('2019年年报为空,20190930为空的数据特殊处理,临时更改数据已经完成写入，有{0}'.format(len(select_infos)))
    num = len(select_infos)
    # 不含有2018年半年报
    remove_NULL_20190930(tradeDate, null_list)
    select_sql = """ select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.AnpCut as npCut, e.lastYearnpCut1231 as lastYearnpCut1231, e.lastYearnpCut as lastYearnpCut,  (d.totmktcap*10000)as totmktcap, e.newNpCut, ((d.totmktcap*10000)/e.newNpCut)as pettm, e.endDate from (SELECT A.chiName, A.secuAbbr, A.secuCode, A.innerCode, A.companyCode,A.tradeDate,A.totmktcap, B.min, B.max  FROM (select * from sy_ctxt_main_totmktcap where innerCode in {}) AS A INNER JOIN (
                select  aa.innerCode,MAX(aa.publishDate) AS max, MIN(aa.publishDate) AS min FROM (select * from sq_fin_promain_npcut where endDate = '20200331') as aa where (
                        SELECT count(1) FROM (select * from sq_fin_promain_npcut where endDate = '20200331') as bb WHERE aa.innerCode=bb.innerCode and bb.endDate >= aa.endDate
                )<=2  GROUP BY aa.innerCode ) AS B  ON A.innerCode = B.innerCode WHERE A.tradeDate >= '{}') as d inner join (select innerCode, '20200331' as endDate, 0.0 as AnpCut, npCut as lastYearnpCut1231, 0.0 as lastYearnpCut,
                npCut as newNpCut  from sq_fin_promain_npcut WHERE endDate ='20181231')as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null) as g on f.innerCode=g.innerCode
                """.format(isValid, createTime, dataState, str(null_list), tradeDate)
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    cur.executemany(insert_sql, select_infos)
    mysql_conn.commit()
    print('2019年年报为空,20190930为空,2018年半年报为空的数据特殊处理,临时更改数据已经完成写入，有{0}'.format(len(select_infos)))
    num += len(select_infos)

    return num

# ==================================================================================================================== #
# 历史数据
def insert_pettm():
    select_sql = """ select chiName,secuCode,innerCode,tradeDate,npCut,lastYearNpCut1231,lastYearNpCut,totmktcap,newNpCut,pettm,endDate,isValid,createTime,dataState from  sy_ctxt_pettm WHERE modifyUpdateTime >'2020-08-04'"""
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    insert_sql = "insert into `sy_ctxt_pettm_正式表` (chiName, secuCode,innerCode,tradeDate,npCut,lastYearNpCut1231,lastYearNpCut,totmktcap,newNpCut,pettm,endDate,isValid,createTime,dataState) value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cur.executemany(insert_sql, select_infos)
    mysql_conn.commit()
    print('sy_ctxt_pettm_正式表入库{}条'.format(len(select_infos)))

def history_data():
    """计算历史数据"""
    for i in range(1,11):
        endDate = time.strftime("%Y0331", time.localtime(time.time()- (1546185600 - 1514736000)*(i-1)))
        endDate12 = time.strftime("%Y1231", time.localtime(time.time() - (1546185600 - 1514736000)*i )) # 上一年12月31日 例:20191231
        endDate03 = time.strftime("%Y0331", time.localtime(time.time() - (1546185600 - 1514736000)*i))  # 上一年03月31日 例:20190331
        endDate06 = time.strftime("%Y0630", time.localtime(time.time() - (1546185600 - 1514736000)*i )) # 上一年06月30日 例:20190630
        endDate09 = time.strftime("%Y0930", time.localtime(time.time() - (1546185600 - 1514736000)*i )) # 上一年09月30日 例:20190930
        endDatel12 = time.strftime("%Y1231", time.localtime(time.time() - (1546185600 - 1514736000) * (i+1))) # 上两年12月31日 例:20181231
        endDatel03 = time.strftime("%Y0331", time.localtime(time.time() - (1546185600 - 1514736000) * (i+1))) # 上两年03月31日 例:20180331
        endDatel06 = time.strftime("%Y0630", time.localtime(time.time() - (1546185600 - 1514736000) * (i+1))) # 上两年06月30日 例:20180630
        endDatel09 = time.strftime("%Y0930", time.localtime(time.time() - (1546185600 - 1514736000) * (i+1))) # 上两年09月30日 例:20180930 

        # 当计算时间为年报发布后，扣非净利润TTM直接按表取值即可
        year_sql = "select c.chiName, c.secuCode, c.innerCode, c.tradeDate, d.npCut, 0.0 as lastYearNpCut1231, 0.0 as lastYearNpCut,(c.totmktcap*10000)as totmktcap,d.npCut,((c.totmktcap*10000)/d.npCut)as pettm, d.endDate,'{}', '{}', '{}'  from (select a.chiName, a.secuCode, a.innerCode, b.tradeDate, b.totmktcap  FROM (select * from sy_ctxt_basic ) as a right JOIN (select * from sy_ctxt_main_totmktcap where tradeDate < '{}' and tradeDate >= '{}') as b ON a.innerCode=b.innerCode)as c left join (SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as d on d.innerCode=c.innerCode WHERE c.innerCode is not null".format(isValid, createTime, dataState,endDate, endDate12,endDate12)  
        cur.execute(year_sql)
        year_infos = cur.fetchall()
        cur.executemany(insert_sql, year_infos)
        mysql_conn.commit()
        print('年报',endDate12)

        # 当计算时间为一季报发布后、半年报发布前，扣非净利润TTM=一季报中的扣非净利润+上年度年报中的扣非净利润-上年一季报中的扣非净利润
        quarter_sql = "select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.npCut, e.lastYearnpCut1231, e.lastYearnpCut, (d.totmktcap*10000)as totmktcap,e.newNpCut,((d.totmktcap*10000)/e.newNpCut)as pettm, e.endDate from (select * from sy_ctxt_main_totmktcap where tradeDate >= '{}' and tradeDate < '{}') as d inner join (select a.innerCode, a.endDate, a.npCut as npCut, b.npCut as lastYearnpCut1231, c.npCut as lastYearnpCut, (a.npCut+b.npCut -c.npCut)as newNpCut  from (SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate < '20200331' and endDate ='{}')as a,(SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as b,(SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as c WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode and (a.npCut is not NULL and  a.npCut != 0.0 and  c.npCut is not NULL and  c.npCut != 0.0))as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null) as g on f.innerCode=g.innerCode ".format(isValid, createTime, dataState ,endDate03,endDate06,endDate03,endDatel12,endDatel03)
        cur.execute(quarter_sql)
        quarter_infos = cur.fetchall()
        cur.executemany(insert_sql, quarter_infos)
        mysql_conn.commit()
        print('一季报发布后、半年报发布前',endDate03)
        NULL_one_data(endDate03, endDate06, endDatel12, endDatel03)

        # 当计算时间为半年报发布后、三季报发布前，扣非净利润TTM=半年报中的扣非净利润+上年度年报中的扣非净利润-上年半年报的扣非净利润
        yearHalf_sql = "select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate,  e.npCut, e.lastYearnpCut1231, e.lastYearnpCut,  (d.totmktcap*10000)as totmktcap,e.newNpCut,((d.totmktcap*10000)/e.newNpCut)as pettm, e.endDate  from (select * from sy_ctxt_main_totmktcap where tradeDate >= '{}' and tradeDate < '{}') as d left join (select a.innerCode, a.endDate, a.npCut as npCut, b.npCut as lastYearnpCut1231, c.npCut as lastYearnpCut, (a.npCut+b.npCut -c.npCut)as newNpCut  from (SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate < '20200331' and endDate ='{}')as a,(SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as b,(SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as c WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode)as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null ) as g on f.innerCode=g.innerCode".format(isValid, createTime, dataState ,endDate06,endDate09,endDate06,endDatel12,endDatel06 )
        cur.execute(yearHalf_sql)
        yearHalf_infos = cur.fetchall()
        cur.executemany(insert_sql, yearHalf_infos)
        mysql_conn.commit()
        print('半年报发布后、三季报发布前',endDate06)

        # 当计算时间为三季报发布后、年报发布前，扣非净利润TTM=三季报中的扣非净利润+上年度年报中的扣非净利润-上年三季报的扣非净利润
        quarterThre_sql = "select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.npCut, e.lastYearnpCut1231, e.lastYearnpCut,  (d.totmktcap*10000)as totmktcap,e.newNpCut, ((d.totmktcap*10000)/e.newNpCut)as pettm, e.endDate from (select * from sy_ctxt_main_totmktcap where tradeDate >= '{}' and tradeDate < '{}') as d inner join (select a.innerCode, a.endDate, a.npCut as npCut, b.npCut as lastYearnpCut1231, c.npCut as lastYearnpCut, (a.npCut+b.npCut -c.npCut)as newNpCut  from (SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate < '20200331' and endDate ='{}')as a,(SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as b,(SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as c WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode and (a.npCut is not NULL and  a.npCut != 0.0 and  c.npCut is not NULL and  c.npCut != 0.0))as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null ) as g on f.innerCode=g.innerCode".format(isValid, createTime, dataState ,endDate09,endDate12,endDate09,endDatel12,endDatel09)
        cur.execute(quarterThre_sql)
        quarterThre_infos = cur.fetchall()
        cur.executemany(insert_sql, quarterThre_infos)
        mysql_conn.commit()
        print('三季报发布后、年报发布前',endDate09)
        NULL_three_data(endDate09, endDate12, endDate06, endDatel12, endDatel06, endDatel09)

def annual_report_2019(): 
    """2019年报数据特殊处理""" 
    # todo 数据是从20200709开始写入，2019年数据按照历史数据方式进行计算，20200331季报发布日期之前均按照2019年年报方式进行计算,再次声明使用以下这种方式，删掉2019年年报TTM，重新按照2019年年报日期-20200331发布日期这段时间取市值进行TTM计算
    del_2019 = """ DELETE FROM sy_ctxt_pettm WHERE endDate='20191231' """
    cur.execute(del_2019)
    mysql_conn.commit()
    print('删除2019年年报TTM')

    sql_2019 = """
    select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.newNpCut as npCut, 0.0 as lastYearnpCut1231, 0.0 as lastYearnpCut,  (d.totmktcap*10000)as totmktcap, e.newNpCut, ((d.totmktcap*10000)/e.newNpCut)as pettm, e.endDate from (SELECT A.chiName, A.secuAbbr, A.secuCode, A.innerCode, A.companyCode,A.tradeDate,A.totmktcap, B.min, B.max  FROM (select * from sy_ctxt_main_totmktcap) AS A INNER JOIN (
        select  aa.innerCode,MAX(aa.publishDate) AS max, MIN(aa.publishDate) AS min FROM (select * from sq_fin_promain_npcut where endDate <= '20200331') as aa where (
                SELECT count(1) FROM (select * from sq_fin_promain_npcut where endDate <= '20200331') as bb WHERE aa.innerCode=bb.innerCode and bb.endDate >= aa.endDate
        )<=2  GROUP BY aa.innerCode ) AS B  ON A.innerCode = B.innerCode WHERE A.tradeDate < B.max  and tradeDate >= '20191231') as d left join (select a.innerCode, a.endDate, a.npCut as newNpCut  from (SELECT innerCode, endDate, npCut  FROM sq_fin_promain_npcut WHERE endDate ='20191231')as a)as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null) as g on f.innerCode=g.innerCode
    """.format(isValid, createTime, dataState)
    cur.execute(sql_2019)
    sql_2019_infos = cur.fetchall()
    cur.executemany(insert_sql, sql_2019_infos)
    mysql_conn.commit()
    print('添加2019年年报TTM')

def now_data():
    """20200331之后数据"""
    select_sql = """
        select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.npCut, e.lastYearnpCut1231, e.lastYearnpCut,   (d.totmktcap*10000) as totmktcap ,e.newNpCut,((d.totmktcap*10000)/e.newNpCut)as pettm, e.endDate from (SELECT A.chiName, A.secuAbbr, A.secuCode, A.innerCode, A.companyCode,A.tradeDate,A.totmktcap, B.min, B.max  FROM (select * from sy_ctxt_main_totmktcap where tradeDate >'20190930') AS A INNER JOIN (
        select  aa.innerCode,MAX(aa.publishDate) AS max, MIN(aa.publishDate) AS min FROM (select * from sq_fin_promain_npcut where endDate >= '20200331') as aa where (
                SELECT count(1) FROM (select * from sq_fin_promain_npcut  where endDate >= '20200331') as bb WHERE aa.innerCode=bb.innerCode and bb.endDate >= aa.endDate
        )<=2  GROUP BY aa.innerCode) AS B  ON A.innerCode = B.innerCode WHERE A.tradeDate >= B.max) as d left join (select h.innerCode,h.endDate,h.npCut, h.lastYearnpCut1231,  h.lastYearnpCut, h.newNpCut from (select a.innerCode, a.endDate, a.npCut as npCut, (case when substring(a.endDate, 5,8)='1231' then 0.0 else b.npCut end) as lastYearnpCut1231, cnpCut as lastYearnpCut,
        (a.npCut + (case when substring(a.endDate, 5,8)='1231' then 0.0 else b.npCut end) - cnpCut)as newNpCut, (case  when  month(a.endDate)in ('03','09') and (a.npCut is not NULL and  a.npCut != 0.0 and  cnpCut is not NULL and  cnpCut != 0.0) THEN 'Full' when month(a.endDate)in ('12','06') THEN 'Full' ELSE 'Null' END) as SendDate  from 
                
                (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = (SELECT max(endDate) FROM sq_fin_promain_npcut where innerCode=A.innerCode and endDate >= '20200331'))as a,
                
                (SELECT B.innerCode, B.endDate, B.npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1','1231') FROM sq_fin_promain_npcut where innerCode=B.innerCode and endDate >= '20200331'))as b,
                
                (SELECT C.innerCode, C.endDate, (case when substring(C.endDate, 5, 8)='1231' then 0.0 else C.npCut end) as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1',substring(max(endDate),5,8)) FROM sq_fin_promain_npcut where innerCode=C.innerCode and endDate >= '20200331'))as c 
                
                WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode) as h WHERE SendDate = 'Full')as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null) as g on f.innerCode=g.innerCode
        """.format(isValid, createTime, dataState)

    cur.execute(select_sql)
    select_infos = cur.fetchall()
    cur.executemany(insert_sql, select_infos)
    mysql_conn.commit()
    print('20191231至当正常数据已完成写入')

    select_sql = """
        select aa.innerCode from  (select innerCode from sy_ctxt_main_totmktcap group by  innerCode)    as aa inner join  (select h.innerCode  from (select a.innerCode, a.endDate, a.npCut as npCut, (case when substring(a.endDate, 5,8)='1231' then 0.0 else b.npCut end) as lastYearnpCut1231, cnpCut as lastYearnpCut,
        (a.npCut + (case when substring(a.endDate, 5,8)='1231' then 0.0 else b.npCut end) - cnpCut)as newNpCut, (case  when  month(a.endDate)in ('03','09') and (a.npCut is not NULL and  a.npCut != 0.0 and  cnpCut is not NULL and  cnpCut != 0.0) THEN 'Full' when month(a.endDate)in ('12','06') THEN 'Full' ELSE 'Null' END) as SendDate  from 
                
                (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = (SELECT max(endDate) FROM sq_fin_promain_npcut where innerCode=A.innerCode))as a,
                
                (SELECT B.innerCode, B.endDate, B.npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1','1231') FROM sq_fin_promain_npcut where innerCode=B.innerCode))as b,
                
                (SELECT C.innerCode, C.endDate, (case when substring(C.endDate, 5, 8)='1231' then 0.0 else C.npCut end) as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1',substring(max(endDate),5,8)) FROM sq_fin_promain_npcut where innerCode=C.innerCode))as c 
                
                WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode) as h WHERE SendDate = 'Null') as bb on aa.innerCode=bb.innerCode
        """
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    print('2020有特殊数据',str(len(select_infos)))
    NULL_data(tuple([','.join(_) for _ in select_infos]))

def now_data_1():
    """20200331之后数据"""
    select_sql = """
        select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.npCut, e.lastYearnpCut1231, e.lastYearnpCut,   (d.totmktcap*10000) as totmktcap ,e.newNpCut,((d.totmktcap*10000)/e.newNpCut)as pettm, e.endDate from (SELECT A.chiName, A.secuAbbr, A.secuCode, A.innerCode, A.companyCode,A.tradeDate,A.totmktcap, B.min, B.max  FROM (select * from sy_ctxt_main_totmktcap where tradeDate >'20190930' and  innerCode in ('10000688','80055878','80062991','80080276','80089643','80144126','80198868','80305808','80362355','80400098','80456174','80506418','81916255'
)) AS A INNER JOIN (
        select  aa.innerCode,MAX(aa.publishDate) AS max, MIN(aa.publishDate) AS min FROM (select * from sq_fin_promain_npcut where endDate >= '20200331') as aa where (
                SELECT count(1) FROM (select * from sq_fin_promain_npcut  where endDate >= '20200331') as bb WHERE aa.innerCode=bb.innerCode and bb.endDate >= aa.endDate
        )<=2  GROUP BY aa.innerCode) AS B  ON A.innerCode = B.innerCode WHERE A.tradeDate >= B.min and A.tradeDate < B.max) as d left join (select h.innerCode,h.endDate,h.npCut, h.lastYearnpCut1231,  h.lastYearnpCut, h.newNpCut from (select a.innerCode, a.endDate, a.npCut as npCut, (case when substring(a.endDate, 5,8)='1231' then 0.0 else b.npCut end) as lastYearnpCut1231, cnpCut as lastYearnpCut,
        (a.npCut + (case when substring(a.endDate, 5,8)='1231' then 0.0 else b.npCut end) - cnpCut)as newNpCut, (case  when  month(a.endDate)in ('03','09') and (a.npCut is not NULL and  a.npCut != 0.0 and  cnpCut is not NULL and  cnpCut != 0.0) THEN 'Full' when month(a.endDate)in ('12','06') THEN 'Full' ELSE 'Null' END) as SendDate  from 
                
                (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = (SELECT max(endDate) FROM sq_fin_promain_npcut where innerCode=A.innerCode and endDate = '20200331' and  innerCode in ('10000688','80055878','80062991','80080276','80089643','80144126','80198868','80305808','80362355','80400098','80456174','80506418','81916255'
)))as a,
                
                (SELECT B.innerCode, B.endDate, B.npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1','1231') FROM sq_fin_promain_npcut where innerCode=B.innerCode and endDate = '20200331'))as b,
                
                (SELECT C.innerCode, C.endDate, (case when substring(C.endDate, 5, 8)='1231' then 0.0 else C.npCut end) as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1',substring(max(endDate),5,8)) FROM sq_fin_promain_npcut where innerCode=C.innerCode and endDate = '20200331'))as c 
                
                WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode) as h WHERE SendDate = 'Full')as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null) as g on f.innerCode=g.innerCode
        """.format(isValid, createTime, dataState)

    cur.execute(select_sql)
    select_infos = cur.fetchall()
    cur.executemany(insert_sql, select_infos)
    mysql_conn.commit()
    print('20191231至当正常数据已完成写入')

    select_sql = """
        select aa.innerCode from  (select innerCode from sy_ctxt_main_totmktcap where innerCode in ('10000688','80055878','80062991','80080276','80089643','80144126','80198868','80305808','80362355','80400098','80456174','80506418','81916255'
        ) group by  innerCode)    as aa inner join  (select h.innerCode  from (select a.innerCode, a.endDate, a.npCut as npCut, (case when substring(a.endDate, 5,8)='1231' then 0.0 else b.npCut end) as lastYearnpCut1231, cnpCut as lastYearnpCut,
        (a.npCut + (case when substring(a.endDate, 5,8)='1231' then 0.0 else b.npCut end) - cnpCut)as newNpCut, (case  when  month(a.endDate)in ('03','09') and (a.npCut is not NULL and  a.npCut != 0.0 and  cnpCut is not NULL and  cnpCut != 0.0) THEN 'Full' when month(a.endDate)in ('12','06') THEN 'Full' ELSE 'Null' END) as SendDate  from 
                
                (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = (SELECT max(endDate) FROM sq_fin_promain_npcut where innerCode=A.innerCode))as a,
                
                (SELECT B.innerCode, B.endDate, B.npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1','1231') FROM sq_fin_promain_npcut where innerCode=B.innerCode))as b,
                
                (SELECT C.innerCode, C.endDate, (case when substring(C.endDate, 5, 8)='1231' then 0.0 else C.npCut end) as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1',substring(max(endDate),5,8)) FROM sq_fin_promain_npcut where innerCode=C.innerCode))as c 
                
                WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode) as h WHERE SendDate = 'Null' and h.innerCode in ('10000688','80055878','80062991','80080276','80089643','80144126','80198868','80305808','80362355','80400098','80456174','80506418','81916255'
        )) as bb on aa.innerCode=bb.innerCode
        """
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    print('2020有特殊数据',str(len(select_infos)))
    if len(select_infos) >= 1:
        NULL_data_1(tuple([','.join(_) for _ in select_infos]))

def NULL_three_data(endDate09, endDate12, endDate06, endDatel12, endDatel06, endDatel09):
    """个别第三季度和上一年第三季度为零时,进行转化使用半年季度报"""
    # 查询为null或0.0
    select_sql = """ SELECT B.* from  (select * from sy_ctxt_main_totmktcap where tradeDate >= '{}' and tradeDate < '{}')as B INNER JOIN (select a.innerCode  from (SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate < '20200331' and endDate ='{}')as a,(SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as b,(SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as c WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode and (a.npCut is NULL or  a.npCut = 0.0 or  c.npCut is NULL or  c.npCut = 0.0 )) as A ON A.innerCode=B.innerCode 
    """.format(endDate09, endDate12, endDate09, endDatel12, endDatel09)
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    print('第三季度为null有',str(len(select_infos)),'条')
    NULL_sql = """
        select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.npCut, e.lastYearnpCut1231, e.lastYearnpCut, (d.totmktcap*10000)as totmktcap,e.newNpCut,((d.totmktcap*10000)/e.newNpCut)as pettm, 
        e.endDate from (SELECT B.* from  (select * from sy_ctxt_main_totmktcap where tradeDate >= '{}' and tradeDate < '{}')as B 
        INNER JOIN (select a.innerCode  from (SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate < '20200331' and endDate ='{}')as a,(SELECT innerCode, endDate,
         (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as b,
         (SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as c 
         WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode and (a.npCut is NULL or  a.npCut = 0.0 or  c.npCut is NULL or  c.npCut = 0.0 )) as A 
         ON A.innerCode=B.innerCode) as d left join (select a.innerCode, a.endDate, a.npCut as npCut, b.npCut as lastYearnpCut1231, c.npCut as lastYearnpCut, (a.npCut+b.npCut-c.npCut) as newNpCut  from (SELECT innerCode, '{}' as endDate, (case when npCut is null then 0.0 else npCut end ) as npCut  FROM sq_fin_promain_npcut WHERE endDate < '20200331' and endDate ='{}')as a
         ,(SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as b,(SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as c
          WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode )as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null) as g on f.innerCode=g.innerCode
    """.format(isValid, createTime, dataState ,endDate09, endDate12, endDate09, endDatel12, endDatel09, endDate09, endDate06, endDatel12, endDatel06)
    cur.execute(NULL_sql)
    sql_NULL_infos = cur.fetchall()
    cur.executemany(insert_sql, sql_NULL_infos)
    mysql_conn.commit()
    print('第三季度为null已添加',str(len(sql_NULL_infos)))

def NULL_one_data(endDate03, endDate06, endDatel12, endDatel03):
    """个别第一季度和上一年第一季度为零时,进行转化使用年报"""
    # 查询为null或0.0
    select_sql = """ SELECT B.* from  (select * from sy_ctxt_main_totmktcap where tradeDate >= '{}' and tradeDate < '{}')as B INNER JOIN (select a.innerCode  from (SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate < '20200331' and endDate ='{}')as a,(SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as b,(SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as c WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode and (a.npCut is NULL or  a.npCut = 0.0 or  c.npCut is NULL or  c.npCut = 0.0 )) as A ON A.innerCode=B.innerCode 
    """.format(endDate03, endDate06, endDate03, endDatel12, endDatel03)
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    print('第一季度为null有',str(len(select_infos)),'条')
    NULL_sql = """
        select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.npCut, e.lastYearnpCut1231, e.lastYearnpCut, (d.totmktcap*10000)as totmktcap,e.newNpCut,((d.totmktcap*10000)/e.newNpCut)as pettm, 
        e.endDate from (SELECT B.* from  (select * from sy_ctxt_main_totmktcap where tradeDate >= '{}' and tradeDate < '{}')as B 
        INNER JOIN (select a.innerCode  from (SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate < '20200331' and endDate ='{}')as a,(SELECT innerCode, endDate,
         (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as b,
         (SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as c 
         WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode and (a.npCut is NULL or  a.npCut = 0.0 or  c.npCut is NULL or  c.npCut = 0.0 )) as A 
         ON A.innerCode=B.innerCode) as d left join (select a.innerCode, a.endDate, a.npCut as npCut, b.npCut as lastYearnpCut1231, c.npCut as lastYearnpCut, b.npCut as newNpCut  from (SELECT innerCode, '{}' as endDate, (case when npCut is null then 0.0 else npCut end ) as npCut  FROM sq_fin_promain_npcut WHERE endDate < '20200331' and endDate ='{}')as a
         ,(SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as b,(SELECT innerCode, endDate, (case when npCut is null then 0.0 else npCut end )as npCut  FROM sq_fin_promain_npcut WHERE endDate ='{}')as c
          WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode )as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null) as g on f.innerCode=g.innerCode
    """.format(isValid, createTime, dataState ,endDate03, endDate06, endDate03, endDatel12, endDatel03, endDate03, endDate03, endDatel12, endDatel03)
    cur.execute(NULL_sql)
    sql_NULL_infos = cur.fetchall()
    cur.executemany(insert_sql, sql_NULL_infos)
    mysql_conn.commit()
    print('第一季度为null已添加',str(len(sql_NULL_infos)))

def NULL_data(innerCodes):
    # 用于季报出现不存在时的特殊数据处理
    select_sql = """
    select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.npCut, e.lastYearnpCut1231, e.lastYearnpCut, (d.totmktcap*10000)as totmktcap,e.newNpCut,((d.totmktcap*10000)/e.newNpCut)as pettm, 
        e.endDate from (SELECT B.* from  (select BB.* from  (select * from sy_ctxt_main_totmktcap where tradeDate >='20191231' and innerCode in {}) as BB INNER JOIN
                
                        (select  aa.innerCode,MAX(aa.publishDate) AS max, MIN(aa.publishDate) AS min FROM (select * from sq_fin_promain_npcut where endDate >= '20200331') as aa where (
                SELECT count(1) FROM (select * from sq_fin_promain_npcut  where endDate >= '20200331' and innerCode in {}) as bb WHERE aa.innerCode=bb.innerCode and bb.endDate >= aa.endDate
        )<=2  GROUP BY aa.innerCode)as CC ON BB.innerCode=CC.innerCode WHERE BB.tradeDate >= CC.max
                )as B 
        INNER JOIN (select a.innerCode  from (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = (SELECT max(endDate) FROM sq_fin_promain_npcut where innerCode=A.innerCode))as a,(SELECT B.innerCode, B.endDate, B.npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1','1231') FROM sq_fin_promain_npcut where innerCode=B.innerCode))as b,
         (SELECT C.innerCode, C.endDate, (case when substring(C.endDate, 5, 8)='1231' then 0.0 else C.npCut end) as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1',substring(max(endDate),5,8)) FROM sq_fin_promain_npcut where innerCode=C.innerCode))as c 
         WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode and (a.npCut is NULL or  a.npCut = 0.0 or  c.cnpCut is NULL or  c.cnpCut = 0.0 )) as A 
         ON A.innerCode=B.innerCode) as d left join (select a.innerCode, a.endDate, a.npCut as npCut, (case when substring(a.endDate, 5,8)='1231' then 0.0 else b.npCut end) as  lastYearnpCut1231, cnpCut as lastYearnpCut,
        ((case when month(a.endDate)='03' then 0.0 ELSE a.npCut end ) + b.npCut - (case when month(a.endDate)='03' then 0.0 ELSE cnpCut end))as newNpCut  from 
                
                (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = (SELECT (case when month(max(endDate))='09' then CONCAT(substring(max(endDate),1,4),'0930') else max(endDate) end)  FROM sq_fin_promain_npcut where innerCode=A.innerCode) and A.innerCode in {})as a,
                
                (SELECT B.innerCode, B.endDate, B.npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1','1231') FROM sq_fin_promain_npcut where innerCode=B.innerCode))as b,
                
                (SELECT C.innerCode, C.endDate, (case when substring(C.endDate, 5, 8)='1231' then 0.0 else C.npCut end) as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1',substring((case when month(max(endDate))='09' then '00000930' else max(endDate) end),5,8)) FROM sq_fin_promain_npcut where innerCode=C.innerCode))as c WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode
)as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null) as g on f.innerCode=g.innerCode

    """.format(isValid, createTime, dataState,str(innerCodes),str(innerCodes),str(innerCodes))
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    cur.executemany(insert_sql, select_infos)
    mysql_conn.commit()
    print('20191231至特殊数据已完成写入')

def NULL_data_1(innerCodes):
    # 用于季报出现不存在时的特殊数据处理
    select_sql = """
    select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.npCut, e.lastYearnpCut1231, e.lastYearnpCut, (d.totmktcap*10000)as totmktcap,e.newNpCut,((d.totmktcap*10000)/e.newNpCut)as pettm, 
        e.endDate from (SELECT B.* from  (select BB.* from  (select * from sy_ctxt_main_totmktcap where tradeDate >='20191231' and innerCode in {}) as BB INNER JOIN
                
                        (select  aa.innerCode,MAX(aa.publishDate) AS max, MIN(aa.publishDate) AS min FROM (select * from sq_fin_promain_npcut where endDate >= '20200331') as aa where (
                SELECT count(1) FROM (select * from sq_fin_promain_npcut  where endDate >= '20200331' and innerCode in {}) as bb WHERE aa.innerCode=bb.innerCode and bb.endDate >= aa.endDate
        )<=2  GROUP BY aa.innerCode)as CC ON BB.innerCode=CC.innerCode WHERE BB.tradeDate >= CC.min and BB.tradeDate < CC.max
                )as B 
        INNER JOIN (select a.innerCode  from (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = (SELECT max(endDate) FROM sq_fin_promain_npcut where innerCode=A.innerCode and endDate='20200331'))as a,(SELECT B.innerCode, B.endDate, B.npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1','1231') FROM sq_fin_promain_npcut where innerCode=B.innerCode and endDate='20200331'))as b,
         (SELECT C.innerCode, C.endDate, (case when substring(C.endDate, 5, 8)='1231' then 0.0 else C.npCut end) as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1',substring(max(endDate),5,8)) FROM sq_fin_promain_npcut where innerCode=C.innerCode and endDate='20200331'))as c 
         WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode and (a.npCut is NULL or  a.npCut = 0.0 or  c.cnpCut is NULL or  c.cnpCut = 0.0 )) as A 
         ON A.innerCode=B.innerCode) as d left join (select a.innerCode, a.endDate, a.npCut as npCut, (case when substring(a.endDate, 5,8)='1231' then 0.0 else b.npCut end) as  lastYearnpCut1231, cnpCut as lastYearnpCut,
        ((case when month(a.endDate)='03' then 0.0 ELSE a.npCut end ) + b.npCut - (case when month(a.endDate)='03' then 0.0 ELSE cnpCut end))as newNpCut  from 
                
                (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = (SELECT (case when month(max(endDate))='09' then CONCAT(substring(max(endDate),1,4),'0930') else max(endDate) end)  FROM sq_fin_promain_npcut where innerCode=A.innerCode and endDate='20200331') and A.innerCode in {})as a,
                
                (SELECT B.innerCode, B.endDate, B.npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1','1231') FROM sq_fin_promain_npcut where innerCode=B.innerCode and endDate='20200331'))as b,
                
                (SELECT C.innerCode, C.endDate, (case when substring(C.endDate, 5, 8)='1231' then 0.0 else C.npCut end) as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1',substring((case when month(max(endDate))='09' then '00000930' else max(endDate) end),5,8)) FROM sq_fin_promain_npcut where innerCode=C.innerCode and endDate='20200331'))as c WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode
    )as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null) as g on f.innerCode=g.innerCode

    """.format(isValid, createTime, dataState,str(innerCodes),str(innerCodes),str(innerCodes))
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    cur.executemany(insert_sql, select_infos)
    mysql_conn.commit()
    print('20191231至特殊数据已完成写入')

def NULL_20191231_innerCode():
    select_sql = """
        SELECT B.innerCode FROM sq_fin_promain_npcut AS B WHERE B.endDate =(SELECT CONCAT(substring(max(endDate),1,4)-'1','1231') FROM sq_fin_promain_npcut where innerCode=B.innerCode) and B.npCut is NULL and B.innerCode in (select innerCode from sy_ctxt_main_totmktcap GROUP BY innerCode) 
    """
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    annual_report_20191231(tuple([','.join(_) for _ in select_infos]))
    quarterly_reports_20200331(tuple([','.join(_) for _ in select_infos]))

def remove_NULL_20191231(endDate, innerCodes):
    delete_sql = """delete from sy_ctxt_pettm where endDate ='{}' and innerCode in {} """.format(endDate, str(innerCodes))
    cur.execute(delete_sql)
    mysql_conn.commit()
    print('删除{0},2019年年报为空的数据'.format(endDate))

def remove_NULL_20190930(tradeDate, innerCodes):
    # 删除日增量出现endDate=null的情况
    delete_sql = """delete from sy_ctxt_pettm where endDate is null and tradeDate='{}' and innerCode in {} """.format(tradeDate, str(innerCodes))
    cur.execute(delete_sql)
    mysql_conn.commit()
    print('删除{0},2019年年报且无2019年第三季报为空的数据'.format(tradeDate))

def annual_report_20191231(innerCodes):
    """2019年年报为空的特殊处理"""
    endDate = '20191231'
    remove_NULL_20191231(endDate,str(innerCodes))
    select_sql = """
        select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.npCut as npCut, e.lastYearnpCut1231 as lastYearnpCut1231, e.lastYearnpCut as lastYearnpCut,  (d.totmktcap*10000)as totmktcap, e.newNpCut, ((d.totmktcap*10000)/e.newNpCut)as pettm, e.endDate from (SELECT A.chiName, A.secuAbbr, A.secuCode, A.innerCode, A.companyCode,A.tradeDate,A.totmktcap, B.min, B.max  FROM (select * from sy_ctxt_main_totmktcap where innerCode in {}) AS A INNER JOIN (
        select  aa.innerCode,MAX(aa.publishDate) AS max, MIN(aa.publishDate) AS min FROM (select * from sq_fin_promain_npcut where endDate <= '20200331') as aa where (
        SELECT count(1) FROM (select * from sq_fin_promain_npcut where endDate <= '20200331') as bb WHERE aa.innerCode=bb.innerCode and bb.endDate >= aa.endDate
        )<=2  GROUP BY aa.innerCode ) AS B  ON A.innerCode = B.innerCode WHERE A.tradeDate < B.min  and tradeDate >= '20191231') as d left join (select a.innerCode, '20191231' as endDate, a.npCut as npCut, b.npCut as lastYearnpCut1231, cnpCut as lastYearnpCut,
        (a.npCut + b.npCut- cnpCut)as newNpCut  from        
        (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = '20190930' and innerCode in {})as a,
        (SELECT B.innerCode, B.endDate, (case when B.npCut is NULL then 0.0 ELSE B.npCut END)as npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate ='20181231')as b,  
        (SELECT C.innerCode, C.endDate, (case when C.npCut is NULL then 0.0 ELSE C.npCut END)as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate ='20180930')as c 
        WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode)as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null) as g on f.innerCode=g.innerCode
    """.format(isValid, createTime, dataState, str(innerCodes), str(innerCodes))
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    cur.executemany(insert_sql, select_infos)
    mysql_conn.commit()
    print('2019年年报为空,临时更改数据已经完成写入，有{0}'.format(len(select_infos)))

def quarterly_reports_20200331(innerCodes):
    """2019年年报为空,2020年第一季度特殊处理"""
    endDate = '20200331'
    remove_NULL_20191231(endDate,str(innerCodes))

    select_sql = """
            select f.chiName, f.secuCode, g.*,'{}', '{}', '{}'  from sy_ctxt_basic as f inner join (select  d.innerCode, d.tradeDate, e.npCut as npCut, e.lastYearnpCut1231 as lastYearnpCut1231, e.lastYearnpCut as lastYearnpCut,  (d.totmktcap*10000)as totmktcap, e.newNpCut, ((d.totmktcap*10000)/e.newNpCut)as pettm, e.endDate from (SELECT A.chiName, A.secuAbbr, A.secuCode, A.innerCode, A.companyCode,A.tradeDate,A.totmktcap, B.min, B.max  FROM (select * from sy_ctxt_main_totmktcap where innerCode in {}) AS A INNER JOIN (
        select  aa.innerCode,MAX(aa.publishDate) AS max, MIN(aa.publishDate) AS min FROM (select * from sq_fin_promain_npcut where endDate = '20200331') as aa where (
                SELECT count(1) FROM (select * from sq_fin_promain_npcut where endDate = '20200331') as bb WHERE aa.innerCode=bb.innerCode and bb.endDate >= aa.endDate
        )<=2  GROUP BY aa.innerCode ) AS B  ON A.innerCode = B.innerCode WHERE A.tradeDate > B.max) as d left join (select a.innerCode, '20200331' as endDate, a.npCut as npCut, (a.npCut+b.npCut) as lastYearnpCut1231, cnpCut as lastYearnpCut,
        (a.npCut + (a.npCut+b.npCut)- cnpCut)as newNpCut  from 
                
                (SELECT A.innerCode, A.endDate, (case when A.npCut is null then 0.0  else A.npCut end)as npCut  FROM sq_fin_promain_npcut as A WHERE A.endDate = '20200331' and
                            
                            innerCode in {})as a,
                
                (SELECT B.innerCode, B.endDate, (case when B.npCut is NULL then 0.0 ELSE B.npCut END)as npCut FROM sq_fin_promain_npcut AS B WHERE B.endDate ='20190930')as b,
                
                (SELECT C.innerCode, C.endDate, (case when C.npCut is NULL then 0.0 ELSE C.npCut END)as cnpCut FROM sq_fin_promain_npcut AS C WHERE C.endDate ='20190331')as c 
                
                WHERE a.innerCode=b.innerCode and b.innerCode=c.innerCode)as e ON d.innerCode=e.innerCode WHERE d.innerCode is not null) as g on f.innerCode=g.innerCode
            """.format(isValid, createTime, dataState,str(innerCodes), str(innerCodes))
    cur.execute(select_sql)
    select_infos = cur.fetchall()
    cur.executemany(insert_sql, select_infos)
    mysql_conn.commit()
    print('2019年年报为空,2020年第一季度特殊处理,临时更改数据已经完成写入，有{0}'.format(len(select_infos)))

# 关闭数据库
def close_sql():
    """关闭游标关闭数据库"""
    mysql_conn2.close()
    mysql_conn.close()
    db.close()

# 打印时间
def get_time():
    now = datetime.datetime.now()
    print(now)

def main():
    get_time()
    seeyii_to_project()
    # history_data()
    # annual_report_2019()
    # now_data()
    # NULL_20191231_innerCode()
    # now_data_1()
    try:
        daily_data()
    except Exception as e:
        traceback.print_exc()
    finally:
        close_sql()
    # insert_pettm()
    # close_sql() # 执行上边任意一函数都要执行关闭游标
    get_time()
if __name__ == '__main__':
    main()