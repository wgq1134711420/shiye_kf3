# coding:utf-8

'''
文件名：sy_ctxt.py
功能：长投学堂数据处理
代码历史：20200408，徐荣华
'''

import pymysql
import datetime
import time


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


def conn_mysql2():
    mysql_conn = pymysql.connect(
        host='172.17.23.128',
        port=6033,
        user='seeyiidata',
        passwd='shiye1805A',
        db='db_seeyii',
        charset='utf8'
    )

    return mysql_conn


isValid = 1  # 是否有效，0：否，1：是
createTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))  # sql操作时间
dataState = 1  # 数据状态:1->新增;2->更新;3->删除
modifyUpdateTime = createTime  # 最后修改时间


# 证券主表基础信息
def sy_ctxt_basic():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT I.ChiName,I.SecuAbbr,I.innerCode, I.SecuCode, I.SecuMarket,
        I.SecuCategory,I.ListedDate, I.ListedSector,I.ListedState, H.CSRCLEVEL1NAME, H.CSRCLEVEL2NAME,'{}','{}','{}'
        from (select A.*,B.CompanyCode, B.innerCode from
        (SELECT ChiName,SecuAbbr,SecuCode,SecuMarket,SecuCategory,ListedDate,ListedSector,ListedState
        from secumain WHERE SecuCategory = 1 and SecuMarket in (83,90) and ListedSector in (1,2,6,7) and
        ListedState in (1,3,5)) as A inner join (SELECT * from sq_sk_basicinfo WHERE innerCode is not NULL) as  B on A.SecuCode=B.SecuCode) as I LEFT JOIN sq_comp_standard_industry as H on I.SecuCode=H.SecuCode  '''.format(
        isValid, createTime, dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_basic'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_basic数据成功')
        insert_sql = '''insert into sy_ctxt_basic(chiName,secuAbbr,innerCode,secuCode,secuMarket,secuCategory,
        listedDate,listedSector,listedState,csrclevel1Name,csrclevel2Name,
        isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入表sy_ctxt_basic数据成功')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

# 证券主表交易信息
def sy_ctxt_main_basic():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT I.ChiName,I.SecuAbbr,I.InnerCode, I.SecuCode, I.SecuMarket,
    I.SecuCategory,I.ListedDate, I.ListedSector,I.ListedState, H.CSRCLEVEL1NAME, H.CSRCLEVEL2NAME
    ,I.DividendRatioLYR, I.tradeDate, I.tOpen,I.tClose, I.pchg,'{}','{}','{}'
    from (select * from (SELECT E.ChiName,E.SecuAbbr,E.binnerCode as InnerCode, E.SecuCode, E.SecuMarket, E.SecuCategory,E.ListedDate, E.ListedSector,E.ListedState,F.DividendRatioLYR, E.tradeDate, E.tOpen,E.tClose, E.pchg FROM (
    SELECT C.InnerCode,C.ChiName,C.SecuAbbr,C.binnerCode, C.SecuCode, C.SecuMarket, C.SecuCategory,C.ListedDate, C.ListedSector,C.ListedState,D.tradeDate, D.tOpen,D.tClose, D.pchg  FROM (select A.*,B.CompanyCode, B.innerCode as binnerCode from (SELECT InnerCode,ChiName,SecuAbbr,SecuCode,SecuMarket,SecuCategory,ListedDate,ListedSector,ListedState 
    from secumain WHERE SecuCategory = 1 and SecuMarket in (83,90) and ListedSector in (1,2,6,7) and ListedState in (1,3,5)
    ) as A left join sq_sk_basicinfo as  B on A.SecuCode=B.SecuCode) as C left join (select * from sq_qt_skdailyprice WHERE tradeDate>="20190101" and tradeDate<"20200101") as D on D.companyCode=C.CompanyCode) as E left JOIN (
    SELECT * FROM lc_dindicesforvaluation WHERE TradingDay>="20190101" and TradingDay<"20200101"
    ) as F ON E.InnerCode=F.InnerCode and F.TradingDay=E.tradeDate)as G WHERE G.tradeDate is not NULL)as I LEFT JOIN sq_comp_standard_industry as H on I.SecuCode=H.SecuCode '''.format(
        isValid, createTime, dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        # truncate_table_sql = '''truncate table sy_ctxt_main_basic'''
        # cur.execute(truncate_table_sql)
        # mysql_conn.commit()
        # print('清空表sy_ctxt_main_basic数据成功')
        insert_sql = '''insert into sy_ctxt_main_basic(chiName,secuAbbr,innerCode,secuCode,secuMarket,secuCategory,
        listedDate,listedSector,listedState,csrclevel1Name,csrclevel2Name,
        dividendRatioLYR,tradeDate,tOpen,tClose,pchg,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入表sy_ctxt_main_basic数据成功')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()


# 财务指标
def sy_ctxt_promain():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT C.ChiName,C.InnerCode,C.SecuCode,D.endDate,D.mainBusiIncome,
    (CASE WHEN D.netProfit is not NULL THEN D.netProfit*10000 ELSE D.netProfit END) as netProfit,D.mainbusIncGrowRate,
    D.netIncGrowRate,D.roeWeighted,D.fcff,'{}','{}','{}'    
    FROM (select B.innerCode, A.chiName, A.secuCode from (SELECT SecuCode,InnerCode, ChiName FROM secumain 
    WHERE SecuCategory = 1 and SecuMarket in (83,90) and ListedSector in (1,2,6,7) and ListedState in (1,3,5))as A
    LEFT JOIN  sq_sk_basicinfo as B ON A.SecuCode=B.secuCode) as C INNER JOIN (select * FROM
    sq_fin_promain WHERE endDate >="20130331")as D ON C.innerCode=D.innerCode'''.format(isValid, createTime, dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_promain'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_promain数据成功')
        insert_sql = '''insert into sy_ctxt_promain(chiName,innerCode,secuCode,endDate,mainBusiIncome,netProfit,mainbusIncGrowRate,
        netIncGrowRate,roeWeighted,fcff,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入表sy_ctxt_promain数据成功')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

# 获取主表市值
def sy_ctxt_main_totmktcap():
    i = 0
    for SecuCode in main_code_list[2000:]:
        i += 1
        mysql_conn = conn_mysql()
        cur = mysql_conn.cursor()
        mysql_conn2 = conn_mysql2()
        cur2 = mysql_conn2.cursor()
        select_sql = '''SELECT c.ChiName,c.SecuAbbr,c.SecuCode,c.innerCode,c.companyCode,d.tradeDate,d.totmktcap from (SELECT a.*,b.innerCode,b.companyCode from 
        (SELECT ChiName,SecuAbbr,SecuCode
           from secumain WHERE SecuCategory = 1 and SecuMarket in (83,90) and ListedSector in (1,2,6,7) and ListedState in (1,3,5) and SecuCode = '{}') as a LEFT JOIN
        (SELECT secuCode,innerCode, companyCode from sq_sk_basicinfo) as b on a.SecuCode = b.secuCode GROUP BY a.SecuCode) as c LEFT JOIN
        (SELECT tradeDate, companyCode, totmktcap from sq_qt_skdailyprice) as d on c.companyCode = d.companyCode'''.format(SecuCode)
        try:
            cur2.execute(select_sql)
            infos = cur2.fetchall()
            # truncate_table_sql = '''truncate table sy_ctxt_main_totmktcap'''
            # cur.execute(truncate_table_sql)
            # mysql_conn.commit()
            # print('清空表sy_ctxt_main_totmktcap数据成功')
            insert_sql = '''insert into sy_ctxt_main_totmktcap(chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap)
                            value (%s,%s,%s,%s,%s,%s,%s)'''
            cur2.executemany(insert_sql, infos)
            mysql_conn2.commit()
            print('插入表sy_ctxt_main_totmktcap数据成功')

        except Exception as e:
            print(e)
        finally:
            mysql_conn2.close()
            mysql_conn.close()
        print(i)


# 计算pe值
def sy_ctxt_main_pe():
    i = 0
    for innerCode in main_innerCode_list[200:1500]:
        i += 1
        mysql_conn = conn_mysql()
        cur = mysql_conn.cursor()
        mysql_conn2 = conn_mysql2()
        cur2 = mysql_conn2.cursor()
        select_sql = '''SELECT E.chiName,E.secuAbbr,E.secuCode,E.innerCode,E.companyCode,E.tradeDate,E.totmktcap, 
        (CASE WHEN E.totmktcap is not NULL and F.netProfit is NOT NULL THEN (E.totmktcap/F.netProfit) ELSE NULL END) as pe from 
    (
    select chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,min(daynum) as daymin from 
    (
    select A.*,B.*,DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d')) as daynum from 
    (
    SELECT chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap FROM sy_ctxt_main_totmktcap WHERE innerCode='{}'
    ) 
    as A
    ,
    (
    select   
        innerCode as innerCode1,
      enDdate,
        netProfit   
         from sq_fin_promain where innerCode='{}' AND substring(endDate,5,4) = '1231'
         ) as B 
    where    A.innerCode=B.innerCode1 
             and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))>=0
                     and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))<500
    ) as C   
    group by secuCode,innerCode,tradeDate

    ) as E ,

    (
    select A.innerCode,A.tradeDate,B.*,DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d')) as daynum from 
    (
    SELECT * FROM sy_ctxt_main_totmktcap WHERE innerCode='{}'
    ) 
    as A
    ,
    (
    select   
        innerCode as innerCode1,
      enDdate,
        netProfit   
         from sq_fin_promain where innerCode='{}' AND substring(endDate,5,4) = '1231'
         ) as B 
    where    A.innerCode=B.innerCode1 
            and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))>=0
                    and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))<500
    )       
    as F 
    where E.innerCode=F.innerCode and E.tradeDate=F.tradeDate  and E.daymin=F.daynum ORDER BY tradeDate DESC'''.format(innerCode,innerCode,innerCode,innerCode)
        try:
            cur2.execute(select_sql)
            infos = cur2.fetchall()
            # truncate_table_sql = '''truncate table sy_ctxt_main_totmktcap'''
            # cur.execute(truncate_table_sql)
            # mysql_conn.commit()
            # print('清空表sy_ctxt_main_totmktcap数据成功')
            insert_sql = '''insert into sy_ctxt_main_pe(chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,pe)
                            value (%s,%s,%s,%s,%s,%s,%s,%s)'''
            cur2.executemany(insert_sql, infos)
            mysql_conn2.commit()
            print('插入表sy_ctxt_main_pe数据成功')

        except Exception as e:
            print(e)
        finally:
            mysql_conn2.close()
            mysql_conn.close()
        print(i)

# 计算pb值
def sy_ctxt_main_pb():
    i = 0
    for innerCode in main_innerCode_list[200:1500]:
        i += 1
        mysql_conn = conn_mysql()
        cur = mysql_conn.cursor()
        mysql_conn2 = conn_mysql2()
        cur2 = mysql_conn2.cursor()
        select_sql = '''SELECT E.chiName,E.secuAbbr,E.secuCode,E.innerCode,E.companyCode,E.tradeDate,E.totmktcap,E.pe,F.totsharEqui,
        (CASE WHEN E.totmktcap is not NULL and F.totsharEqui is NOT NULL THEN (E.totmktcap/F.totsharEqui)*10000 ELSE NULL END) as pb from 
    (
    select chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,pe,min(daynum) as daymin from 
    (
    select A.*,B.*,DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d')) as daynum from 
    (
    SELECT chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,pe FROM sy_ctxt_main_pe WHERE innerCode='{}'
    ) 
    as A
    ,
    (
    select   
        innerCode as innerCode1,
      enDdate,
        totsharEqui   
         from sq_fin_promain where innerCode='{}'
         ) as B 
    where    A.innerCode=B.innerCode1 
             and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))>=0
                     and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))<500
    ) as C   
    group by secuCode,innerCode,tradeDate

    ) as E ,

    (
    select A.innerCode,A.tradeDate,B.*,DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d')) as daynum from 
    (
    SELECT * FROM sy_ctxt_main_pe WHERE innerCode='{}'
    ) 
    as A
    ,
    (
    select   
        innerCode as innerCode1,
      enDdate,
        totsharEqui   
         from sq_fin_promain where innerCode='{}'
         ) as B 
    where    A.innerCode=B.innerCode1 
            and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))>=0
                    and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))<500
    )       
    as F 
    where E.innerCode=F.innerCode and E.tradeDate=F.tradeDate  and E.daymin=F.daynum ORDER BY tradeDate DESC'''.format(innerCode,innerCode,innerCode,innerCode)
        try:
            cur2.execute(select_sql)
            infos = cur2.fetchall()
            # truncate_table_sql = '''truncate table sy_ctxt_main_totmktcap'''
            # cur.execute(truncate_table_sql)
            # mysql_conn.commit()
            # print('清空表sy_ctxt_main_totmktcap数据成功')
            insert_sql = '''insert into sy_ctxt_main_pb(chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,pe,totsharEqui,pb)
                            value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            cur2.executemany(insert_sql, infos)
            mysql_conn2.commit()
            print('插入表sy_ctxt_main_pb数据成功')

        except Exception as e:
            print(e)
        finally:
            mysql_conn2.close()
            mysql_conn.close()
        print(i)

# 计算pbb值
def sy_ctxt_main_pbb():
    i = 0
    for innerCode in main_innerCode_list[200:1500]:
        i += 1
        mysql_conn = conn_mysql()
        cur = mysql_conn.cursor()
        mysql_conn2 = conn_mysql2()
        cur2 = mysql_conn2.cursor()
        select_sql = '''SELECT E.chiName,E.secuAbbr,E.secuCode,E.innerCode,E.companyCode,E.tradeDate,E.totmktcap,E.pe,E.totsharEqui,E.pb,
        (CASE WHEN E.totmktcap is not NULL and E.totsharEqui is NOT NULL and F.goodWill is NOT NULL THEN (E.totmktcap/(E.totsharEqui - F.goodWill))*10000
        WHEN F.goodWill is NULL THEN E.pb ELSE NULL END) as pbb from 
    (
    select chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,pe,totsharEqui,pb,min(daynum) as daymin from 
    (
    select A.*,B.*,DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d')) as daynum from 
    (
    SELECT chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,pe,totsharEqui,pb FROM sy_ctxt_main_pb WHERE innerCode='{}'
    ) 
    as A
    ,
    (
    select   
        innerCode as innerCode1,
      enDdate,
        goodWill   
         from sq_fin_probalsheetnew where innerCode='{}' and reportType = '3'
         ) as B 
    where    A.innerCode=B.innerCode1 
             and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))>=0
                     and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))<500
    ) as C   
    group by secuCode,innerCode,tradeDate

    ) as E ,

    (
    select A.innerCode,A.tradeDate,B.*,DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d')) as daynum from 
    (
    SELECT * FROM sy_ctxt_main_pb WHERE innerCode='{}'
    ) 
    as A
    ,
    (
    select   
        innerCode as innerCode1,
      enDdate,
        goodWill   
         from sq_fin_probalsheetnew where innerCode='{}' and reportType = '3'
         ) as B 
    where    A.innerCode=B.innerCode1 
            and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))>=0
                    and DATEDIFF( STR_TO_DATE(A.tradeDate,'%Y%m%d'),STR_TO_DATE(B.endDate,'%Y%m%d'))<500
    )       
    as F 
    where E.innerCode=F.innerCode and E.tradeDate=F.tradeDate  and E.daymin=F.daynum ORDER BY tradeDate DESC'''.format(innerCode,innerCode,innerCode,innerCode)
        try:
            cur2.execute(select_sql)
            infos = cur2.fetchall()
            # truncate_table_sql = '''truncate table sy_ctxt_main_totmktcap'''
            # cur.execute(truncate_table_sql)
            # mysql_conn.commit()
            # print('清空表sy_ctxt_main_totmktcap数据成功')
            insert_sql = '''insert into sy_ctxt_main_pbb(chiName,secuAbbr,secuCode,innerCode,companyCode,tradeDate,totmktcap,pe,totsharEqui,pb,pbb)
                            value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            cur2.executemany(insert_sql, infos)
            mysql_conn2.commit()
            print('插入表sy_ctxt_main_pbb数据成功')

        except Exception as e:
            print(e)
        finally:
            mysql_conn2.close()
            mysql_conn.close()
        print(i)

# 基金概况表
def sy_ctxt_fund_basic():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT InnerCode, SecurityCode, MainCode,Type,FundType,InvestmentType,InvestStyle,
    EstablishmentDate,ListedDate,StartDate,ExpireDate,FoundedSize,'{}','{}','{}' FROM mf_fundarchives '''.format(isValid, createTime, dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_fund_basic'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_fund_basic数据成功')
        insert_sql = '''insert into sy_ctxt_fund_basic(innerCode, securityCode, mainCode,type,fundType,investmentType,
        investStyle,establishmentDate,listedDate,startDate,expireDate,foundedSize,isValid, createTime, dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入表sy_ctxt_fund_basic数据成功')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

# 基金产品名称表
def sy_ctxt_fund_name():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT InnerCode,InfoPublDate,InfoSource,InfoType,DisclName,EffectiveDate,ExpiryDate,
    IfEffected,'{}','{}','{}' FROM MF_FundProdName '''.format(isValid, createTime, dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_fund_name'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_fund_name数据成功')
        insert_sql = '''insert into sy_ctxt_fund_name(innerCode,infoPublDate,infoSource,infoType,disclName,
        effectiveDate,expiryDate,ifEffected,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入表sy_ctxt_fund_name数据成功')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

# 基金费率表
def sy_ctxt_fund_rate():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT InnerCode,InfoPublDate,ChargeRateDes,DivIntervalDes,
    Notes,ChargeRateType,MinChargeRate,MaxChargeRate,'{}','{}','{}' FROM mf_chargeratenew '''.format(isValid, createTime, dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_fund_rate'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_fund_rate数据成功')
        insert_sql = '''insert into sy_ctxt_fund_rate(innerCode,infoPublDate,chargeRateDes,divIntervalDes,
    notes,chargeRateType,minChargeRate,maxChargeRate,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入表sy_ctxt_fund_rate数据成功')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

# 基金经理表
def sy_ctxt_fund_manager():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT InnerCode,InfoPublDate,InfoSource,Name,Incumbent,
    AccessionDate,DimissionDate,ManagementTime,'{}','{}','{}' FROM mf_fundmanagernew '''.format(isValid, createTime, dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_fund_manager'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_fund_manager数据成功')
        insert_sql = '''insert into sy_ctxt_fund_manager(innerCode,infoPublDate,infoSource,name,incumbent,
    accessionDate,dimissionDate,managementTime,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入表sy_ctxt_fund_manager数据成功')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

# 基金回报率表
def sy_ctxt_fund_performance():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT InnerCode,TradingDay,NVDailyGrowthRate,RRInSingleWeek,
    RRInSingleMonth,RRInSingleYear,RRInThreeYear,'{}','{}','{}' FROM mf_netvalueperformanceHis where TradingDay >= '2017-01-01' '''.format(isValid, createTime, dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_fund_performance'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_fund_performance数据成功')
        insert_sql = '''insert into sy_ctxt_fund_performance(innerCode,tradingDay,nVDailyGrowthRate,rRInSingleWeek,
    rRInSingleMonth,rRInSingleYear,rRInThreeYear,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入表sy_ctxt_fund_performance数据成功')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()


def sy_ctxt_avg_bond():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT date_format(TradingDay, '%Y-%m-%d') as TradingDay, round(avg(ClosePrice),4) as avgClosePrice
    FROM bond_conbdexchangequote GROUP BY TradingDay '''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_avg_bond'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_avg_bond数据成功')
        insert_sql = '''insert into sy_ctxt_avg_bond(tradingDay,avgClosePrice)
                        value (%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入表sy_ctxt_avg_bond数据成功')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()


# 债类表
def sy_ctxt_bond():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    # mysql_conn2 = conn_mysql2()
    # cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT A.tradingDay,A.avgClosePrice,B.dailyTime,'{}','{}','{}' FROM sy_ctxt_avg_bond as A LEFT JOIN 
    sy_ctxt_chinabond_10year as B ON A.tradingDay=B.timeDate'''.format(isValid, createTime, dataState)
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_bond'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_bond数据成功')
        insert_sql = '''insert into sy_ctxt_bond(tradingDay,avgClosePrice,tenYearYield,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入表sy_ctxt_bond数据成功')

    except Exception as e:
        print(e)
    finally:
        # mysql_conn2.close()
        mysql_conn.close()


# 指数指标表取pe,pb值
def sy_ctxt_index_pe():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    select_sql = '''SELECT C.ChiName,C.SecuCode,C.SecuAbbr, D.TradingDay,D.PE_LYR, D.PB_LF,'{}','{}','{}' from 
    (SELECT A.TargetIndexInnerCode,A.InnerCode, A.InfoSource, B.SecuCode, B.ChiName, B.SecuAbbr 
    FROM (SELECT * from mf_etfprlist WHERE TargetIndexInnerCode IS NOT NULL GROUP BY TargetIndexInnerCode) as A
    LEFT JOIN secumain as B ON A.TargetIndexInnerCode=B.InnerCode) 
    as C INNER JOIN lc_indexderivative as D ON C.TargetIndexInnerCode=D.IndexCode ORDER BY SecuCode,TradingDay'''.format(isValid, createTime, dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_index_pe'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_index_pe数据成功')
        insert_sql = '''insert into sy_ctxt_index_pe(chiName,secuCode,secuAbbr,tradingDay,pe,pb,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入表sy_ctxt_index_pe数据成功')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

# 指数指标表取pe,pb值
def sy_ctxt_index():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    select_sql = '''SELECT C.ChiName,C.SecuCode,C.SecuAbbr,D.TradingDay,D.IndicatorType,D.StatisPeriod,D.IndiPercentile,'{}','{}','{}' from 
    (SELECT A.TargetIndexInnerCode,A.InnerCode, A.InfoSource, B.SecuCode, B.ChiName, B.SecuAbbr 
    FROM (SELECT * from mf_etfprlist WHERE TargetIndexInnerCode IS NOT NULL GROUP BY TargetIndexInnerCode) as A
    LEFT JOIN secumain as B ON A.TargetIndexInnerCode=B.InnerCode) as C INNER JOIN 
        (SELECT * from index_derivpercentile WHERE IndicatorType in (1,2) and StatisPeriod in (60,120,999)) as D
        ON C.TargetIndexInnerCode=D.IndexCode GROUP BY SecuCode,TradingDay,IndicatorType,StatisPeriod  ORDER BY SecuCode,TradingDay'''.format(isValid, createTime, dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_index'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_index数据成功')
        insert_sql = '''insert into sy_ctxt_index(chiName,secuCode,secuAbbr,tradingDay,indicatorType,statisPeriod,indiPercentile,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入表sy_ctxt_index数据成功')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

# 指数基金与指数估值对照表
def sy_ctxt_index_contrast():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT C.InnerCode,C.ChiName,C.SecuCode,C.SecuAbbr,D.ChiName,D.SecuCode,D.SecuAbbr,'{}','{}','{}'
    from (SELECT A.InnerCode,A.TargetIndexInnerCode, B.SecuCode, B.ChiName, B.SecuAbbr 
    FROM (SELECT * from mf_etfprlist WHERE InnerCode IS NOT NULL GROUP BY InnerCode) as A
    LEFT JOIN secumain as B ON A.InnerCode=B.InnerCode) as C LEFT JOIN secumain as D ON C.TargetIndexInnerCode=D.InnerCode'''.format(isValid, createTime,
                                                                                            dataState)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sy_ctxt_index_contrast'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空表sy_ctxt_index_contrast数据成功')
        insert_sql = '''insert into sy_ctxt_index_contrast(innerCode,indexName,indexSecuCode,indexAbbr,fundName,
        fundSecuCode,fundAbbr,isValid,createTime,dataState)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入表sy_ctxt_index_contrast数据成功')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()


if __name__ == "__main__":
    sy_ctxt_basic()
    # sy_ctxt_main_basic()
    # sy_ctxt_promain()
    # sy_ctxt_main_totmktcap()
    # sy_ctxt_main_pe()
    # sy_ctxt_main_pb()
    # sy_ctxt_main_pbb()
    # sy_ctxt_fund_basic()
    # sy_ctxt_fund_name()
    # sy_ctxt_fund_rate()
    # sy_ctxt_fund_manager()
    # sy_ctxt_fund_performance()
    # sy_ctxt_avg_bond()
    # sy_ctxt_bond()
    # sy_ctxt_index_pe()
    # sy_ctxt_index()
    # sy_ctxt_index_contrast()