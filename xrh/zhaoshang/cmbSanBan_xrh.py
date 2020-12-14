'''
文件名：cmbSanBan.py
功能：招商项目三板需求处理
代码历史：20190522，徐荣华
'''

import pymysql
import datetime
import time

def conn_mysql():
    mysql_conn = pymysql.connect(
        host='0.0.0.0',
        port=33006,
        user='root',
        passwd='123456',
        db='cmb_sanban',
        charset='utf8'
    )

    return mysql_conn

# 公司和董监高个人信息合并
def tmp_sq_comp_manager_main_1():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT C.*, D.PersonalCode,D.LeaderName,D.Gender,D.EducationLevel,D.PosnName,D.BeginDate,D.EndDate,D.ChangeType from 
(SELECT A.SecuCode,A.SecuAbbr,A.innerCode,A.industryName,B.CompanyCode,B.ChiName from(
SELECT * from nq_comlist WHERE PriorityLevel = '100') as A LEFT JOIN
(SELECT * FROM secumain where ListedSector='3' and  ListedState='1' AND SecuCategory = '1') as B on A.innerCode=B.innerCode) as C LEFT JOIN
(SELECT PersonalCode,LeaderName,Gender,EducationLevel,GROUP_CONCAT(PosnName) as PosnName,GROUP_CONCAT(DATE_FORMAT(BeginDate,'%Y%m%d') separator ';') as BeginDate,GROUP_CONCAT(DATE_FORMAT(EndDate,'%Y%m%d') separator ';') as EndDate,
GROUP_CONCAT(ChangeType) as ChangeType,CompanyCode from nq_leaderposn WHERE ifPosition = '1' and PosnNum != 106001 GROUP BY CompanyCode,LeaderName) 
as D ON C.CompanyCode = D.CompanyCode'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_1'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功1')
        insert_sql = '''insert into tmp_sq_comp_manager_main_1(SecuCode,SecuAbbr,innerCode,industryName,companyCode,
        ChiName,personalCode,LeaderName,Gender,EducationLevel,PosnName,BeginDate,EndDate,ChangeType) value 
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功1')


    except Exception as e:
        print(e)
    finally:
        mysql_conn.close()

# 获取5%以上股东
def tmp_sq_comp_manager_main_2():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT A.SecuCode,A.SecuAbbr,A.innerCode,A.industryName,A.companyCode,A.ChiName, A.personalCode, A.LeaderName,A.Gender,A.EducationLevel, 
(case when B.sholdingPro is not NULL THEN CONCAT(A.PosnName,',',B.sholdingPro) ELSE A.PosnName END) as PosnName,
A.BeginDate,A.EndDate,A.ChangeType
from (SELECT * from tmp_sq_comp_manager_main_1) as A LEFT JOIN
(SELECT C.* FROM (SELECT CompanyCode, SHName, HoldingRatioEnd,(case when HoldingRatioEnd > 0.05 THEN '5%以上股东' END) as sholdingPro
 from nq_top10sh WHERE HoldingRatioEnd > 0.05 and date(EndDate) = STR_TO_DATE('20191231','%Y%m%d') and SHKindNum = 1 GROUP BY CompanyCode,SHName) AS C INNER JOIN
(SELECT companyCode,LeaderName from tmp_sq_comp_manager_main_1) as D ON C.CompanyCode = D.companyCode GROUP BY CompanyCode, SHName) as B
ON A.companyCode = B.CompanyCode and A.LeaderName = B.SHName
UNION 
SELECT A.SecuCode,A.SecuAbbr,A.innerCode,A.industryName,(case when A.companyCode is NULL then B.CompanyCode ELSE A.companyCode END) as companyCode,A.ChiName, 
(case when A.personalCode is NULL then B.SHID ELSE A.personalCode END) as personalCode,
(case when A.LeaderName is null then B.SHName ELSE A.LeaderName END) as LeaderName,
A.Gender,A.EducationLevel, 
(case when A.PosnName is not NULL THEN CONCAT(A.PosnName,',',B.sholdingPro) ELSE B.sholdingPro END) as PosnName,
A.BeginDate,A.EndDate,A.ChangeType
from (SELECT * from tmp_sq_comp_manager_main_1) as A RIGHT JOIN
(SELECT C.* FROM (SELECT CompanyCode,SHID, SHName, HoldingRatioEnd,(case when HoldingRatioEnd > 0.05 THEN '5%以上股东' END) as sholdingPro
 from nq_top10sh WHERE HoldingRatioEnd > 0.05 and date(EndDate) = STR_TO_DATE('20191231','%Y%m%d') and SHKindNum = 1 GROUP BY CompanyCode,SHName) AS C INNER JOIN
(SELECT companyCode,LeaderName from tmp_sq_comp_manager_main_1) as D ON C.CompanyCode = D.companyCode GROUP BY CompanyCode, SHName) as B
ON A.companyCode = B.CompanyCode and A.LeaderName = B.SHName'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_2'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功2')
        insert_sql = '''insert into tmp_sq_comp_manager_main_2(SecuCode,SecuAbbr,innerCode,industryName,companyCode,
        ChiName,personalCode,LeaderName,Gender,EducationLevel,PosnName,BeginDate,EndDate,ChangeType) value 
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功2')


    except Exception as e:
        print(e)
    finally:
        mysql_conn.close()

# 获取实际控制人
def tmp_sq_comp_manager_main_3():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT A.SecuCode,A.SecuAbbr,A.innerCode,A.industryName,A.companyCode,A.ChiName, A.personalCode, A.LeaderName,A.Gender,A.EducationLevel, 
(case when B.SHName = A.LeaderName THEN CONCAT(A.PosnName,',实际控制人') ELSE A.PosnName END) as PosnName,A.BeginDate,A.EndDate,A.ChangeType FROM (SELECT * FROM tmp_sq_comp_manager_main_2) as A LEFT JOIN
(SELECT E.* FROM (SELECT C.* FROM nq_contmainsh AS C INNER JOIN (SELECT CompanyCode, MAX(infoPublDate) as infoPublDate from nq_contmainsh GROUP BY CompanyCode) as D
 ON C.infoPublDate = D.infoPublDate AND C.CompanyCode = D.CompanyCode AND C.MSHNumber = 2 AND C.SHKind = 1 GROUP BY CompanyCode,SHName) AS E INNER JOIN
(SELECT companyCode from tmp_sq_comp_manager_main_2) as F ON E.CompanyCode = F.companyCode GROUP BY CompanyCode, SHName) as B ON A.companyCode = B.CompanyCode AND B.SHName = A.LeaderName
UNION
SELECT A.SecuCode,A.SecuAbbr,A.innerCode,A.industryName,(case when A.companyCode is null then B.CompanyCode else A.companyCode END) as companyCode,
A.ChiName, A.personalCode, (case when A.LeaderName is null then B.SHName ELSE A.LeaderName END) as LeaderName,A.Gender,A.EducationLevel, 
(case when A.PosnName is null THEN '实际控制人' ELSE CONCAT(A.PosnName,',实际控制人') END) as PosnName,A.BeginDate,A.EndDate,A.ChangeType FROM (SELECT * FROM tmp_sq_comp_manager_main_2) as A RIGHT JOIN
(SELECT E.* FROM (SELECT C.* FROM nq_contmainsh AS C INNER JOIN (SELECT CompanyCode, MAX(infoPublDate) as infoPublDate from nq_contmainsh GROUP BY CompanyCode) as D
 ON C.infoPublDate = D.infoPublDate AND C.CompanyCode = D.CompanyCode AND C.MSHNumber = 2 AND C.SHKind = 1 GROUP BY CompanyCode,SHName) AS E INNER JOIN
(SELECT companyCode from tmp_sq_comp_manager_main_2) as F ON E.CompanyCode = F.companyCode GROUP BY CompanyCode, SHName) as B ON A.companyCode = B.CompanyCode and B.SHName = A.LeaderName'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_3'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功3')
        insert_sql = '''insert into tmp_sq_comp_manager_main_3(SecuCode,SecuAbbr,innerCode,industryName,companyCode,
        ChiName,personalCode,LeaderName,Gender,EducationLevel,PosnName,BeginDate,EndDate,ChangeType) value 
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功3')
        # del_same_sql = '''SELECT * FROM (SELECT * FROM tmp_sq_comp_manager_main_3 ORDER BY LENGTH(PosnName) DESC) AS A GROUP BY A.companyCode , A.LeaderName'''
        # cur.execute(del_same_sql)
        # infos = cur.fetchall()
        # truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_3'''
        # cur.execute(truncate_table_sql)
        # mysql_conn.commit()
        # print('清空数据成功2')
        # insert_sql = '''insert into tmp_sq_comp_manager_main_3(SecuCode,SecuAbbr,innerCode,industryName,companyCode,
        #         ChiName,personalCode,LeaderName,Gender,EducationLevel,PosnName,BeginDate,EndDate,ChangeType) value
        #         (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        # cur.executemany(insert_sql, infos)
        # mysql_conn.commit()



    except Exception as e:
        print(e)
    finally:
        mysql_conn.close()

# 补全缺少公司信息的股东信息
def tmp_sq_comp_manager_main_3puls():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT (CASE when A.SecuCode is null then B.SecuCode ELSE A.SecuCode END) AS SecuCode,
(CASE when A.SecuAbbr is null then B.ChiNameAbbr ELSE A.SecuAbbr END) AS SecuAbbr,
(CASE when A.innerCode is null then B.innerCode ELSE A.innerCode END) AS innerCode,
A.industryName,A.companyCode,
(CASE when A.ChiName is null then B.ChiName ELSE A.ChiName END) AS ChiName,A.personalCode,A.LeaderName,
A.Gender,A.EducationLevel,A.PosnName,A.BeginDate,A.EndDate,A.ChangeType FROM (SELECT * from tmp_sq_comp_manager_main_3) as A LEFT JOIN
(SELECT * FROM secumain) as B ON A.companyCode = B.CompanyCode GROUP BY companyCode, LeaderName'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_3'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功3plus')
        insert_sql = '''insert into tmp_sq_comp_manager_main_3(SecuCode,SecuAbbr,innerCode,industryName,companyCode,
        ChiName,personalCode,LeaderName,Gender,EducationLevel,PosnName,BeginDate,EndDate,ChangeType) value 
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功3plus')



    except Exception as e:
        print(e)
    finally:
        mysql_conn.close()

# 获取上一年的个人分红报酬数
def tmp_one_year_bonus_money():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT D.CompanyCode,D.SecuCode,D.ChiName,D.personalCode,D.LeaderName, SUM(ROUND(C.EndCSHoldingPro * D.TotalCashDivi, 4)) as oneYearBonusMoney from (SELECT CompanyCode, PersonalCode, PersonName, EndCSHoldingPro, EndDate
 from nq_leadershares WHERE date(EndDate) = STR_TO_DATE('20191231','%Y%m%d') GROUP BY CompanyCode, PersonalCode, EndDate) as C, 
(SELECT A.*,B.SecuCode,B.ChiName,B.personalCode,B.LeaderName FROM
(SELECT CompanyCode, TotalCashDivi,EndDate FROM nq_dividend WHERE ifDividend = '1' and date(EndDate) = STR_TO_DATE('20191231','%Y%m%d') AND TotalCashDivi > 0) AS A,
(SELECT SecuCode, companyCode, ChiName,personalCode, LeaderName FROM tmp_sq_comp_manager_main_3) AS B
WHERE A.CompanyCode = B.companyCode GROUP BY CompanyCode, LeaderName, EndDate) AS D
WHERE C.CompanyCode = D.CompanyCode AND C.EndDate = D.EndDate AND C.PersonName = D.LeaderName GROUP BY C.CompanyCode, LeaderName'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_one_year_bonus_money'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空上一年分红数据成功')
        insert_sql = '''insert into tmp_one_year_bonus_money(CompanyCode,SecuCode,ChiName,personalCode,LeaderName,oneYearBonusMoney) value 
        (%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入上一年分红数据成功')


    except Exception as e:
        print(e)
    finally:
        mysql_conn.close()

# 补全上一年的个人缺失分红报酬数
def tmp_fill_one_year_bonus_money():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT D.CompanyCode,D.SecuCode,D.ChiName,D.personalCode,D.LeaderName, SUM(ROUND(C.HoldingRatioEnd * D.TotalCashDivi, 4)) as oneYearBonusMoney from (SELECT CompanyCode, SHName, HoldingRatioEnd, EndDate
 from nq_top10sh WHERE date(EndDate) = STR_TO_DATE('20191231','%Y%m%d') GROUP BY CompanyCode, SHName, EndDate) as C, 
(SELECT A.*,B.SecuCode,B.ChiName,B.personalCode,B.LeaderName FROM
(SELECT CompanyCode, TotalCashDivi,EndDate FROM nq_dividend WHERE ifDividend = '1' and date(EndDate) = STR_TO_DATE('20191231','%Y%m%d') AND TotalCashDivi > 0) AS A,
(SELECT SecuCode, companyCode, ChiName,personalCode, LeaderName FROM tmp_sq_comp_manager_main_3) AS B
WHERE A.CompanyCode = B.companyCode GROUP BY CompanyCode, LeaderName, EndDate) AS D
WHERE C.CompanyCode = D.CompanyCode AND C.EndDate = D.EndDate AND C.SHName = D.LeaderName GROUP BY C.CompanyCode, LeaderName'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_fill_one_year_bonus_money'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空补全上一年分红数据成功')
        insert_sql = '''insert into tmp_fill_one_year_bonus_money(CompanyCode,SecuCode,ChiName,personalCode,LeaderName,oneYearBonusMoney) value 
        (%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入补全上一年分红数据成功')


    except Exception as e:
        print(e)
    finally:
        mysql_conn.close()

# 获取个人五年分红总报酬数
def tmp_five_year_bonus_money():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT D.CompanyCode,D.SecuCode,D.ChiName,D.personalCode,D.LeaderName, SUM(ROUND(C.EndCSHoldingPro * D.TotalCashDivi, 4)) as fiveYearBonusMoney from (SELECT CompanyCode, PersonalCode, PersonName, EndCSHoldingPro, EndDate
 from nq_leadershares WHERE date(EndDate) in (STR_TO_DATE('20191231','%Y%m%d'),STR_TO_DATE('20181231','%Y%m%d'),STR_TO_DATE('20171231','%Y%m%d'),
STR_TO_DATE('20161231','%Y%m%d'),STR_TO_DATE('20151231','%Y%m%d')) GROUP BY CompanyCode, PersonalCode, EndDate) as C, 
(SELECT A.*,B.SecuCode,B.ChiName,B.personalCode,B.LeaderName FROM
(SELECT CompanyCode, TotalCashDivi,EndDate FROM nq_dividend WHERE ifDividend = '1' and date(EndDate) IN (STR_TO_DATE('20191231','%Y%m%d'),STR_TO_DATE('20181231','%Y%m%d'),STR_TO_DATE('20171231','%Y%m%d'),
STR_TO_DATE('20161231','%Y%m%d'),STR_TO_DATE('20151231','%Y%m%d')) AND TotalCashDivi > 0) AS A,
(SELECT SecuCode, companyCode, ChiName,personalCode, LeaderName FROM tmp_sq_comp_manager_main_3) AS B
WHERE A.CompanyCode = B.companyCode GROUP BY CompanyCode, LeaderName, EndDate) AS D
WHERE C.CompanyCode = D.CompanyCode AND C.EndDate = D.EndDate AND C.PersonName = D.LeaderName GROUP BY C.CompanyCode, LeaderName'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_five_year_bonus_money'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空五年分红总额数据成功')
        insert_sql = '''insert into tmp_five_year_bonus_money(CompanyCode,SecuCode,ChiName,personalCode,LeaderName,fiveYearBonusMoney) value 
        (%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入五年分红总额数据成功')


    except Exception as e:
        print(e)
    finally:
        mysql_conn.close()

# 主表和一年分红报酬总额合并
def tmp_sq_comp_manager_main_4():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT A.*,B.oneYearBonusMoney FROM (SELECT * FROM tmp_sq_comp_manager_main_3) AS A LEFT JOIN
(SELECT * FROM tmp_one_year_bonus_money) AS B ON A.companyCode = B.CompanyCode AND A.LeaderName = B.LeaderName'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_4'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功4')
        insert_sql = '''insert into tmp_sq_comp_manager_main_4(SecuCode,SecuAbbr,innerCode,industryName,companyCode,
        ChiName,personalCode,LeaderName,Gender,EducationLevel,PosnName,BeginDate,EndDate,ChangeType,oneYearBonusMoney) value 
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功4')


    except Exception as e:
        print(e)
    finally:
        mysql_conn.close()

# 主表补全一年分红报酬总额
def tmp_sq_comp_manager_main_4plus():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT A.SecuCode,A.SecuAbbr,A.innerCode,A.industryName,A.companyCode,A.ChiName,A.personalCode,
     A.LeaderName,A.Gender,A.EducationLevel,A.PosnName,A.BeginDate,A.EndDate,A.ChangeType,
     (CASE WHEN  A.oneYearBonusMoney IS NULL THEN B.oneYearBonusMoney ELSE A.oneYearBonusMoney END) AS oneYearBonusMoney 
     FROM (SELECT * FROM tmp_sq_comp_manager_main_4) AS A LEFT JOIN
(SELECT * FROM tmp_fill_one_year_bonus_money) AS B ON A.companyCode = B.CompanyCode AND A.LeaderName = B.LeaderName'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_4'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功4plus')
        insert_sql = '''insert into tmp_sq_comp_manager_main_4(SecuCode,SecuAbbr,innerCode,industryName,companyCode,
        ChiName,personalCode,LeaderName,Gender,EducationLevel,PosnName,BeginDate,EndDate,ChangeType,oneYearBonusMoney) value 
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功4plus')


    except Exception as e:
        print(e)
    finally:
        mysql_conn.close()


# 主表和五年分红报酬总额合并
def tmp_sq_comp_manager_main_5():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT A.*,B.fiveYearBonusMoney FROM (SELECT * FROM tmp_sq_comp_manager_main_4) AS A LEFT JOIN
(SELECT * FROM tmp_five_year_bonus_money) AS B ON A.companyCode = B.CompanyCode AND A.LeaderName = B.LeaderName'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_5'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功5')
        insert_sql = '''insert into tmp_sq_comp_manager_main_5(SecuCode,SecuAbbr,innerCode,industryName,companyCode,
        ChiName,personalCode,LeaderName,Gender,EducationLevel,PosnName,BeginDate,EndDate,ChangeType,oneYearBonusMoney,fiveYearBonusMoney) value 
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功5')


    except Exception as e:
        print(e)
    finally:
        mysql_conn.close()

# 指标22-26, 9
def tmp_sq_comp_manager_main_6():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()

    try:
        # 获取最近收盘日勤
        TradingDay_sql = '''select TradingDay from nq_dailyquote  order by TradingDay DESC LIMIT 1'''
        cur.execute(TradingDay_sql)
        new_date_info = cur.fetchone()
        TradingDay = new_date_info[0]

        # todo 有些公司截止日期很接近当前日期暂不考虑取最新
        # EndDate2 = '''select EndDate from nq_top10sh group by EndDate order by EndDate DESC LIMIT 1'''
        # cur.execute(EndDate2)
        # new_date_infos = cur.fetchall()
        # EndDate2 = new_date_infos[0][0]

        # 年报日期
        # todo 这里年报日期按照当前年的上一年来计算
        timeStamp = int(time.time()) - (1546185600 - 1514736000)
        timeArray = time.localtime(timeStamp)
        EndDate = time.strftime("%Y1231", timeArray)
        EndDate1 = time.strftime("%Y-12-31 00:00:00", timeArray)
        EndDate = '20181231'
        EndDate1 = '2018-12-31 00:00:00'

        endDater = time.strftime("%Y1231", time.localtime(int(time.time()) - (1546185600 - 1514736000)))
        endDatel = time.strftime("%Y0101", time.localtime(int(time.time()) - (1546185600 - 1388505600)))

        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_6'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功6')

        mk_sql = '''SELECT O.SecuCode,O.SecuAbbr,O.innerCode,O.industryName,O.companyCode,O.ChiName,O.personalCode,O.LeaderName,O.Gender,O.EducationLevel,O.PosnName,O.BeginDate,O.EndDate,O.ChangeType,O.oneYearBonusMoney,O.fiveYearBonusMoney,O.holderMoney,O.TransValue,O.Last_TransDate,O.Last_TransValue,O.IfPledgeHoldSum,O.MarketValue,(O.TransValue+(case when O.fiveYearBonusMoney is NULL then 0 else O.fiveYearBonusMoney end))as fiveYearSumMoney FROM (
    SELECT M.*,ROUND((CASE WHEN N.ClosePrice is NULL THEN 0 ELSE N.ClosePrice END)*(CASE WHEN M.MVTotalShares is NULL THEN 0 ELSE M.MVTotalShares END), 2)as MarketValue FROM (SELECT K.*,(CASE WHEN L.TotalShares is NULL THEN 0 ELSE L.TotalShares END) as MVTotalShares FROM (
    SELECT I.*, (case WHEN J.IfPledgeHoldSum is NULL THEN 0 ELSE J.IfPledgeHoldSum END)as IfPledgeHoldSum FROM (
    SELECT G.*,H.Last_TransDate, (case WHEN H.Last_TransValue is NULL THEN 0 ELSE H.Last_TransValue END)as Last_TransValue FROM (
    SELECT E.*,(case WHEN F.TransValue is NULL then 0 ELSE F.TransValue END)as TransValue FROM (
    SELECT  C.SecuCode,C.SecuAbbr,C.innerCode,C.industryName,C.companyCode,
    C.ChiName,C.personalCode,C.LeaderName,C.Gender,C.EducationLevel,C.PosnName,C.BeginDate,C.EndDate,C.ChangeType,C.oneYearBonusMoney,C.fiveYearBonusMoney
    ,(case WHEN D.ClosePrice is NULL THEN 0 ELSE D.ClosePrice END)as ClosePrice,(case WHEN C.EndCSHoldings is NULL THEN 0 ELSE C.EndCSHoldings END)as EndCSHoldings,ROUND(ClosePrice*EndCSHoldings,2)as holderMoney  FROM 

    (SELECT A.*,B.EndCSHoldings FROM tmp_sq_comp_manager_main_5 as A LEFT JOIN (

    SELECT PersonName,  EndCSHoldings,PersonalCode,CompanyCode from nq_leadershares WHERE EndDate = "{}" GROUP BY CompanyCode,PersonName 
    ) as B ON B.CompanyCode=A.companyCode and A.LeaderName=B.PersonName) as C

    LEFT JOIN(
    SELECT InnerCode,ClosePrice  FROM nq_dailyquote WHERE TradingDay="{}" GROUP BY InnerCode
    ) as D ON D.InnerCode=C.innerCode )as E 

    LEFT JOIN (
    SELECT TransName,CompanyCode, SUM(TransValue)as TransValue  FROM nq_sharetransfer 
    WHERE EventProcedure="1022" and TransDate >= "{}" and TransDate <= "{}" and TransValue is not NULL and TransName is not NULL  GROUP BY TransName,CompanyCode
    )as F ON F.TransName=E.LeaderName and F.CompanyCode=E.companyCode) as G 

    LEFT JOIN (

    SELECT CompanyCode,TransName,max(TransDate)as Last_TransDate,(TransValue)as Last_TransValue  FROM nq_sharetransfer WHERE EventProcedure="1022" and TransName is not NULL GROUP BY CompanyCode,TransName
    ) as H ON H.TransName=G.LeaderName and H.CompanyCode=G.companyCode) as I

    LEFT JOIN (

    SELECT  CompanyCode,SHName, (case WHEN IfPledgeHoldSum is NULL THEN 0 ELSE IfPledgeHoldSum END)as IfPledgeHoldSum FROM nq_top10sh WHERE EndDate="{}"
    ) as J ON J.SHName=I.LeaderName and J.CompanyCode=I.companyCode) as K

    LEFT JOIN (
    SELECT CompanyCode,TotalShares from  nq_sharestru WHERE EndDate='{}' GROUP BY CompanyCode
    ) as L ON K.companyCode=L.CompanyCode ) as M
    LEFT JOIN(
    SELECT ClosePrice,innerCode FROM nq_dailyquote WHERE TradingDay="{}"
    )as N ON M.innerCode=N.innerCode ) as O'''.format(EndDate,TradingDay,endDatel,endDater,EndDate1,EndDate1,TradingDay)
        cur.execute(mk_sql)
        mk_sql = cur.fetchall()

        insert_sql = """INSERT INTO tmp_sq_comp_manager_main_6(SecuCode,SecuAbbr,innerCode,industryName,companyCode,ChiName,personalCode,LeaderName,Gender,EducationLevel,PosnName,BeginDate,EndDate,ChangeType,oneYearBonusMoney,fiveYearBonusMoney,holderMoney,TransValue,Last_TransDate,Last_TransValue,IfPledgeHoldSum,MarketValue,fiveYearSumMoney)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        val = mk_sql

        cur.executemany(insert_sql, val)
        mysql_conn.commit()
        print('tmp_sq_comp_manager_main_6批量插入数据完成')

    except Exception as e:
        print('出现报错---',e)
    else:
        pass
    finally:
        mysql_conn.close()


# 主表和工商信息合并
def tmp_sq_comp_manager_main_7():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT A.*,B.CreditCode,B.RegAddr,B.RegCity,B.OfficeAddr,B.OfficeCity,B.StateComp FROM (SELECT * FROM tmp_sq_comp_manager_main_6) AS A LEFT JOIN
(SELECT * FROM comp_commerical_message) AS B ON A.SecuCode = B.SecuCode'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_7'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功7')
        insert_sql = '''insert into tmp_sq_comp_manager_main_7(SecuCode,SecuAbbr,innerCode,industryName,companyCode,
        ChiName,personalCode,LeaderName,Gender,EducationLevel,PosnName,BeginDate,EndDate,ChangeType,oneYearBonusMoney,fiveYearBonusMoney,
        holderMoney,TransValue,Last_TransDate,Last_TransValue,IfPledgeHoldSum,MarketValue,fiveYearSumMoney,CreditCode,RegAddr,RegCity,OfficeAddr,OfficeCity,StateComp) value 
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功7')


    except Exception as e:
        print(e)
    finally:
        mysql_conn.close()

# 个人信息和个人学校信息合并
def merge_personal_school_message():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    select_sql = '''SELECT A.*,B.school,B.person_title FROM(SELECT * FROM nq_personalinfo) AS A LEFT JOIN
(SELECT PersonalCode,school,person_title from sanban_extract_school) as B ON A.PersonalCode = B.PersonalCode'''
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table nq_personalinfos'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空学校数据成功')
        insert_sql = '''insert into nq_personalinfos(ID,InfoPublDate,PersonalCode,PersonalNum,PersonalName,
        Gender,Nation,Nationality,BirthDate,Age,PermResidency,EducationLevel,GraduateInst,MajorName,Background,IDCard,Address,
        InsertTime,UpdateTime,JSID,_MASK_FROM_V2,_MASK_TO_V2,school,person_title) value 
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入学校数据成功')


    except Exception as e:
        print(e)
    finally:
        mysql_conn.close()


# 主表和个人信息合并, 最后一步处理
def tmp_sq_comp_manager_main_8():
    now = datetime.datetime.now()
    year = datetime.datetime.strftime(now, '%Y')
    year = int(year)
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    TradingDay_sql = '''select TradingDay from nq_dailyquote order by TradingDay DESC LIMIT 1'''
    cur.execute(TradingDay_sql)
    new_date_info = cur.fetchone()
    TradingDay = new_date_info[0]
    timeStamp = int(time.time()) - (1546185600 - 1514736000)
    timeArray = time.localtime(timeStamp)
    EndDate1 = time.strftime("%Y-12-31 00:00:00", timeArray)
    select_sql = '''SELECT A.*,B.BirthDate,B.age,B.school,B.person_title,B.Background FROM(SELECT * FROM tmp_sq_comp_manager_main_7) AS A LEFT JOIN
(SELECT BirthDate,(case when BirthDate is not null then ("{}" - CAST(DATE_FORMAT(BirthDate,'%Y') AS signed)) ELSE 0 END) as age,
Background,school,person_title,PersonalNum from nq_personalinfos) as B ON A.personalCode = B.PersonalNum'''.format(year)
    try:
        cur.execute(select_sql)
        infos = cur.fetchall()
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_8'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功8')
        insert_sql = '''insert into tmp_sq_comp_manager_main_8(SecuCode,SecuAbbr,innerCode,industryName,companyCode,
        ChiName,personalCode,LeaderName,Gender,EducationLevel,PosnName,BeginDate,EndDate,ChangeType,oneYearBonusMoney,fiveYearBonusMoney,
        holderMoney,TransValue,Last_TransDate,Last_TransValue,IfPledgeHoldSum,MarketValue,fiveYearSumMoney,CreditCode,RegAddr,RegCity,
        OfficeAddr,OfficeCity,StateComp,BirthDate,age,school,person_title,Background) value 
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功8')
        update_sql = '''UPDATE tmp_sq_comp_manager_main_8 SET  Background = REPLACE(REPLACE(Background, CHAR(10), ''), CHAR(13), '');'''
        cur.execute(update_sql)
        mysql_conn.commit()
        print('更新成功')
        nian_sql = '''UPDATE tmp_sq_comp_manager_main_8 as D INNER JOIN(
        SELECT G.*,F.ClosePrice FROM 
        (
        SELECT A.companyCode,A.LeaderName,A.innerCode,A.PosnName,B.HoldSumEnd FROM (SELECT * from tmp_sq_comp_manager_main_8 WHERE holderMoney="0" or holderMoney is NULL)  as A
        LEFT JOIN(
        SELECT CompanyCode,SHName,HoldSumEnd FROM nq_top10sh WHERE EndDate="{}" GROUP BY CompanyCode,SHName
        ) as B ON A.companyCode=B.CompanyCode and B.SHName=A.LeaderName WHERE B.HoldSumEnd is not NULL and B.HoldSumEnd <> 0
        )as G LEFT JOIN(
        SELECT ClosePrice,innerCode FROM nq_dailyquote WHERE TradingDay="{}" 
        )as F ON F.innerCode=G.innerCode
        )as E ON D.companyCode=E.companyCode and D.LeaderName=E.LeaderName set D.holderMoney=ROUND((CASE WHEN E.HoldSumEnd is NULL THEN 0 ELSE E.HoldSumEnd END)*(CASE WHEN E.ClosePrice is NULL THEN 0 ELSE E.ClosePrice END),2)
        '''.format(EndDate1, TradingDay)

        cur.execute(nian_sql)
        mysql_conn.commit()
        print('更新holderMoney,已经完成')


    except Exception as e:
        print(e)
    finally:
        mysql_conn.close()

# 对主表添加相关人物上一年的持股比例
def tmp_sq_comp_manager_main_9():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()

    timeStamp = int(time.time()) - (1546185600 - 1514736000)
    timeArray = time.localtime(timeStamp)
    EndDate1 = time.strftime("%Y-12-31 00:00:00", timeArray)

    try:
        truncate_table_sql = '''truncate table tmp_sq_comp_manager_main_9'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功9')

        ratio_sql = '''SELECT C.SecuCode,C.SecuAbbr,C.InnerCode,C.IndustryName,C.companyCode,C.ChiName,C.personalCode,C.LeaderName,
        C.Gender,C.EducationLevel,C.PosnName,C.BeginDate,C.EndDate,C.ChangeType,C.oneYearBonusMoney,C.fiveYearBonusMoney,
        round((CASE WHEN C.SHRatio1 is NULL THEN D.EndCSHoldingPro ELSE C.SHRatio1 END),4)as SHRatio,C.holderMoney,C.TransValue,
        (case when C.Last_TransValue >0 then C.Last_TransDate else NULL end) as Last_TransDate,C.Last_TransValue,C.IfPledgeHoldSum,
        C.MarketValue,C.fiveYearSumMoney,C.CreditCode,C.RegAddr,C.RegCity,C.OfficeAddr,C.OfficeCity,C.StateComp,
        (case when C.BirthDate is not null then date_format(C.BirthDate, '%Y%m') else null end) as BirthDate,C.age,C.school,C.person_title,C.Background  FROM (

SELECT A.*,round((B.HoldingRatioEnd),4)as SHRatio1 FROM tmp_sq_comp_manager_main_8 as A 

LEFT JOIN(
SELECT HoldingRatioEnd,CompanyCode,SHName FROM nq_top10sh  WHERE EndDate="{}" 
)as B ON A.companyCode=B.CompanyCode and A.LeaderName=B.SHName GROUP BY CompanyCode,LeaderName) as C

 LEFT JOIN (
SELECT CompanyCode,EndCSHoldingPro,PersonName FROM nq_leadershares WHERE EndDate="{}") as D
 ON C.companyCode=D.CompanyCode and C.LeaderName=D.PersonName GROUP BY CompanyCode,LeaderName'''.format(EndDate1,EndDate1)
        cur.execute(ratio_sql)
        ratio_sql = cur.fetchall()

        insert_sql = """INSERT INTO tmp_sq_comp_manager_main_9(SecuCode,SecuAbbr,innerCode,industryName,companyCode,
ChiName,personalCode,LeaderName,Gender,EducationLevel,PosnName,BeginDate,EndDate,ChangeType,oneYearBonusMoney,fiveYearBonusMoney,SHRatio,
holderMoney,TransValue,Last_TransDate,Last_TransValue,IfPledgeHoldSum,MarketValue,fiveYearSumMoney,CreditCode,RegAddr,RegCity,
OfficeAddr,OfficeCity,StateComp,BirthDate,age,school,person_title,Background)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        val = ratio_sql

        cur.executemany(insert_sql, val)
        mysql_conn.commit()
        print('tmp_sq_comp_manager_main_9批量插入数据完成')


    except Exception as e:
        print(e)
    else:
        pass
    finally:
        mysql_conn.close()


"""
# 三板新数据和上一次历史数据对比查找新增数据, 9为新数据, 10为历史数据 （每次执行10和11表清空）
INSERT into tmp_sq_comp_manager_main_11 SELECT a.* FROM (SELECT LeaderName,(case WHEN personalCode is not NULL THEN(CONCAT(innerCode, personalCode)) ELSE innerCode END) as number,
ChiName,SecuAbbr,CreditCode,RegAddr,RegCity,OfficeAddr,OfficeCity,SecuCode,MarketValue,StateComp,
 PosnName,BeginDate,EndDate,ChangeType,Gender,BirthDate,age,school,EducationLevel,person_title,Background,holderMoney,fiveYearSumMoney,
TransValue,Last_TransValue,Last_TransDate,IfPledgeHoldSum,fiveYearBonusMoney,oneYearBonusMoney from tmp_sq_comp_manager_main_9) as a LEFT JOIN
(SELECT LeaderName, ChiName FROM tmp_sq_comp_manager_main_10) as b on a.LeaderName = b.LeaderName and a.ChiName = b.ChiName
WHERE b.LeaderName is NULL GROUP BY LeaderName,ChiName ORDER BY ChiName

# 三板新数据和上一次历史数据对比查找同一人同一家公司减持金额和质押期数和分红金额是否变动, 9为新数据, 10为历史数据
INSERT into tmp_sq_comp_manager_main_11 SELECT a.LeaderName,b.number,a.ChiName,a.SecuAbbr,a.CreditCode,a.RegAddr,a.RegCity,a.OfficeAddr,a.OfficeCity,
a.SecuCode,a.MarketValue,a.StateComp,a.PosnName,a.BeginDate,a.EndDate,a.ChangeType,a.Gender,a.BirthDate,
a.age,a.school,a.EducationLevel,a.person_title,a.Background,a.holderMoney,a.fiveYearSumMoney,a.TransValue,
a.Last_TransValue,a.Last_TransDate,a.IfPledgeHoldSum,a.fiveYearBonusMoney,a.oneYearBonusMoney FROM (SELECT LeaderName,(case WHEN personalCode is not NULL THEN(CONCAT(innerCode, personalCode)) ELSE innerCode END) as number,
ChiName,SecuAbbr,CreditCode,RegAddr,RegCity,OfficeAddr,OfficeCity,SecuCode,MarketValue,StateComp,
 PosnName,BeginDate,EndDate,ChangeType,Gender,BirthDate,age,school,EducationLevel,person_title,Background,holderMoney,fiveYearSumMoney,
TransValue,Last_TransValue,Last_TransDate,IfPledgeHoldSum,fiveYearBonusMoney,oneYearBonusMoney from tmp_sq_comp_manager_main_9) as a LEFT JOIN
(SELECT LeaderName, number, ChiName, TransValue,Last_TransValue,IfPledgeHoldSum,fiveYearBonusMoney,oneYearBonusMoney
 FROM tmp_sq_comp_manager_main_10) as b on a.LeaderName = b.LeaderName and a.ChiName = b.ChiName
WHERE a.TransValue != b.TransValue and b.TransValue != 0 or a.Last_TransValue != b.Last_TransValue and b.Last_TransValue != 0 OR
a.IfPledgeHoldSum != b.IfPledgeHoldSum and b.IfPledgeHoldSum != 0 or a.fiveYearBonusMoney != b.fiveYearBonusMoney and b.fiveYearBonusMoney != 0 OR
a.oneYearBonusMoney != b.oneYearBonusMoney and b.oneYearBonusMoney != 0 GROUP BY LeaderName,ChiName ORDER BY ChiName
"""

"""
SELECT LeaderName,number,ChiName,SecuAbbr,CreditCode,RegAddr,RegCity,OfficeAddr,OfficeCity,
SecuCode,MarketValue,StateComp,PosnName,BeginDate,EndDate,ChangeType,Gender,BirthDate,
age,school,EducationLevel,person_title,Background,
(CASE WHEN holderMoney = 0.00 THEN NULL ELSE holderMoney END) as holderMoney,
(CASE WHEN fiveYearSumMoney = 0.00 THEN NULL ELSE fiveYearSumMoney END) as fiveYearSumMoney,
(CASE WHEN TransValue = 0.00 THEN NULL ELSE TransValue END) as TransValue,
(CASE WHEN Last_TransValue = 0.00 THEN NULL ELSE Last_TransValue END) as Last_TransValue,
Last_TransDate,(CASE WHEN IfPledgeHoldSum = 0.00 THEN NULL ELSE IfPledgeHoldSum END) as IfPledgeHoldSum,
(CASE WHEN fiveYearBonusMoney = 0.00 THEN NULL ELSE fiveYearBonusMoney END) as fiveYearBonusMoney,
(CASE WHEN oneYearBonusMoney = 0.00 THEN NULL ELSE oneYearBonusMoney END) as oneYearBonusMoney from tmp_sq_comp_manager_main_11
"""

if __name__ == "__main__":
    # tmp_sq_comp_manager_main_1()
    # tmp_sq_comp_manager_main_2()
    # tmp_sq_comp_manager_main_3()
    # tmp_sq_comp_manager_main_3puls()
    # tmp_one_year_bonus_money()
    # tmp_fill_one_year_bonus_money()
    # tmp_five_year_bonus_money()
    # tmp_sq_comp_manager_main_4()
    # tmp_sq_comp_manager_main_4plus()
    # tmp_sq_comp_manager_main_5()
    tmp_sq_comp_manager_main_6()
    # tmp_sq_comp_manager_main_7()
    # merge_personal_school_message()
    # tmp_sq_comp_manager_main_8()
    # tmp_sq_comp_manager_main_9()