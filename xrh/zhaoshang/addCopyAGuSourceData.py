'''
文件名：copyAGuSourceData.py
功能：招商项目A股数据源表更新
代码历史：20190522，徐荣华
'''

import pymysql
import datetime

def conn_mysql():
    mysql_conn = pymysql.connect(
        host='0.0.0.0',
        port=33006,
        user='root',
        passwd='123456',
        db='CMB',
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

def copySourceData1():
    mysql_conn = conn_mysql()   # docker
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2() # 数据源表
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(_MASK_FROM_V2) from sq_comp_manager'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from sq_comp_manager where _MASK_FROM_V2 > '{}'  '''.format(_MASK_FROM_V2)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
        insert_sql = '''insert into sq_comp_manager(id,updateDate,innerCode,posType,dutyCode,dutyMod,actdutyName,personalCode,cName,mgentrys,dentrys,nowStatus,beginDate,enDdate,dimReason,isReldim,memo,sourceTable,sourceId,isValid,tmstamp,entryDate,entryTime,insertTime,_MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功1')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData1_1():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT * from sq_comp_manager'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sq_comp_manager'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功1')
        insert_sql = '''insert into sq_comp_manager(id,updateDate,innerCode,posType,dutyCode,dutyMod,actdutyName,personalCode,cName,mgentrys,dentrys,nowStatus,beginDate,enDdate,dimReason,isReldim,memo,sourceTable,sourceId,isValid,tmstamp,entryDate,entryTime,insertTime,_MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功1')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData2():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(_MASK_FROM_V2) from sq_sk_basicinfo'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from sq_sk_basicinfo where _MASK_FROM_V2 > '{}'  '''.format(_MASK_FROM_V2)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
        insert_sql = '''insert into sq_sk_basicinfo(id,secuCode,companyCode,innerCode,exchange,setype,companyName,secuAbbr,chiSpelling,engName,parValue,mainIndustryCode,mainIndustryName,regAddr,briefIntroText,totalShare,listStatus,listDate,delistDate,provinceCode,provinceName,cityCode,cityName,lat,lng,isvalid,entryDate,industryType,sourceId,inserTime,_MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功2')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData2_1():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT * from sq_sk_basicinfo'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sq_sk_basicinfo'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功2')
        insert_sql = '''insert into sq_sk_basicinfo(id,secuCode,companyCode,innerCode,exchange,setype,companyName,secuAbbr,chiSpelling,engName,parValue,mainIndustryCode,mainIndustryName,regAddr,briefIntroText,totalShare,listStatus,listDate,delistDate,provinceCode,provinceName,cityCode,cityName,lat,lng,isvalid,entryDate,industryType,sourceId,inserTime,_MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功2')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData3():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    s = '''select tradeDate from sq_qt_skdailyprice order by tradeDate desc limit 1'''
    cur.execute(s)
    info = cur.fetchone()
    tradeDate = info[0]
    select_sql = '''SELECT * from sq_qt_skdailyprice where tradeDate > "{}" '''.format(tradeDate)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        insert_sql = '''insert into sq_qt_skdailyprice(id,tradeDate,companyCode,exchange,sName,lClose,tOpen,tClose,tHigh,tLow,vol,amount,deals,avgPrice,avgVol,avgTramt,changes,pchg,amplitude,negotiablemv,totmktcap,turnRate,isvalid,tmstamp,entryDate,entryTime,MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功3')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData4():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(_MASK_FROM_V2) from sq_sk_shareholder'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from sq_sk_shareholder where endDate >= "{}" and _MASK_FROM_V2 > '{}'  '''.format('20140101',_MASK_FROM_V2)
    cur2.execute(select_sql)
    infos = cur2.fetchall()
    print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
    for info in infos:
        insert_sql = '''insert into sq_sk_shareholder(id,publishdate,endDate,innerCode,shHolderCode,shHolderName,shHolderType,shHolderNature,rank,shareStype,holderamt,holderRTO,limitHolderamt,unlimHolderamt,circamt,ncircamt,curchg,isvalid,tmstamp,entryDate,entryTime,pfHolderamt,shHolderid,shHolderSecode,actShareStype,shholderRelememo,shholdrEleGroup,actconcertgroup,datamod,ishis,isReportDate,updateDate,inserTime,_MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        try:
            cur.execute(insert_sql, info)
            mysql_conn.commit()
        except Exception as e:
            print(e)
    print('插入数据成功4')
    mysql_conn2.close()
    mysql_conn.close()

def copySourceData4_1():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    count_sql = '''SELECT * from sq_sk_shareholder where endDate >= "{}" '''.format('20140101')
    cur2.execute(count_sql)
    count = cur2.fetchone()[0]
    truncate_table_sql = '''truncate table sq_sk_shareholder'''
    cur.execute(truncate_table_sql)
    mysql_conn.commit()
    print('清空数据成功4')
    try:
        for i in range(count)[::100000]:
            select_sql = '''SELECT * from sq_sk_shareholder where endDate >= "{}" limit ,100000 '''.format('20140101',i)
            cur2.execute(select_sql)
            infos = cur2.fetchall()
            insert_sql = '''insert into sq_sk_shareholder(id,publishdate,endDate,innerCode,shHolderCode,shHolderName,shHolderType,shHolderNature,rank,shareStype,holderamt,holderRTO,limitHolderamt,unlimHolderamt,circamt,ncircamt,curchg,isvalid,tmstamp,entryDate,entryTime,pfHolderamt,shHolderid,shHolderSecode,actShareStype,shholderRelememo,shholdrEleGroup,actconcertgroup,datamod,ishis,isReportDate,updateDate,inserTime,_MASK_FROM_V2)
                            value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            cur.executemany(insert_sql, infos)
            mysql_conn.commit()
        print('插入数据成功4')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData4_2():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    try:
        select_sql = '''SELECT * from sq_sk_shareholder where endDate >= "{}" '''.format('20140101')
        truncate_table_sql = '''truncate table sq_sk_shareholder'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功4')
        cur2.execute(select_sql)
        # infos = cur2.fetchall()
        infos = ()
        for num,data in enumerate(cur2):
            infos = infos + (data,)
            if num % 1000 == 0:
                insert_sql = '''insert into sq_sk_shareholder(id,publishdate,endDate,innerCode,shHolderCode,shHolderName,shHolderType,shHolderNature,rank,shareStype,holderamt,holderRTO,limitHolderamt,unlimHolderamt,circamt,ncircamt,curchg,isvalid,tmstamp,entryDate,entryTime,pfHolderamt,shHolderid,shHolderSecode,actShareStype,shholderRelememo,shholdrEleGroup,actconcertgroup,datamod,ishis,isReportDate,updateDate,inserTime,_MASK_FROM_V2)
                                value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                cur.executemany(insert_sql, infos)
                mysql_conn.commit()
                infos = ()
                
        print('插入数据成功4')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData5():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(_MASK_FROM_V2) from sq_comp_personrecord'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from sq_comp_personrecord where _MASK_FROM_V2 > '{}'  '''.format(_MASK_FROM_V2)
    cur2.execute(select_sql)
    infos = cur2.fetchall()
    print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
    for info in infos:
        insert_sql = '''insert into sq_comp_personrecord(id,declareDate,updateDate,personalCode,tableSource,cName,otherName,gender,birthday,nationality,nation,birthplace,titles,degree,highestDegree,idCardNumber,passportNumber,rightOfResidence,address,educationLevel,paffili,cPosition,professionalTitles,majorWorks,finpDate,bBackground,mate,parents,children,brotherSsisters,dieDate,hobby,memo,isvalid,tmstamp,entryDate,entryTime,insertTime,_MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        try:
            cur.execute(insert_sql, info)
            mysql_conn.commit()
        except Exception as e:
            print(e)
    print('插入数据成功5')
    mysql_conn2.close()
    mysql_conn.close()

def copySourceData6():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(entryDate) from sq_sk_sharehdchg'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from sq_sk_sharehdchg where entryDate > '{}'  '''.format(_MASK_FROM_V2)
    cur2.execute(select_sql)
    infos = cur2.fetchall()
    print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
    for info in infos:
        insert_sql = '''insert into sq_sk_sharehdchg(id,publishDate,beginDate,endDate,compCode,shHolderType,shHolderCode,shHolderName,shHolderNature,sharesType,duty,changeDire,bidChgAMT,bidAvgPrice,blockChgAMT,blockAvgPrice,othChgAMT,othAvgPrice,totChgAMT,totAvgPrice,bfShareAMT,afShareAMT,limSkAMT,circSkAMT,dataSource,memo,isValid,tmStamp,entryDate,entryTime,shareHdChgId)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        try:
            cur.execute(insert_sql, info)
            mysql_conn.commit()
        except Exception as e:
            print(e)
    print('插入数据成功6')
    mysql_conn2.close()
    mysql_conn.close()

def copySourceData7():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(entryDate) from sq_comp_freezingsk'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from sq_comp_freezingsk where entryDate > '{}'  '''.format(_MASK_FROM_V2)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
        insert_sql = '''insert into sq_comp_freezingsk(id,sfrzId,compCode,firstPublishDate,latestPublishDate,shHolderCode,shHolderName,frozenRSNType,frozenRSN,frozenSkamt,holdingSk,frozenRTOH,frozenRTOT,frozenBegDate,unFreezeDate,pdeFrozenDate,freezingPeriod,memo,isValid,tmStamp,entryDate,entryTime)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功7')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData7_1():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT * from sq_comp_freezingsk '''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sq_comp_freezingsk'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功7')
        insert_sql = '''insert into sq_comp_freezingsk(id,sfrzId,compCode,firstPublishDate,latestPublishDate,shHolderCode,shHolderName,frozenRSNType,frozenRSN,frozenSkamt,holdingSk,frozenRTOH,frozenRTOT,frozenBegDate,unFreezeDate,pdeFrozenDate,freezingPeriod,memo,isValid,tmStamp,entryDate,entryTime)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功7')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData8():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(_MASK_FROM_V2) from sq_sk_dividents'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from sq_sk_dividents where diviYear >= "{}" and  _MASK_FROM_V2 > '{}'  '''.format('2014',_MASK_FROM_V2)
    cur2.execute(select_sql)
    infos = cur2.fetchall()
    print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
    for info in infos:
        insert_sql = '''insert into sq_sk_dividents(id,publishDate,updateDate,securityId,companyCode,secuCode,innerCode,diviYear,dateType,diviType,graObjType,graObj,projecttype,equRecordate,xdrDate,cur,shCapBaseDate,shCapBaseQTY,lastTradDae,preTaxCashMaxDv,preTaxCashMinDv,aftTaxCashDv,aftTaxCashDvQFII,preTaxCashMaxDvcny,preTaxCashMinDvcny,aftTaxCashDvcny,aftTaxCashDvcnyQFII,exchangert,cashDvarrBegDate,cashDvarrEndDate,proBonusRt,tranAddRt,surPubResTranAdd,capPubResTranAdd,bonuSrt,totCashDv,totProBonus,totTranAdd,totBonus,sharrDate,listDate,shhdMeetResPubDate,isNewEst,diviExpMemo,dishtyid,isvalId,tmstamp,entryDate,entryTime,insertTime,_MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        try:
            cur.execute(insert_sql, info)
            mysql_conn.commit()
        except Exception as e:
            print(e)
    print('数据插入成功8')
    mysql_conn2.close()
    mysql_conn.close()

def copySourceData9():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(_MASK_FROM_V2) from secumain'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from secumain where _MASK_FROM_V2 > '{}'  '''.format(_MASK_FROM_V2)
    cur2.execute(select_sql)
    infos = cur2.fetchall()
    print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
    for info in infos:
        insert_sql = '''insert into secumain(ID,InnerCode,CompanyCode,SecuCode,ChiName,ChiNameAbbr,EngName,EngNameAbbr,SecuAbbr,ChiSpelling,SecuMarket,SecuCategory,ListedDate,ListedSector,ListedState,XGRQ,JSID,ISIN,_MASK_TO_V2,_MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        try:
            cur.execute(insert_sql, info)
            mysql_conn.commit()
        except Exception as e:
            print(e)
    print('数据插入成功9')
    mysql_conn2.close()
    mysql_conn.close()

def copySourceData10():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    select_sql = '''SELECT * from lc_actualcontroller '''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table lc_actualcontroller'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功10')
        insert_sql = '''insert into lc_actualcontroller(ID,CompanyCode,InfoPublDate,EndDate,ControllerCode,ControllerName,EconomicNature,NationalityCode,NationalityDesc,PermanentResidency,UpdateTime,JSID,_MASK_TO_V2,ControllerNature)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功10')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData11():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(XGRQ) from lc_executivesholdings'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from lc_executivesholdings where date(EndDate) >= STR_TO_DATE('20140101', '%Y%m%d') and XGRQ > "{}"'''.format(_MASK_FROM_V2)
    cur2.execute(select_sql)
    infos = cur2.fetchall()
    print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
    insert_sql = '''insert into lc_executivesholdings(ID,CompanyCode,InfoPublDate,EndDate,InfoSource,SequenceNumber,LeaderName, Position, ShareAmount,XGRQ,JSID,AnnualReward,IfIn,OffStatement,PositionDescription,ShareAmountBeginning,IndirectShares,ChangeReason,Subsidy,SharesExercisable,SharesExercised,ExercisePrice,EndingMarketPrice,_MASK_TO_V2,CurrencyUnit)
                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    for info in infos:
        try:
            cur.execute(insert_sql, info)
            mysql_conn.commit()
        except Exception as e:
            print(e)
    print('数据插入成功11')
    mysql_conn2.close()
    mysql_conn.close()

def copySourceData12():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(XGRQ) from lc_rewardstat'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from lc_rewardstat where XGRQ > "{}"'''.format(_MASK_FROM_V2)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
        insert_sql = '''insert into lc_rewardstat(ID,CompanyCode,InfoPublDate,InfoSource,EndDate,TotalYearPay,NumPayManagers,High3Directors,High3Managers,TotalIndeSupeYearPay,Statement,XGRQ,JSID,TotalIndeSubsidy,_MASK_TO_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功12')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData12_1():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT * from lc_rewardstat '''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table lc_rewardstat'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功12')
        insert_sql = '''insert into lc_rewardstat(ID,CompanyCode,InfoPublDate,InfoSource,EndDate,TotalYearPay,NumPayManagers,High3Directors,High3Managers,TotalIndeSupeYearPay,Statement,XGRQ,JSID,TotalIndeSubsidy,_MASK_TO_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功12')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData13():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(entryDate) from 

    '''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from sq_sk_incentive where entryDate > "{}"'''.format(_MASK_FROM_V2)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
        insert_sql = '''insert into sq_sk_incentive(id,skIncentiveCode,symbol,seCode,exchange,compCode,declareDate,incenTimes,incenObjNum,incenTarget,grantMethod,incenPeriod,targetSkSouce,fundSouce,gFundPrepared,targetSkType,targetSkNum,totalSkRTO,preExerPrctoPlan,preGrantPrctoPlan,grantCondition,waitPeriod,lockPeriod,floatPeriod,cur,memo,isValid,tmStamp,entryDate,entryTime,mainPlanCode,nYear,exrtactAMT,incenPeriodUnit,waitPeriodUnit,lockPeriodUnit,floatPeriodUnit,indeFinAdviser,lawfirm)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功13')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData13_1():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT * from sq_sk_incentive'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sq_sk_incentive'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功13')
        insert_sql = '''insert into sq_sk_incentive(id,skIncentiveCode,symbol,seCode,exchange,compCode,declareDate,incenTimes,incenObjNum,incenTarget,grantMethod,incenPeriod,targetSkSouce,fundSouce,gFundPrepared,targetSkType,targetSkNum,totalSkRTO,preExerPrctoPlan,preGrantPrctoPlan,grantCondition,waitPeriod,lockPeriod,floatPeriod,cur,memo,isValid,tmStamp,entryDate,entryTime,mainPlanCode,nYear,exrtactAMT,incenPeriodUnit,waitPeriodUnit,lockPeriodUnit,floatPeriodUnit,indeFinAdviser,lawfirm)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功13')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData14():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(entryDate) from sq_sk_incentiveobjlist'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from sq_sk_incentiveobjlist where entryDate > "{}"'''.format(_MASK_FROM_V2)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
        insert_sql = '''insert into sq_sk_incentiveobjlist(id,skIncentiveCode,declareDate,incenObjCode,incenObj,incenObjPosition,grantNum,grantNumComp,grantRTO,totalSkRTO,grantStatus,memo,isValid,tmStamp,entryDate,entryTime,skIncengCode)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功14')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData14_1():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT * from sq_sk_incentiveobjlist '''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sq_sk_incentiveobjlist'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功14')
        insert_sql = '''insert into sq_sk_incentiveobjlist(id,skIncentiveCode,declareDate,incenObjCode,incenObj,incenObjPosition,grantNum,grantNumComp,grantRTO,totalSkRTO,grantStatus,memo,isValid,tmStamp,entryDate,entryTime,skIncengCode)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功14')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData15():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT compCode,limskHolderCode,limskHolderName,listDate,newListingSKAmt,projectFeature from tq_comp_limskholder WHERE listDate >= '20190101' and listDate <= '20211231' and limskHolderType = '3' '''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table tq_comp_limskholder'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功15')
        insert_sql = '''insert into tq_comp_limskholder(compCode,limskHolderCode,limskHolderName,listDate,newListingSKAmt,projectFeature)
                        value (%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功15')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData_lc_instiarchive():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT * from lc_instiarchive '''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table lc_instiarchive'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功lc_instiarchive')
        insert_sql = '''insert into lc_instiarchive(ID,
                            CompanyCode,
                            ParentCompany,
                            ListedCode,
                            InvestAdvisorName,
                            TrusteeName,
                            ChiName,
                            AbbrChiName,
                            NameChiSpelling,
                            EngName,
                            AbbrEngName,
                            RegCapital,
                            CurrencyUnit,
                            EstablishmentDate,
                            EconomicNature,
                            CompanyNature,
                            CompanyType,
                            RegAddr,
                            RegZip,
                            RegCity,
                            OfficeAddr,
                            ContactAddr,
                            ContactZip,
                            ContactCity,
                            Email,
                            Website,
                            LegalPersonRepr,
                            GeneralManager,
                            OtherManager,
                            Contactman,
                            Tel,
                            Fax,
                            BriefIntroText,
                            BusinessMajor,
                            Industry,
                            StartDate,
                            CloseDate,
                            CloseReason,
                            IfExisted,
                            XGRQ,
                            JSID,
                            OrganizationCode,
                            CompanyCval,
                            CreditCode,
                            RegArea,
                            RegOrg,
                            RegStatus,
                            _MASK_TO_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功lc_instiarchive')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData_lc_areacode():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT * from lc_areacode '''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table lc_areacode'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功lc_areacode')
        insert_sql = '''insert into lc_areacode(ID,
                            AreaInnerCode,
                            AreaCode,
                            FirstLevelCode,
                            SecondLevelCode,
                            AreaChiName,
                            AreaEngName,
                            AreaEngNameAbbr,
                            ParentNode,
                            ParentName,
                            IfEffected,
                            CancelDate,
                            ChangeNote,
                            Remark,
                            UpdateTime,
                            JSID,
                            InsertTime,
                            _MASK_TO_V2,
                            _MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功lc_areacode')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData_sq_comp_info():
    mysql_conn = conn_mysql()   # CMB 表
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()     # 源表
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(_MASK_FROM_V2) from sq_comp_info'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from sq_comp_info where _MASK_FROM_V2 >"{}"'''.format(_MASK_FROM_V2)
    cur2.execute(select_sql)
    infos = cur2.fetchall()
    print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
    insert_sql = '''insert into sq_comp_info(id,
                    publishDate,
                    innerCode,
                    compName,
                    compSname,
                    engName,
                    engSname,
                    compType1,
                    compType2,
                    isList,
                    isBranch,
                    foundDate,
                    orgType,
                    regCapital,
                    authCapsk,
                    cur,
                    orgCode,
                    region,
                    country,
                    chairman,
                    manager,
                    legrep,
                    bsecretary,
                    bSecretaryTel,
                    bSecretaryFax,
                    seaffrepr,
                    seagtTel,
                    seagtFax,
                    seagteMail,
                    authreprsbd,
                    leconstant,
                    accFirm,
                    bizscale,
                    regAddr,
                    regptCode,
                    officeAddr,
                    officeZIPCode,
                    compTel,
                    compFax,
                    compEmail,
                    compUrl,
                    disUrl,
                    dispApers,
                    serviceTel,
                    serviceFax,
                    compIntro,
                    bizScope,
                    majorBiz,
                    bizLicenseNo,
                    taxRegisNo,
                    landTaxRegisNo,
                    compStatus,
                    existBegDate,
                    existEndDate,
                    isvalId,
                    tmstamp,
                    entryDate,
                    entryTime,
                    bSecretaryMail,
                    workForce,
                    insertTime,
                    _MASK_FROM_V2)
                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    for info in infos:
        try:
            cur.execute(insert_sql, info)
            mysql_conn.commit()
        except Exception as e:
            print(e)
    print('数据插入成功sq_comp_info')
    mysql_conn2.close()
    mysql_conn.close()

def get_time():
    now = datetime.datetime.now()
    print(now)

# 获取表列名
def columns_message():
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    sql = '''SELECT COLUMN_NAME  FROM information_schema.columns
WHERE TABLE_NAME = 'sq_sk_incentiveobjlist' '''
    try:
        cur2.execute(sql)
        info = cur2.fetchall()
        l = []
        for i in info:
            l.append(i[0])
        s = ','.join(l)
        print(len(l))
        print(s)


    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()

def main_Ag():
    # 增量更新 copySourceData1()
    copySourceData1_1()
    copySourceData2_1()
    copySourceData3()   # ok
    # 全量更新暂不使用 copySourceData4_1()
    # 全量更新暂不使用 copySourceData4_2()
    copySourceData4()
    copySourceData5()
    copySourceData6()
    copySourceData7_1()
    copySourceData8()
    copySourceData9()
    copySourceData10()  # ok
    copySourceData11()
    copySourceData12_1()  # ok    
    copySourceData13_1()
    copySourceData14_1()
    copySourceData15()  # ok
    copySourceData_lc_instiarchive()
    copySourceData_lc_areacode()
    copySourceData_sq_comp_info()

if __name__ == "__main__":
    get_time()
    main_Ag()
    get_time()
    # columns_message()