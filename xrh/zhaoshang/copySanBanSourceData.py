'''
文件名：copySanBanSourceData.py
功能：招商项目三板数据源表更新
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
        db='cmb_sanban',
        charset='utf8'
    )

    return mysql_conn

def conn_mysql2():
    mysql_conn = pymysql.connect(
        host='172.17.23.128',
        port=6033,
        user='check_industry',
        passwd='shiye1805A',
        db='db_seeyii',
        charset='utf8'
    )

    return mysql_conn

def conn_mysql3():
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
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT * from nq_comarchive'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table nq_comarchive'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功1')
        insert_sql = '''insert into nq_comarchive(ID,CompanyCode,ParentCompany,ListedCode,InvestAdvisorNM,TrusteeName,ChiName,AbbrChiName,NameChiSpelling,EngName,AbbrEngName,RegCapital,CurrencyUnit,EstablishmentDt,EconomicNature,CompanyNature,CompanyType,CompanyCval,RegAddr,RegZip,RegCity,OfficeAddr,ContactAddr,ContactZip,ContactCity,Email,Website,LegalPersonRepr,GeneralManager,OtherManager,Contactman,Tel,Fax,BriefIntroText,BusinessMajor,Industry,StartDate,CloseDate,CloseReason,IfExisted,InsertTime,UpdateTime,JSID,_MASK_TO_V2,_MASK_FROM_V2,CreditCode,RegArea)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
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
    select_sql = '''SELECT * from nq_comlist'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table nq_comlist'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功2')
        insert_sql = '''insert into nq_comlist(ID,SecuCode,SecuAbbr,InnerCode,TransType,IndustryName,IndustryCode,IfEffective,InsertTime,UpdateTime,JSID,PriorityLevel,BrokerCode,AreaCode,_MASK_TO_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
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
    select_sql = '''SELECT * from nq_contmainsh'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table nq_contmainsh'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功3')
        insert_sql = '''insert into nq_contmainsh(ID,InfoPublDate,CompanyCode,InfoSource,MSHNumber,SHName,SHID,SHKind,RelationShip,BeginDate,EndDate,InsertTime,UpdateTime,JSID,_MASK_TO_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
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
    select_sql1 = '''SELECT TradingDay from nq_dailyquote order by TradingDay DESC LIMIT 1'''
    cur2.execute(select_sql1)
    info = cur2.fetchone()
    TradingDay = info[0]
    TradingDay  = '2020-07-02 00:00:00'
    select_sql2 = '''SELECT * from nq_dailyquote where TradingDay > "{}"'''.format(TradingDay)
    try:
        cur2.execute(select_sql2)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table nq_dailyquote'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功4')
        insert_sql = '''insert into nq_dailyquote(ID,InnerCode,TradingDay,ClosePrice,ChangeOfPrice,ChangePCT,TurnoverVolume,TurnoverValue,TurnoverDeals,TransType,PrevClosePrice,OpenPrice,HighPrice,LowPrice,InsertTime,UpdateTime,JSID,_MASK_FROM_V2,_MASK_TO_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
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
    select_sql = '''SELECT ID,InfoPublDate,InnerCode,CompanyCode,EndDate,EventProcedure,IfDividend,AdvanceDate,SMDeciPublDate,ItemsSN,DividendBase,BonusShareRatio,TranAddShareRto,CashDivPreTax,CashDivAfterTax,TotalCashDivi,RightRegDate,ExDivDate,DivObject,ToAccountDate,BonusShareArrDt,BonusShareListD,Notes,InsertTime,UpdateTime,JSID,_MASK_FROM_V2,_MASK_TO_V2 from nq_dividend'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table nq_dividend'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功5')
        insert_sql = '''insert into nq_dividend(ID,InfoPublDate,InnerCode,CompanyCode,EndDate,EventProcedure,IfDividend,AdvanceDate,SMDeciPublDate,ItemsSN,DividendBase,BonusShareRatio,TranAddShareRto,CashDivPreTax,CashDivAfterTax,TotalCashDivi,RightRegDate,ExDivDate,DivObject,ToAccountDate,BonusShareArrDt,BonusShareListD,Notes,InsertTime,UpdateTime,JSID,_MASK_FROM_V2,_MASK_TO_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功5')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData6():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT * from nq_leaderposn'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table nq_leaderposn'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功6')
        insert_sql = '''insert into nq_leaderposn(ID,InfoPublDate,CompanyCode,PersonalCode,LeaderName,Gender,GenderCode,EducationLevel,EduLevelCode,PosnNum,PosnName,BeginDate,EndDate,ChangeType,ChangeReason,IfReward,IfPosition,Notes,InsertTime,UpdateTime,JSID,_MASK_FROM_V2,_MASK_TO_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功6')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData7():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT * from nq_leadershares'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table nq_leadershares'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功7')
        insert_sql = '''insert into nq_leadershares(ID,InfoPublDate,CompanyCode,EndDate,PersonalCode,PersonName,BeginCSHoldings,CSHoldingChange,EndCSHoldings,EndCSHoldingPro,EndStockOption,InsertTime,UpdateTime,JSID,EndIndHolding,EndIndHoldPro,_MASK_FROM_V2,_MASK_TO_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功7')

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
    select_sql = '''SELECT * from nq_personalinfo'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table nq_personalinfo'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功8')
        insert_sql = '''insert into nq_personalinfo(ID,InfoPublDate,PersonalCode,PersonalNum,PersonalName,Gender,Nation,Nationality,BirthDate,Age,PermResidency,EducationLevel,GraduateInst,MajorName,Background,IDCard,Address,InsertTime,UpdateTime,JSID,_MASK_FROM_V2,_MASK_TO_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功8')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData9():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT * from nq_sharestru'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table nq_sharestru'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功9')
        insert_sql = '''insert into nq_sharestru(ID,InfoPublDate,CompanyCode,EndDate,TotalShares,ChangeReason,URShares,URShareProp,URController,URControllProp,UREMBA,UREMBAProp,URCoreStaffs,URCoreStaffProp,OtherURShares,OtherURSProp,RSTRShares,RSTRSharesProp,RController,RControllerProp,REMBA,REMBAProp,RCoreStaffs,RCoreStaffsProp,OtherRShares,OtherRShareProp,InsertTime,UpdateTime,JSID,_MASK_FROM_V2,_MASK_TO_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功9')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData10():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT * from nq_sharetransfer'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table nq_sharetransfer'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功10')
        insert_sql = '''insert into nq_sharetransfer(ID,InfoPublDate,CompanyCode,TransDate,TransType,EventProcedure,TransPrice,TransVolume,TransValue,TransCurrency,TransInfo,TransName,SumBeforeTran,PCTBeforeTran,SumAfterTran,PCTAfterTran,ReceiverName,SumBeforeRece,PCTBeforeRece,SumAfterRece,PCTAfterRece,InsertTime,UpdateTime,JSID,TransCode,ReceiverCode,_MASK_TO_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功10')

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
    select_sql = '''SELECT * from secumain'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table secumain'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功11')
        insert_sql = '''insert into secumain(ID,InnerCode,CompanyCode,SecuCode,ChiName,ChiNameAbbr,EngName,EngNameAbbr,SecuAbbr,ChiSpelling,SecuMarket,SecuCategory,ListedDate,ListedSector,ListedState,XGRQ,JSID,ISIN,_MASK_TO_V2,_MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功11')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData12():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT * from nq_top10sh'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table nq_top10sh'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功12')
        insert_sql = '''insert into nq_top10sh(ID,InfoPublDate,CompanyCode,EndDate,SH,SHID,SHName,SHKind,SHKindNum,HoldSumStart,HoldSumEnd,HoldingRatioEnd,RestriHoldSum,UnRestriHoldSum,HoldChange,ChangeReason,IfPledgeHoldSum,InsertTime,UpdateTime,JSID,_MASK_TO_V2,_MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功12')

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
    select_sql = '''SELECT * from sq_thsk_compmanger'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table sq_thsk_compmanger'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功13')
        insert_sql = '''insert into sq_thsk_compmanger(id,compCode,personalCode,cName,actdutyName,posType,beginDate,degree,holdAmt,holdDate,rembeftax,cur,rembeftaxMemo,gender,birthday,resume,isValid,tmstamp,entryDate,entryTime)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功13')

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
    mysql_conn3 = conn_mysql3()
    cur3 = mysql_conn3.cursor()
    select_sql = '''SELECT * from lc_instiarchive '''
    try:
        cur3.execute(select_sql)
        infos = cur3.fetchall()
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
    mysql_conn3 = conn_mysql3()
    cur3 = mysql_conn3.cursor()
    select_sql = '''SELECT * from lc_areacode '''
    try:
        cur3.execute(select_sql)
        infos = cur3.fetchall()
        truncate_table_sql = '''truncate table lc_areacode'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功lc_instiarchive')
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


def get_time():
    now = datetime.datetime.now()
    print(now)

def columns_message():
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    sql = '''SELECT COLUMN_NAME  FROM information_schema.columns
WHERE TABLE_NAME = 'sq_thsk_compmanger' '''
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


if __name__ == "__main__":
    get_time()
    copySourceData1()
    copySourceData2()
    copySourceData3()
    copySourceData4()
    copySourceData5()
    copySourceData6()
    copySourceData7()
    copySourceData8()
    copySourceData9()
    copySourceData10()
    copySourceData11()
    copySourceData12()
    copySourceData13()
    copySourceData_lc_instiarchive()
    copySourceData_lc_areacode()
    get_time()