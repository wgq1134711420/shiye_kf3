'''
文件名：copyAGuSourceData.py
功能：招商项目港股数据源表更新
代码历史：20200529，邢冬梅
'''

import pymysql
import datetime

def conn_mysql():
    mysql_conn = pymysql.connect(
        host='0.0.0.0',
        port=33006,
        user='root',
        passwd='123456',
        db='cmb_ganggu',
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
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(_MASK_FROM_V2) from hk_dividend'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from hk_dividend where _MASK_FROM_V2 > '{}'  '''.format(_MASK_FROM_V2)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
        insert_sql = '''insert into hk_dividend(
                ID,
                InnerCode,
                InitialInfoPublDate,
                Process,
                DMPublDate,
                DMSignDate,
                SMDeciPublDate,
                ListingPublDate,
                EndDate,
                DividendPeriod,
                FiscalYear,
                IfDividend,
                DividendType,
                DividendUnit,
                CashDividendPS,
                OtherOption,
                SpecialDividendPS,
                SpecialDividendSubstitute,
                ShareDividendRateX,
                ShareDividendRateY,
                WarrantDividendRateX,
                WarrantDividendRateY,
                PhysicalDividendRateX,
                PhysicalDividendRateY,
                Statement,
                TotalCashDividend,
                DividendBase,
                DividendBaseShares,
                TotalShareDividend,
                TotalWarrantDividend,
                LastTradeDay,
                ExDate,
                RecordDate,
                TransferBeginDate,
                TransferEndDate,
                DividendPayableDate,
                PayoutDate,
                ShareDiviListingDate,
                ScripDividendIssuePrice,
                ScripDividendPayoutDate,
                ScripDividendListingDate,
                ScripDividendSum,
                XGRQ,
                JSID,
                CashDividendPSHKD,
                SpecialDividendPSHKD,
                TotCashDivUnit,
                AssReportType,
                TransferRatX,
                TransferRatY,
                TransferTotShares,
                PhysicalRefInnerCode,
                PhysicalRefFirmCode,
                PhysicalRefDeliverDay,
                PhysicalRefListDay,
                CancelDividendDate,
                InsertTime,
                _MASK_TO_V2,
                _MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功hk_dividend')

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
    select_sql = '''SELECT * from hk_dividend'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table hk_dividend'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功hk_dividend')
        insert_sql = '''insert into hk_dividend(
                ID,
                InnerCode,
                InitialInfoPublDate,
                Process,
                DMPublDate,
                DMSignDate,
                SMDeciPublDate,
                ListingPublDate,
                EndDate,
                DividendPeriod,
                FiscalYear,
                IfDividend,
                DividendType,
                DividendUnit,
                CashDividendPS,
                OtherOption,
                SpecialDividendPS,
                SpecialDividendSubstitute,
                ShareDividendRateX,
                ShareDividendRateY,
                WarrantDividendRateX,
                WarrantDividendRateY,
                PhysicalDividendRateX,
                PhysicalDividendRateY,
                Statement,
                TotalCashDividend,
                DividendBase,
                DividendBaseShares,
                TotalShareDividend,
                TotalWarrantDividend,
                LastTradeDay,
                ExDate,
                RecordDate,
                TransferBeginDate,
                TransferEndDate,
                DividendPayableDate,
                PayoutDate,
                ShareDiviListingDate,
                ScripDividendIssuePrice,
                ScripDividendPayoutDate,
                ScripDividendListingDate,
                ScripDividendSum,
                XGRQ,
                JSID,
                CashDividendPSHKD,
                SpecialDividendPSHKD,
                TotCashDivUnit,
                AssReportType,
                TransferRatX,
                TransferRatY,
                TransferTotShares,
                PhysicalRefInnerCode,
                PhysicalRefFirmCode,
                PhysicalRefDeliverDay,
                PhysicalRefListDay,
                CancelDividendDate,
                InsertTime,
                _MASK_TO_V2,
                _MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功hk_dividend')

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
    cmb_time_sql = '''SELECT max(_MASK_FROM_V2) from hk_leaderintroduce'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT ID,
            CompanyCode,
            InfoPublDate,
            LeaderID,
            LeaderName,
            LeaderGender,
            BirthYM,
            Age,
            LeaderDegree,
            LeaderTitle,
            EarliestInDate,
            Background,
            Statement,
            XGRQ,
            JSID,
            BirthYMInfo,
            _MASK_TO_V2,
            _MASK_FROM_V2 from hk_leaderintroduce where _MASK_FROM_V2 > '{}'  '''.format(_MASK_FROM_V2)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
        insert_sql = '''insert into hk_leaderintroduce(
            ID,
            CompanyCode,
            InfoPublDate,
            LeaderID,
            LeaderName,
            LeaderGender,
            BirthYM,
            Age,
            LeaderDegree,
            LeaderTitle,
            EarliestInDate,
            Background,
            Statement,
            XGRQ,
            JSID,
            BirthYMInfo,
            _MASK_TO_V2,
            _MASK_FROM_V2
        )
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功hk_leaderintroduce')

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
    select_sql = '''SELECT ID,
            CompanyCode,
            InfoPublDate,
            LeaderID,
            LeaderName,
            LeaderGender,
            BirthYM,
            Age,
            LeaderDegree,
            LeaderTitle,
            EarliestInDate,
            Background,
            Statement,
            XGRQ,
            JSID,
            BirthYMInfo,
            _MASK_TO_V2,
            _MASK_FROM_V2 from hk_leaderintroduce'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table hk_leaderintroduce'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功hk_leaderintroduce')
        insert_sql = '''insert into hk_leaderintroduce(
            ID,
            CompanyCode,
            InfoPublDate,
            LeaderID,
            LeaderName,
            LeaderGender,
            BirthYM,
            Age,
            LeaderDegree,
            LeaderTitle,
            EarliestInDate,
            Background,
            Statement,
            XGRQ,
            JSID,
            BirthYMInfo,
            _MASK_TO_V2,
            _MASK_FROM_V2
        )
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功hk_leaderintroduce')

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

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(_MASK_FROM_V2) from hk_leaderposition'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from hk_leaderposition where _MASK_FROM_V2 > '{}'  '''.format(_MASK_FROM_V2)
    cur2.execute(select_sql)
    infos = cur2.fetchall()
    print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
    insert_sql = '''insert into hk_leaderposition(ID,
                        CompanyCode,
                        InfoPublDate,
                        LeaderID,
                        LeaderName,
                        PositionName,
                        Position,
                        InDate,
                        OffDate,
                        ChangeType,
                        ChangeReason,
                        IfIn,
                        Statement,
                        XGRQ,
                        JSID,
                        PositionType,
                        _MASK_TO_V2,
                        _MASK_FROM_V2)
                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    for info in infos:
        try:
            cur.execute(insert_sql, info)
            mysql_conn.commit()
        except Exception as e:
            print(e)
    print('插入数据成功hk_leaderposition')
    mysql_conn2.close()
    mysql_conn.close()

def copySourceData4():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(_MASK_SYNC_V2) from hk_secumain'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from hk_secumain where _MASK_SYNC_V2 > '{}'  '''.format(_MASK_FROM_V2)
    cur2.execute(select_sql)
    infos = cur2.fetchall()
    print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
    insert_sql = '''insert into hk_secumain(
                    ID,
                    InnerCode,
                    CompanyCode,
                    SecuCode,
                    ChiName,
                    ChiNameAbbr,
                    EngName,
                    EngNameAbbr,
                    SecuAbbr,
                    ChiSpelling,
                    SecuMarket,
                    SecuCategory,
                    ListedDate,
                    ListedSector,
                    ListedState,
                    XGRQ,
                    JSID,
                    DelistingDate,
                    ISIN,
                    FormerName,
                    TradingUnit,
                    _MASK_TO_V2,
                    TraCurrUnit,
                    InsertTime,
                    _MASK_SYNC_V2
                    )
                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    for info in infos:
        try:
            cur.execute(insert_sql, info)
            mysql_conn.commit()
        except Exception as e:
            print(e)
    print('插入数据成功hk_secumain')
    mysql_conn2.close()
    mysql_conn.close()

def copySourceData5():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(_MASK_FROM_V2) from hk_sharestru'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from hk_sharestru where _MASK_FROM_V2 > '{}'  '''.format(_MASK_FROM_V2)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
        insert_sql = '''insert into hk_sharestru(
                        ID,
                        CompanyCode,
                        InfoPublDate,
                        InfoSource,
                        EndDate,
                        ParValueUnitComShare,
                        ParValueComShare,
                        ParValueUnitPreShare,
                        ParValuePreShare,
                        AuthorizedCapitalComShare,
                        AuthorizedSharesComShare,
                        PaidUpCapitalComShare,
                        PaidUpSharesComShare,
                        ListedShares,
                        UnlistedShares,
                        NotHKShares,
                        AuthorizedCapitalPreShare,
                        AuthorizedSharesPreShare,
                        PaidUpCapitalPreShare,
                        PaidUpSharesPreShare,
                        AuthorizedCapitalTotal,
                        AuthorizedSharesTotal,
                        PaidUpCapitalTotal,
                        PaidUpSharesTotal,
                        PaidUpSharesChgComShare,
                        PaidUpSharesChgPreShare,
                        ChangeType,
                        ChangeReason,
                        XGRQ,
                        JSID,
                        _MASK_TO_V2,
                        AuthCapitalComShareA,
                        AuthCapitalComShareB,
                        AuthSharesComShareA,
                        AuthSharesComShareB,
                        PaidUpCapitalComShareA,
                        PaidUpCapitalComShareB,
                        PaidUpSharesComShareA,
                        PaidUpSharesComShareB,
                        ListedSharesA,
                        ListedSharesB,
                        UnlistedSharesA,
                        UnlistedSharesB,
                        PaidUpShChgComShareA,
                        PaidUpShChgComShareB,
                        _MASK_FROM_V2
        )
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功hk_sharestru')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData5_1():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT * from hk_sharestru'''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table hk_sharestru'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功hk_sharestru')
        insert_sql = '''insert into hk_sharestru(
                        ID,
                        CompanyCode,
                        InfoPublDate,
                        InfoSource,
                        EndDate,
                        ParValueUnitComShare,
                        ParValueComShare,
                        ParValueUnitPreShare,
                        ParValuePreShare,
                        AuthorizedCapitalComShare,
                        AuthorizedSharesComShare,
                        PaidUpCapitalComShare,
                        PaidUpSharesComShare,
                        ListedShares,
                        UnlistedShares,
                        NotHKShares,
                        AuthorizedCapitalPreShare,
                        AuthorizedSharesPreShare,
                        PaidUpCapitalPreShare,
                        PaidUpSharesPreShare,
                        AuthorizedCapitalTotal,
                        AuthorizedSharesTotal,
                        PaidUpCapitalTotal,
                        PaidUpSharesTotal,
                        PaidUpSharesChgComShare,
                        PaidUpSharesChgPreShare,
                        ChangeType,
                        ChangeReason,
                        XGRQ,
                        JSID,
                        _MASK_TO_V2,
                        AuthCapitalComShareA,
                        AuthCapitalComShareB,
                        AuthSharesComShareA,
                        AuthSharesComShareB,
                        PaidUpCapitalComShareA,
                        PaidUpCapitalComShareB,
                        PaidUpSharesComShareA,
                        PaidUpSharesComShareB,
                        ListedSharesA,
                        ListedSharesB,
                        UnlistedSharesA,
                        UnlistedSharesB,
                        PaidUpShChgComShareA,
                        PaidUpShChgComShareB,
                        _MASK_FROM_V2
        )
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功hk_sharestru')

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

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(UpdateTime) from hk_shdirectholding'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from hk_shdirectholding where UpdateTime > '{}'  '''.format(_MASK_FROM_V2)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
        insert_sql = '''insert into hk_shdirectholding(
                    ID,
                    InnerCode,
                    InfoPublDate,
                    EndDate,
                    InfoSource,
                    SHID,
                    SHName,
                    SHNameAbbr,
                    UltControllerCode,
                    UltimateController,
                    EquityVolume,
                    Holdratio,
                    PosCharacter,
                    StockType,
                    Remark,
                    InsertTime,
                    UpdateTime,
                    JSID,
                    HoldratioOfTotEqu,
                    _MASK_TO_V2
                )
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功hk_shdirectholding')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def copySourceData6_1():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()
    select_sql = '''SELECT * from hk_shdirectholding '''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table hk_shdirectholding'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功hk_shdirectholding')
        insert_sql = '''insert into hk_shdirectholding(
                    ID,
                    InnerCode,
                    InfoPublDate,
                    EndDate,
                    InfoSource,
                    SHID,
                    SHName,
                    SHNameAbbr,
                    UltControllerCode,
                    UltimateController,
                    EquityVolume,
                    Holdratio,
                    PosCharacter,
                    StockType,
                    Remark,
                    InsertTime,
                    UpdateTime,
                    JSID,
                    HoldratioOfTotEqu,
                    _MASK_TO_V2
                )
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('插入数据成功hk_shdirectholding')

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

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(_MASK_FROM_V2) from hk_shequity'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from hk_shequity where _MASK_FROM_V2 > '{}'  '''.format(_MASK_FROM_V2)
    cur2.execute(select_sql)
    infos = cur2.fetchall()
    print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
    insert_sql = '''insert into hk_shequity(
                    ID,
                    CompanyCode,
                    InfoPublDate,
                    EndDate,
                    InfoSource,
                    SHName,
                    SHKind,
                    SHNo,
                    EquityType,
                    EquityCharacter,
                    Unit,
                    SHStatus,
                    SHStatusDesc,
                    RelatingSHStatus,
                    RelatingSHStatusDesc,
                    EquityVolume,
                    RatioInTotalShares,
                    HoldSum,
                    HSInTotalShares,
                    PersonalEquity,
                    FamilyEquity,
                    CompanyEquity,
                    OtherEquity,
                    RelatingEquityHoldSum,
                    REHSInTotalShares,
                    StockrightEquity,
                    WarrantEquity,
                    ConvertibleBondEquity,
                    OtherRelatingEquity,
                    Statement,
                    XGRQ,
                    JSID,
                    InsertTime,
                    _MASK_TO_V2,
                    _MASK_FROM_V2)
                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    for info in infos:
        try:
            cur.execute(insert_sql, info)
            mysql_conn.commit()
        except Exception as e:
            print(e)
    print('数据插入成功hk_shequity')
    mysql_conn2.close()
    mysql_conn.close()

def copySourceData8():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(_MASK_FROM_V2) from hk_stodiscdetailinf'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from hk_stodiscdetailinf where _MASK_FROM_V2 > '{}'  '''.format(_MASK_FROM_V2)
    cur2.execute(select_sql)
    infos = cur2.fetchall()
    print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
    insert_sql = '''insert into hk_stodiscdetailinf(ID,
                                RID,
                                PosCharacter,
                                EventCode,
                                BefEventStas,
                                AfEventStas,
                                InvolvedShares,
                                Currency,
                                HPriPSOnExchg,
                                AvePPSOnExchg,
                                AConPSOffExchg,
                                CCodeOffExchg,
                                UpdateTime,
                                JSID,
                                _MASK_TO_V2,
                                _MASK_FROM_V2)
                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    for info in infos:
        try:
            cur.execute(insert_sql, info)
            mysql_conn.commit()
        except Exception as e:
            print(e)
    print('数据插入成功hk_stodiscdetailinf')
    mysql_conn2.close()
    mysql_conn.close()

def copySourceData9():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(_MASK_FROM_V2) from hk_stodiscinf'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from hk_stodiscinf where _MASK_FROM_V2 > '{}'  '''.format(_MASK_FROM_V2)
    cur2.execute(select_sql)
    infos = cur2.fetchall()
    print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
    insert_sql = '''insert into hk_stodiscinf(ID,
                            Companyode,
                            CompanyName,
                            InnerCode,
                            SecuCode,
                            ShareCategory,
                            IssuedShares,
                            ChiHolderName,
                            HolderCharacter,
                            EventDate,
                            HolderNotcDate,
                            SN,
                            UpdateTime,
                            JSID,
                            _MASK_TO_V2,
                            _MASK_FROM_V2)
                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    for info in infos:
        try:
            cur.execute(insert_sql, info)
            mysql_conn.commit()
        except Exception as e:
            print(e)
    print('数据插入成功hk_stodiscinf')
    mysql_conn2.close()
    mysql_conn.close()

def copySourceData10():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()   # 本地数据
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor() # 数据源
    select_TradingDay_sql = '''SELECT max(TradingDay) from qt_hkdailyquoteindex '''
    cur.execute(select_TradingDay_sql)
    TradingDay = cur.fetchone()[0]
    select_sql = '''SELECT * from qt_hkdailyquoteindex where TradingDay >'{}'  '''.format(TradingDay)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        insert_sql = '''insert into qt_hkdailyquoteindex(ID,
                            InnerCode,
                            TradingDay,
                            ClosePrice,
                            MinPriceChg,
                            TurnoverVolume,
                            TurnoverValue,
                            HKStkShares,
                            HKStkMV,
                            NotHKStkShares,
                            Ashares,
                            Bshares,
                            TurnoverRate,
                            PERatio,
                            FPE,
                            PETTM,
                            PS,
                            PB,
                            PCF,
                            UpdateTime,
                            JSID,
                            DividendRatioFY,
                            DividendRatioRW,
                            _MASK_TO_V2,
                            LimitedShares,
                            NegotiableMV,
                            InsertTime,
                            _MASK_SYNC_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功qt_hkdailyquoteindex')

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
    cmb_time_sql = '''SELECT max(UpdateTime) from hk_shincredhold'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from hk_shincredhold where UpdateTime > '{}'  '''.format(_MASK_FROM_V2)
    cur2.execute(select_sql)
    infos = cur2.fetchall()
    print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
    insert_sql = '''insert into hk_shincredhold(ID,
                                    InnerCode,
                                    SecuCode,
                                    SHNo,
                                    ChiHolderName,
                                    EventDate,
                                    IssuedShares,
                                    HoldSumBefEvent,
                                    HoldSumAfEvent,
                                    ChangeDirect,
                                    VariableQuantity,
                                    AvePPSOnExchg,
                                    InsertTime,
                                    UpdateTime,
                                    JSID,
                                    _MASK_TO_V2)
                    value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    for info in infos:
        try:
            cur.execute(insert_sql, info)
            mysql_conn.commit()
        except Exception as e:
            print(e)
    print('数据插入成功hk_shincredhold')

    mysql_conn2.close()
    mysql_conn.close()

def copySourceData12():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(_MASK_FROM_V2) from hk_companyarchives'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from hk_companyarchives where _MASK_FROM_V2 > '{}'  '''.format(_MASK_FROM_V2)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
        insert_sql = '''insert into hk_companyarchives(ID,
                                        CompanyCode,
                                        ChiName,
                                        ChiNameAbbr,
                                        ChiSpelling,
                                        EngName,
                                        EngNameAbbr,
                                        EstablishmentDate,
                                        RegCapital,
                                        AuthorizedCapital,
                                        ParValue,
                                        CurrencyUnit,
                                        RegAddr,
                                        RegCountry,
                                        RegArea,
                                        RegZip,
                                        HeadOfficeAddress,
                                        HKOffice,
                                        Chairman,
                                        Tel,
                                        Fax,
                                        Email,
                                        Website,
                                        BriefIntroText,
                                        MainBusiness,
                                        UpdateTime,
                                        InsertTime,
                                        JSID,
                                        _MASK_TO_V2,
                                        _MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功hk_companyarchives')

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
    select_sql = '''SELECT * from hk_companyarchives '''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table hk_companyarchives'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功hk_companyarchives')
        insert_sql = '''insert into hk_companyarchives(ID,
                                        CompanyCode,
                                        ChiName,
                                        ChiNameAbbr,
                                        ChiSpelling,
                                        EngName,
                                        EngNameAbbr,
                                        EstablishmentDate,
                                        RegCapital,
                                        AuthorizedCapital,
                                        ParValue,
                                        CurrencyUnit,
                                        RegAddr,
                                        RegCountry,
                                        RegArea,
                                        RegZip,
                                        HeadOfficeAddress,
                                        HKOffice,
                                        Chairman,
                                        Tel,
                                        Fax,
                                        Email,
                                        Website,
                                        BriefIntroText,
                                        MainBusiness,
                                        UpdateTime,
                                        InsertTime,
                                        JSID,
                                        _MASK_TO_V2,
                                        _MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功hk_companyarchives')

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

def copySourceData14():
    mysql_conn = conn_mysql()
    cur = mysql_conn.cursor()
    mysql_conn2 = conn_mysql2()
    cur2 = mysql_conn2.cursor()

    # 取本地表最大同步字段时间
    cmb_time_sql = '''SELECT max(_MASK_FROM_V2) from hk_stockarchives'''
    cur.execute(cmb_time_sql)
    _MASK_FROM_V2 = cur.fetchone()[0]

    select_sql = '''SELECT * from hk_stockarchives where _MASK_FROM_V2 > '{}'  '''.format(_MASK_FROM_V2)
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        print('%s一共%s条数据'%(_MASK_FROM_V2,len(infos)))
        insert_sql = '''insert into hk_stockarchives(ID,
                                CompanyCode,
                                EstablishmentDate,
                                RegAbbr,
                                Business,
                                InduCHKE,
                                InduCHS,
                                Chairman,
                                CompanySecretary,
                                CertifiedAccountant,
                                RegisteredOffice,
                                GeneralOffice,
                                Registrars,
                                Tel,
                                Fax,
                                Eail,
                                Website,
                                BriefIntroduction,
                                XGRQ,
                                JSID,
                                CompanyType,
                                CompanyTypeDesc,
                                ChiName,
                                AuditInstitution,
                                RegCapital,
                                RegCapitalCurrency,
                                _MASK_TO_V2,
                                _MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功hk_stockarchives')

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
    select_sql = '''SELECT * from hk_stockarchives '''
    try:
        cur2.execute(select_sql)
        infos = cur2.fetchall()
        truncate_table_sql = '''truncate table hk_stockarchives'''
        cur.execute(truncate_table_sql)
        mysql_conn.commit()
        print('清空数据成功hk_stockarchives')
        insert_sql = '''insert into hk_stockarchives(ID,
                                CompanyCode,
                                EstablishmentDate,
                                RegAbbr,
                                Business,
                                InduCHKE,
                                InduCHS,
                                Chairman,
                                CompanySecretary,
                                CertifiedAccountant,
                                RegisteredOffice,
                                GeneralOffice,
                                Registrars,
                                Tel,
                                Fax,
                                Eail,
                                Website,
                                BriefIntroduction,
                                XGRQ,
                                JSID,
                                CompanyType,
                                CompanyTypeDesc,
                                ChiName,
                                AuditInstitution,
                                RegCapital,
                                RegCapitalCurrency,
                                _MASK_TO_V2,
                                _MASK_FROM_V2)
                        value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cur.executemany(insert_sql, infos)
        mysql_conn.commit()
        print('数据插入成功hk_stockarchives')

    except Exception as e:
        print(e)
    finally:
        mysql_conn2.close()
        mysql_conn.close()

def get_time():
    now = datetime.datetime.now()
    print(now)

def main_Gg():
    copySourceData1_1()
    # 全量更新暂不使用 copySourceData1_1()
    copySourceData2_1()
    copySourceData3()
    copySourceData4()
    copySourceData5_1()
    copySourceData6_1()
    copySourceData7()
    copySourceData8()
    copySourceData9()
    copySourceData10()
    copySourceData11()
    copySourceData12_1()
    copySourceData13()
    copySourceData14_1()

if __name__ == "__main__":
    get_time()
    main_Gg()
    get_time()
    # columns_message()
 