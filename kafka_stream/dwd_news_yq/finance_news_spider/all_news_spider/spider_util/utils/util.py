# Copyright (c) 2015 Shiye Inc.
# All rights reserved.
#
# Author: 赵朋飞
# Date:   19-5-5


import datetime
import os
import re
import sys
import time
import copy
import uuid


is_py3 = sys.version_info.major == 3
if is_py3:
    string_types = (str, bytes)
else:
    string_types = (basestring, )


def text_to_str(text, encoding='utf8'):
    '''
    将传入文本转换为str类型 兼容py2 py3
    '''
    if is_py3:
        if isinstance(text, bytes):
            text = text.decode(encoding)
    else:
        if isinstance(text, unicode):
            text = text.encode(encoding)
    return text

def TimeDeltaYears(years, from_date=None):
    if from_date is None:
        from_date = datetime.datetime.now()
    try:
        return from_date.replace(year=from_date.year + years)
    except:
        # Must be 2/29!
        assert from_date.month == 2 and from_date.day == 29  # can be removed
        return from_date.replace(month=2, day=28,
                                 year=from_date.year + years)


def local_datetime(data):
    '''
    把data转换为日期时间，时区为东八区北京时间，能够识别：今天、昨天、5分钟前等等，如果不能成功识别，则返回datetime.datetime.now()
    '''
    dt = datetime.datetime.now()
    # html实体字符转义
    # data = HTMLParser.HTMLParser().unescape(data)
    data = data.strip()
    if not data:
        return dt
    try:
        data = text_to_str(data)
    except Exception as e:
        # log.logger.error("utc_datetime() error: data is not utf8 or unicode : %s" % data)
        pass

    # 归一化
    data = data.replace("年", "-").replace("月", "-").replace("日", " ").replace("/", "-").replace('.','-').strip()
    data = re.sub("\s+", " ", data)

    year = dt.year

    regex_format_list = [
        # 2013年8月15日 22:46:21
        ("(\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M:%S", ""),

        # "2013年8月15日 22:46"
        ("(\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M", ""),

        # "2014年5月11日"
        ("(\d{4}-\d{1,2}-\d{1,2})", "%Y-%m-%d", ""),

        # "2014年5月"
        ("(\d{4}-\d{1,2})", "%Y-%m", ""),

        # "13年8月15日 22:46:21",
        ("(\d{2}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})", "%y-%m-%d %H:%M:%S", ""),

        # "13年8月15日 22:46",
        ("(\d{2}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2})", "%y-%m-%d %H:%M", ""),

        # "8月15日 22:46:21",
        ("(\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M:%S", "+year"),

        # "8月15日 22:46",
        ("(\d{1,2}-\d{1,2} \d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M", "+year"),

        # "8月15日",
        ("(\d{1,2}-\d{1,2})", "%Y-%m-%d", "+year"),

        # "3 秒前",
        ("(\d+)\s*秒前", "", "-seconds"),

        # "3 分钟前",
        ("(\d+)\s*分钟前", "", "-minutes"),

        # "3 秒前",
        ("(\d+)\s*小时前", "", "-hours"),

        # "3 秒前",
        ("(\d+)\s*天前", "", "-days"),

        # 今天 15:42:21
        ("今天\s*(\d{1,2}:\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M:%S", "date-0"),

        # 昨天 15:42:21
        ("昨天\s*(\d{1,2}:\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M:%S", "date-1"),

        # 前天 15:42:21
        ("前天\s*(\d{1,2}:\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M:%S", "date-2"),

        # 今天 15:42
        ("今天\s*(\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M", "date-0"),

        # 昨天 15:42
        ("昨天\s*(\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M", "date-1"),

        # 前天 15:42
        ("前天\s*(\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M", "date-2"),
        # 前天
    ]

    for regex, dt_format, flag in regex_format_list:
        m = re.search(regex, data)
        if m:

            if not flag:
                dt = datetime.datetime.strptime(m.group(1), dt_format)
            elif flag == "+year":
                # 需要增加年份
                dt = datetime.datetime.strptime("%s-%s" % (year, m.group(1)), dt_format)
            elif flag in ("-seconds", "-minutes", "-hours", "-days"):
                flag = flag.strip("-")
                if flag == 'seconds':
                    dt = dt - datetime.timedelta(seconds = int(m.group(1)))
                elif flag=='minutes':
                    dt = dt - datetime.timedelta(minutes=int(m.group(1)))
                elif flag =="hours":
                    dt = dt - datetime.timedelta(hours=int(m.group(1)))
                elif flag=='days':
                    dt = dt - datetime.timedelta(days=int(m.group(1)))
                # exec("dt = dt - datetime.timedelta(%s = int(m.group(1)))" % flag)
            elif flag.startswith("date"):
                del_days = int(flag.split('-')[1])
                _date = dt.date() - datetime.timedelta(days=del_days)
                _date = _date.strftime("%Y-%m-%d")
                dt = datetime.datetime.strptime("%s %s" % (_date, m.group(1)), dt_format)
            return dt
    else:
        # log.logger.error("unknow datetime format: %s" % data)
        pass
    return dt


def utc_datetime(data):
    """
    将时间转化为国际时区 datetime类型
    :param data:
    :return:
    """
    try:
        utc_dt = local_datetime(data) - datetime.timedelta(hours=8)
    except Exception as e:
        utc_dt = datetime.datetime.utcnow()
        # log.logger.exception(e)
        pass
    return utc_dt


def timestamp_to_date(timestamp, str_format="%Y-%m-%d"):
    """
    将时间戳转化为格式化字符串
    :param timestamp: 时间戳
    :param str_format: 转化时间的格式， 默认 2019-5-5
    :return:
    """
    if len(str(timestamp)) == 13:
        timestamp = int(timestamp) / 1000
    timestamp = float(timestamp)
    time_array = time.localtime(timestamp)
    date_str = time.strftime(str_format, time_array)
    return datetime.datetime.strptime(date_str, str_format)


def local_timestamp(data):
    dt = local_datetime(data)

    tmp = int(time.mktime(dt.timetuple()))
    return tmp


def utc_timestamp(data):
    dt = utc_datetime(data)
    tmp = int(time.mktime(dt.timetuple()))
    return tmp


def add_uuid(data):

    """
    对字符串进行加密
    :return:
    """
    data = uuid.uuid3(uuid.NAMESPACE_DNS, data)
    data = str(data)
    result_data = data.replace('-', '')
    return result_data


def get_type(data):
    punish_type = []
    punish_dict = {
        "罚款": ["罚款", "元", "人民币"],
        "警告": ["警告"],
        "没收违法所得": ["没收", "罚没",],
        "责令改正": ["责令整改", "责令改正", "责令限期改正"],
        "市场禁入": ["市场禁入", "禁止从事", "禁止终身从事", "停止销售"],
        "监管关注": ["监管关注", ],
        "监管谈话": ["监管谈话", ],
        # "警示": ["警示", ],
        "提交书面承诺": ["提交书面承诺", ],
        "通报批评": ["通报批评", ],
        "公开致歉": ["公开致歉", ],
        "公开谴责": ["公开谴责", ],
        "拘留": ["拘留", ],
        "稽查": ["稽查", ],
        "立案调查": ["立案调查", ],
        "失信被执行人": ["失信被执行人", ],
        "被执行人": ["被执行人", ],
        "暂停/取消资格": ["暂停/取消资格", ],
        "经营异常": ["经营异常", ],
        "失联机构": ["失联机构", ],
        "异常机构": ["异常机构", ],
        "不良行为": ["不良行为", ],
        "移送司法机关": ["移送司法机关", ],
        "债券违约": ["债券违约", ],
        "责令停产停业": ["责令停产停业", ],
        "吊销许可证/执照": ["吊销许可证/执照", ],
        "取消任职资格": ["取消任职资格", ],
        "限制业务范围": ["限制业务范围", ],
        "停止接受新业务": ["停止接受新业务", ],
        "自查违规": ["自查违规", ],
        "黑名单": ["黑名单", ],
        "责令停止发行证券": ["责令停止发行证券", ],
        "撤销从业/任职资格": ["撤销从业/任职资格", ],
        "禁止进入相关行业": ["禁止进入相关行业", ],
    }
    for kk, vv in punish_dict.items():
        for v in vv:
            if v in data:
                punish_type.append(kk)
    punish_type = list(set(punish_type))
    return punish_type

if __name__ == '__main__':
    a = local_timestamp('10小时前')
    # b = local_timestamp('今天')
    # c = timestamp_to_date(a, '%Y-%m-%d %H:%M:%S')
    # d = timestamp_to_date(b, '%Y-%m-%d %H:%M:%S')
    print(a)
    # print(d)
    # uid = add_uuid('123423447678')
    # print(uid)
    # import time
    #
    # createTime = '20180804'  # 格式化时间
    # createTime = time.strptime(createTime, "%Y%m%d")  # 转化为时间数组
    # createTime = int(time.mktime(createTime))
    # print(createTime)
