import pymysql
import time
import random
db = pymysql.Connect(
    host='192.168.1.129',
    port=3306,
    user='batchdata_3dep',
    passwd='shiye1805A',
    db='sy_project_raw',
    charset='utf8'
)
cursor = db.cursor()

sql = "INSERT INTO dwd_me_news_search_elasticsearch(srcType,secondLevelCode, thirdLevelCode, fourthLevelCode, risk,importance,pubTime,Industry_breakdown,title,name_uid) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,)"

def times():
    a1=(1976,1,1,0,0,0,0,0,0)              #设置开始日期时间元组（1976-01-01 00：00：00）
    a2=(2020,12,31,23,59,59,0,0,0)    #设置结束日期时间元组（1990-12-31 23：59：59）

    start=time.mktime(a1)    #生成开始时间戳
    end=time.mktime(a2)      #生成结束时间戳
    t=random.randint(start,end)    #在开始和结束时间戳中随机取出一个
    date_touple=time.localtime(t)          #将时间戳生成时间元组
    date=time.strftime("%Y-%m-%d %H:%M:%S",date_touple)  #将时间元组转成格式化字符串（1976-05-21）
    return date
def strings01():
    return  ''.join(random.sample(['z','y','x','w','v','u','t','s','r','q','p','o','n','m','l','k','j','i','h','g','f','e','d','c','b','a'], 3))
def strings02():
    return  ''.join(random.sample(['z','y','x','w','v','u','t','s','r','q','p','o','n','m','l','k','j','i','h','g','f','e','d','c','b','a'], 6))
def strings03():
    return  ''.join(random.sample(['z','y','x','w','v','u','t','s','r','q','p','o','n','m','l','k','j','i','h','g','f','e','d','c','b','a'], 10))

for i in range(1,4):
    for j in range(1,9):
        for o in range(1,5):
            for p in range(1,4):
                pubTime = str(times()).split(" ")[0]
                name_uid = strings01()
                title = strings03()
                Industry_breakdown = strings02()
                print(pubTime,name_uid,title,Industry_breakdown)
                cursor.execute(sql,(str(i),"1",str(j),str(o),str(p),str(p),str(pubTime),Industry_breakdown,title,name_uid))
            db.commit()
db.close()