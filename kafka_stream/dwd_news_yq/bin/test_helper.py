import pymysql
from sshtunnel import SSHTunnelForwarder

def docker_mysql_cmb():
    try:
        server = SSHTunnelForwarder(
            ('47.94.209.102', 22),
            ssh_username='root',
            ssh_password='Ydds2-Sy-Aly-3Th',
            remote_bind_address=('192.168.1.129', 3306),
            local_bind_address=('0.0.0.0', 3306)
        )
        server.start()  # 开启隧道
        db = pymysql.connect(host='127.0.0.1',  # 此处必须是是127.0.0.1
                             port=3306,
                             user='root',
                             passwd='123456',
                             database='cmb')
        cursor = db.cursor()

        CName_list = []
        companyName_list = []
        actdutyName_list = []
        cursor.execute('SELECT * FROM tmp_sq_comp_manager_main_01')
        co_names = list(cursor.fetchall())
        cn = [desc[0] for desc in cursor.description]
        for i in range(len(co_names)):
            CName_list.append(list(co_names[i])[cn.index('CName')])
            companyName_list.append(list(co_names[i])[cn.index('companyName')])
            actdutyName_list.append(list(co_names[i])[cn.index('actdutyName')])
        return CName_list,companyName_list,actdutyName_list

        cursor.close()
        db.close()

    except Exception as e:
        print(e)
        return [],[],[]


CName_list,companyName_list,actdutyName_list = docker_mysql_cmb()

print('CName_list',len(CName_list))
print('companyName_list',len(companyName_list))
print('actdutyName_list',len(actdutyName_list))