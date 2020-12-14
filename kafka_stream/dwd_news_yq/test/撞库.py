
# 创建目标公司名称列表，用于撞库匹配
all_company_list = []
cursor.execute('SELECT stakeholderName FROM sy_bond_stakeholder_name')
l2 = list(cursor.fetchall())
for i in l2:
    all_company_list.append(i[0])
# print(all_company_list)
print('all_company_list:',len(all_company_list))

# 匹配公司库名称
print('匹配公司库名称')
for co in all_company_list:
    if co in text: # 确认提取的公司在原文中
        print('匹配公司库名称',co)