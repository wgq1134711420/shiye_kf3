import requests,json

title = '蹭热点寻风口 上市公司需三思而行'

content = '每经评论员王朋不出意料,*ST盐湖(000792,SZ)以逾400亿元的预亏金额,成为A股新晋亏损王。三个多月前,一纸由债权人提请的重整申请,揭开了*ST盐湖经营状况的最后盖子。自此之后,其公开处置亏损子公司不良资产6次流拍、被青海省政府牵头成立的国资平台受让、因资产处置业绩巨亏面临暂停上市风险……不过,引起雪崩的伏笔,其实早在多年前就已埋下。当年的盐湖股份,何其风光。依托国内面积最大的盐湖察尔汗盐湖,盐湖股份与A股另一家上市公司藏格控股(000408,股吧)(000408,SZ),几乎囊括了国产氯化钾的天下。盐湖股份更是被称为“钾肥之王”,作为格尔木规模最大的企业——每天通勤班车从市区出发,载着员工浩浩荡荡前往几十公里之外的厂区,已成了该市的一道风景。风口与故事,向来是资本角逐的目标。作为上市公司的盐湖股份,站在钾肥的山巅之上,看到了一座又一座貌似即将崛起的高峰。不过,如此体量的公司一旦心动,往往意味着动辄数十亿数百亿元的支出。2008年,一项预投资额在200亿元左右的金属镁一体化项目启动。后期随着项目的严重超期,最终累计投资近400亿元。可当初市场前景良好、附加值高的镁产品在千呼万唤始出来时,价格却早已被同行打到了低位。此外,盐湖股份还投入巨资打造盐湖综合利用项目,主要生产化工产品。规模巨大的四个项目总投资超400亿元,可工期却一再延迟,投产即巨亏。在新能源催生的锂资源领域,盐湖股份同样没有缺席。“3+2”共5万吨电池级碳酸锂项目总投资近80亿元,项目至今仍是犹抱琵琶半遮面……如此热衷故事与风口,但却缺乏足够的“眼光”,再大的家底也经不起折腾。像*ST盐湖这般追风逐利又失败的剧情,在A股市场曾多次上演。心存侥幸者有之、投机取巧者有之、随波逐流者有之,唯独责任心与敬畏心被一些公司弃之如敝屣。例如,数年前手游概念的大热风潮,曾引得无数公司竞折腰,但最终有多少公司是在踏实做事,又有多少股民为看似美妙的故事买了单?与*ST盐湖境遇类似的坚瑞沃能(300116,股吧)(300116,SZ),一度顶着国内“消防第一股”的光环进入A股市场,却在迈上行业巅峰之际以数十亿元的代价转型新能源领域。结果“风口”没追上,连续亏损让这家公司如今站在了破产退市的边缘,一度以出售资产和房产救急。“风口短命,大钱谨慎”,一些人看得到的,是热钱的进入;看不到的,则是市场之手对疯狂与盲目的压制。投资者对上市公司的认可,是基于后者的专业与实力;如若上市公司失去对市场的敬畏,为了所谓“风口”或其他不可明说的目的而肆意挥霍手头资源,到头来只能害人害己。A股有风口,寻找需三思。风向、风速测不准,贸然往里钻,是很容易翻车的。'

# text = {'title':title,'content':content}

# url = 'http://172.17.23.150:11068/emotion/'

# result = requests.post(url,data=json.dumps(text))

# 舆情接口信息
return_result = requests.post('http://172.17.23.150:11068/emotion/',data=json.dumps({'title':title,'content':content}))
print(return_result,type(return_result))
emoScore = ''
emoLabel = ''
keyword = ''
summary = ''
emoConf = ''
if return_result:
    result = return_result.json()
    emoScore = result['score']
    emoLabel = result['sentiment']
    keyword = result['keyWords']
    summary = result['abstract']
    emoConf = result['confidence']
print(emoScore,emoLabel,keyword,summary,emoConf)

# result = requests.post('http://172.17.23.150:11068/emotion/',data=json.dumps({'title':title,'content':content})).json()
# # print(type(result))

# score = result['score']
# sentiment = result['sentiment']
# keyWords = result['keyWords']
# abstract = result['abstract']
# confidence = result['confidence']

# print(score,sentiment,keyWords,abstract,confidence)

# # print(result.json())