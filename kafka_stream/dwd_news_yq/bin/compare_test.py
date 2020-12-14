import sys
sys.path.append("/database/gjs/similarity_test/bert_utils-master-improve")
from extract_feature import *


def sim_calculation(article_list):
    # 需要至少两篇文章才可以比较相似度
    if len(article_list) > 1:

        # print('article_list',article_list,len(article_list))

        # 初始化储存比较结果的列表
        result_list = []

        # 新建BERT词嵌入对象
        bert = BertVector()
        # 使用BERT将文章进行进行词嵌入
        vector = bert.encode(article_list)
        print('vector',len(vector),type(vector))

        for i in range(len(article_list)-1):
            # 计算矩阵之间的余弦距离
            similar_result = bert.mtx_similar(vector[0],vector[i+1])
            # print('similar_result',similar_result)
            result_list.append(similar_result)

        return result_list
    else:
        return[]