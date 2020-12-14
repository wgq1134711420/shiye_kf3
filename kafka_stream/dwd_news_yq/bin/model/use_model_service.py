"""
调用128服务器上的提取公司名称模型服务
"""
from bin.model.ner_bc import BertClient


class GetCompanyNameModel:

    def __init__(self):
        # 连接模型预测服务
        # self.bclient_1 = BertClient(show_server_config=False, check_version=False, check_length=False,
        #                             ip='192.168.1.128', port=5555, port_out=5556, max_seq_length=110, mode='NER')
        # self.bclient_2 = BertClient(show_server_config=False, check_version=False, check_length=False,
        #                             ip='192.168.1.128', port=7777, port_out=7778, max_seq_length=110, mode='NER')
        self.bclient_3 = BertClient(show_server_config=False, check_version=False, check_length=False,
                                    ip='192.168.1.128', port=6668, port_out=6669, mode='NER', timeout=6000)

    def test_service(self):
        """
        测试算法模型
        1. 校验端口是否存活
        2. 校验服务是否正确返回
        :return: 开启 True
                 关闭 False
        """
        import socket
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sk.settimeout(1)

        try:
            # sk.connect(('127.0.0.1', 5555)) and sk.connect(('127.0.0.1', 7777)) or sk.connect(('127.0.0.1', 6668))
            sk.connect(('127.0.0.1', 6668))
            if len(self.bclient_3.process_seq("中国通号")) == 1:
                return True
            else:
                return False
        except:
            return False

    def get_api(self, text, client_num):
        """
        调用模型接口
        :param text: 文本信息 str
        :param client_num: 连接模型服务客户端
        :return: 公司名称 list
        """
        try:
            if text:
                result_list = client_num.process_seq(text)
                if result_list is None:
                    return []

                # Jason 对模型结果进行过滤
                elif result_list != []:
                    company_list = []
                    for co in result_list:
                        pos1 = text.find(co) + len(co)
                        # print('text[pos1]',text[pos1])
                        if len(co) > 1 and '路' != co[-1:] and co != '全国股转公司' and '农村部' not in co and 'ST个股' not in co and 'ST板块' not in co and '记者' != text[pos1:pos1+2] and '讯' != text[pos1] and co not in company_list and text[pos1] != '间' and co != '上海期货':
                            # print('模型提取',co)
                            company_list.append(co)
                    return company_list

                else:
                    return result_list
            else:
                return []
        except:
            return []



