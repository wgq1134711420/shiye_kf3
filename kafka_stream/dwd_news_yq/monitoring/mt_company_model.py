# -*- coding:utf-8 -*-


from bin.model.use_model_service import GetCompanyNameModel


def test_model():
    """
    检测聪聪模型服务端口是否开启
    """
    md = GetCompanyNameModel()
    ret = md.test_service()
    return ret
