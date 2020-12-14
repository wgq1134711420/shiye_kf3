# -*- coding:utf-8 -*-


def get_consumers_count():
    """
    检测rabbitMQ消费者数据
    """
    import sys
    import requests
    import json

    argv = sys.argv
    res = requests.get(
        url='http://{ip}:{port}/api/consumers/%2F{vhost}'.format(ip=argv[1], port=argv[2], vhost=argv[3]),
        auth=('{}'.format(argv[4]), '{}'.format(argv[5])))
    data = json.loads(res.content.decode())
    return len(data)


if __name__ == '__main__':
    get_consumers_count()
