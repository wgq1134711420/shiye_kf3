# Copyright (c) 2015 Shiye Inc.
# All rights reserved.
#
# Author: 牛嘉豪
# Date:   19-5-16


import requests
from tika import parser
from fake_useragent import UserAgent
from wand.image import Image
from PIL import Image as PI
from aip import AipOcr

def get_response(url):
    '''
    根据url获取文件下载的响应
    :param url: 链接地址
    :return:
    '''
    HEADERS = {'User-Agent': UserAgent().random}
    response = requests.get(url, headers=HEADERS, timeout=25)
    return response


def dow_img_acc(path, url):
    '''
    下载附件和图片
    :param path: 文件下载到本地的详细地址加文件名
    :param url: 附件链接地址
    :return:
    '''

    response = get_response(url)
    with open(path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)


def parse_main(path):
    '''
    解析附件
    :param path:文件下载到本地的详细地址加文件名
    :return:
    '''
    try:
        parsed = parser.from_file(path)
        return parsed["content"].strip()
    except Exception as e:
        print('解析出现故障了{}'.format(e))
        return ''


def image_parse_main(path):
    '''
    解析附件，附件内容是图片
    :param path:文件下载到本地的详细地址加文件名
    :return:
    '''
    try:
        req_image = []
        final_text = ''


        APP_ID = '16240782'
        API_KEY = 'exMCgqRatdvGYGY98QjjGob2'
        SECRET_KEY = '7WcadokuLGQbedUTwnozOmrVi3PpWRQL'
        client = AipOcr(APP_ID, API_KEY, SECRET_KEY)


        image_pdf = Image(filename=path, resolution=300)
        image_jpeg = image_pdf.convert('jpeg')

        # with Image(filename="CDD00098254255.pdf",resolution=300) as img:
        #     img.format = 'png'
        #     img.save(filename='img.png')


        for num, img in enumerate(image_jpeg.sequence):
            image_page = Image(image=img)
            # image_page.save(filename="./img"+ str(num) +".jpeg")
            req_image.append(image_page.make_blob('jpeg'))

        for img in req_image:
            # basicGeneral表示通用识别
            msg = client.basicGeneral(img)
            # basicAccurate表示高精度识别
            # msg = client.basicAccurate(img)
            for i in msg.get('words_result'):
                final_text += (i.get('words')+'\n')
        return final_text
    except Exception as e:
        print('解析出现故障了{}'.format(e))
        return ''


if __name__ == '__main__':
    path = 'temp.png'
    # url='http://www.sse.com.cn/aboutus/mediacenter/hotandd/a/20190422/59eda8067918680379b3cf3aad90387d.jpg'
    # dow_img_acc(path,url)
    file_content = image_parse_main(path)
    print(file_content)
