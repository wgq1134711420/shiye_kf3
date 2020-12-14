#!/usr/bin/env python2
# -*- coding:utf-8 -*-


"""
zabbix
监控脚本-企业微信机器人
"""
import sys
import getopt
import requests
import traceback

try:
    opts, args = getopt.getopt(sys.argv[1:], shortopts='', longopts=['webhook_url=', 'alert_message='])

    for opt, value in opts:
        if opt == '--webhook_url':
            webhook_url = value
        elif opt == '--alert_message':
            alert_message = value
    webhook_header = {
        "Content-Type": "application/json",
    }
    webhook_message = {
        "msgtype": "text",
        "text": {
            "content": alert_message
        }
    }
    requests.post(url=webhook_url, headers=webhook_header, json=webhook_message)
except:
    traceback.print_exc(file=open('/tmp/wx.log', 'w+'))
