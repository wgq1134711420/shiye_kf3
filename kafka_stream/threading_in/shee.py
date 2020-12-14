import threading
from queue import Queue
import time

url_queue = Queue()  # 构造一个不限大小的队列
threads = []  # 构造工作线程池


class Spider(threading.Thread):
    """多线程类"""

    def __init__(self, Thread_name, func):
        super().__init__()
        self.Thread_name = Thread_name

    def run(self):
        self.func(self.Thread_name)


def worker(Thread_name):

    while not url_queue.empty():  # 若队列不为空继续运行
        url = url_queue.get()  # 从队列中弹出任务
        print("{}:执行任务{}".format(Thread_name, url))

    print("{}:无任务 结束".format(Thread_name))


if __name__ == "__main__":

    url_ls = []  # 我们的待采集url列表
    url = ""
    for i in range(0, len(url, ls)):
        url_queue.put(url_list[i])

    threadNum = 5

    for i in range(1, threadNum + 1):
        thread = Spider("{}_{}".format("Thread", i), worker)  # 线程工作方法
        thread.start()  # 启动线程
        threads.append(thread)  # 线程列表中加入线程

    for thread in threads:
        thread.join()  # 每个线程加入阻塞