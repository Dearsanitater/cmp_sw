import os
import sys
import io
import time
from queue import Queue
from django.apps import AppConfig
from threading import Thread
from multiprocessing import Manager


class QueueMaintainer:
    def __init__(self):
        self.que_dic = {}

    def add_queue(self, pid, uuid):
        """为指定进程添加队列"""
        self.que_dic[pid] = {'uuid': uuid, 'queue': Queue()}

    def write(self, message, pid):
        """向指定进程的队列中写入消息"""
        if message.strip() and pid in self.que_dic:
            self.que_dic[pid]['queue'].put((pid, message))

    def get_queue(self, pid):
        """获取指定进程的队列"""
        return self.que_dic.get(pid, {}).get('queue')

    def flush_queue(self, pid, uuid):
        """清空指定进程的队列并重置"""
        self.que_dic[pid] = {'uuid': uuid, 'queue': Queue()}


# 定义消费者类，在主进程中消费队列中的数据
class QueueConsumer:
    def __init__(self, queue_maintainer):
        self.queue_maintainer = queue_maintainer
        self.running = True

    def start(self):
        """启动消费者线程"""
        self.thread = Thread(target=self.consume, daemon=True)
        self.thread.start()

    def stop(self):
        """停止消费者线程"""
        self.running = False
        if self.thread.is_alive():
            self.thread.join()

    def consume(self):
        """消费所有队列中的消息"""
        while self.running:
            for pid, data in self.queue_maintainer.que_dic.items():
                queue_obj = data['queue']
                try:
                    while not queue_obj.empty():
                        pid, message = queue_obj.get_nowait()
                        self.process_message(pid, message)
                except Queue.Empty:
                    continue

    @staticmethod
    def process_message(pid, message):
        """处理消息（这里打印到控制台）"""
        print(f"[Process {pid}]: {message}")


# 在 Django 配置类中初始化 QueueMaintainer 和 QueueConsumer
class ApiConfig(AppConfig):
    name = 'api'

    def ready(self):
        self.queue_maintainer = QueueMaintainer()
        self.queue_consumer = QueueConsumer(self.queue_maintainer)
        self.queue_consumer.start()

    def add_queue(self, pid, uuid):
        """在进程启动时为每个进程添加队列"""
        self.queue_maintainer.add_queue(pid, uuid)

    def write_to_queue(self, pid, message):
        """写入消息到指定进程的队列"""
        self.queue_maintainer.write(message, pid)

    def stop_consumer(self):
        """停止消费者线程"""
        self.queue_consumer.stop()
