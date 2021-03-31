# -*- coding: UTF-8 -*-
import os
import sys
import time
from loguru import logger


class LogHandler:

    def __init__(self, work_name):
        self.work_name = work_name

    @staticmethod
    def make_dir(file_path):
        if not os.path.exists(file_path):
            mydir = os.path.dirname(file_path)
            if not os.path.exists(mydir):
                os.makedirs(mydir)
            os.makedirs(file_path)

    def get_logger(self):
        # todo 日志 上线切换
        log_file_path = os.path.join(os.path.dirname(os.getcwd()), 'logs', self.work_name) if sys.platform == 'win32' else '/data/logs/pyflow/{}'.format(self.work_name)
        # log_file_path = '/WU_work/cyt/python_work/logs/pyflow/{}'.format(self.work_name)
        self.make_dir(log_file_path)
        log_files = os.listdir(log_file_path)
        for file_name in log_files:  # 删除一周以前的文件
            log_file = os.path.join(log_file_path, file_name)
            create_time = int(os.path.getctime(log_file))
            current_time = int(time.time())
            if current_time - create_time > 7 * 24 * 3600:
                os.remove(log_file)
        is_exist = os.path.exists(log_file_path)  # 判断是否存在文件
        if not is_exist:
            os.makedirs(log_file_path)
        log_file_name = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        log_path = '{}/{}.log'.format(log_file_path, log_file_name)  # 日志名称格式
        # format日志记录格式，green表示打印的字体颜色为绿色
        trace = logger.add(log_path, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                                "<level>{level: <8}</level> | "
                                "<cyan>{file}</cyan>:<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
                                "- <level>{message}</level>", enqueue=True)
        return trace, logger

    def info(self, message):
        trace, self_logger = self.get_logger()
        self_logger.info(message)
        self_logger.remove(trace)

    def warning(self, message):
        trace, self_logger = self.get_logger()
        self_logger.warning(message)
        self_logger.remove(trace)

    def error(self, message):
        trace, self_logger = self.get_logger()
        self_logger.error(message)
        self_logger.remove(trace)

    def critical(self, message):
        trace, self_logger = self.get_logger()
        self_logger.critical(message)
        self_logger.remove(trace)

    def exception(self, message):
        trace, self_logger = self.get_logger()
        self_logger.exception(message)
        self_logger.remove(trace)


def write_log(work_name):
    logObj = LogHandler(work_name=work_name)
    return logObj
