#!/usr/bin/env python
# _*_ coding:utf-8 _*_
"""
    version: 1.0
    author:  frank xiong
    mail:    panda198727@hotmail.com
    description: ***
    
    :copyright: (c) ***
    :license: ***
"""

import sqlite3
import os

import logging


class DBHandler(object):
    """
    该类处理对数据库的各项操作
    """

    def __init__(self, db_path=u"E:/crawler_db/spider.db"):
        self.db_path = db_path.decode("utf-8")
        self.check_dir(self.check_dir())
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def check_dir(self):
        """
        检查文件夹是否存在，如果不存在就创建目录
        :param path:
        :return:
        """
        if not os.path.exists(self.db_path):
            path_dir = self.get_dir()
            logging.info(u"目录不存在，正在创建目录...")
            os.mkdir(path_dir)
            logging.info(u"目录创建完成...")

    def get_dir(self):
        """
        从文件地址中提取文件所在的目录
        :return:
        """
        dir_list = self.db_path.split("/")
        # 删除文件名
        dir_list.pop()
        db_dir = "".join(dir_list)
        logging.info(u"数据库文件的存放路径:" + db_dir)

        return db_dir

    def create_table(self, sql):
        """
        创建表，如果表存则忽略
        :param sql:
        :return:
        """
        sql_str = r"CREATE TABLE IF NOT EXISTS movies_info(" \
                  r"id INTEGER PRIMARY KEY AUTOINCREMENT, " \
                  r"movie_name VARCHAR(100), " \
                  r"movie_score FLOAT)"

        self.cursor.execute(sql_str)
        self.commit_data()

    def insert_data(self, *data):
        """
        插入数据
        :return:
        """
        sql_str = r"INSERT INTO movies_info(movie_name,movie_score) VALUES (?,?)"
        self.cursor.execute(sql_str, data)
        self.commit_data()

    def commit_data(self):
        """
        提交操作
        :return:
        """
        try:
            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            logging.info(u"数据库操作失败，操作已回滚...")
            logging.debug(e)

    def close_db(self):
        """
        关闭游标和连接
        :return:
        """
        self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    database = DBHandler()
    database.create_table()