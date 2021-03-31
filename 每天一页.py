#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：公司政策爬虫
@File    ：每天一页.py
@IDE     ：PyCharm
@Author  ：13192272582
@Date    ：2021/1/22 15:17
'''
import os

import pymysql
import threading
from w3lib.html import remove_comments
from DBUtils.PooledDB import PooledDB
import asyncio
import multiprocessing
from  Policy_acquisition.aiohttps_GP import  *
from  Policy_acquisition.Ray_num import  *
from  Policy_acquisition.ALL_sql import  *
from  Policy_acquisition.Columnpage_process import  *
sem = asyncio.Semaphore(100) # 信号量，控制协程数，防止爬的过快


all_urls=[]
POOL = PooledDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxconnections=20,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=10,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=10,  # 链接池中最多闲置的链接，0和None不限制
    maxshared=0,  # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
    blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
    maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
    setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
    ping=0,
    #ping MySQL服务端，检查是否服务可用。# 如：0 = None = never, 1 = default = whenever it is requested, 2 = when a cursor is created, 4 = when a query is executed, 7 = always
    host='192.168.5.105',
    port=3306,
    user='collector',
    password='zhengce365%123',
    database='test_zhengce365_collect',
    charset='utf8',
)


class WORK():
    def __init__(self):
        pass


    def cx_sql(self):
        conn, cursor = create_conn()
        item = ED_SQL( cursor,None)
        close_conn(conn, cursor)
        return  item

    def update_tttsql(self):
        try:
            conn, cursor = create_conn()
            sql = "update tbl_collect_info set eveday_status=0 where id>1 "
            cursor.execute(sql)
            conn.commit()
            close_conn(conn, cursor)
        except:
            pass

def one(start,rizhi_q):
    print("当前进程id={}".format(os.getpid()))
    item_list = start.cx_sql()
    all_urls = []
    print(len(item_list))
    if not item_list:
        start.update_tttsql()
    else:
        loop = asyncio.get_event_loop()
        tasks = []  # 获取事件循环
        for resultss in item_list:
            argument = argument_get(resultss)
            argument['info_page'] = 1
            all_urls = Handle_url(all_urls, **argument)
        for i in all_urls:
            task = next_one(i,rizhi_q,)
            tasks.append(task)
        loop.run_until_complete(asyncio.wait(tasks))  # 激活协程
        loop.close()  # 关闭事件循环
    rizhi_q.put(None)
    print("完成")
async  def next_one(j,rizhi_q):
    async with(sem):
        html=""
        kwargs = j[5]
        rizhi_q.put((kwargs['cid'], 3))
        num = 0
        print(j)
        if int(j[3]) == 1:
            ##post采集
            url = j[0]
            while num < 3:
                try:
                    if num > 0:
                        proxies = await  Aiohttp_ip()
                    else:
                        proxies = None
                    html, pd = await Aiohttp_post(url, kwargs['cookie'], proxies, j[2])
                    if int(pd) == 1:
                        html = html
                        break
                    elif int(pd) == 2:
                        url = html
                        num += 1
                    elif int(pd) == 3:
                        return "附件"
                except:
                    num += 1
        else:
            if int(j[5]['immit_js']) == 1:
                html = Ray_html(j[2])
            else:
                url = j[2]
                while num < 3:
                    try:
                        if num>0:
                            proxies = await  Aiohttp_ip()
                        else:
                            proxies=None
                        html, pd = await Aiohttp_get(url, kwargs['cookie'], proxies)
                        if int(pd) == 1:
                            html = html
                            break
                        elif int(pd) == 2:
                            url = html
                            num += 1
                        elif int(pd) == 3:
                            break
                    except:
                        num += 1
        first_url = kwargs['url']
        title_tag = kwargs['title_tag']
        title_re = kwargs['title_re']
        xpath_list = kwargs['xpath_list']
        cid = kwargs['cid']
        host_name = kwargs['host_name']
        name = kwargs['name']
        if len(html)<5:
            tuple_one = (cid, first_url, 1, str(host_name + name), 1) ###插入日志 页面请求不到
            rizhi_q.put((tuple_one,2))
        else:
            if "http://tjj.gz.gov.cn/zwgk/gfxwj" in first_url:
                pass
            else:
                html = remove_comments(html)
            kwargs['html'] = html
            if title_tag:
                json_list = Handle_tttt(**kwargs)
            elif title_re:
                json_list = Handle_title_re(**kwargs)
            elif xpath_list:
                json_list = Handle_xpath(**kwargs)
            if len(json_list) > 0 and json_list != None:
                for i in json_list:
                    rizhi_q.put((i,1))
            else:
                tuple_one = (cid, first_url, 3, str(host_name + name), 1)###插入日志 标签有问题
                rizhi_q.put((tuple_one, 2))


def ins_sql(rizhi_q):
    print("当前进程id={}".format(os.getpid()))
    while True:
        item = rizhi_q.get()
        if item == None:
            print("队列为空,退出程序")
            break
        else:
            info=item[0]
            pd=item[1]
            if pd==1:
                ins_info(info)
            elif pd==2:
                log_work(info)
            elif pd==3:
                up_eday(info)
def ins_info(item):
    """
    插入栏目页采集到的信息
    :return:
    """
    num = 0
    print(item)
    url = item[6]
    cid = item[2]
    item_url = str(url) + str(cid)
    pd = select_cf(item_url)
    if pd:
        pass
    else:
        try:
            conn, cursor = create_conn()
            sql = """
                                                  insert into tbl_article_info_temp(post_data,title,cid,type,status,release_at,url) values (%s,%s,%s,%s,%s,%s,%s)
                                                  """
            cursor.execute(sql, item)
            conn.commit()
            close_conn(conn, cursor)
            print(" 插入一条成功  " + item[-1])
        except Exception as err:
            num = 1
        if num == 1:
            try:
                conn, cursor = create_conn()
                sql = " UPDATE  tbl_article_info_temp  SET post_data=%s, title=%s,type=%s,status=%s,release_at=%s  WHERE url=%s and cid=%s"
                cursor.execute(sql, (item[0], item[1], item[3], item[4], item[5], item[6], item[2]))
                conn.commit()
                print("更新详情页数据一条", item[6])
                close_conn(conn, cursor)
            except Exception as err:
                print(err,7)

def  select_cf(url):
    m = hashlib.md5()
    m.update(str(url).encode('utf-8'))
    md = m.hexdigest()
    sql_i = "select id from tbl_article_info_md5 where md5=%s"
    try:
        conn, cursor = create_conn()
        cursor.execute(sql_i, md)
        all = cursor.fetchall()
        close_conn(conn, cursor)
    except  Exception as err:
        print(err,2)

    try:
        return all
    except:
        all=""
        return all


def log_work(item):
    """
    日志函数,报错插入日志信息

    :param url:
    :param err:
    :param title:
    :param type:
    :return:
    """
    if "index_1" in item[1]:
        pass
    else:
        num = 0
        sql = " insert into tbl_log_info(cid,url,errors,title,type) values (%s,%s,%s,%s,%s)"
        try:
            conn, cursor = create_conn()
            cursor.execute(sql, item)
            conn.commit()
            close_conn(conn, cursor)
            print("插入一条日志")
        except Exception as errr:
            print(errr,8)
            num = 1

        if num == 1:
            sql = "update  tbl_log_info set cid=%s,errors=%s,title=%s,type=%s where url=%s"
            try:
                conn, cursor = create_conn()
                cursor.execute(sql, (item[0], item[2], item[3], item[4], item[1]))
                conn.commit()
                close_conn(conn, cursor)
                print("更新日志成功")
            except Exception as err:
                print(err,3)
        else:
            pass

def up_eday(cid):

    sql = "update tbl_collect_info set eveday_status=1 where id=%s "
    try:
        conn, cursor = create_conn()
        cursor.execute(sql, cid)
        conn.commit()
        close_conn(conn, cursor)
        print("更新每天采集数据成功" + str(cid))
    except Exception as err:
        print( str(err))


def create_conn():
    conn = POOL.connection()
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

    return conn, cursor

# 关闭连接
def close_conn(conn, cursor):
    cursor.close()
    conn.close()


if __name__ == '__main__':
    start=WORK()
    rizhi_q = multiprocessing.Queue(maxsize=0)  ###队列1 用来放日志的   2各自的识别码
    p1 =  multiprocessing.Process(target=one, args=(start,rizhi_q,))
    c1 =  multiprocessing.Process(target=ins_sql, args=(rizhi_q,))
    c1.daemon = True
    p1.start()
    c1.start()
    c1.join()
