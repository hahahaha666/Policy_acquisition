# -*- coding: utf-8 -*-
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
sem = asyncio.Semaphore(5) # 信号量，控制协程数，防止爬的过快


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
    def cs_sql(self):
        """
            从数据表中查询出需要采集网站
            return id
        """
        try:
            conn, cursor = create_conn()
            sql = "select id,cid from tbl_sdpc_info where type=0 and xq_type=0 limit 1"#type=0 limit 5
            # sql="select id,cid from tbl_sdpc_info where id=55150"
            cursor.execute(sql)
            results = cursor.fetchall()
            close_conn(conn, cursor)
            all=[]
            for i in results:
                id=i['id']
                cid=i['cid']
                tuple=(id,cid)
                all.append(tuple)
        except:
            all = ''
        return all


    def update_id(self,id):
        try:
            conn, cursor = create_conn()
            sql = "update tbl_sdpc_info set type=1 where id=%s"
            cursor.execute(sql, id)
            conn.commit()
            close_conn(conn, cursor)
            print("更新手动采集队列数据成功")
        except:
            pass

def create_conn():
    conn = POOL.connection()
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

    return conn, cursor

# 关闭连接
def close_conn(conn, cursor):
    cursor.close()
    conn.close()

def one(start,q_item):
    all_id = start.cs_sql()
    # all_id=[5]
    all_urls=[]
    if len(all_id) ==0:
        q_item.put(None)
        return 1
    for i in all_id:
        id=i[0]
        # id=31289
        cid=i[1]
        # cid=43410
        conn, cursor = create_conn()
        item = ED_SQL(cursor, cid)
        close_conn(conn, cursor)
        argument = argument_get(item[0])
        all_urls=Handle_url(all_urls,**argument)
        start.update_id(id)
    loop = asyncio.get_event_loop()
    tasks = []  # 获取事件循环
    for j in all_urls:
        kwargs = j[5]
        task=next_one(j,kwargs,q_item)
        tasks.append(task)
    loop.run_until_complete(asyncio.wait(tasks))  # 激活协程
    loop.close()  # 关闭事件循环  # pool.submit(,j,headers,kwargs)
    q_item.put(None)
    print("完成")
async  def next_one(j,argument,q_item):
    async with(sem):
        html=""
        kwargs = j[5]
        num = 0
        if int(j[3]) == 1:
            ##post采集
            url = j[0]
            while num < 3:
                try:
                    if num > 0:
                        proxies = await  Aiohttp_ip()
                    else:
                        proxies = None
                    html, pd = await Aiohttp_post(url, argument['cookie'], proxies, j[2])
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
                        html, pd = await Aiohttp_get(url, argument['cookie'], proxies)
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
        if len(html)<5:
            pass
        else:
            first_url = kwargs['url']
            title_tag = kwargs['title_tag']
            title_re = kwargs['title_re']
            xpath_list = kwargs['xpath_list']
            cid = kwargs['cid']
            host_name = kwargs['host_name']
            name = kwargs['name']
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
            print(url,json_list)
            if len(json_list) > 0 and json_list != None:
                for i in json_list:
                    q_item.put(i)
            else:
                tuple_one = (cid, first_url, 3, str(host_name + name), 1)


def ins_sql(q_item):
    while True:
        item = q_item.get()
        if item == None:
            print("队列为空,退出程序")
            break
        else:
            num = 0
            try:
                conn, cursor = create_conn()
                sql = """
                                          insert into tbl_article_info_temp(post_data,title,cid,type,status,release_at,url) values (%s,%s,%s,%s,%s,%s,%s)
                                          """
                cursor.execute(sql, item)
                conn.commit()
                close_conn(conn, cursor)
                print(threading.currentThread().name + " 插入一条成功"+ item[-1])
            except Exception as err:
                num = 1
            if num==1:
                try:
                    conn, cursor = create_conn()
                    sql = " UPDATE  tbl_article_info_temp  SET post_data=%s, title=%s,type=%s,status=%s,release_at=%s  WHERE url=%s and cid=%s"
                    cursor.execute(sql, (item[0],item[1],item[3],item[4],item[5],item[6],item[2]))
                    conn.commit()
                    print("更新详情页数据一条",item[6])
                    close_conn(conn, cursor)
                except Exception as err:
                    print(err)
                    pass

if __name__ == '__main__':
    start=WORK()
    q_item = multiprocessing.Queue()
    p1 =  multiprocessing.Process(target=one, args=(start,q_item,))
    c1 =  multiprocessing.Process(target=ins_sql, args=(q_item,))
    c1.daemon = True
    p1.start()
    c1.start()
    c1.join()
