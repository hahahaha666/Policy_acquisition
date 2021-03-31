#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：公司政策爬虫 
@File    ：详情页采集.py
@IDE     ：PyCharm 
@Author  ：13192272582
@Date    ：2021/1/22 18:15 
'''


# -*- coding: utf-8 -*-
import multiprocessing
import time

import pymysql
from w3lib import html
import random
import asyncio
import threading
from DBUtils.PooledDB import PooledDB
import  os
import  traceback
from  Policy_acquisition.aiohttps_GP import  *
from  Policy_acquisition.Ray_num import  *
from  Policy_acquisition.ALL_sql import  *
from  Policy_acquisition.Columnpage_process import  *
from Policy_acquisition .Detailpage_process import  *
sem = asyncio.Semaphore(400) # 信号量，控制协程数，防止爬的过快


POOL = PooledDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxconnections=10,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=5,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
    maxshared=0,  # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
    blocking=False,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
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

class producer(object):
    def __init__(self):
        # self.ua = UserAgent(use_cache_server=False)
        pass

    def select_SQL(self):
        conn, cursor = create_conn()
        NOT_js=SQL_not_js(conn,cursor)
        # NOT_js=SQL_ALL(cursor)
        close_conn(conn, cursor)

        conn, cursor = create_conn()
        XW_id = SQL_XW(conn, cursor)
        close_conn(conn, cursor)

        conn, cursor = create_conn()
        DIS_info = SQL_disposition(conn, cursor)
        close_conn(conn, cursor)

        conn, cursor = create_conn()
        delete_sql(conn, cursor)
        close_conn(conn, cursor)

        return NOT_js,XW_id,DIS_info


    def fj_get(self, md, url, title, l_type, host,Annex_queue):
        ##附件英文名字
        name_eng = url.split("/")[-1]
        if int(l_type) == 1:
            path_two = 'zhengcezixun'
        elif int(l_type) == 2:
            path_two = 'zhengcewenjian'
        elif int(l_type) == 4:
            path_two = 'xinwenyaowen'
        elif int(l_type) == 5:
            path_two = 'zhaobiaocaigou'
        suffix = get_suffix(url)
        path = "public/" + str(host).strip() + "/" + str(path_two) + "/wyfj/" + str(md) + "." + str(suffix).strip()  ###拼接路径
        name_cn = str(title) + "的附件"  #####附件中文名字
        fj_one = (name_cn, name_eng, path, md, url)
        Annex_queue.put((fj_one))




def create_conn():
    conn = POOL.connection()
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

    return conn, cursor


# 关闭连接
def close_conn(conn, cursor):
    cursor.close()
    conn.close()


def ins_one(item,verdict):
    """
    更新详情页中间表信息

    :param source: 来源
    :param c_time: 日期
    :param content: 正文
    :param md: 附件md5值列表
    :param title:标题
    :return:
    """
    source = item[0]
    c_time = item[1]
    content = item[2]
    md = item[3]
    title = item[4]
    url = item[5]
    cid = item[6]
    a=time.time()
    try:
        soup = BeautifulSoup(content, 'html.parser')
        content = soup.prettify()
    except:
        content = item[2]

    try:
        content = html.remove_tags(content,which_ones=('font', 'frame', 'iframe', 'span', 'o:p', 'form', 'link'))
        content = html.remove_tags_with_content(content, which_ones=('script', 'form'))
    except:
        content = item[2]
    if cid in verdict:
        try:
            img_md = fir_img_ins( content)
        except:
            img_md=None
    else:
        img_md = None
    try:
        conn, cursor = create_conn()
        sql = " UPDATE  tbl_article_info_temp  SET release_at=%s,source=%s,content=%s,attachment_id=%s,type=0,title=%s,status=1,img_md5=%s  WHERE url=%s"
        cursor.execute(sql, (c_time, source, content, ",".join(md), title, img_md, url))
        conn.commit()
        print(" 更新详情表成功" + str(title) + str(c_time) + "  " + str(time.time()-a))
        close_conn(conn, cursor)
    except Exception as err:
        print(str(err) + "定位3")


def fir_img_ins( content):
    imgs = re.compile("<img.*?(?:>|\/>)", re.IGNORECASE).findall(content)
    if imgs:
        for i in imgs:
            first_img = "".join(re.compile(" src=[\'\"]?([^\'\"]*)[\'\"]?", re.IGNORECASE).findall(i))
            m = hashlib.md5()
            m.update(str(first_img).encode('utf-8'))
            img_md = m.hexdigest()
            path = 'public/images/' + str(random.randint(1, 11)) + '/' + str(img_md) + ".png"
            try:
                conn, cursor = create_conn()
                sql_i = "insert into tbl_img_info(md5,url,status,path) values (%s,%s,%s,%s)"
                cursor.execute(sql_i, (img_md, first_img, 0, path))
                conn.commit()
                close_conn(conn, cursor)
                print(threading.currentThread().name + " 插入图片表数据成功")
            except Exception as err:
                print(str(err) + "定位4")
            break
    else:
        img_md = None
    return img_md


def log_work(item):
    """
    日志函数,报错插入日志信息
    :param url:
    :param err:
    :param title:
    :param type:
    :return:
    """
    cid = item[0]
    url = item[1]
    err = item[2]
    title = item[3]
    type = item[4]
    try:
        conn, cursor = create_conn()
        sql_selec = "select id from tbl_log_info where url=%s "
        cursor.execute(sql_selec, url)
        id = cursor.fetchall()
        close_conn(conn, cursor)
    except:
        id = ""
    if id:
        sql = "update  tbl_log_info set cid=%s,errors=%s,title=%s,type=%s where url=%s"
        try:
            conn, cursor = create_conn()
            cursor.execute(sql, (str(cid), str(err), title, str(type), url))
            conn.commit()
            close_conn(conn, cursor)
        except Exception as per:
            print(per,66)

    else:
        sql = " insert ignore  into tbl_log_info(cid,url,errors,title,type) values (%s,%s,%s,%s,%s)"
        try:
            conn, cursor = create_conn()
            cursor.execute(sql, (str(cid), url, str(err), title, str(type)))
            conn.commit()
            close_conn(conn, cursor)
        except Exception as errr:
            print(errr,77)


def ins_fj(info):
    """
    插入附件表

    :param name_cn: 中文名
    :param name_eng: 英文名
    :param path: 路径
    :return:
    """
    name_cn = info[0]
    name_eng = info[1]
    path = info[2]
    md = info[3]
    fj_url = info[4]
    try:
        conn, cursor = create_conn()
        sql = "insert ignore into tbl_attachment_info(name,url_name,url,path,md5,type) values (%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, (name_cn, name_eng, fj_url, path, md, 1))
        conn.commit()
        close_conn(conn, cursor)
        print("插入附件一条" + name_cn + name_eng + fj_url + path + md)
    except Exception as  err:
        print(str(err) + "定位5")


def update_sql(url):
    """
    更新详情temp表 状态码
    :param url:
    :return:
    """
    try:
        conn, cursor = create_conn()
        sql = " UPDATE  tbl_article_info_temp  SET status=4  WHERE id=%s"
        cursor.execute(sql,url)
        conn.commit()
        close_conn(conn, cursor)
        print("更新一条成功")
    except Exception as err:
        print(str(err) + "定位6")


def update_fail(url):
    """
    更新详情temp表 状态码
    :param url:
    :return:
    """
    try:
        conn, cursor = create_conn()
        sql = " UPDATE  tbl_article_info_temp  SET status=3  WHERE id=%s"
        cursor.execute(sql, url)
        conn.commit()
        close_conn(conn, cursor)
    except Exception as err:
        print(str(err))

def  affix(start,url,title,argument,pub_time,cid,Annex_queue):
    list_md = []
    fj_url = url
    m = hashlib.md5()
    m.update(str(fj_url).encode('utf-8'))
    md = m.hexdigest()  ####拿到url 哈希值
    list_md.append(md)
    start.fj_get(md, url, title, argument['l_type'], argument['host'], Annex_queue)
    source = argument['host_name']
    if len(pub_time) > 2:
        c_time = pub_time
    else:
        c_time = "2020-01-01"
    content = "此网页是附件"
    tuple_1 = (source, c_time, content, list_md, title, url, cid)
    Annex_queue.put((tuple_1, 1))  ###入列

def  one(start,Annex_queue,detail_list, verdict, item,hu):
    print("当前进程id={}".format(os.getpid()))
    time.sleep(2)
    if detail_list == None or not detail_list:
        Annex_queue.put(None)
        print("队列为空 退出")
        os._exit(0)
    loop = asyncio.get_event_loop()
    tasks = []  # 获取事件循环
    for i in detail_list:
        task=cor_parse(start,i,item,Annex_queue)
        tasks.append(task)
    loop.run_until_complete(asyncio.wait(tasks))  # 激活协程
    loop.close()  # 关闭事件循环  # pool.submit(,j,headers,kwargs)
    Annex_queue.put(None)
    print(time.time()-hu)


async  def cor_parse(start,i,item,Annex_queue):
    async with(sem):
        cid = i['cid']
        title = i['title']
        old_id=i['id']
        url = i['url'].strip()
        print(title,url)
        pub_time = i['release_at']
        post_data = i['post_data']
        if post_data == None or not post_data:
            post_data = ""
        try:
            info_item = item[cid]
            argument = dispose_dict(info_item)
        except:
            traceback.print_exc()
            return  1
        argument['cid'] = cid
        argument['title'] = title
        argument['url'] = url
        argument['pub_time'] = pub_time
        url_low = str(url).lower()  ####把网址转成小写
        ###判断网页后缀是否是附件
        judge = get_suffix(url_low)
        # Annex_queue.put((old_id,4))
        if len(judge) > 1:
            affix(start,url,title,argument,pub_time,cid,Annex_queue)
        else:
            html=None
            try:
                if int(argument['immit_js']) == 1:
                    # html = Ray_html(url)
                    html=None
                else:
                    num = 0
                    if int(argument['detail_type']) == 0:  ##详情页是get：
                        while num < 3:
                            try:
                                if num > 0:
                                    proxies = await  Aiohttp_ip()
                                else:
                                    proxies = None
                                html, pd = await Aiohttp_get(url, argument['cookie'], proxies)
                                if int(pd) == 1:
                                    html = html
                                    break
                                elif int(pd) == 2:
                                    url = html
                                    num += 1
                                elif int(pd) == 3:
                                    affix(start, url, title, argument, pub_time, cid,Annex_queue)
                                    return "附件"
                            except:
                                num += 1
                    else:  ###详情页是post
                        while num < 3:
                            try:
                                if num > 0:
                                    proxies = await  Aiohttp_ip()
                                else:
                                    proxies = None
                                html, pd = await Aiohttp_post(url, argument['cookie'], proxies, post_data)
                                if int(pd) == 1:
                                    html = html
                                    break
                                elif int(pd) == 2:
                                    url = html
                                    num += 1
                                elif int(pd) == 3:
                                    affix(start, url, title, argument, pub_time, cid,Annex_queue)
                                    return "附件"
                            except:
                                num += 1
                if html == None:
                    tuple_one = (cid, url, 2, str(title), 2)
                    Annex_queue.put((tuple_one, 3))
                    Annex_queue.put((old_id, 5))
                    return 1  ####网页请求有问题 写入日志
                if int(argument['detail_tran']) == 1:
                    html = html.encode('utf-8').decode('unicode_escape', 'ignore')
                else:
                    pass
                if r"\u003c" in html:
                    html = str(json.loads(html))
                argument['html'] = html
                argument['cid'] = id
                content_tag = argument['content_tag']
                argument['url'] = url
                xpath_xq = argument['xpath_xq']
                json_xq = ['json_xq']
                if content_tag:  ####正文标签
                    content_tag_start = content_tag.split(',')[0].strip()
                    content_tag_end = content_tag.split(',')[1].strip()
                else:
                    content_tag_start = ""
                    content_tag_end = ""
                argument['content_tag'] = content_tag_start
                argument['content_tag_end'] = content_tag_end
                argument['html'] = html
                if content_tag_start:
                    Details, adjunct = DPP_contentr_e(**argument)
                elif xpath_xq:
                    Details, adjunct = DPP_content_xpath(**argument)
                elif json_xq:
                    Details, adjunct = DPP_content_json(**argument)
                if Details != None:
                    Annex_queue.put((Details, 1))
                else:
                    tuple_one = (cid, url, 3, str(title), 2)
                    Annex_queue.put((tuple_one, 3))
                    Annex_queue.put((old_id, 5))

                if adjunct != None:
                    for i in adjunct:
                        Annex_queue.put((i, 2))
            except:
                traceback.print_exc()


def ins_sql(Annex_queue,verdict):
    print("当前进程id={}".format(os.getpid()))
    while True:
        item = Annex_queue.get()
        if item == None:
            print("队列为空,退出程序")
            break
        else:
            info=item[0]
            pd=item[1]
            if pd==1:
                ins_one(info,verdict)
            elif pd==3:
                log_work(info)
            elif pd==2:
                ins_fj(info)
            elif  pd==4:
                update_sql(info)
            elif pd==5:
                update_fail(info)

if __name__ == '__main__':
    start = producer()
    detail_list, verdict, item = start.select_SQL()
    hu=time.time()
    Annex_queue = multiprocessing.Queue(maxsize=0)  ###队列 用来放信息的  1代表详情页  2附件  3日志  4更新  5采集失败
    p1 =  multiprocessing.Process(target=one, args=(start,Annex_queue,detail_list, verdict, item,hu,))
    c1 =  multiprocessing.Process(target=ins_sql, args=(Annex_queue,verdict,))
    c1.daemon = True
    p1.start()
    c1.start()
    c1.join()
    POOL.close()