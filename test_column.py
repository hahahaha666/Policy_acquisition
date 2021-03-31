# -*- coding: utf-8 -*-
import asyncio

import  pymysql
from fake_useragent import UserAgent
import  json
import  hashlib
from w3lib.html import remove_comments
# from 政策爬虫_优化_09_23.function_class import *
from DBUtils.PooledDB import PooledDB
from  Policy_acquisition.aiohttps_GP import  *
from  Policy_acquisition.Ray_num import  *
from  Policy_acquisition.ALL_sql import  *
from  Policy_acquisition.Columnpage_process import  *
import nest_asyncio
nest_asyncio.apply()
####测试栏目页函数
POOL = PooledDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxconnections=2,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
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
class work():
    def __init__(self):
        pass
    async def sql(self,a,b):
        all_urls = []
        if b=="zc":
            conn, cursor = create_conn()
            item=ED_SQL(cursor,a)
            close_conn(conn, cursor)
        elif b=="hy":
            conn, cursor = create_conn()
            item = HY_SQL(cursor, a)
            close_conn(conn, cursor)
        argument = argument_get(item[0])
        argument['info_page']=1
        all_urls = Handle_url(all_urls, **argument)
        for j in all_urls:
            kwargs = j[5]
            num=0
            if int(j[3]) == 1:
                ##post采集
                url=j[0]
                while  num<3:
                    try:
                        if num > 0:
                            proxies = await  Aiohttp_ip()
                        else:
                            proxies = None
                        html,pd=await Aiohttp_post(url,argument['cookie'],proxies,j[2])
                        if int(pd)==1:
                            html=html
                            break
                        elif int(pd)==2:
                            url=html
                            num+=1
                        elif int(pd)==3:
                            return  "附件"
                    except:
                        num+=1
            else:
                if int(j[5]['immit_js']) == 1:
                    html = Ray_html(j[2])
                else:
                    url=j[2]
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
                                return "附件"
                        except:
                            num+=1
            first_url = kwargs['url']
            title_tag = kwargs['title_tag']
            title_re = kwargs['title_re']
            xpath_list = kwargs['xpath_list']
            if "http://tjj.gz.gov.cn/zwgk/gfxwj" in first_url:
                pass
            else:
                html = remove_comments(html)
            kwargs['html'] = html
            print(html)
            kwargs['judge_model']="test"##区分正常采集和测试采集返回的数据不一样
            if title_tag:
                json_list = Handle_tttt(**kwargs)
            elif title_re:
                json_list = Handle_title_re(**kwargs)
            elif xpath_list:
                json_list = Handle_xpath(**kwargs)
        print(json_list)
        all = {'info': json_list}
        ck = json.dumps(all)
        now_time = datetime.now()
        m = hashlib.md5()
        pp = str(first_url) + str(now_time)
        m.update(str(pp).encode('utf-8'))
        md = m.hexdigest()  ####把url转码md5
        self.sql_insert(md, ck)
        POOL.close()
        return  md

    def sql_insert(self,md,ck):
        try:
            conn, cursor = create_conn()
            sql = "insert into tbl_collect_temp(md5,data) values (%s,%s)"
            cursor.execute(sql, (md, ck))
            conn.commit()
            close_conn(conn, cursor)
        except Exception as  err:
            pass




def create_conn():
    conn = POOL.connection()
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

    return conn, cursor

# 关闭连接
def close_conn(conn, cursor):
    cursor.close()
    conn.close()

def  main(a,b):
    start=work()
    loop = asyncio.get_event_loop()
    get_future = asyncio.ensure_future(start.sql(a,b))  # 相当于开启一个future
    loop.run_until_complete(get_future)  # 事件循环
    print(get_future.result())  # 获取结果
    POOL.close()
    return  get_future.result()


