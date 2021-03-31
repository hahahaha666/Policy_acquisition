import asyncio

import  pymysql
from fake_useragent import UserAgent
# from 政策爬虫_优化_09_23.function_class import *
from DBUtils.PooledDB import PooledDB
from  Policy_acquisition.aiohttps_GP import  *
from  Policy_acquisition.Detailpage_process import  *
from  Policy_acquisition.Ray_num import  *
from  Policy_acquisition.ALL_sql import  *
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

    async def work_b(self,id,url,c,post_data):
        c_all = self.select_sql(id,c)
        argument = dispose_dict(c_all[0])
        if int(argument['immit_js']) == 1:
            html=Ray_html(url)
        else:
            num = 0
            if int(argument['detail_type']) == 0: ##详情页是get：
                while  num<3:
                    try:
                        if num > 0:
                            proxies = await  Aiohttp_ip()
                        else:
                            proxies = None
                        html,pd=await Aiohttp_get(url,argument['cookie'],proxies)
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
            else:###详情页是post
                while  num<3:
                    try:
                        if num > 0:
                            proxies = await  Aiohttp_ip()
                        else:
                            proxies = None
                        print(url,post_data)
                        html,pd=await Aiohttp_post(url,argument['cookie'],proxies,post_data)
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
        if int(argument['detail_tran']) == 1:
            html=html.encode('utf-8').decode('unicode_escape', 'ignore')
        else:
            pass
        if r"\u003c" in html:
            html = str(json.loads(html))
        argument['html']=html
        argument['cid']=id
        argument['pub_time']=None
        argument['judge_model']="test"
        content_tag = argument['content_tag']
        argument['url'] = url
        xpath_xq = argument['xpath_xq']
        json_xq=['json_xq']
        print(html)
        if content_tag:  ####正文标签
            content_tag_start = content_tag.split(',')[0].strip()
            content_tag_end = content_tag.split(',')[1].strip()
        else:
            content_tag_start = ""
            content_tag_end = ""
        argument['content_tag'] = content_tag_start
        argument['content_tag_end'] = content_tag_end
        if content_tag:
            item=DPP_contentr_e(**argument)
        elif xpath_xq:
            item=DPP_content_xpath(**argument)
        elif json_xq:
            item = DPP_content_json(**argument)
        #####测试不加入自动获取文本 以防不打标签
        if item!=None:
            all = []
            all.append(item)
            all_info = {'info': all}
            ck = json.dumps(all_info,ensure_ascii=False)
            print(ck.replace(r'\"',"'").replace(r'\r','').replace(r'\n','').replace(r'\t',''))
            now_time = datetime.now()
            m = hashlib.md5()
            pp = str(url) + str(now_time)
            m.update(str(pp).encode('utf-8'))
            md = m.hexdigest()  ####把url转码md5
            print(md)
            self.sql_insert(md, ck)
        else:
            md=''
        return md



    def sql_insert(self,md,ck):
        try:
            conn, cursor = create_conn()
            sql = "insert into tbl_collect_temp(md5,data) values (%s,%s)"
            cursor.execute(sql, (md, ck))
            conn.commit()
            close_conn(conn, cursor)
        except Exception as  err:
            print(err)
            pass



    def select_sql(self,id,c):
        if c=="zc":
            sql = "select content_tag, title_type, host_name,id,xpath_xq,type,cookie,host,immit_js,judge_time,accessory_xpath,accessory_re,accessory_judge,detail_tran,detail_type,title_details,re_time,xpath_time,json_xq       from tbl_collect_info  where id>=%s"%id
        elif c=="hy":
            sql = "select content_tag, title_type, host_name,id,xpath_xq,type,cookie,host,immit_js,judge_time,accessory_xpath,accessory_re,accessory_judge,detail_tran,detail_type,title_details,re_time,xpath_time,json_xq      from tbl_industry_info where id=%s " % id
        conn, cursor = create_conn()
        cursor.execute(sql )
        results = cursor.fetchall()
        close_conn(conn, cursor)
        return results



def create_conn():
    conn = POOL.connection()
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

    return conn, cursor

# 关闭连接
def close_conn(conn, cursor):
    cursor.close()
    conn.close()

def main_i(a,b,c,post_data):
    start=work()
    loop = asyncio.get_event_loop()
    get_future = asyncio.ensure_future(start.work_b(a,b,c,post_data))  # 相当于开启一个future
    loop.run_until_complete(get_future)  # 事件循环
    POOL.close()
    return  get_future.result()

