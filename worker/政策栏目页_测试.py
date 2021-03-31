# -*- coding: utf-8 -*-

from w3lib.html import remove_comments
from  aiohttps_GP import  *
from  Ray_num import  *
from  ALL_sql import  *
from  Columnpage_process import  *
from utils.async_proxy_util import *
import nest_asyncio
from utils.PoolDB import POOL_DB

from utils import log

logger = log.write_log(work_name='政策栏目页测试')
nest_asyncio.apply()
####测试栏目页函数

class ZC_getcolumn():
    def __init__(self):
        pass

    async  def AIO_POST(self,argument,j,url,logger):
        num=0
        while num < 3:
            try:
                if num > 0:
                    proxies = await  get_proxy()
                else:
                    proxies = None
                html, pd = await Aiohttp_post(url, argument['cookie'], proxies, j[2],argument['res_headers'],logger)
                if int(pd) == 1:
                    html = html
                    return  html
                elif int(pd) == 2:
                    url = html
                    num += 1
                elif int(pd) == 3:
                    return "附件"
            except Exception as err:
                logger.error("政策{} post 循环 {}次 {}".format(url ,str(num),err))
                num += 1
        return  None

    async  def AIO_GET(self,argument,url,logger):
        num=0
        while num < 3:
            try:
                if num > 0:
                    proxies = await  get_proxy()
                else:
                    proxies = None
                html, pd = await Aiohttp_get(url, argument['cookie'], proxies,argument['res_headers'],logger)
                if int(pd) == 1:
                    html = html
                    return html
                elif int(pd) == 2:
                    url = html
                    num += 1
                elif int(pd) == 3:
                    return "附件"
            except Exception as err:
                logger.error("政策  {} get 循环 {}  {}".format(url,str(num),err))
                num += 1
        return None



    async def sql(self,a):
        all_urls = []
        conn, cursor = POOL_DB().create_conn()
        item=ED_SQL(cursor,a)
        POOL_DB().close_conn(conn, cursor)
        argument = argument_get(item[0])
        argument['info_page']=1
        all_urls = Handle_url(all_urls, **argument)
        for j in all_urls:
            kwargs = j[5]
            kwargs['judge_model']="test"##区分正常采集和测试采集返回的数据不一样
            if int(j[3]) == 1:
                ##post采集
                url=j[0]
                html =await  self.AIO_POST(argument,j,url,logger)
            else:
                if int(j[5]['immit_js']) == 1: ####瑞树js
                    html =await  Ray_html(j[2],argument['res_headers'],logger)
                else:
                    ####get采集
                    url=j[2]
                    html = await self.AIO_GET(argument,url,logger)
            first_url = kwargs['url']
            title_tag = kwargs['title_tag']
            title_re = kwargs['title_re']
            xpath_list = kwargs['xpath_list']
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
        all = {'info': json_list}
        ck = json.dumps(all)
        now_time = datetime.now()
        m = hashlib.md5()
        pp = str(first_url) + str(now_time)
        m.update(str(pp).encode('utf-8'))
        md = m.hexdigest()  ####把url转码md5
        self.sql_insert(md, ck)
        POOL_DB().close_db()
        return  md

    def sql_insert(self,md,ck):
        try:
            conn, cursor = POOL_DB().create_conn()
            sql = "insert into tbl_collect_temp(md5,data) values (%s,%s)"
            cursor.execute(sql, (md, ck))
            conn.commit()
            POOL_DB().close_conn(conn, cursor)
        except Exception as  err:
            pass







