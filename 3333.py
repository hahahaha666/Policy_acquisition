import re
import  requests
import xlsxwriter
import  time

def GetxxintoExcel(html):
    global count
    a = re.findall(r'"title-text":"(.*?)"', html)
    b = re.findall(r'"coupon-price-afterCoupon":"(.*?)"', html)
    c = re.findall(r'"sell-info":"(.*?)"', html)
    d = re.findall(r'"seller-name":"(.*?)"', html)
    x = []
    for i in range(len(a)):
        try:
            x.append((a[i],b[i],c[i],d[i]))
        except IndexError:
            break
    i = 0
    for i in range(len(x)):
        worksheet.write(count + i + 1, 0, x[i][0])
        worksheet.write(count + i + 1, 1, x[i][1])
        worksheet.write(count + i + 1, 2, x[i][2])
        worksheet.write(count + i + 1, 3, x[i][3])
    count = count +len(x)
    return print("已完成")


def Geturls(q, x):
    url = "https://uland.taobao.com/sem/tbsearch?refpid=mm_26632258_3504122_32538762&keyword="+q+"&clk1=d6327d601c8a4300b00438399d8d2b0b&upsId=d6327d601c8a4300b00438399d8d2b0b&spm=a2e0b.20350158.search.1\
    &pid=mm_26632258_3504122_32538762&union_lens=recoveryid%3A201_11.29.174.48_31741_1617109215622%3Bprepvid%3A201_11.29.174.48_31741_1617109215622 "
    urls = []
    urls.append(url)
    if x == 1:
        return urls
    for i in range(1, x ):
        url = "https://uland.taobao.com/sem/tbsearch?refpid=mm_26632258_3504122_32538762&keyword="+p+"&clk1=d6327d601c8a4300b00438399d8d2b0b&upsId=d6327d601c8a4300b00438399d8d2b0b&spm=a2e0b.20350158.31919782.1\
        &pid=mm_26632258_3504122_32538762&union_lens=recoveryid%3A201_11.29.174.48_31741_1617109215622\
        %3Bprepvid%3A201_11.29.174.48_31741_1617109215622&pnum=" + str(
            i * 44)
        urls.append(url)
    return urls


def GetHtml(url):
    r = requests.get(url,headers =headers)
    r.raise_for_status()
    r.encoding = r.apparent_encoding
    return r

if __name__ == "__main__":
    count = 0
    headers = {
        "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36"
        ,"cookie":"cna=2iJpF3IxWE8CAbfLgqVs0zAA; sca=5bb0f55d; tbsa=b6679a66e28be064bb3e2c91_1617109221_2; atpsida=7af86bd64e06a9e841c9ee84_1617109221_2"
                }
    q = input("输入货物")
    x = int(input("你想爬取几页"))
    urls = Geturls(q,x)
    workbook = xlsxwriter.Workbook(q+".xlsx")
    worksheet = workbook.add_worksheet()
    worksheet.set_column('A:A', 70)
    worksheet.set_column('B:B', 20)
    worksheet.set_column('C:C', 20)
    worksheet.set_column('D:D', 20)
    worksheet.write('A1', '名字')
    worksheet.write('B1', '价格')
    worksheet.write('C1', '销量')
    worksheet.write('D1', '店名')
    xx = []
    for url in urls:
        html = GetHtml(url)
        s = GetxxintoExcel(html.text)
        time.sleep(5)
    workbook.close()
