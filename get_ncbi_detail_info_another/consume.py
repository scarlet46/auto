# -*- encoding:utf-8 -*-
# @time: 2023/5/17 22:22
# @author: ifeng
import os
import json
import threading

import redis
import requests
from lxml import html
import csv
from concurrent.futures import ThreadPoolExecutor

# 加上线程锁
lock = threading.Lock()
pool = redis.ConnectionPool(host='localhost', port=6379, db=12, decode_responses=True)
# pool = redis.ConnectionPool(host='localhost', port=6379, db=12, password='123456', decode_responses=True)
client = redis.Redis(connection_pool=pool)
dir_path = os.path.abspath("../newest")

if not os.path.exists(dir_path):
    dir_path = os.path.abspath("./newest")


filename = os.path.join(dir_path, 'ncbi_data(all).csv')
output_csv = os.path.join(dir_path, 'ncbi_data(all)_output.csv')

def parse_detail(url):
    for item in range(3):
        try:
            resp = requests.get(url, timeout=120)
            etree = html.etree
            et = etree.HTML(resp.text)
            # contains的.表示当前节点 Summary表示要搜索的子字符串
            # parent::*选择当前节点的所有父节点，无论它们的名称是什么, 也可以使用parent::tr
            accession_xpath = "//*[@id='geo_acc']/@value"
            title_xpath = "//tr[@valign='top']/td[text()[contains(., 'Title')]]/parent::tr/td[2]//text()"
            organism_xpath = "//tr[@valign='top']/td[text()[contains(., 'Organism')]]/parent::tr/td[2]//text()"
            summary_design_xpath = "//tr[@valign='top']/td[text()[contains(., 'Summary')]]/parent::tr/td[2]//text()"
            overall_design_xpath = "//tr[@valign='top']/td[text()[contains(., 'Overall design')]]/parent::tr/td[2]//text()"
            # citation_xpath = "//tr[@valign='top']/td[text()[contains(., 'Citation(s)')]]/parent::tr/td[2]//text()"
            pmid_xpath = "//tr[@valign='top']/td[text()[contains(., 'Citation(s)')]]/parent::tr/td[2]/span/@id"
            accession = et.xpath(accession_xpath)[0]
            title = "".join([item.strip() for item in et.xpath(title_xpath)]).replace(',', '，')
            organism = "".join([item.strip() for item in et.xpath(organism_xpath)]).replace(',', '，')
            summary = "".join([item.strip() for item in et.xpath(summary_design_xpath)]).replace(',', '，')
            overall_design = "".join([item.strip() for item in et.xpath(overall_design_xpath)]).replace(',', '，')
            pmid = ",".join([item.strip() for item in et.xpath(pmid_xpath)])
            journal = ""
            # 向动态加载页面发送请求
            if pmid:
                citation_url = f'https://www.ncbi.nlm.nih.gov/sites/PubmedCitation?id={pmid}&_=1684923729520'
                resp = requests.get(citation_url)
                page_source = resp.text.strip('<?xml version="1.0" encoding="UTF-8"?>')
                etree = html.etree
                et = etree.HTML(page_source)
                journal = et.xpath('//div[@class="PubmedCitation"]/ul/li/span[@class="source"]/text()')
                journal = "，".join(journal)
            pmid = pmid.replace(',', '，')
            # 储存到csv中
            if summary:
                with lock:
                    # 备份原始文件 -> 防止写入的时候出错
                    # shutil.copy2('ncbi_data(Homo sapiens).csv', 'backup.csv')
                    with open(filename, 'a', encoding='utf8', errors='ignore', newline="") as f:
                        writer = csv.writer(f)
                        # 使用 writerow 方法写入整行数据
                        writer.writerow([title, accession, organism, summary, overall_design, str(pmid), journal])
            print(url, "抓取完毕")
            return
        except Exception as e:
            # 写入出错，恢复原始的CSV文件数据
            # shutil.copy2('backup.csv', 'ncbi_data(Homo sapiens).csv')
            print(url, "-->", e)


def consume_data():
    while True:
        queue_name = 'ncbi_detail_url'
        timeout = 3  # 超时时间，防止阻塞
        message = client.blpop(queue_name, timeout=timeout)
        if message is not None:
            detail_url = message[1]
            if not detail_url.split('?')[-1].split('=')[-1].startswith('GSM'):
                yield detail_url  # 将detail_url传入
        else:
            print('没有数据啦')
            break

#def consume_data():
#    txt_path = os.path.join(dir_path, 'ncbi_href.txt')
#    with open(txt_path, "r") as fp:
#        data = json.loads(fp.read())
#    for detail_url in data:
#        yield detail_url


def main():
    with ThreadPoolExecutor(max_workers=300) as executor:
        futures = [executor.submit(parse_detail, url) for url in consume_data()]
        for future in futures:
            future.result()  # 等待所有任务完成


# 定义一个函数来读取CSV文件并去除重复行
def remove_duplicates():
    with open(filename, 'r',encoding='utf8', newline='') as infile:
        reader = csv.reader(infile)
        # 使用集合来存储唯一的行，因为集合不允许重复的元素
        seen = set()
        with open(output_csv, 'w',encoding="utf8", newline='') as outfile:
            writer = csv.writer(outfile)
            for row in reader:
                # 将行转换为元组，因为列表是可变的，不能作为集合的元素
                if tuple(row) not in seen:
                    seen.add(tuple(row))
                    writer.writerow(row)

    # 将原始文件重命名为.bak后缀
    os.rename(filename, filename + '.bak')

    # 将去重后的文件重命名为原始文件名
    os.rename(output_csv, filename)


if __name__ == '__main__':
    main()
    # 执行去重操作
    # remove_duplicates()





