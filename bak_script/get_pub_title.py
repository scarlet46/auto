# -*- encoding:utf-8 -*-
# @time: 2023/5/13 9:52
# @author: ifeng
"""
    后面迭代title的时候需要将逗号改成英文的!!!
"""
import csv
import os.path
import threading

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from urllib.parse import urljoin
from urllib.parse import urlparse, parse_qs

import pandas
import requests
from lxml import html
import chardet
import saveData
import logging

# logging.basicConfig(level=logging.DEBUG)


dir_path = os.path.abspath("newest")
pdf_path = os.path.join(dir_path, 'pdf')

if not os.path.exists(dir_path):
    os.mkdir(dir_path)

if not os.path.exists(pdf_path):
    os.mkdir(pdf_path)

# 爬取表
# filename = os.path.join(dir_path, "pubmed_data_new(all).csv")
filename = os.path.join(dir_path, "pubmed_s.csv")

# 下载表
download_csv_path = os.path.join(dir_path, "pubmed_d.csv")

# 合并表
merge_csv_path = os.path.join(dir_path, "pubmed_merge.csv")

# 加上线程锁
lock = threading.Lock()


def parse_detail(url, title, journal, pubdate, pubmed_id):
    for i in range(2):  # 爬虫的自省
        try:
            resp = requests.get(url)
            # resp.encoding="utf-8"
            # encoding = chardet.detect(resp.content)['encoding']
            # content = resp.content.decode(encoding)
            # etree = html.etree
            # et = etree.HTML(content)
            
            etree = html.etree
            et = etree.HTML(resp.text)
            
            abstract = "".join([item.strip() for item in et.xpath('//*[@id="abstract"]//text()')]).strip().replace(',',
                                                                                                                   '，').lstrip(
                'Abstract')
            if not abstract:
                print(url, '无abstract')
            # filename = "newest/pubmed_data_new(2022-2023).csv"
            data = [title, journal, pubdate, pubmed_id, url, abstract]
            with lock:
                with open(filename, mode='a', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(data)

            return
        except Exception as e:
            with open('log.txt', 'a', encoding='utf-8') as f:
                error_msg = url + " --> " + str(e)
                f.write(error_msg)
                f.write('\n')


def get_first_page(url):
    pmid_list = []
    resp = requests.get(url)
    # resp.encoding='utf-8'
    # encoding = chardet.detect(resp.content)['encoding']
    # content = resp.content.decode(encoding)
    # etree = html.etree
    # et = etree.HTML(content)
    etree = html.etree
    et = etree.HTML(resp.text)
    total_page = 1000
    try:
        total_page = int(et.xpath("/html/body/main/div[9]/div[2]/div[6]/div/label[2]/text()")[0].split(" ")[-1])
    except Exception as e:
        print("total_page 获取失败，使用默认1000")
    article_ls = et.xpath('//div[@class="search-results-chunk results-chunk"]/article')
    for article in article_ls:
        href = article.xpath('./div[2]/div[1]/a/@href')[0]
        detail_url = urljoin(url, href)
        title = "".join(article.xpath('./div[2]/div[1]/a//text()')).strip().replace(',', '，')
        journal, pubdate = article.xpath('./div[2]/div[1]/div[1]/span[3]/text()')[0].split(';')[0].split('.')
        journal = journal.replace(',', '，')
        pubmed_id = "".join(article.xpath('./div[2]/div[1]/div[1]/span[@class="citation-part"]/span/text()'))

        parse_detail(detail_url, title, journal, pubdate, pubmed_id)

        pmid_list.append(pubmed_id)
    print('第1页抓取完毕')
    return total_page, pmid_list


def get_other_page(url, page, term, filter):
    all_id_list = []
    for item in range(2):
        try:
            data = {
                # "term": "(((((single cell transcriptomics) OR (single-nuclei transcriptomics) OR (single-cell transcriptomics) OR (Zemin Zhang[Author]) OR (single-cell RNA sequencing) OR (single cell RNA sequencing) OR (single-nuclei RNA sequencing) OR (single cell RNA-seq) OR (single-cell RNA-seq) OR (single-nuclei RNA-seq) OR (cell atlas) OR (snRNA-seq) OR (scRNA-seq) OR (Single-cell))) AND (Perturbation)) OR (Perturb-Seq)) OR ((((((((((((CRISP-seq) OR (CROP-seq)) OR (TAP-seq)) OR (MIX-seq)) OR (*CRISPR* screening*)) OR (Mosaic-seq)) OR (CombiGEM-CRISPR)) OR (perturb* seq*)) OR (Pooled genetic screens)) OR (CRISPR activation)) OR (CRISPR interference)) OR (Multiplexed perturbations))",
                "term": term,
                "page": page,
                # 'filter': 'years.2023-2024',
                "no-cache": "1719132164019",
                "csrfmiddlewaretoken": "Ym3igRZeNcra3M4DfSlbhR2SHRz75bWrO9oB86n7EuaMU2AuMo3xLpoDGRqK5730"
            }
            if filter:
                data['filter'] = filter
            headers = {
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
                'referer': 'https://pubmed.ncbi.nlm.nih.gov/?term=((((Mus*)%20OR%20(Mouse*))%20OR%20(Mice))%20OR%20((Homo%20sapiens%20OR%20(human*%20OR%20(patient*)))))%20AND%20(single%20cell%20RNA-seq)AND%20(2016%3A2022%5Bpdat%5D)&filter=simsearch1.fha&filter=datesearch.y_10',
                'cookie': 'ncbi_sid=0C42616145B55E01_0000SID; pm-csrf=hZD3i5JJtEZX7hYHxt1sAPcVZfZdim2t7MYmak7CkWIzYxuy4ZJO4nyGYfQQii92; _gid=GA1.2.1877547667.1684739353; pm-sessionid=ohbrzs8fy4s0qllt7xzwt81qkeubchrr; pm-sid=1HGimTwtF2-o8TlbMAZhlg:b5c94918a010ec8b537e8a96184a83a3; pm-adjnav-sid=SI-5BTQVCo6_z-ngmhJfdg:b5c94918a010ec8b537e8a96184a83a3; _gat_ncbiSg=1; _gat_dap=1; pm-iosp=; ncbi_pinger=N4IgDgTgpgbg+mAFgSwCYgFwgAwBECMA7NifrgGwBiFALAJwCsJpuATJQKIfbECCAQgA4AdPmEBbODRABfIA; _ga_DP2X732JSX=GS1.1.1684909005.76.1.1684910754.0.0.0; _ga=GA1.1.1026152423.1683707361'
                # 存在反爬: cookie机制
            }
            resp = requests.post(url, data=data, headers=headers)
            # resp.encoding='utf-8' 
            # encoding = chardet.detect(resp.content)['encoding']
            # content = resp.content.decode(encoding)
            # etree = html.etree
            # et = etree.HTML(content)
            etree = html.etree
            et = etree.HTML(resp.text)
            article_ls = et.xpath('//div[@class="search-results-chunk results-chunk"]/article')
            count = 0
            for article in article_ls:
                try:
                    href = article.xpath('./div[2]/div[1]/a/@href')[0]
                    detail_url = urljoin(url, href)
                    title = "".join(article.xpath('./div[2]/div[1]/a//text()')).strip().replace(',', '，')
                    obj = article.xpath('./div[2]/div[1]/div[1]/span[3]/text()')[0].split(':')[0]
                    if ';' in obj:
                        obj = obj.split(';')[0]
                        journal = obj.split('.')[0]
                        pubdate = obj.split('.')[1]
                    else:
                        journal = obj.split('.')[0]
                        pubdate = obj.split('.')[1]
                    journal = journal.replace(',', '，')
                    pubmed_id = "".join(
                        article.xpath('./div[2]/div[1]/div[1]/span[@class="citation-part"]/span/text()'))
                    count += 1
                except Exception as e:
                    with open('log.txt', 'a', encoding='utf-8') as f:
                        error_msg = url + " " + page + " --> " + str(e)
                        f.write(error_msg)
                        f.write('\n')
                    continue

                parse_detail(detail_url, title, journal, pubdate, pubmed_id)

                all_id_list.append(pubmed_id)
            print("第%s页抓取完毕, 共%s个" % (page, count))
           # logging.debug(all_id_list)

            return all_id_list
        except Exception as e:
            with open('log.txt', 'a', encoding='utf-8') as f:
                error_msg = url + " " + str(page) + " --> " + str(e)
                f.write(error_msg)
                f.write('\n')


def write_header():
    # with open('newest/pubmed_data_new(2022-2023).csv', 'w', encoding='utf8') as f:
    with open(filename, 'w', encoding='utf-8') as f:
        f.write('Title')
        f.write(',')
        f.write('Journal')
        f.write(',')
        f.write('PubDate')
        f.write(',')
        f.write('PubmedID')
        f.write(',')
        f.write('Url')
        f.write(',')
        f.write("Abstract")
        f.write('\n')


def merge_spide_download():
    # 读取CSV文件
    pubmed_s_df = pandas.read_csv(filename)
    pubmed_d_df = pandas.read_csv(download_csv_path)
    pubmed_d_df['Title'] = pubmed_d_df['Title'].str.rstrip('.')
    
    pubmed_s_df_unique = pubmed_s_df[~(pubmed_s_df['Journal'].str.startswith('bioRxiv') &  pubmed_s_df.duplicated(subset='Title', keep=False))]
    # pubmed_s_df_unique = pubmed_s_df_unique.drop_duplicates(subset='Title', keep='first')
    print(len(pubmed_s_df_unique))
 
    pubmed_d_df_unique = pubmed_d_df[~(pubmed_d_df['Journal/Book'].str.startswith('bioRxiv') & pubmed_d_df.duplicated(subset='Title', keep=False))]
    # pubmed_d_df_unique = pubmed_d_df_unique.drop_duplicates(subset='Title', keep='first')
    print(len(pubmed_d_df_unique))
 
    pubmed_d_df_unique.to_csv(filename, index=False)
    pubmed_s_df_unique.to_csv(download_csv_path, index=False)

    # 确保Title和Journal字段一致，可以先进行一次筛选
    # pubmed_s_df = pubmed_s_df[pubmed_s_df['Title'].isin(pubmed_d_df['Title'])]
    # pubmed_s_df = pubmed_s_df[pubmed_s_df['Journal'].isin(pubmed_d_df['Journal/Book'])]



    # check(s,d)条目一致
    if len(pubmed_d_df_unique) != len(pubmed_s_df_unique):  
        print("pubmed_s和pubmed_d条目数不一致，无法合并")  
    else:
        print("pubmed_s和pubmed_d条目数一致，可以合并") 
        
    # # check(s,d)的title列是否完全一致  
    # if not pubmed_s_df['Title'].equals(pubmed_d_df['Title']):  
    #     print("pubmed_s和pubmed_d的title列不一致，无法合并")  
    # else:
    #     print("pubmed_s和pubmed_d的title列一致，可以合并")
    #s表
    
    
    # 合并DataFrame，基于PubmedID和PMID字段
    merged_df = pandas.merge(
        pubmed_s_df_unique,
        pubmed_d_df_unique,
        left_on='PubmedID',
        right_on='PMID',
        how='inner'
    )

    # 删除多余的列
    merged_df.drop(columns=['PMID', 'Journal'], inplace=True)

    # 重命名字段以统一为PMID
    merged_df.rename(columns={'PubmedID': 'PubmedID', 'Title_x': 'Title', 'Journal_x': 'Journal/Book'}, inplace=True)

    # 选择需要保留的列
    columns_to_keep = [
        'PubmedID', 'Title', 'Authors', 'Citation', 'First Author', 'Journal/Book',
        'Publication Year', 'Create Date', 'PMCID', 'NIHMS ID', 'DOI', 'PubDate', 'Url', 'Abstract'
    ]
    merged_df = merged_df[columns_to_keep]
    
    # check(s,d,merge)条目一致
    if len(pubmed_d_df_unique) != len(pubmed_s_df_unique) or len(pubmed_d_df_unique) != len(merged_df):  
        print("pubmed_s、pubmed_d和pubmed_merge条目数不一致")  
    else:
        print("pubmed_s、pubmed_d和pubmed_merge条目数一致") 
        
    # 检查merge的title列是否有重复
    if not merged_df['Title'].is_unique:
        print("pubmed_merge中的title列有重复项")  
    else:
        print("pubmed_merge中的title列无重复项")  
        
    # 保存合并后的DataFrame为新的CSV文件
    merged_df.to_csv(merge_csv_path, index=False)
    

def main():
    # 写入表头
    write_header()
    # 第一页
    # https://pubmed.ncbi.nlm.nih.gov/?term=%28%28%28%28%28single+cell+transcriptomics%29+OR+%28single-nuclei+transcriptomics%29+OR+%28single-cell+transcriptomics%29+OR+%28Zemin+Zhang%5BAuthor%5D%29+OR+%28single-cell+RNA+sequencing%29+OR+%28single+cell+RNA+sequencing%29+OR+%28single-nuclei+RNA+sequencing%29+OR+%28single+cell+RNA-seq%29+OR+%28single-cell+RNA-seq%29+OR+%28single-nuclei+RNA-seq%29+OR+%28cell+atlas%29+OR+%28snRNA-seq%29+OR+%28scRNA-seq%29+OR+%28Single-cell%29%29%29+AND+%28Perturbation%29%29+OR+%28Perturb-Seq%29%29+OR+%28%28%28%28%28%28%28%28%28%28%28%28CRISP-seq%29+OR+%28CROP-seq%29%29+OR+%28TAP-seq%29%29+OR+%28MIX-seq%29%29+OR+%28&filter=years.2022-2023
    # https://pubmed.ncbi.nlm.nih.gov/?term=%28%28%28%28%28single+cell+transcriptomics%29+OR+%28single-nuclei+transcriptomics%29+OR+%28single-cell+transcriptomics%29+OR+%28Zemin+Zhang%5BAuthor%5D%29+OR+%28single-cell+RNA+sequencing%29+OR+%28single+cell+RNA+sequencing%29+OR+%28single-nuclei+RNA+sequencing%29+OR+%28single+cell+RNA-seq%29+OR+%28single-cell+RNA-seq%29+OR+%28single-nuclei+RNA-seq%29+OR+%28cell+atlas%29+OR+%28snRNA-seq%29+OR+%28scRNA-seq%29+OR+%28Single-cell%29%29%29+AND+%28Perturbation%29%29+OR+%28Perturb-Seq%29%29+OR+%28%28%28%28%28%28%28%28%28%28%28%28CRISP-seq%29+OR+%28CROP-seq%29%29+OR+%28TAP-seq%29%29+OR+%28MIX-seq%29%29+OR+%28*CRISPR*+screening*%29%29+OR+%28Mosaic-seq%29%29+OR+%28CombiGEM-CRISPR%29%29+OR+%28perturb*+seq*%29%29+OR+%28Pooled+genetic+screens%29%29+OR+%28CRISPR+activation%29%29+OR+%28CRISPR+interference%29%29+OR+%28Multiplexed+perturbations%29%29&filter=years.2023-2024
    # first_page_url = 'https://pubmed.ncbi.nlm.nih.gov/?term=%28%28%28%28%28single+cell+transcriptomics%29+OR+%28single-nuclei+transcriptomics%29+OR+%28single-cell+transcriptomics%29+OR+%28Zemin+Zhang%5BAuthor%5D%29+OR+%28single-cell+RNA+sequencing%29+OR+%28single+cell+RNA+sequencing%29+OR+%28single-nuclei+RNA+sequencing%29+OR+%28single+cell+RNA-seq%29+OR+%28single-cell+RNA-seq%29+OR+%28single-nuclei+RNA-seq%29+OR+%28cell+atlas%29+OR+%28snRNA-seq%29+OR+%28scRNA-seq%29+OR+%28Single-cell%29%29%29+AND+%28Perturbation%29%29+OR+%28Perturb-Seq%29%29+OR+%28%28%28%28%28%28%28%28%28%28%28%28CRISP-seq%29+OR+%28CROP-seq%29%29+OR+%28TAP-seq%29%29+OR+%28MIX-seq%29%29+OR+%28&filter=years.2022-2023'
    first_page_url = 'https://pubmed.ncbi.nlm.nih.gov/?term=%28+%28%22RNA-Seq%22+OR+%22RNA+sequencing%22+OR+%22bulk+RNA-Seq%22+OR+transcriptomic+OR+transcriptome+OR+%22transcriptome+analysis%22+OR+%22transcriptome+profiling%22%29+AND+%28%22in+vitro%22+OR+%22cell+line%22+OR+%22cell+lines%22+OR+%22cell+culture%22+OR+%22cultured+cells%22%29+AND+%28chemotherapy+OR+immunotherapy+OR+%22targeted+therapy%22+OR+%22hormone+therapy%22+OR+%22kinase+inhibitor*%22+OR+%22immune+checkpoint+inhibitor*%22+OR+%22small+molecule+inhibitor*%22+OR+%22biologic+therapy%22+OR+%22biological+therapy%22+OR+%22epigenetic+inhibitor*%22+OR+%22anti-cancer+drug*%22+OR+%22cytotoxic+drug*%22+OR+%22GPCR+agonist*%22+OR+%22ion+channel+modulator*%22+OR+%22enzyme+activator*%22+OR+%22receptor+agonist*%22+OR+%22transcriptional+modulator*%22+OR+%22metabolic+modulator*%22+OR+%22pathway+inhibitor*%22+OR+%22allosteric+modulator*%22%29+%29+NOT+%28proteom*+OR+metabolom*+OR+%22mass+spectrometry%22+OR+%22single-cell%22+OR+%22single+cell%22+OR+%22scRNA-seq%22+OR+mouse+OR+mice+OR+murine+OR+rat+OR+xenograft+OR+%22in+vivo%22+OR+patient+OR+patients%29'   

    if not first_page_url:
        print("请输入url")
        first_page_url = input("请输入url::>>>>")

    for url in first_page_url.split(';'):
        pmid_list = []

        total_page, first_pmid_list = get_first_page(url)
        pmid_list.extend(first_pmid_list)
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        term = query_params.get('term', [''])[0]
        filter = query_params.get('filter', [''])[0]

        print("Term:", term)
        print("Filter:", filter)

        print(f"""
        url:{url}
        total_page:{total_page}
        filter:{filter}
        term:{term}
        """.strip())
        # 其他页
        other_page_url = "https://pubmed.ncbi.nlm.nih.gov/more/"

        with ThreadPoolExecutor(30) as t:
            results = []
            for page in range(2, total_page + 1):  # 526
                # get_other_page(other_page_url, page)
                future = t.submit(get_other_page, other_page_url, page, term, filter)
                results.append(future)

            print("+++++++++++++++++")
            t.shutdown()
           # print("---------------")
           # for future in results:
           #     result = future.result()  # Get the result of the completed task
           #     # Process the result as needed
           #     print("Result:", result)
           #     # print(all_id_list)
           #     pmid_list.extend(result)

        # saveData.save_data(
        #     term,
        #     pmid_list,
        #     os.path.join(dir_path, f"{str(datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))}.csv"),
        #     pdf_path
        # )

        saveData.download_singlecell_csv(term, filter, download_csv_path)
        # all_id_list = []


def get_GEO_title():
    main()
    merge_spide_download()
    data = pandas.read_csv(filename)
    print(data.info())


if __name__ == '__main__':
    get_GEO_title()

