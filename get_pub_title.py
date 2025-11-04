# -*- encoding:utf-8 -*-
# @time: 2023/5/13 9:52
# @author: ifeng
"""
    后面迭代title的时候需要将逗号改成英文的!!!
"""
import csv
import logging
import os.path
import threading
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote
from urllib.parse import urljoin
from urllib.parse import urlparse, parse_qs

import pandas
import requests
from lxml import html

import saveData

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


def read_search_query(file_path="search_query.txt"):
    """
    从文件中读取搜索查询参数
    
    Args:
        file_path (str): 搜索查询文件的路径，默认为 "search_query.txt"
    
    Returns:
        str: 搜索查询字符串，如果文件不存在则返回默认查询
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            query = f.read().strip()
            if query:
                return query
            else:
                print(f"警告: {file_path} 文件为空，使用默认搜索查询")
                return get_default_query()
    except FileNotFoundError:
        print(f"警告: 未找到 {file_path} 文件，使用默认搜索查询")
        return get_default_query()
    except Exception as e:
        print(f"读取搜索查询文件时出错: {e}，使用默认搜索查询")
        return get_default_query()


def get_default_query():
    """
    返回默认的搜索查询
    
    Returns:
        str: 默认搜索查询字符串
    """
    return '( ("RNA-Seq" OR "RNA sequencing" OR "bulk RNA-Seq" OR transcriptomic OR transcriptome OR "transcriptome analysis" OR "transcriptome profiling") AND ("in vitro" OR "cell line" OR "cell lines" OR "cell culture" OR "cultured cells") AND (chemotherapy OR immunotherapy OR "targeted therapy" OR "hormone therapy" OR "kinase inhibitor*" OR "immune checkpoint inhibitor*" OR "small molecule inhibitor*" OR "biologic therapy" OR "biological therapy" OR "epigenetic inhibitor*" OR "anti-cancer drug*" OR "cytotoxic drug*" OR "GPCR agonist*" OR "ion channel modulator*" OR "enzyme activator*" OR "receptor agonist*" OR "transcriptional modulator*" OR "metabolic modulator*" OR "pathway inhibitor*" OR "allosteric modulator*") ) NOT (proteom* OR metabolom* OR "mass spectrometry" OR "single-cell" OR "single cell" OR "scRNA-seq" OR mouse OR mice OR murine OR rat OR xenograft OR "in vivo" OR patient OR patients)'


def build_pubmed_url(search_query, filter_param=""):
    """
    根据搜索查询构建PubMed URL
    
    Args:
        search_query (str): 搜索查询字符串
        filter_param (str): 过滤参数，可选
    
    Returns:
        str: 构建好的PubMed URL
    """
    base_url = "https://pubmed.ncbi.nlm.nih.gov/"
    encoded_query = quote(search_query)

    url = f"{base_url}?term={encoded_query}"

    if filter_param:
        url += f"&filter={filter_param}"

    return url


def extract_abstract(et):
    """
    从解析后的HTML中提取abstract，尝试多种XPath表达式
    
    Args:
        et: lxml解析后的HTML元素树
    
    Returns:
        str: 提取到的abstract文本，如果未找到则返回空字符串
    """
    # 定义多个可能的XPath表达式，按优先级排序
    xpath_expressions = [
        '//*[@id="abstract"]//text()',  # 原始XPath
        '//div[@class="abstract"]//text()',  # 新的页面结构
        '//div[contains(@class, "abstract-content")]//text()',  # 包含abstract-content类的div
        '//*[contains(@class, "abstract")]//text()',  # 任何包含abstract类的元素
        '//section[@id="abstract"]//text()',  # section标签
        '//div[@id="enc-abstract"]//text()',  # 编码后的abstract ID
    ]
    
    for xpath in xpath_expressions:
        try:
            abstract_parts = et.xpath(xpath)
            if abstract_parts:
                # 合并所有文本片段
                abstract = "".join([item.strip() for item in abstract_parts]).strip()
                if abstract:
                    # 清理abstract文本
                    abstract = abstract.replace(',', '，')  # 替换逗号
                    # 移除开头的"Abstract"标签（可能重复出现）
                    abstract = abstract.lstrip('Abstract').lstrip('abstract').strip()
                    # 移除多余的"Abstract"重复
                    while abstract.startswith('Abstract'):
                        abstract = abstract[8:].strip()
                    return abstract
        except Exception as e:
            # 如果某个XPath表达式出错，继续尝试下一个
            continue
    
    return ""  # 如果所有XPath都失败，返回空字符串


def parse_detail(url, title, journal, pubdate, pubmed_id):
    for i in range(5):  # 爬虫的自省
        try:
            resp = requests.get(url)
            # resp.encoding="utf-8"
            # encoding = chardet.detect(resp.content)['encoding']
            # content = resp.content.decode(encoding)
            # etree = html.etree
            # et = etree.HTML(content)
            
            etree = html.etree
            et = etree.HTML(resp.text)
            
            # 使用新的abstract提取函数
            abstract = extract_abstract(et)
            
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
    for item in range(5):
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
            logging.debug(all_id_list)

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

    # 从文件读取搜索查询
    search_query = read_search_query()
    print(f"使用的搜索查询: {search_query}")

    # 构建URL（可以根据需要添加过滤参数）
    # 例如：filter_param = "years.2022-2024" 
    filter_param = ""  # 可以根据需要设置过滤参数
    first_page_url = build_pubmed_url(search_query, filter_param)

    print(f"构建的URL: {first_page_url}")
    
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

        with ThreadPoolExecutor(5) as t:
            results = []
            for page in range(2, total_page + 1):  # 526
                # get_other_page(other_page_url, page)
                future = t.submit(get_other_page, other_page_url, page, term, filter)
                results.append(future)

            t.shutdown()

            for future in results:
                result = future.result()  # Get the result of the completed task
                # Process the result as needed
                print("Result:", result)
                # print(all_id_list)
                pmid_list.extend(result)

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

