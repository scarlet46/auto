import pandas
import requests
# from scihub_cn.scihub import SciHub
from scihub import SciHub


def save_data(term, all_id_list, csv_path, pdf_path):
    cookies = {
        'pm-csrf': 'Av152OHEldC7AtAMCp1wj3T78LW3BPb7',
        'pm-sessionid': '2kof714qvsemp4inxgdz98x12h3n4vgr',
        'ncbi_sid': '4C1900B16888C3F3_7351SID',
        '_gid': 'GA1.2.1122699659.1720228984',
        'pm-sid': 'S0uFFECyWbEdcdO4aiADHA:15977434a591b983d7648d7a5ed0ee9e',
        'pm-adjnav-sid': 'HSGDwiTwmJdFOqpsmch7nA:15977434a591b983d7648d7a5ed0ee9e',
        'pm-iosp': '',
        '_ga': 'GA1.1.119167169.1720228984',
        '_ga_CSLL4ZEK4L': 'GS1.1.1720228985.1.1.1720231222.0.0.0',
        '_ga_DP2X732JSX': 'GS1.1.1720228985.1.1.1720231815.0.0.0',
        'ncbi_pinger': 'N4IgDgTgpgbg+mAFgSwCYgFwgKIDYCCuADCQIykCsuAwgBzW4UkkBMh+AYh9QOwUCcFAMwA6UiIC2cISAA0IAMYAbZAoDWAOygAPAC6ZQRTCACGC3cgD2GgLRgTWpTbMXrAZ1kurt+45sLrXSgNXU9zbxsAM0sICVlkDS0IGwB3CBMwWTcTGChncOs7ByglMNcfYqUAAh1fVChULKgTCAVEIoBzKDkQUmNs3Pzyor9o2KycvK9C3xKmlrbO7vkhfmMBChYAFjX5LaMsAVpBIz2t42VVTR19PYpjc73cY0iTJTdlkC2eB+Ierdo61wfT2ayw0wqfjcAFcAEYSZC3EBMYw9Ch9LBoljGajQExBKqRZBKboAX3k0I0SksJlQWj0BhAQhkWFe70+QnOWF0EGhHMBmJWYORghYtBBXwOIuO4v+XJAlOptPpSK290OaOeWBYpGx8goPywpxAwOMpBlIHJiksEgk1hVjOxWDAcIkDR6LPArvde36zVaiGgMKUSPVIB65DNPRYUrwhGY5CodAYTGYbAIXF4G1E4ikMnkOuMLvhDQwxbdqAwHwWgagwd0GAAcgB5RvYaNOr0l1AiDQKWHIXtKCS95CIEQdSwwaPClhUY1CKWkLb7D0YkBzv4rTukVZ6r6eogifYiY3CM27wH6+U8XA/eS4LUgasBy2koA',
    }

    headers = {
        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'content-type': 'application/x-www-form-urlencoded',
        'referer': 'https://pubmed.ncbi.nlm.nih.gov/?term=%28%28single+cell+transcriptomics%29+OR+%28single-nuclei+transcriptomics%29+OR+%28single-cell+transcriptomics%29+OR+%28Zemin+Zhang%5BAuthor%5D%29+OR+%28single-cell+RNA+sequencing%29+OR+%28single+cell+RNA+sequencing%29+OR+%28single-nuclei+RNA+sequencing%29+OR+%28single+cell+RNA-seq%29+OR+%28single-cell+RNA-seq%29+OR+%28single-nuclei+RNA-seq%29+OR+%28cell+atlas%29+OR+%28snRNA-seq%29+OR+%28scRNA-seq%29+OR+%28Single-cell%29%29+AND+%28%28Mus*%29+OR+%28Mouse*%29+OR+%28Mice%29+OR+%28Homo+sapiens%29+OR+%28human*%29+OR+%28patient*%29%29+NOT+%28review*%29',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    }

    # with open('./all_id_list.txt', "r", encoding='utf-8') as fp:
    #     all_id_list = json.loads(fp.read())

    all_id = ",".join(all_id_list)

    data = {
        'csrfmiddlewaretoken': 'fiQxn7ZDUC5ZOPdNboYOYG98O9K7gkYXFDHsfLw75FxWe8DpDDPa7zS5MKw0HZZU',
        'results-format': 'csv',
        'result-ids': all_id,
        'term': term,
    }

    response = requests.post(
        'https://pubmed.ncbi.nlm.nih.gov/results-export-ids/',
        headers=headers,
        data=data,
        cookies=cookies
    )

    ret = response.content
    with open(csv_path, 'wb',encoding='utf-8') as fp:
        fp.write(ret)

    data = pandas.read_csv(csv_path)

    sh = SciHub()
    for i in range(len(data)):
        doi = data["DOI"][i]

        try:
            # result = sh.download({"doi": doi}, destination=pdf_path)
            # result = sh.download({"doi": '10.1063/1.3149495'}, destination="./newest")
            fetch_url = sh.search(doi)
            print(fetch_url)

            sh.download(url=fetch_url, outdir=pdf_path)
        except Exception as e:
            print("scihub::pdf:::", e)
            pass

    print("保存成功")


def download_singlecell_csv(term, filter, download_csv_path):
    cookies = {
        'pm-csrf': 'XCrX8DOj9SCYBdRv2glUjIgGd1TkV3sG'
    }

    headers = {
        'referer': 'https://pubmed.ncbi.nlm.nih.gov/?term=%28+%28%22RNA-Seq%22+OR+%22RNA+sequencing%22+OR+%22bulk+RNA-Seq%22+OR+transcriptomic+OR+transcriptome+OR+%22transcriptome+analysis%22+OR+%22transcriptome+profiling%22%29+AND+%28%22in+vitro%22+OR+%22cell+line%22+OR+%22cell+lines%22+OR+%22cell+culture%22+OR+%22cultured+cells%22%29+AND+%28chemotherapy+OR+immunotherapy+OR+%22targeted+therapy%22+OR+%22hormone+therapy%22+OR+%22kinase+inhibitor*%22+OR+%22immune+checkpoint+inhibitor*%22+OR+%22small+molecule+inhibitor*%22+OR+%22biologic+therapy%22+OR+%22biological+therapy%22+OR+%22epigenetic+inhibitor*%22+OR+%22anti-cancer+drug*%22+OR+%22cytotoxic+drug*%22+OR+%22GPCR+agonist*%22+OR+%22ion+channel+modulator*%22+OR+%22enzyme+activator*%22+OR+%22receptor+agonist*%22+OR+%22transcriptional+modulator*%22+OR+%22metabolic+modulator*%22+OR+%22pathway+inhibitor*%22+OR+%22allosteric+modulator*%22%29+%29+NOT+%28proteom*+OR+metabolom*+OR+%22mass+spectrometry%22+OR+%22single-cell%22+OR+%22single+cell%22+OR+%22scRNA-seq%22+OR+mouse+OR+mice+OR+murine+OR+rat+OR+xenograft+OR+%22in+vivo%22+OR+patient+OR+patients%29',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    }

    data = {
        'csrfmiddlewaretoken': 'cBwAH3UbWfzvIXzO1RaYrF7SP7nNyxJiZ3NnFwykVX1j90g9TXlIAddoSY6Xjq1O',
        'results-format': 'csv',
        'term': '( ("RNA-Seq" OR "RNA sequencing" OR "bulk RNA-Seq" OR transcriptomic OR transcriptome OR "transcriptome analysis" OR "transcriptome profiling") AND ("in vitro" OR "cell line" OR "cell lines" OR "cell culture" OR "cultured cells") AND (chemotherapy OR immunotherapy OR "targeted therapy" OR "hormone therapy" OR "kinase inhibitor*" OR "immune checkpoint inhibitor*" OR "small molecule inhibitor*" OR "biologic therapy" OR "biological therapy" OR "epigenetic inhibitor*" OR "anti-cancer drug*" OR "cytotoxic drug*" OR "GPCR agonist*" OR "ion channel modulator*" OR "enzyme activator*" OR "receptor agonist*" OR "transcriptional modulator*" OR "metabolic modulator*" OR "pathway inhibitor*" OR "allosteric modulator*") ) NOT (proteom* OR metabolom* OR "mass spectrometry" OR "single-cell" OR "single cell" OR "scRNA-seq" OR mouse OR mice OR murine OR rat OR xenograft OR "in vivo" OR patient OR patients)',
    }

    response = requests.post(
        'https://pubmed.ncbi.nlm.nih.gov/results-export-search-data/',
        cookies=cookies,
        headers=headers,
        data=data,
    )

    with open(download_csv_path, "w",encoding='utf8') as fp:
        fp.write(response.text.replace("\r\n", '\n'))

    print("下载 csv-RNA-SeqORR-set.csv 成功")

# 第一个参数输入论文的网站地址
# path: 文件保存路径
# from scihub_cn.scihub import SciHub
# sh = SciHub()
# result = sh.download({"doi": '10.1063/1.3149495'}, destination="./newest")


# from scihub.util.download import SciHub
#
#
# sh = SciHub()
# fetch_url = sh.search('10.1038/s41524-017-0032-0')
# print(fetch_url)
# sh.download(url=fetch_url, outdir="./newest")

# dois = ['10.1101/2023.03.28.534017']
# for doi in dois:
#     fetch_url = sh.search(doi)
#     print(fetch_url)
#     if fetch_url:
#         sh.download(url=fetch_url, outdir="./newest/pdf")
# scihub -s 10.1038/s41524-017-0032-0

# https://sci-hub.se/10.1063/1.3149495
# resp = requests.get("https://sci-hub.se/10.1038/s41524-017-0032-0")
#
# etree = html.etree
# et = etree.HTML(resp.text)
# print(et.xpath('//*[@id="minu"]/div[@id="buttons"]/text()'))
# article_ls = (et.xpath('//*[@id="buttons"]/text()'))

# result = sh.download({"doi": '10.1016/j.cell.2023.01.002'}, destination="./newest")

# import re
# import warnings
#
# from scholarly import scholarly
# from scihub.util.download import SciHub
#
# keyword = 'Surgery'
#
# search = scholarly.search_pubs(keyword)
# scihub = SciHub()
#
# downloaded, total = 0, 10
# while downloaded < total:
#     try:
#         paper = next(search)
#
#         title = paper['bib']['title']
#         title = re.sub(r'[\\ /:"*?<>|]+', ' ', title)
#
#         pub_year = paper['bib']['pub_year']
#
#         num_citations = paper['num_citations']
#         num_citations = '{:>{width}}'.format(num_citations, width=5)
#
#         filename = f'{pub_year}_{title}_{num_citations}.pdf'
#
#         url = scihub.search(paper['pub_url'])
#         scihub.download(url, keyword, filename)
#
#         downloaded += 1
#         print(f'{downloaded + 1}/{total}', filename)
#     except Exception as e:
#         warnings.warn(str(e))

