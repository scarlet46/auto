# -*- encoding:utf-8 -*-
# @time: 2023/5/15 7:08
# @author: ifeng
import os
import re
import time
import redis
import json
import shutil
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service  
from selenium.webdriver.common.by import By

pool = redis.ConnectionPool(host='localhost', port=6379, db=12, decode_responses=True)
client = redis.Redis(connection_pool=pool)

dir_path = os.path.abspath("../newest")

if not os.path.exists(dir_path):
    dir_path = os.path.abspath("./newest")

filename = os.path.join(dir_path, 'ncbi_data(all).csv')

download_path = os.path.join(os.getcwd(), 'download')  # 创建一个下载文件夹

if not os.path.exists(download_path):
    os.makedirs(download_path)

down_gds_res_file = os.path.join(os.path.dirname(download_path), 'gds_result.txt')
down_gds_res_file_tmp = os.path.join(os.path.dirname(download_path), 'gds_result.txt.crdownload')

target_path = os.path.join(download_path, 'gds_result.txt')

if os.path.exists(down_gds_res_file):
    os.remove(down_gds_res_file)
elif os.path.exists(down_gds_res_file_tmp):
    os.remove(down_gds_res_file_tmp)
    os.remove(target_path)

def chrome_download_file(url):
    try:
        service=Service(r"/Users/Z/Downloads/chromedriver-mac-arm64/chromedriver")
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--headless')
        prefs = {
            "download.default_directory": os.path.dirname(download_path),  # 设置下载目录
            "download.prompt_for_download": False,  # 启用下载提示
            "download.directory_upgrade": False,
            "safebrowsing.enabled": False
        }
        chrome_options.add_experimental_option("prefs", prefs)
                
        web = Chrome(service=service,options=chrome_options)
        # web = Chrome(options=chrome_options)
        web.get(url)    
        print("web get url")
        time.sleep(3)
        pages = web.find_element(By.XPATH, '//*[@id="maincontent"]/div/div[3]/div[2]/h3').text.split(' of ')[-1]
        print(f'共{pages}个页面')
        web.find_element(By.XPATH, '/html/body/div[1]/div[1]/form/div[1]/div[4]/div/div[1]/h4/a').click()
        print("click sent to")
        time.sleep(3)
        web.find_element(By.XPATH,
                          '/html/body/div[1]/div[1]/form/div[1]/div[4]/div/div[1]/div[4]/fieldset/ul/li[1]/input').click()
        print("click File")
        time.sleep(3)
        web.find_element(By.XPATH,
                          '/html/body/div[1]/div[1]/form/div[1]/div[4]/div/div[1]/div[4]/div[1]/button').click()
        print('click Create File')
        time.sleep(1)

        directory_path = os.path.dirname(download_path)

        # 初始文件名
        temp_file_name = 'gds_result.txt.crdownload'
        # 最终文件名
        final_file_name = 'gds_result.txt'
        # 检查间隔时间（秒）
        check_interval = 1
        # 超时时间（秒），例如10秒内没有变化则认为下载完成
        timeout = 50

        temp_file_path = os.path.join(directory_path, temp_file_name)
        final_file_path = os.path.join(directory_path, final_file_name)

        last_size = 0
        start_time = time.time()

        while True:
            if os.path.exists(final_file_path):
                print("下载完成")
                shutil.move(final_file_path, download_path)
                print(f"文件已移动到 {download_path}")
                break

            if os.path.exists(temp_file_path):
                current_size = os.path.getsize(temp_file_path)
                if current_size == last_size:
                    print('current_size == last_size::::',current_size)
                    time.sleep(10)
                    # 如果文件大小在一定时间内没有变化，则认为下载完成
                    if time.time() - start_time > timeout:
                        print("下载完成")
                        break
                else:
                    # 文件大小有变化，重置开始时间
                    start_time = time.time()
                    last_size = current_size
                    print("正在下载...")
            else:
                print("下载文件不存在，请检查")

            time.sleep(check_interval)

        web.close()

    except Exception as e:
        with open('ncbi_log.txt', 'a', encoding='utf8') as f:
            error_msg = url + " -->" + str(e)
            f.write(error_msg)
            f.write('\n')


def send_to_redis(href):
    queue_name = 'ncbi_detail_url'
    message = f"{href}"
    client.lpush(queue_name, message)  # 将消息放到队列中


def write_header():
    with open(filename, 'w', encoding="utf8") as f:
        f.write("Title")
        f.write(',')
        f.write("Accession")
        f.write(',')
        f.write("Organism")
        f.write(',')
        f.write("Summary")
        f.write(',')
        f.write("Overall_design")
        f.write(',')
        f.write('PubmedID')
        f.write(',')
        f.write('Journal')
        f.write('\n')


def send_txt_to_redis():
    href_li = []
    with open(target_path, "r") as fp:
        data = fp.read()
        accession_list = re.findall(r'Accession: (.*)?	ID:', data)

    for accession in accession_list:
        href = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={accession}"
        # print(href)
        send_to_redis(href)
        href_li.append(href)
    print("上传redis完毕")
    return href_li


def quick_sent_txt_redis(txt_path):
    with open(txt_path, "r") as fp:
        href_li = json.loads(fp.read())
    for href in href_li:
        send_to_redis(href)
    print("上传redis完毕")


def main():
    # 'https://www.ncbi.nlm.nih.gov/gds?term=(((single%20cell%20transcriptomics)%20OR%20(single-nuclei%20transcriptomics)%20OR%20(single-cell%20transcriptomics)%20OR%20(single-cell%20RNA%20sequencing)%20OR%20(single%20cell%20RNA%20sequencing)%20OR%20(single-nuclei%20RNA%20sequencing)%20OR%20(single%20cell%20RNA-seq)%20OR%20(single-cell%20RNA-seq)%20OR%20(single-nuclei%20RNA-seq)%20OR%20(cell%20atlas)%20OR%20(snRNA-seq)%20OR%20(scRNA-seq)%20OR%20(Single-cell)))'
    # url = 'https://www.ncbi.nlm.nih.gov/gds?term=%28%28%28%28%28single+cell+transcriptomics%29+OR+%28single-nuclei+transcriptomics%29+OR+%28single-cell+transcriptomics%29+OR+%28Zemin+Zhang%5BAuthor%5D%29+OR+%28single-cell+RNA+sequencing%29+OR+%28single+cell+RNA+sequencing%29+OR+%28single-nuclei+RNA+sequencing%29+OR+%28single+cell+RNA-seq%29+OR+%28single-cell+RNA-seq%29+OR+%28single-nuclei+RNA-seq%29+OR+%28cell+atlas%29+OR+%28snRNA-seq%29+OR+%28scRNA-seq%29+OR+%28Single-cell%29%29%29+AND+%28Perturbation%29%29+OR+%28Perturb-Seq%29%29+OR+%28%28%28%28%28%28%28%28%28%28%28%28CRISP-seq%29+OR+%28CROP-seq%29%29+OR+%28TAP-seq%29%29+OR+%28MIX-seq%29%29+OR+%28&filter=years.2022-2023'
#    url = 'https://www.ncbi.nlm.nih.gov/gds/?term=(+(%22RNA-Seq%22+OR+%22RNA+sequencing%22+OR+%22bulk+RNA-Seq%22+OR+transcriptomic+OR+transcriptome+OR+%22transcriptome+analysis%22+OR+%22transcriptome+profiling%22)+AND+(%22in+vitro%22+OR+%22cell+line%22+OR+%22cell+lines%22+OR+%22cell+culture%22+OR+%22cultured+cells%22)+AND+(chemotherapy+OR+immunotherapy+OR+%22targeted+therapy%22+OR+%22hormone+therapy%22+OR+%22kinase+inhibitor*%22+OR+%22immune+checkpoint+inhibitor*%22+OR+%22small+molecule+inhibitor*%22+OR+%22biologic+therapy%22+OR+%22biological+therapy%22+OR+%22epigenetic+inhibitor*%22+OR+%22anti-cancer+drug*%22+OR+%22cytotoxic+drug*%22+OR+%22GPCR+agonist*%22+OR+%22ion+channel+modulator*%22+OR+%22enzyme+activator*%22+OR+%22receptor+agonist*%22+OR+%22transcriptional+modulator*%22+OR+%22metabolic+modulator*%22+OR+%22pathway+inhibitor*%22+OR+%22allosteric+modulator*%22)+)+NOT+(proteom*+OR+metabolom*+OR+%22mass+spectrometry%22+OR+%22single-cell%22+OR+%22single+cell%22+OR+%22scRNA-seq%22+OR+mouse+OR+mice+OR+murine+OR+rat+OR+xenograft+OR+%22in+vivo%22+OR+patient+OR+patients)'
    url = 'https://www.ncbi.nlm.nih.gov/gds/?term=(+(%22RNA-Seq%22+OR+%22RNA+sequencing%22+OR+%22bulk+RNA-Seq%22+OR+transcriptomic+OR+transcriptome+OR+%22transcriptome+analysis%22+OR+%22transcriptome+profiling%22)+AND+(%22in+vitro%22+OR+%22cell+line%22+OR+%22cell+lines%22+OR+%22cell+culture%22+OR+%22cultured+cells%22)+AND+(chemotherapy+OR+immunotherapy+OR+%22targeted+therapy%22+OR+%22hormone+therapy%22+OR+%22kinase+inhibitor*%22+OR+%22immune+checkpoint+inhibitor*%22+OR+%22small+molecule+inhibitor*%22+OR+%22biologic+therapy%22+OR+%22biological+therapy%22+OR+%22epigenetic+inhibitor*%22+OR+%22anti-cancer+drug*%22+OR+%22cytotoxic+drug*%22+OR+%22GPCR+agonist*%22+OR+%22ion+channel+modulator*%22+OR+%22enzyme+activator*%22+OR+%22receptor+agonist*%22+OR+%22transcriptional+modulator*%22+OR+%22metabolic+modulator*%22+OR+%22pathway+inhibitor*%22+OR+%22allosteric+modulator*%22)+)+NOT+(proteom*+OR+metabolom*+OR+%22mass+spectrometry%22+OR+%22single-cell%22+OR+%22single+cell%22+OR+%22scRNA-seq%22+OR+mouse+OR+mice+OR+murine+OR+rat+OR+xenograft+OR+%22in+vivo%22+OR+patient+OR+patients)'
    # 1. 写表头
    write_header()  # 这里要注意
    chrome_download_file(url)
    href_li = send_txt_to_redis()
    href_file_path = os.path.join(dir_path, 'ncbi_href.txt')
    with open(href_file_path, 'w') as fp:
        fp.write(json.dumps(href_li))

if __name__ == '__main__':
    #main()
    txt_path = os.path.join(dir_path, 'ncbi_href.txt')
    quick_sent_txt_redis(txt_path)
    
    
    
    
    
    
    
