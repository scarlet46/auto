import time

# 1.搜索pubmed获取文献
from bak_script.get_pub_title import get_GEO_title
print("1.begin search pubmed...")
print("------------------------")
print("\n")
get_GEO_title()

# 2.GEO数据抓取
#from get_ncbi_detail_info_another import create
#print("2.1 begin create")
#create.main()  # 生产

#print("2.2 begin consume")
#from get_ncbi_detail_info_another import consume
#consume.main()  # 消费

#print("2.3 beging remove duplicate")
#consume.remove_duplicates()  # 去重

# 3.合并1和2的csv
#from bak_script import merge_data
#merge_data.main()

##done###

# #4.数据补充
# # 4.1accession数据抓取
# import get_accession
# get_accession.main()

# # 4.2pubmed数据补充
# import get_pubmed
# get_pubmed.main()



# 4.通过gpt抽取对应字段
#from bak_script import gpt_handler
#gpt_handler.main()

# 执行 05_download_GEO_family.py

#from bak_script import download_GEO_family
#download_GEO_family.main()
#print("----------执行完毕----------")

