# auto

1. `bak_script/get_pub_title.py` -> 搜索 PubMed 获取文献
2. `get_ncbi_detail_info_another` -> 生产者与消费者
3. `bak_script/merge_data.py` -> 合并 1、2 的结果
4. 分两个部分:
   - `get_accession.py` -> accession 数据抓取
   - `get_pubmed.py` -> pubmed 数据补充

附加处理：
- 遍历 title 获得的详细页处理: `gpt_handler_modify.py`
- GEO 直接搜索的结果: `gpt_handler.py`
- 下载 GEO family: `download_GEO_family.py`

运行示例：
`nohup python total_start.py > nohup.out 2>&1 &`
